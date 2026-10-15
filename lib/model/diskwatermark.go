// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
)

// DiskWatermarkStatus is the current watermark state for a single physical
// disk backing one or more running folders.
type DiskWatermarkStatus struct {
	// Device is a stable identifier of the physical disk.
	Device string `json:"deviceId"`
	// Disk is a human readable disk name (mount point or volume).
	Disk string `json:"disk"`

	FreeBytes  int64 `json:"freeBytes"`
	TotalBytes int64 `json:"totalBytes"`

	MinFree    config.Size `json:"minFree"`
	ResumeFree config.Size `json:"resumeFree"`

	// Suspended is true when write pulls to this disk are currently held
	// back by the low watermark.
	Suspended bool `json:"suspended"`

	// Folders are the running folders backed by this disk.
	Folders []string `json:"folders"`
}

// DiskWatermarkEventData is emitted on a disk watermark state transition.
type DiskWatermarkEventData struct {
	Device string `json:"deviceId"`
	Disk   string `json:"disk"`

	FreeBytes  int64 `json:"freeBytes"`
	TotalBytes int64 `json:"totalBytes"`

	MinFree    config.Size `json:"minFree"`
	ResumeFree config.Size `json:"resumeFree"`

	Suspended bool     `json:"suspended"`
	Folders   []string `json:"folders"`
}

// diskWatermarkTarget represents a running folder that performs write pulls
// and thus can be suspended by a low data-disk watermark.
type diskWatermarkTarget struct {
	id   string
	root string
	// notify wakes the folder serve loop so it can adopt the new state.
	notify func()
	// schedulePull asks the folder to run a pull, used when suspension is
	// lifted.
	schedulePull func()
}

// diskWatermarkRunner is implemented by folder runners that can be
// suspended by the data-disk watermark.
type diskWatermarkRunner interface {
	diskWatermarkTarget() (diskWatermarkTarget, bool)
}

// diskProbeFunc returns the free/total space for a folder root path.
type diskProbeFunc func(root string) (fs.Usage, error)

type diskWatermarkState struct {
	device    string
	name      string
	usage     fs.Usage
	folders   []string
	suspended bool
}

type folderAction struct {
	target     diskWatermarkTarget
	resumePull bool
}

type diskWatermarkService struct {
	cfg       config.Wrapper
	evLogger  events.Logger
	targetsFn func() []diskWatermarkTarget
	sl        *slog.Logger

	probeDisk    diskProbeFunc
	identifyDisk func(root string, mounts []fs.DiskMount) (fs.DiskIdentity, error)
	listMounts   func() ([]fs.DiskMount, error)

	wake chan struct{}

	mut              sync.Mutex
	states           map[string]*diskWatermarkState // disk key -> state
	suspendedFolders map[string]string              // folder ID -> disk key
}

func newDiskWatermarkService(cfg config.Wrapper, evLogger events.Logger, targetsFn func() []diskWatermarkTarget) *diskWatermarkService {
	return &diskWatermarkService{
		cfg:              cfg,
		evLogger:         evLogger,
		targetsFn:        targetsFn,
		sl:               slog.Default().With("subsys", "diskwatermark"),
		probeDisk:        defaultDiskProbe,
		identifyDisk:     fs.IdentifyDisk,
		listMounts:       fs.DiskMounts,
		wake:             make(chan struct{}, 1),
		states:           make(map[string]*diskWatermarkState),
		suspendedFolders: make(map[string]string),
	}
}

func defaultDiskProbe(root string) (fs.Usage, error) {
	return fs.NewFilesystem(fs.FilesystemTypeBasic, root).Usage(".")
}

func (s *diskWatermarkService) String() string {
	return "diskWatermarkService"
}

// CommitConfiguration implements config.Committer. A config commit wakes the
// service so that enabling/disabling or changing the watermarks takes effect
// immediately.
func (s *diskWatermarkService) CommitConfiguration(from, to config.Configuration) bool {
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return true
}

func (s *diskWatermarkService) Serve(ctx context.Context) error {
	s.cfg.Subscribe(s)
	defer s.cfg.Unsubscribe(s)

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			s.evaluate(ctx)
			timer.Reset(s.checkInterval())
		case <-s.wake:
			s.evaluate(ctx)
			timer.Reset(s.checkInterval())
		}
	}
}

func (s *diskWatermarkService) checkInterval() time.Duration {
	dwc := s.cfg.Options().DiskWatermark
	if !dwc.Enabled || dwc.CheckIntervalS < 1 {
		// Disabled: wakeups only happen on config commits, but keep the
		// timer long instead of spinning.
		return time.Hour
	}
	return time.Duration(dwc.CheckIntervalS) * time.Second
}

// isFolderSuspended reports whether write pulls for the given folder are
// currently held back by a low data-disk watermark.
func (s *diskWatermarkService) isFolderSuspended(folderID string) bool {
	s.mut.Lock()
	defer s.mut.Unlock()
	_, ok := s.suspendedFolders[folderID]
	return ok
}

// statuses returns a snapshot of all managed disk states.
func (s *diskWatermarkService) statuses() []DiskWatermarkStatus {
	s.mut.Lock()
	defer s.mut.Unlock()

	dwc := s.cfg.Options().DiskWatermark

	res := make([]DiskWatermarkStatus, 0, len(s.states))
	for _, st := range s.states {
		folders := slices.Clone(st.folders)
		res = append(res, DiskWatermarkStatus{
			Device:     st.device,
			Disk:       st.name,
			FreeBytes:  int64(st.usage.Free),
			TotalBytes: int64(st.usage.Total),
			MinFree:    dwc.MinFree,
			ResumeFree: dwc.ResumeFree,
			Suspended:  st.suspended,
			Folders:    folders,
		})
	}
	slices.SortFunc(res, func(a, b DiskWatermarkStatus) int {
		if a.Device < b.Device {
			return -1
		} else if a.Device > b.Device {
			return 1
		}
		return 0
	})
	return res
}

type diskGroup struct {
	key     string
	name    string
	root    string
	targets []diskWatermarkTarget
}

// evaluate runs a single watermark check. It groups the running folders by
// physical disk, probes each disk once, applies the hysteresis state
// machine and notifies folders on transitions.
func (s *diskWatermarkService) evaluate(ctx context.Context) {
	dwc := s.cfg.Options().DiskWatermark
	if !dwc.Enabled {
		s.clearSuspensions(ctx)
		return
	}

	targets := s.targetsFn()
	if len(targets) == 0 {
		// Nothing to watch, but any previously known state is stale.
		s.clearSuspensions(ctx)
		return
	}

	mounts, _ := s.listMounts()

	// Group folders by physical disk, one probe per disk.
	groups := make(map[string]*diskGroup)
	groupOrder := make([]string, 0)
	for _, t := range targets {
		id, err := s.identifyDisk(t.root, mounts)
		if err != nil {
			// Missing/unreadable folder path: the regular folder health
			// check handles this, the watermark protection does not.
			s.sl.DebugContext(ctx, "Skipping folder in disk watermark check", slog.String("folder", t.id), slog.String("root", t.root), slog.Any("error", err))
			continue
		}
		g, ok := groups[id.Key]
		if !ok {
			g = &diskGroup{key: id.Key, name: id.Name, root: t.root}
			groups[id.Key] = g
			groupOrder = append(groupOrder, id.Key)
		}
		g.targets = append(g.targets, t)
	}
	slices.Sort(groupOrder)

	var actions []folderAction
	var eventList []DiskWatermarkEventData

	s.mut.Lock()
	newSuspended := make(map[string]string)

	for _, key := range groupOrder {
		g := groups[key]

		st, existed := s.states[key]
		if !existed {
			st = &diskWatermarkState{device: g.key, name: g.name}
			s.states[key] = st
		}
		st.name = g.name
		st.folders = folderIDs(g.targets)

		usage, err := s.probeDisk(g.root)
		if err != nil {
			// Can't read the disk: leave the current state untouched and
			// retain previously suspended folders on this disk.
			s.sl.DebugContext(ctx, "Failed to probe disk usage", slog.String("disk", st.name), slog.Any("error", err))
			for _, t := range g.targets {
				if _, wasSuspended := s.suspendedFolders[t.id]; wasSuspended {
					newSuspended[t.id] = key
				}
			}
			continue
		}
		st.usage = usage

		wasSuspended := st.suspended
		switch {
		case !wasSuspended:
			below, err := config.FreeSpaceBelow(dwc.MinFree, usage)
			if err != nil {
				s.sl.DebugContext(ctx, "Can't evaluate low watermark", slog.String("disk", st.name), slog.Any("error", err))
			} else if below {
				st.suspended = true
			}
		case wasSuspended:
			above, err := config.FreeSpaceAbove(dwc.ResumeFree, usage)
			if err != nil {
				s.sl.DebugContext(ctx, "Can't evaluate resume watermark", slog.String("disk", st.name), slog.Any("error", err))
			} else if above {
				st.suspended = false
			}
		}

		for _, t := range g.targets {
			_, wasSuspendedFolder := s.suspendedFolders[t.id]
			if st.suspended {
				newSuspended[t.id] = key
			}
			if st.suspended != wasSuspendedFolder {
				actions = append(actions, folderAction{target: t, resumePull: !st.suspended})
			}
		}

		if st.suspended != wasSuspended {
			ev := DiskWatermarkEventData{
				Device:     st.device,
				Disk:       st.name,
				FreeBytes:  int64(usage.Free),
				TotalBytes: int64(usage.Total),
				MinFree:    dwc.MinFree,
				ResumeFree: dwc.ResumeFree,
				Suspended:  st.suspended,
				Folders:    slices.Clone(st.folders),
			}
			eventList = append(eventList, ev)
		}
	}

	// Remove states for disks that no longer back any running folder.
	for key := range s.states {
		if _, ok := groups[key]; !ok {
			delete(s.states, key)
		}
	}

	s.suspendedFolders = newSuspended
	s.mut.Unlock()

	for _, ev := range eventList {
		if ev.Suspended {
			s.sl.WarnContext(ctx, "Data disk free space below watermark, suspending pulls", slog.String("disk", ev.Disk), slog.Int64("freeBytes", ev.FreeBytes), slog.Int64("totalBytes", ev.TotalBytes), slog.Any("folders", ev.Folders))
		} else {
			s.sl.InfoContext(ctx, "Data disk free space recovered, resuming pulls", slog.String("disk", ev.Disk), slog.Int64("freeBytes", ev.FreeBytes), slog.Int64("totalBytes", ev.TotalBytes), slog.Any("folders", ev.Folders))
		}
		s.evLogger.Log(events.DiskWatermarkChanged, ev)
	}

	for _, a := range actions {
		a.target.notify()
		if a.resumePull {
			a.target.schedulePull()
		}
	}
}

// clearSuspensions lifts all watermark suspensions, used when the feature is
// disabled or there are no targets anymore.
func (s *diskWatermarkService) clearSuspensions(ctx context.Context) {
	var actions []folderAction

	s.mut.Lock()
	if len(s.suspendedFolders) == 0 && len(s.states) == 0 {
		s.mut.Unlock()
		return
	}
	targets := s.targetsFn()
	byID := make(map[string]diskWatermarkTarget, len(targets))
	for _, t := range targets {
		byID[t.id] = t
	}
	for folderID := range s.suspendedFolders {
		if t, ok := byID[folderID]; ok {
			actions = append(actions, folderAction{target: t, resumePull: true})
		}
	}
	clear(s.states)
	clear(s.suspendedFolders)
	s.mut.Unlock()

	for _, a := range actions {
		s.sl.InfoContext(ctx, "Disk watermark protection disabled, resuming pulls", slog.String("folder", a.target.id))
		a.target.notify()
		a.target.schedulePull()
	}
}

func folderIDs(targets []diskWatermarkTarget) []string {
	ids := make([]string, 0, len(targets))
	for _, t := range targets {
		ids = append(ids, t.id)
	}
	slices.Sort(ids)
	return ids
}
