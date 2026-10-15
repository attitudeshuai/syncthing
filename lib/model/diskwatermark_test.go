// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
)

type fakeWatermarkTarget struct {
	id       string
	root     string
	mut      sync.Mutex
	notifies int
	pulls    int
}

func (t *fakeWatermarkTarget) target() diskWatermarkTarget {
	return diskWatermarkTarget{
		id:   t.id,
		root: t.root,
		notify: func() {
			t.mut.Lock()
			t.notifies++
			t.mut.Unlock()
		},
		schedulePull: func() {
			t.mut.Lock()
			t.pulls++
			t.mut.Unlock()
		},
	}
}

func (t *fakeWatermarkTarget) counts() (notifies, pulls int) {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.notifies, t.pulls
}

type fakeDiskProbe struct {
	mut    sync.Mutex
	usages map[string]fs.Usage
	errors map[string]error
	calls  int
	roots  []string
}

func (p *fakeDiskProbe) probe(root string) (fs.Usage, error) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.calls++
	p.roots = append(p.roots, root)
	if err, ok := p.errors[root]; ok {
		return fs.Usage{}, err
	}
	return p.usages[root], nil
}

func (p *fakeDiskProbe) callCount() int {
	p.mut.Lock()
	defer p.mut.Unlock()
	return p.calls
}

func newTestDiskWatermarkService(t *testing.T, dwc config.DiskWatermarkConfiguration, targetsFn func() []diskWatermarkTarget, identities map[string]fs.DiskIdentity, probe *fakeDiskProbe) (*diskWatermarkService, config.Wrapper, events.Logger) {
	t.Helper()

	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree = config.Size{} // avoid unrelated disk checks
	cfg.Options.DiskWatermark = dwc
	w, cancel := newConfigWrapper(cfg)
	t.Cleanup(cancel)

	evLogger := events.NewLogger()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go evLogger.Serve(ctx)

	svc := newDiskWatermarkService(w, evLogger, targetsFn)
	svc.identifyDisk = func(root string, _ []fs.DiskMount) (fs.DiskIdentity, error) {
		if id, ok := identities[root]; ok {
			return id, nil
		}
		return fs.DiskIdentity{}, fs.ErrNotExist
	}
	svc.listMounts = func() ([]fs.DiskMount, error) {
		return nil, nil
	}
	if probe != nil {
		svc.probeDisk = probe.probe
	}

	return svc, w, evLogger
}

func watermarkCfg(enabled bool, min, resume float64) config.DiskWatermarkConfiguration {
	dwc := config.DiskWatermarkConfiguration{
		Enabled:        enabled,
		MinFree:        config.Size{Value: min, Unit: ""},
		ResumeFree:     config.Size{Value: resume, Unit: ""},
		CheckIntervalS: 60,
	}
	return dwc
}

func drainEvents(sub events.Subscription) []events.Event {
	var evs []events.Event
	for {
		select {
		case ev := <-sub.C():
			evs = append(evs, ev)
		default:
			return evs
		}
	}
}

func waitForEvent(t *testing.T, sub events.Subscription) events.Event {
	t.Helper()
	select {
	case ev := <-sub.C():
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for disk watermark event")
		return events.Event{}
	}
}

func TestDiskWatermarkGroupsDisksAndSuspends(t *testing.T) {
	a1 := &fakeWatermarkTarget{id: "a1", root: "/diskA/1"}
	a2 := &fakeWatermarkTarget{id: "a2", root: "/diskA/2"}
	b1 := &fakeWatermarkTarget{id: "b1", root: "/diskB/1"}
	targets := []diskWatermarkTarget{a1.target(), a2.target(), b1.target()}

	identities := map[string]fs.DiskIdentity{
		"/diskA/1": {Key: "dev:A", Name: "/diskA"},
		"/diskA/2": {Key: "dev:A", Name: "/diskA"},
		"/diskB/1": {Key: "dev:B", Name: "/diskB"},
	}
	probe := &fakeDiskProbe{
		usages: map[string]fs.Usage{
			"/diskA/1": {Free: 50, Total: 1000},
			"/diskB/1": {Free: 500, Total: 1000},
		},
	}

	svc, _, evLogger := newTestDiskWatermarkService(t, watermarkCfg(true, 100, 200), func() []diskWatermarkTarget { return targets }, identities, probe)
	sub := evLogger.Subscribe(events.DiskWatermarkChanged)

	svc.evaluate(context.Background())

	if got := probe.callCount(); got != 2 {
		t.Fatalf("expected one probe per distinct disk (2), got %d", got)
	}

	if !svc.isFolderSuspended("a1") || !svc.isFolderSuspended("a2") {
		t.Error("folders on the below-watermark disk should be suspended")
	}
	if svc.isFolderSuspended("b1") {
		t.Error("folder on a healthy disk should not be suspended")
	}

	statuses := svc.statuses()
	if len(statuses) != 2 {
		t.Fatalf("expected statuses for 2 disks, got %d", len(statuses))
	}
	var suspended *DiskWatermarkStatus
	for i := range statuses {
		if statuses[i].Device == "dev:A" {
			suspended = &statuses[i]
		}
	}
	if suspended == nil || !suspended.Suspended || suspended.FreeBytes != 50 || suspended.TotalBytes != 1000 {
		t.Fatalf("unexpected suspended disk status: %+v", suspended)
	}
	if len(suspended.Folders) != 2 {
		t.Errorf("expected both folders listed on the disk, got %v", suspended.Folders)
	}

	ev := waitForEvent(t, sub)
	data := ev.Data.(DiskWatermarkEventData)
	if !data.Suspended || data.Disk != "/diskA" || len(data.Folders) != 2 {
		t.Fatalf("unexpected event data: %+v", data)
	}

	if n, _ := a1.counts(); n != 1 {
		t.Errorf("a1 should be notified once on suspend, got %d", n)
	}
	if n, _ := b1.counts(); n != 0 {
		t.Errorf("b1 should not be notified, got %d", n)
	}
	if _, pulls := a1.counts(); pulls != 0 {
		t.Error("no pull should be scheduled on suspend")
	}
}

func TestDiskWatermarkHysteresis(t *testing.T) {
	a1 := &fakeWatermarkTarget{id: "a1", root: "/diskA/1"}
	targets := []diskWatermarkTarget{a1.target()}
	identities := map[string]fs.DiskIdentity{
		"/diskA/1": {Key: "dev:A", Name: "/diskA"},
	}
	probe := &fakeDiskProbe{usages: map[string]fs.Usage{
		"/diskA/1": {Free: 50, Total: 1000},
	}}

	svc, _, evLogger := newTestDiskWatermarkService(t, watermarkCfg(true, 100, 200), func() []diskWatermarkTarget { return targets }, identities, probe)
	sub := evLogger.Subscribe(events.DiskWatermarkChanged)

	// Below the low watermark: suspend.
	svc.evaluate(context.Background())
	waitForEvent(t, sub)
	if !svc.isFolderSuspended("a1") {
		t.Fatal("should be suspended")
	}

	// Inside the hysteresis band while suspended: stay suspended, no event.
	probe.mut.Lock()
	probe.usages["/diskA/1"] = fs.Usage{Free: 150, Total: 1000}
	probe.mut.Unlock()
	svc.evaluate(context.Background())
	if !svc.isFolderSuspended("a1") {
		t.Fatal("should remain suspended in the hysteresis band")
	}
	if evs := drainEvents(sub); len(evs) != 0 {
		t.Fatalf("no event expected in the band, got %d", len(evs))
	}
	if _, pulls := a1.counts(); pulls != 0 {
		t.Fatal("should not resume pulls in the band")
	}

	// Equal to the low watermark is not below: still suspended, though.
	probe.mut.Lock()
	probe.usages["/diskA/1"] = fs.Usage{Free: 100, Total: 1000}
	probe.mut.Unlock()
	svc.evaluate(context.Background())
	if !svc.isFolderSuspended("a1") {
		t.Fatal("should remain suspended at exactly the low watermark")
	}

	// Equal to the resume watermark is not above it: stay suspended.
	probe.mut.Lock()
	probe.usages["/diskA/1"] = fs.Usage{Free: 200, Total: 1000}
	probe.mut.Unlock()
	svc.evaluate(context.Background())
	if !svc.isFolderSuspended("a1") {
		t.Fatal("should remain suspended at exactly the resume watermark")
	}

	// Above the resume watermark: resume, pull scheduled.
	probe.mut.Lock()
	probe.usages["/diskA/1"] = fs.Usage{Free: 201, Total: 1000}
	probe.mut.Unlock()
	svc.evaluate(context.Background())
	ev := waitForEvent(t, sub)
	if ev.Data.(DiskWatermarkEventData).Suspended {
		t.Fatal("resume event expected")
	}
	if svc.isFolderSuspended("a1") {
		t.Fatal("should be resumed")
	}
	if _, pulls := a1.counts(); pulls != 1 {
		t.Fatalf("one pull should be scheduled on resume, got %d", pulls)
	}

	// Healthy disk dipping into the band but not below the low watermark:
	// nothing happens.
	probe.mut.Lock()
	probe.usages["/diskA/1"] = fs.Usage{Free: 150, Total: 1000}
	probe.mut.Unlock()
	svc.evaluate(context.Background())
	if svc.isFolderSuspended("a1") {
		t.Fatal("should not suspend above the low watermark")
	}
	if evs := drainEvents(sub); len(evs) != 0 {
		t.Fatalf("no event expected, got %d", len(evs))
	}
}

func TestDiskWatermarkSkipsUnreadableFoldersAndDisks(t *testing.T) {
	// Missing folder path: identity lookup fails.
	missing := &fakeWatermarkTarget{id: "missing", root: "/nope"}
	// Identity resolves but usage probe fails.
	broken := &fakeWatermarkTarget{id: "broken", root: "/diskX/1"}

	targets := []diskWatermarkTarget{missing.target(), broken.target()}
	identities := map[string]fs.DiskIdentity{
		"/diskX/1": {Key: "dev:X", Name: "/diskX"},
	}
	probe := &fakeDiskProbe{
		errors: map[string]error{
			"/diskX/1": fs.ErrNotExist,
		},
		usages: map[string]fs.Usage{},
	}

	svc, _, evLogger := newTestDiskWatermarkService(t, watermarkCfg(true, 100, 200), func() []diskWatermarkTarget { return targets }, identities, probe)
	sub := evLogger.Subscribe(events.DiskWatermarkChanged)

	svc.evaluate(context.Background())

	if svc.isFolderSuspended("missing") || svc.isFolderSuspended("broken") {
		t.Fatal("unreadable folders/disks must not be suspended")
	}
	if evs := drainEvents(sub); len(evs) != 0 {
		t.Fatalf("no events expected for unreadable disks, got %d", len(evs))
	}
	if n, _ := missing.counts(); n != 0 {
		t.Error("missing folder should not be notified")
	}
}

func TestDiskWatermarkDisableClearsSuspension(t *testing.T) {
	a1 := &fakeWatermarkTarget{id: "a1", root: "/diskA/1"}
	targets := []diskWatermarkTarget{a1.target()}
	identities := map[string]fs.DiskIdentity{
		"/diskA/1": {Key: "dev:A", Name: "/diskA"},
	}
	probe := &fakeDiskProbe{usages: map[string]fs.Usage{
		"/diskA/1": {Free: 50, Total: 1000},
	}}

	svc, w, _ := newTestDiskWatermarkService(t, watermarkCfg(true, 100, 200), func() []diskWatermarkTarget { return targets }, identities, probe)

	svc.evaluate(context.Background())
	if !svc.isFolderSuspended("a1") {
		t.Fatal("should be suspended")
	}

	waiter, err := w.Modify(func(cfg *config.Configuration) {
		cfg.Options.DiskWatermark.Enabled = false
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	svc.evaluate(context.Background())
	if svc.isFolderSuspended("a1") {
		t.Fatal("disabling should clear suspension")
	}
	if len(svc.statuses()) != 0 {
		t.Fatal("states should be cleared when disabled")
	}
	if _, pulls := a1.counts(); pulls != 1 {
		t.Fatalf("pull should be scheduled when disabled, got %d", pulls)
	}

	// Further disabled checks do nothing.
	notifiesBefore, _ := a1.counts()
	svc.evaluate(context.Background())
	if n, _ := a1.counts(); n != notifiesBefore {
		t.Fatal("disabled service should stay idle")
	}
}

func TestDiskWatermarkJoiningFolderOnSuspendedDisk(t *testing.T) {
	a1 := &fakeWatermarkTarget{id: "a1", root: "/diskA/1"}
	a2 := &fakeWatermarkTarget{id: "a2", root: "/diskA/2"}
	targets := []diskWatermarkTarget{a1.target(), a2.target()}
	identities := map[string]fs.DiskIdentity{
		"/diskA/1": {Key: "dev:A", Name: "/diskA"},
		"/diskA/2": {Key: "dev:A", Name: "/diskA"},
	}
	probe := &fakeDiskProbe{usages: map[string]fs.Usage{
		"/diskA/1": {Free: 50, Total: 1000},
	}}

	svc, _, evLogger := newTestDiskWatermarkService(t, watermarkCfg(true, 100, 200), func() []diskWatermarkTarget { return targets }, identities, probe)
	sub := evLogger.Subscribe(events.DiskWatermarkChanged)

	svc.evaluate(context.Background())
	waitForEvent(t, sub)

	// A new folder appears on the already suspended disk.
	a3 := &fakeWatermarkTarget{id: "a3", root: "/diskA/3"}
	targets = append(targets, a3.target())
	identities["/diskA/3"] = fs.DiskIdentity{Key: "dev:A", Name: "/diskA"}
	probe.usages["/diskA/3"] = fs.Usage{Free: 50, Total: 1000}

	svc.evaluate(context.Background())

	if !svc.isFolderSuspended("a3") {
		t.Error("new folder on suspended disk should be suspended")
	}
	if n, _ := a3.counts(); n != 1 {
		t.Errorf("new folder should be notified once, got %d", n)
	}
	// Existing folders are not re-notified, and no new disk-level event
	// because the disk state didn't transition.
	if n, _ := a1.counts(); n != 1 {
		t.Errorf("existing folder should not be re-notified, got %d", n)
	}
	if evs := drainEvents(sub); len(evs) != 0 {
		t.Fatalf("no disk transition event expected, got %d", len(evs))
	}
}

func TestDiskWatermarkDisabledDoesNotProbe(t *testing.T) {
	a1 := &fakeWatermarkTarget{id: "a1", root: "/diskA/1"}
	targets := []diskWatermarkTarget{a1.target()}
	identities := map[string]fs.DiskIdentity{
		"/diskA/1": {Key: "dev:A", Name: "/diskA"},
	}
	probe := &fakeDiskProbe{usages: map[string]fs.Usage{
		"/diskA/1": {Free: 50, Total: 1000},
	}}

	svc, _, _ := newTestDiskWatermarkService(t, watermarkCfg(false, 100, 200), func() []diskWatermarkTarget { return targets }, identities, probe)
	svc.evaluate(context.Background())

	if probe.callCount() != 0 {
		t.Fatalf("disabled service should not probe, got %d probes", probe.callCount())
	}
	if svc.isFolderSuspended("a1") {
		t.Fatal("folder should not be suspended when disabled")
	}
}
