// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
)

const (
	// MaxKeepConfigBackups is the largest configurable number of config
	// backup generations.
	MaxKeepConfigBackups = 100

	// Backup file naming and storage. The directory lives in the
	// configuration base directory, which is never part of a synced
	// folder, so backups never participate in the sync protocol.
	backupDirSuffix  = ".backups"
	backupTimeFormat = "20060102-150405"
	backupDirPerm    = 0o700
)

var (
	// ErrBackupNotFound is returned when a manual rollback refers to a
	// backup generation that doesn't exist.
	ErrBackupNotFound = errors.New("config backup generation not found")
	// ErrBackupCorrupt is returned when a manual rollback refers to a
	// backup generation that can't be parsed.
	ErrBackupCorrupt = errors.New("config backup generation is corrupt")

	backupFileRe = regexp.MustCompile(`^(\d{6})-(\d{8}-\d{6})\.xml$`)
)

// Rollback triggers recorded in RollbackInfo.
const (
	RollbackTriggerStartup = "startup"
	RollbackTriggerManual  = "manual"
)

// BackupInfo describes a single config backup generation.
type BackupInfo struct {
	Generation int       `json:"generation"`
	Time       time.Time `json:"time"`
	Parseable  bool      `json:"parseable"`
	Error      string    `json:"error,omitempty"`

	path string
}

// RollbackInfo records that the active configuration was taken from a
// backup generation, either automatically at startup or through a manual
// rollback.
type RollbackInfo struct {
	Time       time.Time `json:"time"`
	Generation int       `json:"generation"`
	Trigger    string    `json:"trigger"`
	Reason     string    `json:"reason,omitempty"`
}

type backupFile struct {
	generation int
	saved      time.Time
	path       string
}

func backupDirFor(configPath string) string {
	return configPath + backupDirSuffix
}

// listBackupFiles returns the config backup generations for the given config
// file path, ordered from newest to oldest generation. Entries that don't
// match the backup naming scheme are ignored.
func listBackupFiles(configPath string) []backupFile {
	entries, err := os.ReadDir(backupDirFor(configPath))
	if err != nil {
		return nil
	}
	var backups []backupFile
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		m := backupFileRe.FindStringSubmatch(ent.Name())
		if m == nil {
			continue
		}
		generation, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		saved, err := time.ParseInLocation(backupTimeFormat, m[2], time.Local)
		if err != nil {
			info, infoErr := ent.Info()
			if infoErr != nil {
				continue
			}
			saved = info.ModTime()
		}
		backups = append(backups, backupFile{
			generation: generation,
			saved:      saved,
			path:       filepath.Join(backupDirFor(configPath), ent.Name()),
		})
	}
	slices.SortFunc(backups, func(a, b backupFile) int {
		return b.generation - a.generation
	})
	return backups
}

// inspectBackup reads and attempts to parse the given backup generation.
func inspectBackup(b backupFile, myID protocol.DeviceID) BackupInfo {
	info := BackupInfo{
		Generation: b.generation,
		Time:       b.saved,
		path:       b.path,
	}
	data, err := os.ReadFile(b.path)
	if err != nil {
		info.Error = err.Error()
		return info
	}
	if _, _, err := ReadXML(bytes.NewReader(data), myID); err != nil {
		info.Error = err.Error()
		return info
	}
	info.Parseable = true
	return info
}

// writeBackupFile stores data as a new backup generation next to the config
// file and prunes generations beyond keep.
func writeBackupFile(configPath string, data []byte, keep int) error {
	dir := backupDirFor(configPath)
	if err := os.MkdirAll(dir, backupDirPerm); err != nil {
		return err
	}

	generation := 1
	if backups := listBackupFiles(configPath); len(backups) > 0 {
		generation = backups[0].generation + 1
	}
	name := fmt.Sprintf("%06d-%s.xml", generation, time.Now().Format(backupTimeFormat))
	if err := writeAtomic(filepath.Join(dir, name), data); err != nil {
		return err
	}

	pruneBackups(configPath, keep)
	return nil
}

func pruneBackups(configPath string, keep int) {
	backups := listBackupFiles(configPath)
	if len(backups) <= keep {
		return
	}
	for _, b := range backups[keep:] {
		if err := os.Remove(b.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("Failed to remove old config backup", slogutil.FilePath(b.path), slogutil.Error(err))
		}
	}
}

// writeAtomic writes data to path atomically with secure permissions.
func writeAtomic(path string, data []byte) error {
	fd, err := osutil.CreateAtomic(path)
	if err != nil {
		return err
	}
	if _, err := fd.Write(data); err != nil {
		fd.Close()
		return err
	}
	return fd.Close()
}

// WrapRolledBack wraps a configuration like Wrap and records that it was
// loaded from a backup generation.
func WrapRolledBack(path string, cfg Configuration, myID protocol.DeviceID, evLogger events.Logger, info *RollbackInfo) Wrapper {
	w := Wrap(path, cfg, myID, evLogger).(*wrapper)
	w.rollback = info
	return w
}

// LoadOrFallback loads the configuration at path. If the main config can't
// be loaded (it's corrupt or truncated), or it's a configuration version
// newer than supported and allowNewerConfig is false, backup generations are
// tried from newest to oldest. The first usable backup is atomically
// restored over the main config path and the returned wrapper records the
// rollback. A missing main config file, or failure to find any usable
// backup, results in the original error being returned.
func LoadOrFallback(path string, myID protocol.DeviceID, evLogger events.Logger, allowNewerConfig bool) (Wrapper, int, error) {
	w, originalVersion, err := Load(path, myID, evLogger)
	if err == nil && (originalVersion <= CurrentVersion || allowNewerConfig) {
		return w, originalVersion, nil
	}

	// A missing main config means "create a default one" to the caller.
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, err
	}

	mainErr := err
	if w != nil {
		// The main config loaded, but its version is newer than we
		// support.
		mainErr = fmt.Errorf("config file version (%d) is newer than supported version (%d)", originalVersion, CurrentVersion)
	}
	slog.Warn("Failed to load config; trying backup generations", slogutil.FilePath(path), slogutil.Error(mainErr))

	for _, b := range listBackupFiles(path) {
		bw, v, berr := Load(b.path, myID, evLogger)
		if berr != nil {
			slog.Warn("Skipping unreadable config backup", slogutil.FilePath(b.path), slogutil.Error(berr))
			continue
		}
		if v > CurrentVersion && !allowNewerConfig {
			slog.Warn("Skipping config backup with newer unsupported version", slogutil.FilePath(b.path), slog.Int("version", v))
			continue
		}

		data, rerr := os.ReadFile(b.path)
		if rerr != nil {
			slog.Warn("Skipping unreadable config backup", slogutil.FilePath(b.path), slogutil.Error(rerr))
			continue
		}
		if rerr := writeAtomic(path, data); rerr != nil {
			return nil, 0, fmt.Errorf("restore config backup: %w", rerr)
		}

		info := &RollbackInfo{
			Time:       time.Now(),
			Generation: b.generation,
			Trigger:    RollbackTriggerStartup,
			Reason:     mainErr.Error(),
		}
		slog.Warn("Restored config from backup generation", slog.Int("generation", b.generation), slogutil.FilePath(path))
		return WrapRolledBack(path, bw.RawCopy(), myID, evLogger, info), v, nil
	}

	if w != nil {
		// The main config loaded but was too new and no usable backup
		// existed; return it so the caller emits its standard error.
		return w, originalVersion, nil
	}
	return nil, 0, mainErr
}
