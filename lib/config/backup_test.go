// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/syncthing/syncthing/lib/events"
)

func backupTestWrapper(t *testing.T, path string, keep int) *wrapper {
	t.Helper()
	cfg := New(device1)
	cfg.Options.KeepConfigBackups = keep
	return Wrap(path, cfg, device1, events.NoopLogger).(*wrapper)
}

func backupTempPath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "syncthing-config-backup-*.xml")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	t.Cleanup(func() {
		os.Remove(path)
		os.RemoveAll(backupDirFor(path))
	})
	return path
}

func (w *wrapper) saveWithURURL(t *testing.T, urURL string) {
	t.Helper()
	w.mut.Lock()
	w.cfg.Options.URURL = urURL
	w.mut.Unlock()
	if err := w.Save(); err != nil {
		t.Fatal(err)
	}
}

func TestSaveBackupsDisabled(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 0)

	if err := w.Save(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(backupDirFor(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("no backup directory should be created when backups are disabled")
	}
}

func TestSaveCreatesAndPrunesBackups(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 2)

	w.saveWithURURL(t, "https://a.example")
	w.saveWithURURL(t, "https://b.example")
	w.saveWithURURL(t, "https://c.example")

	backups := listBackupFiles(path)
	if len(backups) != 2 {
		t.Fatalf("expected 2 retained generations, got %d: %+v", len(backups), backups)
	}
	if backups[0].generation != 3 || backups[1].generation != 2 {
		t.Errorf("expected generations [3 2] newest first, got %d %d", backups[0].generation, backups[1].generation)
	}
	for _, b := range backups {
		if !backupFileRe.MatchString(filepath.Base(b.path)) {
			t.Errorf("backup file name does not encode generation and time: %q", filepath.Base(b.path))
		}
		if b.saved.IsZero() {
			t.Errorf("backup generation %d has no parsed save time", b.generation)
		}
	}

	mainBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	newestBytes, err := os.ReadFile(backups[0].path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mainBytes, newestBytes) {
		t.Error("newest backup should be identical to the main config")
	}
}

func TestBackupsListParseability(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 3)

	w.saveWithURURL(t, "https://a.example")
	w.saveWithURURL(t, "https://b.example")

	// Corrupt the newest generation on disk.
	newest := listBackupFiles(path)[0]
	if err := writeAtomic(newest.path, []byte("<configuration>not valid xml")); err != nil {
		t.Fatal(err)
	}

	infos := w.Backups()
	if len(infos) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(infos))
	}
	if infos[0].Generation != newest.generation || infos[0].Parseable || infos[0].Error == "" {
		t.Errorf("expected corrupt newest generation with error, got %+v", infos[0])
	}
	if infos[1].Generation != 1 || !infos[1].Parseable || infos[1].Error != "" {
		t.Errorf("expected parseable oldest generation, got %+v", infos[1])
	}
}

func TestLoadOrFallbackCorruptMain(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 3)
	w.saveWithURURL(t, "https://a.example")
	backup := listBackupFiles(path)[0]

	if err := writeAtomic(path, []byte("<<<not xml>>>")); err != nil {
		t.Fatal(err)
	}

	rw, originalVersion, err := LoadOrFallback(path, device1, events.NoopLogger, false)
	if err != nil {
		t.Fatal(err)
	}
	if originalVersion != CurrentVersion {
		t.Errorf("expected original version %d, got %d", CurrentVersion, originalVersion)
	}
	info := rw.RollbackInfo()
	if info == nil {
		t.Fatal("expected rollback info")
	}
	if info.Trigger != RollbackTriggerStartup || info.Generation != backup.generation {
		t.Errorf("unexpected rollback info: %+v", info)
	}
	if rw.ConfigPath() != path {
		t.Errorf("rolled back wrapper should be tied to main config path, got %q", rw.ConfigPath())
	}
	if rw.Options().URURL != "https://a.example" {
		t.Error("expected restored config content")
	}

	mainBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	backupBytes, err := os.ReadFile(backup.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mainBytes, backupBytes) {
		t.Error("main config should have been replaced with the backup generation")
	}
}

func TestLoadOrFallbackNewestUsableGeneration(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 3)
	w.saveWithURURL(t, "https://a.example")
	w.saveWithURURL(t, "https://b.example")

	// Corrupt both the main config and the newest backup generation.
	if err := writeAtomic(path, []byte("truncated")); err != nil {
		t.Fatal(err)
	}
	newest := listBackupFiles(path)[0]
	if err := writeAtomic(newest.path, []byte("<configuration>broken")); err != nil {
		t.Fatal(err)
	}

	rw, _, err := LoadOrFallback(path, device1, events.NoopLogger, false)
	if err != nil {
		t.Fatal(err)
	}
	info := rw.RollbackInfo()
	if info == nil || info.Generation != 1 {
		t.Fatalf("expected fallback to generation 1, got %+v", info)
	}
	if rw.Options().URURL != "https://a.example" {
		t.Error("corrupt newest generation should be skipped in favor of the older one")
	}
}

func TestLoadOrFallbackAllFail(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 0)
	if err := w.Save(); err != nil {
		t.Fatal(err)
	}

	if err := writeAtomic(path, []byte("not xml")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadOrFallback(path, device1, events.NoopLogger, false); err == nil {
		t.Error("expected error when neither main config nor any backup can be loaded")
	}

	// An empty/truncated config retains the EOF error.
	if err := writeAtomic(path, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadOrFallback(path, device1, events.NoopLogger, false); !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF for empty config, got %v", err)
	}
}

func TestLoadOrFallbackMissingMain(t *testing.T) {
	path := backupTempPath(t)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadOrFallback(path, device1, events.NoopLogger, false); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected not-exist error, got %v", err)
	}
}

func TestRestoreBackup(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 3)
	w.saveWithURURL(t, "https://a.example")
	w.saveWithURURL(t, "https://b.example")

	waiter, err := w.RestoreBackup(1)
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	info := w.RollbackInfo()
	if info == nil || info.Trigger != RollbackTriggerManual || info.Generation != 1 {
		t.Fatalf("unexpected rollback info: %+v", info)
	}
	if w.RawCopy().Options.URURL != "https://a.example" {
		t.Error("expected in-memory config restored from generation 1")
	}

	// A safety generation capturing the pre-rollback config must exist.
	safety := listBackupFiles(path)[0]
	if safety.generation != 3 {
		t.Fatalf("expected new safety generation 3, got %d", safety.generation)
	}
	safetyData, err := os.ReadFile(safety.path)
	if err != nil {
		t.Fatal(err)
	}
	safetyCfg, _, err := ReadXML(bytes.NewReader(safetyData), device1)
	if err != nil {
		t.Fatal(err)
	}
	if safetyCfg.Options.URURL != "https://b.example" {
		t.Error("safety generation should contain the pre-rollback configuration")
	}

	// Emulate the REST layer finishing the change (waiter done + Save),
	// then reload through the normal load path.
	if err := w.Save(); err != nil {
		t.Fatal(err)
	}
	reloaded, _, err := Load(path, device1, events.NoopLogger)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Options().URURL != "https://a.example" {
		t.Error("expected reloaded config to be the restored generation")
	}
}

func TestRestoreBackupUnknownGeneration(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 3)
	w.saveWithURURL(t, "https://a.example")

	if _, err := w.RestoreBackup(42); !errors.Is(err, ErrBackupNotFound) {
		t.Errorf("expected ErrBackupNotFound, got %v", err)
	}
}

func TestRestoreBackupCorruptGeneration(t *testing.T) {
	path := backupTempPath(t)
	w := backupTestWrapper(t, path, 3)
	w.saveWithURURL(t, "https://a.example")
	w.saveWithURURL(t, "https://b.example")

	target := listBackupFiles(path)[0]
	if err := writeAtomic(target.path, []byte("<configuration>broken")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.RestoreBackup(target.generation); !errors.Is(err, ErrBackupCorrupt) {
		t.Errorf("expected ErrBackupCorrupt, got %v", err)
	}

	// The main config must remain untouched.
	if _, _, err := Load(path, device1, events.NoopLogger); err != nil {
		t.Errorf("main config should be untouched after failed rollback: %v", err)
	}
}

func TestKeepConfigBackupsClamp(t *testing.T) {
	opts := OptionsConfiguration{KeepConfigBackups: -1}
	opts.prepare(false)
	if opts.KeepConfigBackups != 0 {
		t.Errorf("negative value should disable backups, got %d", opts.KeepConfigBackups)
	}

	opts = OptionsConfiguration{KeepConfigBackups: 100000}
	opts.prepare(false)
	if opts.KeepConfigBackups != MaxKeepConfigBackups {
		t.Errorf("should clamp to %d, got %d", MaxKeepConfigBackups, opts.KeepConfigBackups)
	}
}
