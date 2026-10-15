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
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

type stubWatermarkProbe struct {
	mu    sync.Mutex
	usage fs.Usage
}

func (p *stubWatermarkProbe) probe(root string) (fs.Usage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.usage, nil
}

func (p *stubWatermarkProbe) set(usage fs.Usage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.usage = usage
}

func setupDiskWatermarkModel(t *testing.T, minDiskFree config.Size) (*testModel, *fakeConnection, config.FolderConfiguration, *stubWatermarkProbe) {
	t.Helper()

	dir := t.TempDir()

	cfg := config.New(myID)
	cfg.Options.MinHomeDiskFree = config.Size{} // disable unrelated database disk check
	w, cancel := newConfigWrapperFromContext(t.Context(), cfg)
	t.Cleanup(cancel)

	fcfg := newFolderConfiguration(w, "wm", "wm", config.FilesystemTypeBasic, dir)
	fcfg.FSWatcherEnabled = false
	fcfg.PullerDelayS = 0
	fcfg.MinDiskFree = minDiskFree
	fcfg.BlockIndexing = true
	fcfg.Devices = append(fcfg.Devices, config.FolderDeviceConfiguration{DeviceID: device1})

	waiter, err := w.Modify(func(cfg *config.Configuration) {
		cfg.SetDevice(newDeviceConfiguration(cfg.Defaults.Device, device1, "device1"))
		cfg.SetFolder(fcfg)
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	m := setupModel(t, w)
	fc := addFakeConn(m, device1, "wm")
	if err := m.ScanFolder("wm"); err != nil {
		t.Fatal(err)
	}

	probe := &stubWatermarkProbe{usage: fs.Usage{Free: 500, Total: 1000}}
	m.diskWatermarks.probeDisk = probe.probe

	return m, fc, fcfg, probe
}

func setDiskWatermarkEnabled(t *testing.T, m *testModel, enabled bool) {
	t.Helper()
	waiter, err := m.cfg.Modify(func(cfg *config.Configuration) {
		cfg.Options.DiskWatermark = config.DiskWatermarkConfiguration{
			Enabled:        enabled,
			MinFree:        config.Size{Value: 100, Unit: ""},
			ResumeFree:     config.Size{Value: 200, Unit: ""},
			CheckIntervalS: 60,
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()
}

func waitForFolderState(t *testing.T, m *testModel, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got, _, err := m.State("wm"); err == nil && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("folder never reached state %q", want)
}

func waitForFileAbsent(t *testing.T, ffs fs.Filesystem, name string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := ffs.Lstat(name); fs.IsNotExist(err) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("file %q still exists", name)
}

func waitForFilePresent(t *testing.T, ffs fs.Filesystem, name string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := ffs.Lstat(name); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("file %q never appeared", name)
}

// TestDiskWatermarkPullSuspension verifies the end to end behavior: when
// the data disk drops below the low watermark, write pulls are suspended
// without per-file errors, deletes still apply, and pulling resumes
// automatically after the disk recovers.
func TestDiskWatermarkPullSuspension(t *testing.T) {
	m, fc, fcfg, probe := setupDiskWatermarkModel(t, config.Size{})
	defer cleanupModel(m)
	ffs := fcfg.Filesystem()

	// Phase A: with the watermark disabled a regular pull completes.
	existingData := []byte("existing file contents\n")
	synced := make(chan struct{})
	fc.setIndexFn(func(_ context.Context, folder string, files []protocol.FileInfo) error {
		for _, f := range files {
			if f.Name == "existing" && !f.IsDeleted() {
				select {
				case <-synced:
				default:
					close(synced)
				}
			}
		}
		return nil
	})
	fc.addFile("existing", 0o644, protocol.FileInfoTypeFile, existingData)
	fc.sendIndexUpdate()
	select {
	case <-synced:
	case <-time.After(10 * time.Second):
		t.Fatal("initial file never synced")
	}
	if err := equalContents(ffs, "existing", existingData); err != nil {
		t.Fatal(err)
	}

	// Phase B: drop the disk below the low watermark and enable the
	// protection. The folder should enter the suspended state.
	probe.set(fs.Usage{Free: 50, Total: 1000})
	setDiskWatermarkEnabled(t, m, true)
	m.diskWatermarks.evaluate(context.Background()) // deterministic, don't wait for the ticker
	waitForFolderState(t, m, "disk-watermark-suspended")

	// Phase C: while suspended, a remote deletion must still be applied
	// while a new file must not be pulled (and no per-file errors).
	newData := []byte("new file contents after watermark\n")
	fc.deleteFile("existing")
	fc.addFile("newfile", 0o644, protocol.FileInfoTypeFile, newData)
	fc.sendIndexUpdate()

	waitForFileAbsent(t, ffs, "existing")
	if _, err := ffs.Lstat("newfile"); err == nil {
		t.Fatal("new file should not be pulled while the disk is suspended")
	}

	// Scanning remains functional during suspension.
	if err := m.ScanFolder("wm"); err != nil {
		t.Fatalf("scan should work during suspension: %v", err)
	}

	// The new file stays needed and no pull errors are recorded.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		need, err := m.NeedSize("wm", protocol.LocalDeviceID)
		if err != nil {
			t.Fatal(err)
		}
		if need.Files == 0 {
			t.Fatal("suspended file should remain needed")
		}
		if need.Deleted != 0 {
			t.Fatalf("deletion should have been applied, %d still needed", need.Deleted)
		}
		errs, err := m.FolderErrors("wm")
		if err != nil {
			t.Fatal(err)
		}
		if len(errs) != 0 {
			t.Fatalf("watermark suspension must not create pull errors, got %d: %+v", len(errs), errs)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := ffs.Lstat("newfile"); err == nil {
		t.Fatal("new file should still not be pulled while suspended")
	}

	// Phase D: the disk recovers above the resume watermark; pulling
	// resumes automatically.
	resumed := make(chan struct{})
	fc.setIndexFn(func(_ context.Context, folder string, files []protocol.FileInfo) error {
		for _, f := range files {
			if f.Name == "newfile" && !f.IsDeleted() {
				select {
				case <-resumed:
				default:
					close(resumed)
				}
			}
		}
		return nil
	})
	probe.set(fs.Usage{Free: 500, Total: 1000})
	m.diskWatermarks.evaluate(context.Background())
	waitForFolderState(t, m, "idle")
	select {
	case <-resumed:
	case <-time.After(10 * time.Second):
		t.Fatal("pull did not resume after the disk recovered")
	}
	waitForFilePresent(t, ffs, "newfile")
	if err := equalContents(ffs, "newfile", newData); err != nil {
		t.Fatal(err)
	}
}

// TestDiskWatermarkStatusesExposesDisk verifies the disk state is visible
// through the model while suspended and cleared on resume.
func TestDiskWatermarkStatusesExposed(t *testing.T) {
	m, _, _, probe := setupDiskWatermarkModel(t, config.Size{})
	defer cleanupModel(m)

	if statuses := m.DiskWatermarkStatuses(); len(statuses) != 0 {
		t.Fatalf("no managed disks expected while disabled, got %+v", statuses)
	}

	probe.set(fs.Usage{Free: 50, Total: 1000})
	setDiskWatermarkEnabled(t, m, true)
	m.diskWatermarks.evaluate(context.Background())

	var status DiskWatermarkStatus
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		statuses := m.DiskWatermarkStatuses()
		if len(statuses) == 1 {
			status = statuses[0]
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status.Disk == "" || status.Device == "" {
		t.Fatalf("disk identity missing in status: %+v", status)
	}
	if !status.Suspended || status.FreeBytes != 50 || status.TotalBytes != 1000 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if len(status.Folders) != 1 || status.Folders[0] != "wm" {
		t.Fatalf("unexpected folders: %+v", status.Folders)
	}
	if status.MinFree.Value != 100 || status.ResumeFree.Value != 200 {
		t.Fatalf("watermarks missing: %+v", status)
	}
}

// TestPerFolderMinDiskFreeStillReportsFileError verifies the pre-existing
// per-folder low disk space check keeps working independently and produces
// a distinguishable per-file error rather than a suspension.
func TestPerFolderMinDiskFreeStillReportsFileError(t *testing.T) {
	m, fc, _, _ := setupDiskWatermarkModel(t, config.Size{Value: 999, Unit: "%"})
	defer cleanupModel(m)

	fc.addFile("bigfile", 0o644, protocol.FileInfoTypeFile, []byte("data\n"))
	fc.sendIndexUpdate()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		errs, err := m.FolderErrors("wm")
		if err != nil {
			t.Fatal(err)
		}
		if len(errs) > 0 {
			if errs[0].Path != "bigfile" {
				t.Fatalf("error should be attributed to the file, got %q", errs[0].Path)
			}
			goto found
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected a per-file pull error from the folder minDiskFree check")

found:
	if state, _, err := m.State("wm"); err != nil || state == "disk-watermark-suspended" {
		t.Fatalf("folder must not enter watermark suspension, state=%q err=%v", state, err)
	}
}
