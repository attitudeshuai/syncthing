// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestBlobRange(t *testing.T) {
	exp := `
hash < x'249249'
hash >= x'249249' AND hash < x'492492'
hash >= x'492492' AND hash < x'6db6db'
hash >= x'6db6db' AND hash < x'924924'
hash >= x'924924' AND hash < x'b6db6d'
hash >= x'b6db6d' AND hash < x'db6db6'
hash >= x'db6db6'
	`

	ranges := blobRanges(7)
	buf := new(bytes.Buffer)
	for _, r := range ranges {
		fmt.Fprintln(buf, r.SQL("hash"))
	}

	if strings.TrimSpace(buf.String()) != strings.TrimSpace(exp) {
		t.Log(buf.String())
		t.Error("unexpected output")
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

// testMaintenanceService creates a service with controllable maintenance
// work for deterministic testing. The onMaintenanceStart hook signals
// each run on the returned channel and then blocks until the release
// channel is closed/received on. The work function itself returns the
// current fail error.
func testMaintenanceService(t *testing.T, sdb *DB, evLogger events.Logger) (svc *Service, runStarted <-chan struct{}, release chan<- struct{}, setFail func(bool)) {
	t.Helper()

	svc = newService(sdb, 0, evLogger)
	started := make(chan struct{}, 1)
	releaseCh := make(chan struct{})
	var mu sync.Mutex
	fail := false

	svc.onMaintenanceStart = func() {
		started <- struct{}{}
		<-releaseCh
	}
	svc.run = func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			return errors.New("maintenance failed")
		}
		// Simulate maintenance work taking some time.
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	return svc, started, releaseCh, func(f bool) {
		mu.Lock()
		fail = f
		mu.Unlock()
	}
}

func runMaintenanceService(t *testing.T, svc *Service, ctx context.Context) <-chan error {
	t.Helper()
	serveErr := make(chan error, 1)
	go func() { serveErr <- svc.Serve(ctx) }()
	return serveErr
}

// TestServiceManualMaintenanceStatusAndEvents verifies the on-demand
// trigger semantics: status transitions, started/finished events,
// coalescing of duplicate triggers while running, visible failures, and
// that failures don't prevent subsequent runs.
func TestServiceManualMaintenanceStatusAndEvents(t *testing.T) {
	t.Parallel()

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})

	evLogger := events.NewLogger()
	evCtx, cancelEvLogger := context.WithCancel(context.Background())
	t.Cleanup(cancelEvLogger)
	go func() { _ = evLogger.Serve(evCtx) }()
	sub := evLogger.Subscribe(events.DatabaseMaintenanceStarted | events.DatabaseMaintenanceFinished)
	pollEvent := func() events.Event {
		t.Helper()
		ev, err := sub.Poll(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return ev
	}

	svc, runStarted, release, setFail := testMaintenanceService(t, sdb, evLogger)

	// Before any run the status is idle and nothing has completed.
	st := svc.MaintenanceStatus()
	if st.Running || !st.LastCompleted.IsZero() || st.LastError != nil {
		t.Fatalf("unexpected initial status: %+v", st)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	serveErr := runMaintenanceService(t, svc, ctx)

	// Trigger a run and wait for it to start.
	svc.StartMaintenance()
	<-runStarted

	st = svc.MaintenanceStatus()
	if !st.Running {
		t.Fatal("maintenance should be running")
	}
	if st.Trigger != db.MaintenanceTriggerManual {
		t.Fatalf("trigger should be %q, got %q", db.MaintenanceTriggerManual, st.Trigger)
	}
	if st.Started.IsZero() {
		t.Fatal("started time should be set")
	}
	if st.Progress.Phase != db.MaintenancePhaseTidy {
		t.Fatalf("unexpected phase: %q", st.Progress.Phase)
	}

	// Repeated triggers while running return the running status and
	// don't schedule another run.
	for range 3 {
		st = svc.StartMaintenance()
		if !st.Running || st.Trigger != db.MaintenanceTriggerManual {
			t.Fatalf("duplicate trigger should return running status: %+v", st)
		}
	}

	// The started event carries the trigger.
	ev := pollEvent()
	if ev.Type != events.DatabaseMaintenanceStarted {
		t.Fatalf("unexpected event: %s", ev.Type)
	}
	if data := ev.Data.(map[string]any); data["trigger"] != string(db.MaintenanceTriggerManual) {
		t.Fatalf("unexpected started event data: %+v", data)
	}

	// Let the run finish successfully.
	release <- struct{}{}

	ev = pollEvent()
	if ev.Type != events.DatabaseMaintenanceFinished {
		t.Fatalf("unexpected event: %s", ev.Type)
	}
	data := ev.Data.(map[string]any)
	if data["success"] != true {
		t.Fatalf("expected success, got %+v", data)
	}
	if errp, _ := data["error"].(*string); errp != nil {
		t.Fatalf("expected no error, got %v", *errp)
	}
	if _, ok := data["duration"].(float64); !ok {
		t.Fatalf("expected duration, got %+v", data)
	}
	if data["trigger"] != string(db.MaintenanceTriggerManual) {
		t.Fatalf("unexpected trigger in finished event: %+v", data)
	}

	waitForCondition(t, 5*time.Second, func() bool { return !svc.MaintenanceStatus().Running }, "maintenance should become idle")
	waitForCondition(t, 5*time.Second, func() bool { return !svc.LastMaintenanceTime().IsZero() }, "successful maintenance should be recorded")
	st = svc.MaintenanceStatus()
	if st.LastCompleted.IsZero() || st.LastDuration <= 0 || st.LastError != nil {
		t.Fatalf("unexpected status after success: %+v", st)
	}

	// The duplicate triggers didn't queue another run.
	select {
	case <-runStarted:
		t.Fatal("duplicate triggers should not have scheduled another run")
	case <-time.After(100 * time.Millisecond):
	}

	// A failing run reports the failure in both the event and the
	// status.
	setFail(true)
	svc.StartMaintenance()
	<-runStarted
	if ev := pollEvent(); ev.Type != events.DatabaseMaintenanceStarted {
		t.Fatalf("unexpected event: %s", ev.Type)
	}
	st = svc.MaintenanceStatus()
	if !st.Running || st.LastError != nil {
		t.Fatalf("previous result should remain visible during a run: %+v", st)
	}
	release <- struct{}{}

	ev = pollEvent()
	if ev.Type != events.DatabaseMaintenanceFinished {
		t.Fatalf("unexpected event: %s", ev.Type)
	}
	if data := ev.Data.(map[string]any); data["success"] != false || data["error"] == nil {
		t.Fatalf("expected failure event, got %+v", data)
	}

	// As with periodic failures, Serve exits and the supervisor would
	// restart the service.
	select {
	case err := <-serveErr:
		if err == nil || !strings.Contains(err.Error(), "maintenance failed") {
			t.Fatalf("unexpected serve error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve should return after a failed run")
	}
	st = svc.MaintenanceStatus()
	if st.Running || st.LastError == nil || !strings.Contains(*st.LastError, "maintenance failed") {
		t.Fatalf("failure should be visible in status: %+v", st)
	}

	// Restart (as the supervisor does) and trigger again; the previous
	// failure doesn't affect the next manual run.
	setFail(false)
	runMaintenanceService(t, svc, ctx)
	svc.StartMaintenance()
	<-runStarted
	if ev := pollEvent(); ev.Type != events.DatabaseMaintenanceStarted {
		t.Fatalf("unexpected event: %s", ev.Type)
	}
	release <- struct{}{}

	ev = pollEvent()
	if data := ev.Data.(map[string]any); ev.Type != events.DatabaseMaintenanceFinished || data["success"] != true {
		t.Fatalf("expected successful event after restart, got %s %+v", ev.Type, ev.Data)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		st := svc.MaintenanceStatus()
		return !st.Running && st.LastError == nil
	}, "maintenance should recover after a previous failure")
}

// TestServiceMaintenanceShutdownInterruption verifies that canceling the
// service context during a run interrupts it promptly and that the
// finished event is not emitted during shutdown.
func TestServiceMaintenanceShutdownInterruption(t *testing.T) {
	t.Parallel()

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})

	evLogger := events.NewLogger()
	evCtx, cancelEvLogger := context.WithCancel(context.Background())
	t.Cleanup(cancelEvLogger)
	go func() { _ = evLogger.Serve(evCtx) }()
	sub := evLogger.Subscribe(events.DatabaseMaintenanceStarted | events.DatabaseMaintenanceFinished)

	svc := newService(sdb, 0, evLogger)
	runStarted := make(chan struct{}, 1)
	svc.onMaintenanceStart = func() { runStarted <- struct{}{} }
	svc.run = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := runMaintenanceService(t, svc, ctx)

	svc.StartMaintenance()
	<-runStarted
	if !svc.MaintenanceStatus().Running {
		t.Fatal("should be running")
	}

	cancel()

	select {
	case err := <-serveErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve should return promptly after shutdown")
	}

	st := svc.MaintenanceStatus()
	if st.Running {
		t.Fatal("should not be running after shutdown")
	}
	if st.LastError == nil || !strings.Contains(*st.LastError, "context canceled") {
		t.Fatalf("interruption should be recorded in status: %+v", st)
	}

	// The started event is emitted, but no finished event is emitted
	// during shutdown.
	ev, err := sub.Poll(time.Second)
	if err != nil || ev.Type != events.DatabaseMaintenanceStarted {
		t.Fatalf("expected started event, got %v / %s", err, ev.Type)
	}
	if _, err := sub.Poll(200 * time.Millisecond); err != events.ErrTimeout {
		t.Fatalf("expected no finished event during shutdown, got %v", err)
	}
}

// TestGarbageCollectChunkInterruption verifies that canceling the
// context between chunks rolls back the incomplete transaction and
// leaves the database in a consistent state, from which a subsequent
// full maintenance succeeds.
func TestGarbageCollectChunkInterruption(t *testing.T) {
	t.Parallel()

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})

	files := []protocol.FileInfo{genFile("test1", 1, 1)}
	if err := sdb.Update(folderID, protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}
	fdb, err := sdb.getFolderDB(folderID, false)
	if err != nil {
		t.Fatal(err)
	}

	countRows := func(table string) int {
		t.Helper()
		var n int
		if err := fdb.sql.Get(&n, `SELECT count(*) FROM `+table); err != nil {
			t.Fatal(err)
		}
		return n
	}

	blocklistsBefore := countRows("blocklists")
	blocksBefore := countRows("blocks")
	if blocklistsBefore != 1 || blocksBefore != 1 {
		t.Fatalf("unexpected baseline counts: %d blocklists, %d blocks", blocklistsBefore, blocksBefore)
	}

	// Mark the file deleted; its blocklist and blocks become garbage
	// collection candidates but are not removed until the GC commits.
	deleted := []protocol.FileInfo{genFile("test1", 1, 1)}
	deleted[0].SetDeleted(42)
	if err := sdb.Update(folderID, protocol.LocalDeviceID, deleted); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	err = garbageCollectBlocklistsAndBlocksLocked(ctx, fdb, func(string, int, int) {
		// Interrupt at the first chunk boundary of the first table.
		cancel()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// The transaction was rolled back: nothing was removed.
	if got := countRows("blocklists"); got != blocklistsBefore {
		t.Fatalf("interrupted GC should have rolled back, blocklists: %d != %d", got, blocklistsBefore)
	}
	if got := countRows("blocks"); got != blocksBefore {
		t.Fatalf("interrupted GC should have rolled back, blocks: %d != %d", got, blocksBefore)
	}

	// The database remains usable and a subsequent, uninterrupted
	// maintenance completes the garbage collection.
	svc := newService(sdb, time.Hour, events.NoopLogger)
	if err := svc.garbageCollectFolder(context.Background(), fdb); err != nil {
		t.Fatal(err)
	}
	if got := countRows("blocklists"); got != 0 {
		t.Fatalf("successful GC should remove blocklists, got %d", got)
	}
	if got := countRows("blocks"); got != 0 {
		t.Fatalf("successful GC should remove blocks, got %d", got)
	}

	// The GC sequence marker was advanced, so a second run skips the GC
	// and succeeds as well.
	if err := svc.garbageCollectFolder(context.Background(), fdb); err != nil {
		t.Fatal(err)
	}
}

// TestMaintenanceDoesNotBlockReadsOrOtherFolderWrites verifies that the
// per-folder lock held during garbage collection doesn't block reads of
// the maintained folder or writes to other folders. Only writes to the
// same folder are serialized, and they complete once maintenance is
// done.
func TestMaintenanceDoesNotBlockReadsOrOtherFolderWrites(t *testing.T) {
	t.Parallel()

	const (
		folderA = "folder-a"
		folderB = "folder-b"
	)

	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})

	if err := sdb.Update(folderA, protocol.LocalDeviceID, []protocol.FileInfo{genFile("test1", 1, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := sdb.Update(folderB, protocol.LocalDeviceID, []protocol.FileInfo{genFile("test1", 1, 1)}); err != nil {
		t.Fatal(err)
	}

	fdbA, err := sdb.getFolderDB(folderA, false)
	if err != nil {
		t.Fatal(err)
	}

	// Hold the folder lock the same way the folder GC does.
	fdbA.updateLock.Lock()

	results := make(chan error, 2)
	go func() {
		_, ok, err := sdb.GetDeviceFile(folderA, protocol.LocalDeviceID, "test1")
		if err == nil && !ok {
			err = errors.New("file disappeared")
		}
		results <- err
	}()
	go func() {
		results <- sdb.Update(folderB, protocol.LocalDeviceID, []protocol.FileInfo{genFile("test2", 1, 2)})
	}()

	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				fdbA.updateLock.Unlock()
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			fdbA.updateLock.Unlock()
			t.Fatal("normal reads/writes were blocked during maintenance")
		}
	}

	// A write to the folder being maintained is serialized with the GC
	// and completes once the lock is released.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- sdb.Update(folderA, protocol.LocalDeviceID, []protocol.FileInfo{genFile("test3", 1, 3)})
	}()
	select {
	case err := <-writeDone:
		fdbA.updateLock.Unlock()
		t.Fatalf("same-folder write should wait for the maintenance lock, got %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	fdbA.updateLock.Unlock()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("same-folder write should complete after maintenance")
	}
}
