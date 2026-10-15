// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

const (
	internalMetaPrefix     = "dbsvc"
	lastMaintKey           = "lastMaint"
	lastSuccessfulGCSeqKey = "lastSuccessfulGCSeq"

	gcMinChunks  = 5
	gcChunkSize  = 100_000         // approximate number of rows to process in a single gc query
	gcMaxRuntime = 5 * time.Minute // max time to spend on gc, per table, per run
)

func (s *DB) Service(maintenanceInterval time.Duration, evLogger events.Logger) db.DBService {
	return newService(s, maintenanceInterval, evLogger)
}

type Service struct {
	sdb                 *DB
	maintenanceInterval time.Duration
	internalMeta        *db.Typed
	evLogger            events.Logger

	// start is a capacity-one signal channel. Sending a token requests a
	// maintenance run; a pending token is consumed by the next loop
	// iteration. A send is skipped (non-blocking) when a run is already
	// in progress, so concurrent triggers coalesce into the running run.
	start chan struct{}

	mut    sync.Mutex
	status db.MaintenanceStatus

	// onMaintenanceStart, if set, is called after the running state has
	// been recorded and the started event emitted, but before any work
	// is done. It exists for deterministic testing.
	onMaintenanceStart func()

	// run is the actual maintenance work. It defaults to periodic and
	// exists for deterministic testing.
	run func(ctx context.Context) error
}

func (s *Service) String() string {
	return fmt.Sprintf("sqlite.service@%p", s)
}

func newService(sdb *DB, maintenanceInterval time.Duration, evLogger events.Logger) *Service {
	internalMeta := db.NewTyped(sdb, internalMetaPrefix)
	lastMaint, _, _ := internalMeta.Time(lastMaintKey)
	s := &Service{
		sdb:                 sdb,
		maintenanceInterval: maintenanceInterval,
		internalMeta:        internalMeta,
		evLogger:            evLogger,
		start:               make(chan struct{}, 1),
		status: db.MaintenanceStatus{
			// Seed from the persisted last-success time so there is
			// meaningful status before the first run of this process.
			LastCompleted: lastMaint,
		},
	}
	s.run = s.periodic
	return s
}

// StartMaintenance requests an on-demand maintenance run. If one is
// already running (or one has already been requested and hasn't started
// yet), no additional run is scheduled and the current status is
// returned.
func (s *Service) StartMaintenance() db.MaintenanceStatus {
	s.mut.Lock()
	running := s.status.Running
	s.mut.Unlock()

	if !running {
		select {
		case s.start <- struct{}{}:
		default:
		}
	}
	return s.MaintenanceStatus()
}

func (s *Service) MaintenanceStatus() db.MaintenanceStatus {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.status
}

func (s *Service) Serve(ctx context.Context) error {
	// Run periodic maintenance
	// Figure out when we last ran maintenance and schedule accordingly. If
	// it was never, do it now.
	lastMaint, _, _ := s.internalMeta.Time(lastMaintKey)
	nextMaint := lastMaint.Add(s.maintenanceInterval)
	wait := time.Until(nextMaint)
	if wait < 0 {
		wait = time.Minute
	}
	slog.DebugContext(ctx, "Next periodic run due", "after", wait)
	timer := time.NewTimer(wait)

	if s.maintenanceInterval == 0 {
		timer.Stop()
	}

	for {
		var trigger db.MaintenanceTrigger
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			trigger = db.MaintenanceTriggerPeriodic
		case <-s.start:
			trigger = db.MaintenanceTriggerManual
		}

		started := s.beginRun(trigger)
		err := s.run(ctx)
		s.finishRun(trigger, started, err, ctx.Err() != nil)

		if err != nil {
			return wrap(err)
		}

		if s.maintenanceInterval != 0 {
			timer.Reset(s.maintenanceInterval)
			slog.DebugContext(ctx, "Next periodic run due", "after", s.maintenanceInterval)
		}

		_ = s.internalMeta.PutTime(lastMaintKey, time.Now())
	}
}

func (s *Service) LastMaintenanceTime() time.Time {
	lastMaint, _, _ := s.internalMeta.Time(lastMaintKey)
	return lastMaint
}

// beginRun marks maintenance as running and emits the started event. The
// returned time is the start time of the run.
func (s *Service) beginRun(trigger db.MaintenanceTrigger) time.Time {
	started := time.Now()

	s.mut.Lock()
	s.status.Running = true
	s.status.Trigger = trigger
	s.status.Started = started
	s.status.Progress = db.MaintenanceProgress{Phase: db.MaintenancePhaseTidy}
	s.mut.Unlock()

	s.evLogger.Log(events.DatabaseMaintenanceStarted, map[string]any{
		"trigger": string(trigger),
	})

	if s.onMaintenanceStart != nil {
		s.onMaintenanceStart()
	}

	return started
}

// finishRun records the result of a maintenance run and emits the
// finished event. The finished event is not emitted on shutdown, when the
// run was interrupted due to the context being canceled; the event
// logger may be going away at the same time.
func (s *Service) finishRun(trigger db.MaintenanceTrigger, started time.Time, err error, interrupted bool) {
	finished := time.Now()

	s.mut.Lock()
	s.status.Running = false
	s.status.Trigger = ""
	s.status.Started = time.Time{}
	s.status.LastCompleted = finished
	s.status.LastDuration = finished.Sub(started)
	s.status.LastError = events.Error(err)
	s.status.Progress = db.MaintenanceProgress{}
	s.mut.Unlock()

	if interrupted {
		slog.Debug("Maintenance run interrupted during shutdown", "trigger", string(trigger), slogutil.Error(err))
		return
	}

	s.evLogger.Log(events.DatabaseMaintenanceFinished, map[string]any{
		"trigger":  string(trigger),
		"success":  err == nil,
		"error":    events.Error(err),
		"duration": finished.Sub(started).Seconds(),
	})
}

func (s *Service) setProgress(p db.MaintenanceProgress) {
	s.mut.Lock()
	s.status.Progress = p
	s.mut.Unlock()
}

// setChunkProgress updates the chunk level progress of the table
// currently being garbage collected without touching the folder level
// progress.
func (s *Service) setChunkProgress(table string, chunksTotal, chunksDone int) {
	s.mut.Lock()
	s.status.Progress.Table = table
	s.status.Progress.ChunksTotal = chunksTotal
	s.status.Progress.ChunksDone = chunksDone
	s.mut.Unlock()
}

func (s *Service) periodic(ctx context.Context) error {
	t0 := time.Now()
	slog.DebugContext(ctx, "Periodic start")

	t1 := time.Now()
	defer func() { slog.DebugContext(ctx, "Periodic done in", "t1", time.Since(t1), "t0t1", t1.Sub(t0)) }()

	folders, err := s.sdb.ListFolders()
	if err != nil {
		return wrap(err)
	}
	foldersTotal := len(folders)
	s.setProgress(db.MaintenanceProgress{Phase: db.MaintenancePhaseTidy, FoldersTotal: foldersTotal})

	// Stop at a safe boundary when shutting down, before taking any
	// locks.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.sdb.updateLock.Lock()
	err = tidy(ctx, s.sdb.sql)
	s.sdb.updateLock.Unlock()
	if err != nil {
		return err
	}

	foldersDone := 0
	return wrap(s.sdb.forEachFolder(func(fdb *folderDB) error {
		// Stop between folders at a safe boundary when shutting down.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		s.setProgress(db.MaintenanceProgress{
			Phase:        db.MaintenancePhaseGarbageCollect,
			FoldersTotal: foldersTotal,
			FoldersDone:  foldersDone,
			Folder:       fdb.folderID,
		})

		if err := s.garbageCollectFolder(ctx, fdb); err != nil {
			return wrap(err)
		}

		foldersDone++
		s.setProgress(db.MaintenanceProgress{
			Phase:        db.MaintenancePhaseGarbageCollect,
			FoldersTotal: foldersTotal,
			FoldersDone:  foldersDone,
			Folder:       fdb.folderID,
		})
		return nil
	}))
}

// garbageCollectFolder runs the garbage collection steps for a single
// folder. It does not take the folder update lock itself.
func (s *Service) garbageCollectFolder(ctx context.Context, fdb *folderDB) error {
	// Get the current device sequence, for comparison in the next step.
	seq, err := fdb.GetDeviceSequence(protocol.LocalDeviceID)
	if err != nil {
		return wrap(err)
	}
	// Get the last successful GC sequence. If it's the same as the
	// current sequence, nothing has changed and we can skip the GC
	// entirely.
	meta := db.NewTyped(fdb, internalMetaPrefix)
	if prev, _, err := meta.Int64(lastSuccessfulGCSeqKey); err != nil {
		return wrap(err)
	} else if seq == prev {
		slog.DebugContext(ctx, "Skipping unnecessary GC", "folder", fdb.folderID, "fdb", fdb.baseName)
		return nil
	}

	// Run the GC steps, in a function to be able to use a deferred
	// unlock.
	if err := func() error {
		fdb.updateLock.Lock()
		defer fdb.updateLock.Unlock()

		if err := garbageCollectOldDeletedLocked(ctx, fdb); err != nil {
			return wrap(err)
		}
		if err := garbageCollectNamesAndVersions(ctx, fdb); err != nil {
			return wrap(err)
		}
		if err := garbageCollectBlocklistsAndBlocksLocked(ctx, fdb, s.setChunkProgress); err != nil {
			return wrap(err)
		}
		return tidy(ctx, fdb.sql)
	}(); err != nil {
		return wrap(err)
	}

	// Update the successful GC sequence.
	return wrap(meta.PutInt64(lastSuccessfulGCSeqKey, seq))
}

func tidy(ctx context.Context, db *sqlx.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return wrap(err)
	}
	defer conn.Close()
	_, _ = conn.ExecContext(ctx, `ANALYZE`)
	_, _ = conn.ExecContext(ctx, `PRAGMA optimize`)
	_, _ = conn.ExecContext(ctx, `PRAGMA incremental_vacuum`)
	_, _ = conn.ExecContext(ctx, `PRAGMA journal_size_limit = 8388608`)
	_, _ = conn.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	return nil
}

func garbageCollectNamesAndVersions(ctx context.Context, fdb *folderDB) error {
	l := slog.With("folder", fdb.folderID, "fdb", fdb.baseName)

	res, err := fdb.stmt(`
		DELETE FROM file_names
		WHERE NOT EXISTS (SELECT 1 FROM files f WHERE f.name_idx = idx)
	`).Exec()
	if err != nil {
		return wrap(err, "delete names")
	}
	if aff, err := res.RowsAffected(); err == nil {
		l.DebugContext(ctx, "Removed old file names", "affected", aff)
	}

	res, err = fdb.stmt(`
		DELETE FROM file_versions
		WHERE NOT EXISTS (SELECT 1 FROM files f WHERE f.version_idx = idx)
	`).Exec()
	if err != nil {
		return wrap(err, "delete versions")
	}
	if aff, err := res.RowsAffected(); err == nil {
		l.DebugContext(ctx, "Removed old file versions", "affected", aff)
	}

	return nil
}

func garbageCollectOldDeletedLocked(ctx context.Context, fdb *folderDB) error {
	l := slog.With("folder", fdb.folderID, "fdb", fdb.baseName)
	if fdb.deleteRetention <= 0 {
		slog.DebugContext(ctx, "Delete retention is infinite, skipping cleanup")
		return nil
	}

	// Receive-encrypted files always have the timestamp below. We cannot
	// use this to conclude a deleted file is safe for garbage collection.
	recvEncModified := 1234567890 * time.Second

	// Remove deleted files that are marked as not needed (we have processed
	// them) and they were deleted more than MaxDeletedFileAge ago.
	// Keep the local high-water mark reachable by the index sender.
	l.DebugContext(ctx, "Forgetting deleted files", "retention", fdb.deleteRetention)
	res, err := fdb.stmt(`
		DELETE FROM files
		WHERE deleted
			AND modified != ?
			AND modified < ?
			AND local_flags & {{.FlagLocalNeeded}} == 0
			AND sequence NOT IN (
				SELECT sequence FROM indexids WHERE device_idx = {{.LocalDeviceIdx}}
			)
	`).Exec(recvEncModified, time.Now().Add(-fdb.deleteRetention).UnixNano())
	if err != nil {
		return wrap(err)
	}
	if aff, err := res.RowsAffected(); err == nil {
		l.DebugContext(ctx, "Removed old deleted file records", "affected", aff)
	}
	return nil
}

func garbageCollectBlocklistsAndBlocksLocked(ctx context.Context, fdb *folderDB, onProgress func(table string, chunksTotal, chunksDone int)) error {
	// Remove all blocklists not referred to by any files and, by extension,
	// any blocks not referred to by a blocklist. This is an expensive
	// operation when run normally, especially if there are a lot of blocks
	// to collect.
	//
	// We make this orders of magnitude faster by disabling foreign keys for
	// the transaction and doing the cleanup manually. This requires using
	// an explicit connection and disabling foreign keys before starting the
	// transaction. We make sure to clean up on the way out.

	conn, err := fdb.sql.Connx(ctx)
	if err != nil {
		return wrap(err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = 0`); err != nil {
		return wrap(err)
	}
	defer func() { //nolint:contextcheck
		_, _ = conn.ExecContext(context.Background(), `PRAGMA foreign_keys = 1`)
	}()

	tx, err := conn.BeginTxx(ctx, nil)
	if err != nil {
		return wrap(err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Both blocklists and blocks refer to blocklists_hash from the files table.
	for _, table := range []string{"blocklists", "blocks"} {
		// Count the number of rows
		var rows int64
		if err := tx.GetContext(ctx, &rows, `SELECT count(*) FROM `+table); err != nil {
			return wrap(err)
		}

		chunks := max(gcMinChunks, rows/gcChunkSize)
		l := slog.With("folder", fdb.folderID, "fdb", fdb.baseName, "table", table, "rows", rows, "chunks", chunks)
		if onProgress != nil {
			onProgress(table, int(chunks), 0)
		}

		// Process rows in chunks up to a given time limit. We always use at
		// least gcMinChunks chunks, then increase the number as the number of rows
		// exceeds gcMinChunks*gcChunkSize.
		t0 := time.Now()
		for i, br := range randomBlobRanges(int(chunks)) {
			// Stop at a chunk boundary when shutting down. The
			// transaction is rolled back by the deferred call above, so
			// no partial state is left behind; the GC sequence marker is
			// only advanced after this function returns successfully.
			select {
			case <-ctx.Done():
				l.InfoContext(ctx, "GC was interrupted at a chunk boundary", "processed", i)
				return ctx.Err()
			default:
			}

			if d := time.Since(t0); d > gcMaxRuntime {
				l.InfoContext(ctx, "GC was interrupted due to exceeding time limit", "processed", i, "runtime", time.Since(t0))
				break
			}

			// The limit column must be an indexed column with a mostly random distribution of blobs.
			// That's the blocklist_hash column for blocklists, and the hash column for blocks.
			limitColumn := table + ".blocklist_hash"
			if table == "blocks" {
				limitColumn = "blocks.hash"
			}

			q := fmt.Sprintf(`
				DELETE FROM %s
				WHERE %s AND NOT EXISTS (
					SELECT 1 FROM files WHERE files.blocklist_hash = %s.blocklist_hash
				)`, table, br.SQL(limitColumn), table)

			if res, err := tx.ExecContext(ctx, q); err != nil {
				return wrap(err, "delete from "+table)
			} else {
				l.DebugContext(ctx, "GC query result", "processed", i, "runtime", time.Since(t0), "result", slogutil.Expensive(func() any {
					rows, err := res.RowsAffected()
					if err != nil {
						return slogutil.Error(err)
					}
					return slog.Int64("rows", rows)
				}))
			}
			if onProgress != nil {
				onProgress(table, int(chunks), i+1)
			}
		}
	}

	return wrap(tx.Commit())
}

// blobRange defines a range for blob searching. A range is open ended if
// start or end is nil.
type blobRange struct {
	start, end []byte
}

// SQL returns the SQL where clause for the given range, e.g.
// `column >= x'49249248' AND column < x'6db6db6c'`
func (r blobRange) SQL(name string) string {
	var sb strings.Builder
	if r.start != nil {
		fmt.Fprintf(&sb, "%s >= x'%x'", name, r.start)
	}
	if r.start != nil && r.end != nil {
		sb.WriteString(" AND ")
	}
	if r.end != nil {
		fmt.Fprintf(&sb, "%s < x'%x'", name, r.end)
	}
	return sb.String()
}

// randomBlobRanges returns n blobRanges in random order
func randomBlobRanges(n int) []blobRange {
	ranges := blobRanges(n)
	rand.Shuffle(len(ranges), func(i, j int) { ranges[i], ranges[j] = ranges[j], ranges[i] })
	return ranges
}

// blobRanges returns n blobRanges
func blobRanges(n int) []blobRange {
	// We use three byte (24 bit) prefixes to get fairly granular ranges and easy bit
	// conversions.
	rangeSize := (1 << 24) / n
	ranges := make([]blobRange, 0, n)
	var prev []byte
	for i := range n {
		var pref []byte
		if i < n-1 {
			end := (i + 1) * rangeSize
			pref = intToBlob(end)
		}
		ranges = append(ranges, blobRange{prev, pref})
		prev = pref
	}
	return ranges
}

func intToBlob(n int) []byte {
	var pref [4]byte
	binary.BigEndian.PutUint32(pref[:], uint32(n)) //nolint:gosec
	// first byte is always zero and not part of the range
	return pref[1:]
}
