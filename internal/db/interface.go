// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package db

import (
	"iter"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/thejerf/suture/v4"
)

// MaintenanceTrigger indicates why a maintenance run was started.
type MaintenanceTrigger string

const (
	// MaintenanceTriggerPeriodic is a maintenance run started by the
	// periodic scheduler.
	MaintenanceTriggerPeriodic MaintenanceTrigger = "periodic"
	// MaintenanceTriggerManual is a maintenance run started on demand.
	MaintenanceTriggerManual MaintenanceTrigger = "manual"
)

// MaintenancePhase describes what a running maintenance is currently
// doing.
type MaintenancePhase string

const (
	// MaintenancePhaseTidy is the database level analyze/checkpoint step.
	MaintenancePhaseTidy MaintenancePhase = "tidy"
	// MaintenancePhaseGarbageCollect is the per-folder garbage collection.
	MaintenancePhaseGarbageCollect MaintenancePhase = "garbage-collect"
)

// MaintenanceProgress is the best-effort progress indication for a
// running maintenance. All values are optional and only meaningful while
// maintenance is running.
type MaintenanceProgress struct {
	// Phase is the currently executing maintenance phase.
	Phase MaintenancePhase `json:"phase"`
	// FoldersTotal is the total number of folders to maintain.
	FoldersTotal int `json:"foldersTotal"`
	// FoldersDone is the number of fully maintained folders so far.
	FoldersDone int `json:"foldersDone"`
	// Folder is the folder currently being maintained.
	Folder string `json:"folder"`
	// Table is the table currently being garbage collected, when
	// applicable.
	Table string `json:"table,omitempty"`
	// ChunksTotal is the total number of garbage collection chunks for
	// the current table, when applicable.
	ChunksTotal int `json:"chunksTotal,omitempty"`
	// ChunksDone is the number of garbage collection chunks processed so
	// far for the current table, when applicable.
	ChunksDone int `json:"chunksDone,omitempty"`
}

// MaintenanceStatus describes the current and previous state of database
// maintenance. It is not persisted; it reflects the current process.
type MaintenanceStatus struct {
	// Running is true while a maintenance run is in progress.
	Running bool `json:"running"`
	// Trigger is why the currently running maintenance was started. It is
	// empty when no maintenance is running.
	Trigger MaintenanceTrigger `json:"trigger,omitempty"`
	// Started is the time the currently running maintenance started. It
	// is the zero time when no maintenance is running.
	Started time.Time `json:"started"`
	// LastCompleted is the time the most recent maintenance run
	// completed, regardless of whether it succeeded. It is the zero time
	// if no run has completed in this process.
	LastCompleted time.Time `json:"lastCompleted"`
	// LastError is the error from the most recent maintenance run, or
	// null if the most recent run succeeded or no run has failed.
	LastError *string `json:"lastError"`
	// LastDuration is the duration of the most recent maintenance run.
	LastDuration time.Duration `json:"lastDuration"`
	// Progress is the progress of the currently running maintenance.
	Progress MaintenanceProgress `json:"progress"`
}

// MaintenanceService provides on-demand control and read-only status of
// database maintenance.
type MaintenanceService interface {
	// StartMaintenance triggers a maintenance run. If maintenance is
	// already running it does nothing besides returning the status of
	// the running maintenance; a second run is never started
	// concurrently. The returned status reflects the state at the time
	// of the call.
	StartMaintenance() MaintenanceStatus

	// MaintenanceStatus returns the current maintenance status.
	MaintenanceStatus() MaintenanceStatus
}

type DBService interface {
	suture.Service
	MaintenanceService

	// Returns the last time database maintenance completed
	// successfully. This will return time zero when database maintenance
	// has never completed successfully.
	LastMaintenanceTime() time.Time
}

// UpdateOption modifies the behavior of a DB Update call.
type UpdateOption func(*UpdateOptions)

// UpdateOptions holds options for a DB Update call.
type UpdateOptions struct {
	SkipBlockIndex bool
}

// WithSkipBlockIndex skips inserting individual blocks into the block
// index (the "blocks" table). Blocklists are still stored.
func WithSkipBlockIndex() UpdateOption {
	return func(o *UpdateOptions) {
		o.SkipBlockIndex = true
	}
}

type DB interface {
	// Create a service that performs database maintenance periodically (no
	// more often than the requested interval)
	Service(maintenanceInterval time.Duration, evLogger events.Logger) DBService

	// Basics
	Update(folder string, device protocol.DeviceID, fs []protocol.FileInfo, opts ...UpdateOption) error
	Close() error

	// Single files
	GetDeviceFile(folder string, device protocol.DeviceID, file string) (protocol.FileInfo, bool, error)
	GetGlobalAvailability(folder, file string) ([]protocol.DeviceID, error)
	GetGlobalFile(folder string, file string) (protocol.FileInfo, bool, error)

	// File iterators
	//
	// n.b. there is a slight inconsistency in the return types where some
	// return a FileInfo iterator and some a FileMetadata iterator. The
	// latter is more lightweight, and the discrepancy depends on how the
	// functions tend to be used. We can introduce more variations as
	// required.
	AllGlobalFiles(folder string) (iter.Seq[FileMetadata], func() error)
	AllGlobalFilesPrefix(folder string, prefix string) (iter.Seq[FileMetadata], func() error)
	AllLocalFiles(folder string, device protocol.DeviceID) (iter.Seq[protocol.FileInfo], func() error)
	AllLocalFilesBySequence(folder string, device protocol.DeviceID, startSeq int64, limit int) (iter.Seq[protocol.FileInfo], func() error)
	AllLocalFilesWithPrefix(folder string, device protocol.DeviceID, prefix string) (iter.Seq[protocol.FileInfo], func() error)
	AllLocalFilesWithBlocksHash(folder string, h []byte) (iter.Seq[FileMetadata], func() error)
	AllNeededGlobalFiles(folder string, device protocol.DeviceID, order config.PullOrder, limit, offset int) (iter.Seq[protocol.FileInfo], func() error)
	AllLocalBlocksWithHash(folder string, hash []byte) (iter.Seq[BlockMapEntry], func() error)

	// Block index management
	DropBlockIndex(folder string) error
	PopulateBlockIndex(folder string) error

	// Cleanup
	DropAllFiles(folder string, device protocol.DeviceID) error
	DropFolderDevice(folder string, device protocol.DeviceID) error
	DropDevice(device protocol.DeviceID) error
	DropFilesNamed(folder string, device protocol.DeviceID, names []string) error
	DropFolder(folder string) error

	// Various metadata
	GetDeviceSequence(folder string, device protocol.DeviceID) (int64, error)
	ListFolders() ([]string, error)
	ListDevicesForFolder(folder string) ([]protocol.DeviceID, error)
	RemoteSequences(folder string) (map[protocol.DeviceID]int64, error)

	// Counts
	CountGlobal(folder string) (Counts, error)
	CountLocal(folder string, device protocol.DeviceID) (Counts, error)
	CountNeed(folder string, device protocol.DeviceID) (Counts, error)
	CountReceiveOnlyChanged(folder string) (Counts, error)

	// Index IDs
	DropAllIndexIDs() error
	GetIndexID(folder string, device protocol.DeviceID) (protocol.IndexID, error)
	SetIndexID(folder string, device protocol.DeviceID, id protocol.IndexID) error

	// MtimeFS
	DeleteMtime(folder, name string) error
	GetMtime(folder, name string) (ondisk, virtual time.Time)
	PutMtime(folder, name string, ondisk, virtual time.Time) error

	KV
}

// Generic KV store
type KV interface {
	GetKV(key string) ([]byte, error)
	PutKV(key string, val []byte) error
	DeleteKV(key string) error
	PrefixKV(prefix string) (iter.Seq[KeyValue], func() error)
}

type BlockMapEntry struct {
	BlocklistHash []byte
	Offset        int64
	BlockIndex    int
	Size          int
	FileName      string
}

type KeyValue struct {
	Key   string
	Value []byte
}

type FileMetadata struct {
	Name       string
	Sequence   int64
	ModNanos   int64
	Size       int64
	LocalFlags protocol.FlagLocal
	Type       protocol.FileInfoType
	Deleted    bool
}

func (f *FileMetadata) ModTime() time.Time {
	return time.Unix(0, f.ModNanos)
}

func (f *FileMetadata) IsReceiveOnlyChanged() bool {
	return f.LocalFlags&protocol.FlagLocalReceiveOnly != 0
}

func (f *FileMetadata) IsDirectory() bool {
	return f.Type == protocol.FileInfoTypeDirectory
}

func (f *FileMetadata) ShouldConflict() bool {
	return f.LocalFlags&protocol.LocalConflictFlags != 0
}

func (f *FileMetadata) IsInvalid() bool {
	return f.LocalFlags.IsInvalid()
}
