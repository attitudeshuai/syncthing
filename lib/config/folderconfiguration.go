// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/disk"

	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/structutil"
)

var (
	ErrPathNotDirectory = errors.New("folder path not a directory")
	ErrPathMissing      = errors.New("folder path missing")
	ErrMarkerMissing    = errors.New("folder marker missing (this indicates potential data loss, search docs/forum to get information about how to proceed)")
)

const (
	DefaultMarkerName          = ".stfolder"
	EncryptionTokenName        = "syncthing-encryption_password_token" //nolint: gosec
	maxConcurrentWritesDefault = 16
	maxConcurrentWritesLimit   = 256
)

type FolderDeviceConfiguration struct {
	DeviceID           protocol.DeviceID `json:"deviceID" xml:"id,attr"`
	IntroducedBy       protocol.DeviceID `json:"introducedBy" xml:"introducedBy,attr"`
	EncryptionPassword string            `json:"encryptionPassword" xml:"encryptionPassword"`
}

// EncryptionPasswordRotationState describes whether a peer has been observed
// using the current (old) or the pending (new) encryption password token
// during a password rotation transition.
type EncryptionPasswordRotationState string

const (
	// EncryptionPasswordRotationStateUnseen means the device has not
	// connected (with a verifiable token) since the rotation started.
	EncryptionPasswordRotationStateUnseen EncryptionPasswordRotationState = "unseen"
	// EncryptionPasswordRotationStateOld means the device has connected
	// with a token derived from the current (old) password.
	EncryptionPasswordRotationStateOld EncryptionPasswordRotationState = "old"
	// EncryptionPasswordRotationStateNew means the device has connected
	// with a token derived from the pending (new) password.
	EncryptionPasswordRotationStateNew EncryptionPasswordRotationState = "new"
)

// EncryptionPasswordRotation tracks an in-progress rolling rotation of the
// encryption password of a folder shared with untrusted (receive-encrypted)
// devices. While set, both the password configured on the folder devices and
// NewPassword are accepted on incoming connections. The lists record which
// devices have been observed using which password, so that the rotation can
// be completed only once every paired device has presented a token derived
// from the new password.
type EncryptionPasswordRotation struct {
	// NewPassword is the password being rotated to. It is not in effect
	// until the rotation is completed.
	NewPassword string `json:"newPassword" xml:"newPassword"`
	// StartedAt is the time at which the rotation was initiated.
	StartedAt time.Time `json:"startedAt" xml:"startedAt"`
	// RotatedDevices are the devices that have connected with a token
	// derived from NewPassword.
	RotatedDevices []protocol.DeviceID `json:"rotatedDevices" xml:"rotatedDevices>device,omitempty"`
	// OldDevices are the devices that have connected with a token derived
	// from the current (old) password since the rotation started.
	OldDevices []protocol.DeviceID `json:"oldDevices" xml:"oldDevices>device,omitempty"`
}

// Copy returns a deep copy of the rotation state.
func (r *EncryptionPasswordRotation) Copy() *EncryptionPasswordRotation {
	if r == nil {
		return nil
	}
	c := &EncryptionPasswordRotation{
		NewPassword: r.NewPassword,
		StartedAt:   r.StartedAt,
	}
	c.RotatedDevices = append([]protocol.DeviceID(nil), r.RotatedDevices...)
	c.OldDevices = append([]protocol.DeviceID(nil), r.OldDevices...)
	return c
}

// DeviceState returns the rotation state recorded for the given device.
func (r *EncryptionPasswordRotation) DeviceState(device protocol.DeviceID) EncryptionPasswordRotationState {
	if slices.Contains(r.RotatedDevices, device) {
		return EncryptionPasswordRotationStateNew
	}
	if slices.Contains(r.OldDevices, device) {
		return EncryptionPasswordRotationStateOld
	}
	return EncryptionPasswordRotationStateUnseen
}

// RecordDevice records that the given device was last observed using the new
// (useNew is true) or the old (useNew is false) password token. It returns
// true when the recorded state for the device changed.
func (r *EncryptionPasswordRotation) RecordDevice(device protocol.DeviceID, useNew bool) bool {
	target := &r.OldDevices
	other := &r.RotatedDevices
	if useNew {
		target, other = &r.RotatedDevices, &r.OldDevices
	}

	changed := false
	if i := slices.Index(*other, device); i >= 0 {
		*other = slices.Delete(*other, i, i+1)
		changed = true
	}
	if !slices.Contains(*target, device) {
		*target = append(*target, device)
		changed = true
	}
	return changed
}

// PruneDevices removes devices that are not in the valid set, sorts the
// remaining device lists and drops duplicates.
func (r *EncryptionPasswordRotation) PruneDevices(valid map[protocol.DeviceID]struct{}) {
	prune := func(devices []protocol.DeviceID) []protocol.DeviceID {
		out := devices[:0]
		for _, dev := range devices {
			if _, ok := valid[dev]; ok && !slices.Contains(out, dev) {
				out = append(out, dev)
			}
		}
		slices.SortFunc(out, func(a, b protocol.DeviceID) int { return a.Compare(b) })
		return out
	}
	r.RotatedDevices = prune(r.RotatedDevices)
	r.OldDevices = prune(r.OldDevices)
}

type FolderConfiguration struct {
	ID                         string                      `json:"id" xml:"id,attr" nodefault:"true"`
	Label                      string                      `json:"label" xml:"label,attr" restart:"false"`
	FilesystemType             FilesystemType              `json:"filesystemType" xml:"filesystemType" default:"basic"`
	Path                       string                      `json:"path" xml:"path,attr"`
	Type                       FolderType                  `json:"type" xml:"type,attr"`
	Devices                    []FolderDeviceConfiguration `json:"devices" xml:"device"`
	Group                      string                      `json:"group" xml:"group,attr,omitempty" restart:"false"`
	RescanIntervalS            int                         `json:"rescanIntervalS" xml:"rescanIntervalS,attr" default:"3600"`
	FSWatcherEnabled           bool                        `json:"fsWatcherEnabled" xml:"fsWatcherEnabled,attr" default:"true"`
	FSWatcherDelayS            float64                     `json:"fsWatcherDelayS" xml:"fsWatcherDelayS,attr" default:"10"`
	FSWatcherTimeoutS          float64                     `json:"fsWatcherTimeoutS" xml:"fsWatcherTimeoutS,attr"`
	IgnorePerms                bool                        `json:"ignorePerms" xml:"ignorePerms,attr"`
	AutoNormalize              bool                        `json:"autoNormalize" xml:"autoNormalize,attr" default:"true"`
	MinDiskFree                Size                        `json:"minDiskFree" xml:"minDiskFree" default:"1 %"`
	Versioning                 VersioningConfiguration     `json:"versioning" xml:"versioning"`
	Copiers                    int                         `json:"copiers" xml:"copiers"`
	PullerMaxPendingKiB        int                         `json:"pullerMaxPendingKiB" xml:"pullerMaxPendingKiB"`
	Hashers                    int                         `json:"hashers" xml:"hashers"`
	Order                      PullOrder                   `json:"order" xml:"order"`
	IgnoreDelete               bool                        `json:"ignoreDelete" xml:"ignoreDelete"`
	ScanProgressIntervalS      int                         `json:"scanProgressIntervalS" xml:"scanProgressIntervalS"`
	PullerPauseS               int                         `json:"pullerPauseS" xml:"pullerPauseS"`
	PullerDelayS               float64                     `json:"pullerDelayS" xml:"pullerDelayS" default:"1"`
	MaxConflicts               int                         `json:"maxConflicts" xml:"maxConflicts" default:"10"`
	DisableSparseFiles         bool                        `json:"disableSparseFiles" xml:"disableSparseFiles"`
	Paused                     bool                        `json:"paused" xml:"paused"`
	MarkerName                 string                      `json:"markerName" xml:"markerName"`
	CopyOwnershipFromParent    bool                        `json:"copyOwnershipFromParent" xml:"copyOwnershipFromParent"`
	RawModTimeWindowS          int                         `json:"modTimeWindowS" xml:"modTimeWindowS"`
	MaxConcurrentWrites        int                         `json:"maxConcurrentWrites" xml:"maxConcurrentWrites" default:"0"`
	DisableFsync               bool                        `json:"disableFsync" xml:"disableFsync"`
	BlockPullOrder             BlockPullOrder              `json:"blockPullOrder" xml:"blockPullOrder"`
	CopyRangeMethod            CopyRangeMethod             `json:"copyRangeMethod" xml:"copyRangeMethod" default:"standard"`
	CaseSensitiveFS            bool                        `json:"caseSensitiveFS" xml:"caseSensitiveFS"`
	JunctionsAsDirs            bool                        `json:"junctionsAsDirs" xml:"junctionsAsDirs"`
	SyncOwnership              bool                        `json:"syncOwnership" xml:"syncOwnership"`
	SendOwnership              bool                        `json:"sendOwnership" xml:"sendOwnership"`
	SyncXattrs                 bool                        `json:"syncXattrs" xml:"syncXattrs"`
	SendXattrs                 bool                        `json:"sendXattrs" xml:"sendXattrs"`
	BlockIndexing              bool                        `json:"blockIndexing" xml:"blockIndexing" default:"true"`
	XattrFilter                XattrFilter                 `json:"xattrFilter" xml:"xattrFilter"`
	EncryptionPasswordRotation *EncryptionPasswordRotation `json:"encryptionPasswordRotation,omitempty" xml:"encryptionPasswordRotation,omitempty" restart:"false" nodefault:"true"`
	// Legacy deprecated
	DeprecatedReadOnly       bool    `json:"-" xml:"ro,attr,omitempty"`        // Deprecated: Do not use.
	DeprecatedMinDiskFreePct float64 `json:"-" xml:"minDiskFreePct,omitempty"` // Deprecated: Do not use.
	DeprecatedPullers        int     `json:"-" xml:"pullers,omitempty"`        // Deprecated: Do not use.
	DeprecatedScanOwnership  bool    `json:"-" xml:"scanOwnership,omitempty"`  // Deprecated: Do not use.
}

// Extended attribute filter. This is a list of patterns to match (glob
// style), each with an action (permit or deny). First match is used. If the
// filter is empty, all strings are permitted. If the filter is non-empty,
// the default action becomes deny. To counter this, you can use the "*"
// pattern to match all strings at the end of the filter. There are also
// limits on the size of accepted attributes.
type XattrFilter struct {
	Entries            []XattrFilterEntry `json:"entries" xml:"entry"`
	MaxSingleEntrySize int                `json:"maxSingleEntrySize" xml:"maxSingleEntrySize" default:"1024"`
	MaxTotalSize       int                `json:"maxTotalSize" xml:"maxTotalSize" default:"4096"`
}

type XattrFilterEntry struct {
	Match  string `json:"match" xml:"match,attr"`
	Permit bool   `json:"permit" xml:"permit,attr"`
}

func (f FolderConfiguration) Copy() FolderConfiguration {
	c := f
	c.Devices = make([]FolderDeviceConfiguration, len(f.Devices))
	copy(c.Devices, f.Devices)
	c.Versioning = f.Versioning.Copy()
	c.EncryptionPasswordRotation = f.EncryptionPasswordRotation.Copy()
	return c
}

// Filesystem creates a filesystem for the path and options of this folder.
// The fset parameter may be nil, in which case no mtime handling on top of
// the filesystem is provided.
func (f FolderConfiguration) Filesystem(extraOpts ...fs.Option) fs.Filesystem {
	// This is intentionally not a pointer method, because things like
	// cfg.Folders["default"].Filesystem(nil) should be valid.
	var opts []fs.Option
	if f.FilesystemType == FilesystemTypeBasic && f.JunctionsAsDirs {
		opts = append(opts, new(fs.OptionJunctionsAsDirs))
	}
	if !f.CaseSensitiveFS {
		opts = append(opts, new(fs.OptionDetectCaseConflicts))
	}
	opts = append(opts, extraOpts...)
	return fs.NewFilesystem(f.FilesystemType.ToFS(), f.Path, opts...)
}

func (f FolderConfiguration) ModTimeWindow() time.Duration {
	dur := time.Duration(f.RawModTimeWindowS) * time.Second
	if f.RawModTimeWindowS < 1 && build.IsAndroid {
		if usage, err := disk.Usage(f.Filesystem().URI()); err != nil {
			dur = 2 * time.Second
			l.Debugf(`Detecting FS at "%v" on android: Setting mtime window to 2s: err == "%v"`, f.Path, err)
		} else if strings.HasPrefix(strings.ToLower(usage.Fstype), "ext2") || strings.HasPrefix(strings.ToLower(usage.Fstype), "ext3") || strings.HasPrefix(strings.ToLower(usage.Fstype), "ext4") {
			l.Debugf(`Detecting FS at %v on android: Leaving mtime window at 0: usage.Fstype == "%v"`, f.Path, usage.Fstype)
		} else {
			dur = 2 * time.Second
			l.Debugf(`Detecting FS at "%v" on android: Setting mtime window to 2s: usage.Fstype == "%v"`, f.Path, usage.Fstype)
		}
	}
	return dur
}

func (f *FolderConfiguration) CreateMarker() error {
	if err := f.CheckPath(); !errors.Is(err, ErrMarkerMissing) {
		return err
	}
	if f.MarkerName != DefaultMarkerName {
		// Folder uses a non-default marker so we shouldn't mess with it.
		// Pretend we created it and let the subsequent health checks sort
		// out the actual situation.
		return nil
	}

	ffs := f.Filesystem()

	// Create the marker as a directory
	err := ffs.Mkdir(DefaultMarkerName, fs.ModePerm)
	if err != nil {
		return err
	}

	// Create a file inside it, reducing the risk of the marker directory
	// being removed by automated cleanup tools.
	markerFile := filepath.Join(DefaultMarkerName, f.markerFilename())
	if err := fs.WriteFile(ffs, markerFile, f.markerContents(), 0o666); err != nil {
		return err
	}

	// Sync & hide the containing directory
	if dir, err := ffs.Open("."); err != nil {
		l.Debugln("folder marker: open . failed:", err)
	} else if err := dir.Sync(); err != nil {
		l.Debugln("folder marker: fsync . failed:", err)
	}
	ffs.Hide(DefaultMarkerName)

	return nil
}

func (f *FolderConfiguration) RemoveMarker() error {
	ffs := f.Filesystem()
	_ = ffs.Remove(filepath.Join(DefaultMarkerName, f.markerFilename()))
	return ffs.Remove(DefaultMarkerName)
}

func (f *FolderConfiguration) markerFilename() string {
	h := sha256.Sum256([]byte(f.ID))
	return fmt.Sprintf("syncthing-folder-%x.txt", h[:3])
}

func (f *FolderConfiguration) markerContents() []byte {
	var buf bytes.Buffer
	buf.WriteString("# This directory is a Syncthing folder marker.\n# Do not delete.\n\n")
	fmt.Fprintf(&buf, "folderID: %s\n", f.ID)
	fmt.Fprintf(&buf, "created: %s\n", time.Now().Format(time.RFC3339))
	return buf.Bytes()
}

// CheckPath returns nil if the folder root exists and contains the marker file
func (f *FolderConfiguration) CheckPath() error {
	return f.checkFilesystemPath(f.Filesystem(), ".")
}

func (f *FolderConfiguration) checkFilesystemPath(ffs fs.Filesystem, path string) error {
	fi, err := ffs.Stat(path)
	if err != nil {
		if !fs.IsNotExist(err) {
			return err
		}
		return ErrPathMissing
	}

	// Users might have the root directory as a symlink or reparse point.
	// Furthermore, OneDrive bullcrap uses a magic reparse point to the cloudz...
	// Yet it's impossible for this to happen, as filesystem adds a trailing
	// path separator to the root, so even if you point the filesystem at a file
	// Stat ends up calling stat on C:\dir\file\ which, fails with "is not a directory"
	// in the error check above, and we don't even get to here.
	if !fi.IsDir() && !fi.IsSymlink() {
		return ErrPathNotDirectory
	}

	_, err = ffs.Stat(filepath.Join(path, f.MarkerName))
	if err != nil {
		if !fs.IsNotExist(err) {
			return err
		}
		return ErrMarkerMissing
	}

	return nil
}

func (f *FolderConfiguration) CreateRoot() (err error) {
	filesystem := f.Filesystem()

	if _, err = filesystem.Stat("."); fs.IsNotExist(err) {
		err = filesystem.MkdirAll(".", fs.ModePerm)
	}

	return err
}

func (f FolderConfiguration) Description() string {
	if f.Label == "" {
		return f.ID
	}
	return fmt.Sprintf("%q (%s)", f.Label, f.ID)
}

func (f FolderConfiguration) LogAttr() slog.Attr {
	if f.Label == "" || f.Label == f.ID {
		return slog.Group("folder", slog.String("id", f.ID), slog.String("type", f.Type.String()))
	}
	return slog.Group("folder", slog.String("label", f.Label), slog.String("id", f.ID), slog.String("type", f.Type.String()))
}

func (f *FolderConfiguration) DeviceIDs() []protocol.DeviceID {
	deviceIDs := make([]protocol.DeviceID, len(f.Devices))
	for i, n := range f.Devices {
		deviceIDs[i] = n.DeviceID
	}
	return deviceIDs
}

// EncryptionPasswordDevices returns the per-device entries that have an
// encryption password set, i.e. the untrusted devices the folder data is
// encrypted for.
func (f FolderConfiguration) EncryptionPasswordDevices() []FolderDeviceConfiguration {
	var devices []FolderDeviceConfiguration
	for _, dev := range f.Devices {
		if dev.EncryptionPassword != "" {
			devices = append(devices, dev)
		}
	}
	return devices
}

func (f *FolderConfiguration) prepare(myID protocol.DeviceID, existingDevices map[protocol.DeviceID]*DeviceConfiguration) {
	// Ensure that
	// - any loose devices are not present in the wrong places
	// - there are no duplicate devices
	// - we are part of the devices
	// - folder is not shared in trusted mode with an untrusted device
	f.Devices = ensureExistingDevices(f.Devices, existingDevices)
	f.Devices = ensureNoDuplicateFolderDevices(f.Devices)
	f.Devices = ensureDevicePresent(f.Devices, myID)
	f.Devices = ensureNoUntrustedTrustingSharing(f, f.Devices, existingDevices)

	slices.SortFunc(f.Devices, func(a, b FolderDeviceConfiguration) int {
		return a.DeviceID.Compare(b.DeviceID)
	})

	if f.RescanIntervalS > MaxRescanIntervalS {
		f.RescanIntervalS = MaxRescanIntervalS
	} else if f.RescanIntervalS < 0 {
		f.RescanIntervalS = 0
	}

	if f.FSWatcherDelayS <= 0 {
		f.FSWatcherEnabled = false
		f.FSWatcherDelayS = 10
	} else if f.FSWatcherDelayS < 0.01 {
		f.FSWatcherDelayS = 0.01
	}

	if f.Versioning.CleanupIntervalS > MaxRescanIntervalS {
		f.Versioning.CleanupIntervalS = MaxRescanIntervalS
	} else if f.Versioning.CleanupIntervalS < 0 {
		f.Versioning.CleanupIntervalS = 0
	}

	if f.MarkerName == "" {
		f.MarkerName = DefaultMarkerName
	}

	if f.MaxConcurrentWrites <= 0 {
		f.MaxConcurrentWrites = maxConcurrentWritesDefault
	} else if f.MaxConcurrentWrites > maxConcurrentWritesLimit {
		f.MaxConcurrentWrites = maxConcurrentWritesLimit
	}

	if f.Type == FolderTypeReceiveEncrypted {
		f.IgnorePerms = true
	}

	if f.EncryptionPasswordRotation != nil {
		// Keep the rotation bookkeeping consistent with the devices the
		// folder is currently encrypted for. A rotation without any such
		// devices is meaningless and gets dropped.
		valid := make(map[protocol.DeviceID]struct{})
		for _, dev := range f.EncryptionPasswordDevices() {
			valid[dev.DeviceID] = struct{}{}
		}
		if len(valid) == 0 {
			f.EncryptionPasswordRotation = nil
		} else {
			f.EncryptionPasswordRotation.PruneDevices(valid)
		}
	}
}

// RequiresRestartOnly returns a copy with only the attributes that require
// restart on change.
func (f FolderConfiguration) RequiresRestartOnly() FolderConfiguration {
	copy := f

	// Manual handling for things that are not taken care of by the tag
	// copier, yet should not cause a restart.

	blank := FolderConfiguration{}
	copyMatchingTag(&blank, &copy, "restart", func(v string) bool {
		if len(v) > 0 && v != "false" {
			panic(fmt.Sprintf(`unexpected tag value: %s. expected untagged or "false"`, v))
		}
		return v == "false"
	})
	return copy
}

func (f *FolderConfiguration) Device(device protocol.DeviceID) (FolderDeviceConfiguration, bool) {
	for _, dev := range f.Devices {
		if dev.DeviceID == device {
			return dev, true
		}
	}
	return FolderDeviceConfiguration{}, false
}

func (f *FolderConfiguration) SharedWith(device protocol.DeviceID) bool {
	_, ok := f.Device(device)
	return ok
}

func (f *FolderConfiguration) CheckAvailableSpace(req uint64) error {
	val := f.MinDiskFree.BaseValue()
	if val <= 0 {
		return nil
	}
	fs := f.Filesystem()
	usage, err := fs.Usage(".")
	if err != nil {
		return nil //nolint: nilerr
	}
	if err := checkAvailableSpace(req, f.MinDiskFree, usage); err != nil {
		return fmt.Errorf("insufficient space in folder %v (%v): %w", f.Description(), fs.URI(), err)
	}
	return nil
}

func (f XattrFilter) Permit(s string) bool {
	if len(f.Entries) == 0 {
		return true
	}

	for _, entry := range f.Entries {
		if ok, _ := path.Match(entry.Match, s); ok {
			return entry.Permit
		}
	}
	return false
}

func (f XattrFilter) GetMaxSingleEntrySize() int {
	return f.MaxSingleEntrySize
}

func (f XattrFilter) GetMaxTotalSize() int {
	return f.MaxTotalSize
}

func (f *FolderConfiguration) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	structutil.SetDefaults(f)

	// avoid recursing into this method
	type noCustomUnmarshal FolderConfiguration
	ptr := (*noCustomUnmarshal)(f)

	return d.DecodeElement(ptr, &start)
}
