// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

func testRotationFolder() FolderConfiguration {
	return FolderConfiguration{
		ID:             "foo",
		Path:           "testdata",
		FilesystemType: FilesystemTypeBasic,
		Devices: []FolderDeviceConfiguration{
			{DeviceID: device1, EncryptionPassword: "oldpw"},
			{DeviceID: device2, EncryptionPassword: "oldpw"},
		},
		EncryptionPasswordRotation: &EncryptionPasswordRotation{
			NewPassword:    "newpw",
			StartedAt:      time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			RotatedDevices: []protocol.DeviceID{device2},
			OldDevices:     []protocol.DeviceID{device1},
		},
	}
}

func TestEncryptionPasswordRotationXMLRoundtrip(t *testing.T) {
	folder := testRotationFolder()

	bs, err := xml.MarshalIndent(folder, "", " ")
	if err != nil {
		t.Fatal(err)
	}

	var got FolderConfiguration
	if err := xml.Unmarshal(bs, &got); err != nil {
		t.Fatal(err)
	}

	rot := got.EncryptionPasswordRotation
	if rot == nil {
		t.Fatal("missing rotation after XML roundtrip")
	}
	if rot.NewPassword != "newpw" {
		t.Errorf("new password: %q", rot.NewPassword)
	}
	if !rot.StartedAt.Equal(folder.EncryptionPasswordRotation.StartedAt) {
		t.Errorf("started at: %v != %v", rot.StartedAt, folder.EncryptionPasswordRotation.StartedAt)
	}
	if got := rot.DeviceState(device1); got != EncryptionPasswordRotationStateOld {
		t.Errorf("device1 state: %q", got)
	}
	if got := rot.DeviceState(device2); got != EncryptionPasswordRotationStateNew {
		t.Errorf("device2 state: %q", got)
	}
	if got := rot.DeviceState(device3); got != EncryptionPasswordRotationStateUnseen {
		t.Errorf("device3 state: %q", got)
	}
}

func TestEncryptionPasswordRotationJSONRoundtrip(t *testing.T) {
	folder := testRotationFolder()

	bs, err := json.Marshal(folder)
	if err != nil {
		t.Fatal(err)
	}

	var got FolderConfiguration
	if err := json.Unmarshal(bs, &got); err != nil {
		t.Fatal(err)
	}

	if got.EncryptionPasswordRotation == nil {
		t.Fatal("missing rotation after JSON roundtrip")
	}
	if got.EncryptionPasswordRotation.NewPassword != "newpw" {
		t.Errorf("new password: %q", got.EncryptionPasswordRotation.NewPassword)
	}
	if state := got.EncryptionPasswordRotation.DeviceState(device2); state != EncryptionPasswordRotationStateNew {
		t.Errorf("device2 state: %q", state)
	}
}

func TestEncryptionPasswordRotationOmittedWhenAbsent(t *testing.T) {
	folder := FolderConfiguration{ID: "foo", Path: "testdata"}

	bs, err := xml.MarshalIndent(folder, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(bs, []byte("encryptionPasswordRotation")) {
		t.Errorf("rotation element should be omitted: %s", bs)
	}
}

func TestEncryptionPasswordRotationEmptyListsRoundtrip(t *testing.T) {
	// A freshly started rotation has empty device lists; those must survive
	// a marshal/unmarshal cycle as empty lists.
	folder := FolderConfiguration{
		ID:   "foo",
		Path: "testdata",
		EncryptionPasswordRotation: &EncryptionPasswordRotation{
			NewPassword: "newpw",
			StartedAt:   time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		},
	}

	bs, err := xml.MarshalIndent(folder, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	var got FolderConfiguration
	if err := xml.Unmarshal(bs, &got); err != nil {
		t.Fatal(err)
	}
	if got.EncryptionPasswordRotation == nil || got.EncryptionPasswordRotation.NewPassword != "newpw" {
		t.Fatalf("unexpected rotation after roundtrip: %+v", got.EncryptionPasswordRotation)
	}
	if len(got.EncryptionPasswordRotation.RotatedDevices) != 0 || len(got.EncryptionPasswordRotation.OldDevices) != 0 {
		t.Errorf("device lists should be empty, got %+v %+v", got.EncryptionPasswordRotation.RotatedDevices, got.EncryptionPasswordRotation.OldDevices)
	}
}

func TestFolderConfigurationCopyEncryptionPasswordRotation(t *testing.T) {
	folder := testRotationFolder()
	copy := folder.Copy()

	if copy.EncryptionPasswordRotation == folder.EncryptionPasswordRotation {
		t.Fatal("rotation pointer should be copied")
	}
	copy.EncryptionPasswordRotation.NewPassword = "different"
	copy.EncryptionPasswordRotation.RotatedDevices[0] = device1
	if folder.EncryptionPasswordRotation.NewPassword == "different" {
		t.Error("copy should not alias the new password")
	}
	if folder.EncryptionPasswordRotation.RotatedDevices[0] == device1 {
		t.Error("copy should not alias the rotated devices slice")
	}
}

func TestEncryptionPasswordRotationRecordDevice(t *testing.T) {
	rot := &EncryptionPasswordRotation{}

	if !rot.RecordDevice(device1, false) {
		t.Error("unseen -> old should be a change")
	}
	if rot.DeviceState(device1) != EncryptionPasswordRotationStateOld {
		t.Error("expected old")
	}
	if rot.RecordDevice(device1, false) {
		t.Error("old -> old should not be a change")
	}
	if !rot.RecordDevice(device1, true) {
		t.Error("old -> new should be a change")
	}
	if rot.DeviceState(device1) != EncryptionPasswordRotationStateNew {
		t.Error("expected new")
	}
	// A device seen with the old password after having used the new one
	// moves back (it blocks completion).
	if !rot.RecordDevice(device1, false) {
		t.Error("new -> old should be a change")
	}
	if rot.DeviceState(device1) != EncryptionPasswordRotationStateOld {
		t.Error("expected old after downgrade")
	}
}

func TestEncryptionPasswordRotationPreparePrunes(t *testing.T) {
	cfg := Configuration{
		Devices: []DeviceConfiguration{
			{DeviceID: device1},
			{DeviceID: device2},
		},
		Folders: []FolderConfiguration{testRotationFolder()},
	}
	cfg.Folders[0].Devices = cfg.Folders[0].Devices[:1] // drop device2
	cfg.prepare(device1)

	rot := cfg.Folders[0].EncryptionPasswordRotation
	if rot == nil {
		t.Fatal("rotation should remain as device1 still has a password")
	}
	if len(rot.RotatedDevices) != 0 {
		t.Errorf("rotated devices should be pruned: %v", rot.RotatedDevices)
	}
	if len(rot.OldDevices) != 1 || rot.OldDevices[0] != device1 {
		t.Errorf("old devices should contain device1 only: %v", rot.OldDevices)
	}
}

func TestEncryptionPasswordRotationPrepareDroppedWithoutPasswords(t *testing.T) {
	folder := testRotationFolder()
	for i := range folder.Devices {
		folder.Devices[i].EncryptionPassword = ""
	}
	cfg := Configuration{
		Devices: []DeviceConfiguration{
			{DeviceID: device1, Untrusted: true},
			{DeviceID: device2, Untrusted: true},
		},
		Folders: []FolderConfiguration{folder},
	}
	cfg.prepare(device1)

	if cfg.Folders[0].EncryptionPasswordRotation != nil {
		t.Error("rotation should be dropped when no device has a password")
	}
}

func TestEncryptionPasswordRotationPrepareLoadedFromXML(t *testing.T) {
	// Rotation state stored on disk survives a load (and the device lists
	// survive prepare).
	folder := testRotationFolder()
	cfg := Configuration{
		Devices: []DeviceConfiguration{
			{DeviceID: device1},
			{DeviceID: device2},
		},
		Folders: []FolderConfiguration{folder},
	}
	cfg.prepare(device1)

	wrapped := Wrap(t.TempDir()+"/config.xml", cfg, device1, events.NoopLogger)
	if err := wrapped.Save(); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := Load(wrapped.ConfigPath(), device1, events.NoopLogger)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := loaded.Folder("foo")
	if !ok {
		t.Fatal("folder missing after load")
	}
	rot := got.EncryptionPasswordRotation
	if rot == nil {
		t.Fatal("rotation missing after load")
	}
	if rot.NewPassword != "newpw" || rot.DeviceState(device2) != EncryptionPasswordRotationStateNew {
		t.Errorf("unexpected rotation after load: %+v", rot)
	}
}
