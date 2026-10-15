// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

const (
	rotTestOldPassword = "oldpw"
	rotTestNewPassword = "newpw"
	rotTestFolderID    = "default"
)

func setupEncryptionRotationModel(t *testing.T) *testModel {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping on short testing - generating encryption tokens is slow")
	}
	w, _ := newDefaultCfgWrapper(t)
	m := setupModel(t, w)
	m.cancel()
	t.Cleanup(func() { cleanupModel(m) })

	setFolderEncryptionPasswords(t, w, map[protocol.DeviceID]string{
		device1: rotTestOldPassword,
	})
	return m
}

func setFolderEncryptionPasswords(t *testing.T, w config.Wrapper, passwords map[protocol.DeviceID]string) {
	t.Helper()
	wtr, err := w.Modify(func(cfg *config.Configuration) {
		fcfg, idx, ok := cfg.Folder(rotTestFolderID)
		if !ok {
			t.Error("folder missing")
			return
		}
		for devID, pw := range passwords {
			if !fcfg.SharedWith(devID) {
				devCfg := cfg.Defaults.Device.Copy()
				devCfg.DeviceID = devID
				cfg.SetDevice(devCfg)
				fcfg.Devices = append(fcfg.Devices, config.FolderDeviceConfiguration{DeviceID: devID})
			}
			for i := range fcfg.Devices {
				if fcfg.Devices[i].DeviceID == devID {
					fcfg.Devices[i].EncryptionPassword = pw
				}
			}
		}
		cfg.Folders[idx] = fcfg
	})
	if err != nil {
		t.Fatal(err)
	}
	wtr.Wait()
}

// checkEncryptionToken presents the given token as the remote device's token
// in a cluster config check and returns the resulting error.
func checkEncryptionToken(t *testing.T, m *model, devID protocol.DeviceID, password string, token []byte) error {
	t.Helper()
	fcfg, ok := m.cfg.Folder(rotTestFolderID)
	if !ok {
		t.Fatal("folder missing")
	}
	dev := config.FolderDeviceConfiguration{DeviceID: devID, EncryptionPassword: password}
	info := &clusterConfigDeviceInfo{
		remote: protocol.Device{ID: devID, EncryptionPasswordToken: token},
		local:  protocol.Device{ID: m.id},
	}
	return m.ccCheckEncryption(fcfg, dev, info, false)
}

func rotationDeviceStatus(status EncryptionPasswordRotationStatus, dev protocol.DeviceID) config.EncryptionPasswordRotationState {
	for _, d := range status.Devices {
		if d.DeviceID == dev {
			return d.State
		}
	}
	return ""
}

func ccTokenForDevice(t *testing.T, cc *protocol.ClusterConfig, dev protocol.DeviceID) []byte {
	t.Helper()
	for _, folder := range cc.Folders {
		if folder.ID != rotTestFolderID {
			continue
		}
		for _, d := range folder.Devices {
			if d.ID == dev {
				return d.EncryptionPasswordToken
			}
		}
	}
	t.Fatalf("device %v missing in cluster config folder", dev)
	return nil
}

func TestEncryptionPasswordRotationLifecycle(t *testing.T) {
	m := setupEncryptionRotationModel(t)

	// Rotation can't be started on a folder without encryption passwords.
	w0, _ := newDefaultCfgWrapper(t)
	mm := setupModel(t, w0)
	mm.cancel()
	t.Cleanup(func() { cleanupModel(mm) })
	if err := mm.StartEncryptionPasswordRotation(rotTestFolderID, "x"); !errors.Is(err, ErrEncryptionPasswordRotationNotEncrypted) {
		t.Fatalf("expected ErrEncryptionPasswordRotationNotEncrypted, got %v", err)
	}

	// Start the rotation with precondition checks.
	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, ""); !errors.Is(err, ErrEncryptionPasswordRotationEmptyPassword) {
		t.Fatalf("expected empty password error, got %v", err)
	}
	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, rotTestOldPassword); !errors.Is(err, ErrEncryptionPasswordRotationSamePassword) {
		t.Fatalf("expected same password error, got %v", err)
	}
	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, rotTestNewPassword); err != nil {
		t.Fatal(err)
	}
	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, "third"); !errors.Is(err, ErrEncryptionPasswordRotationActive) {
		t.Fatalf("expected active rotation error, got %v", err)
	}

	// Initially the paired device has not connected during the transition.
	status, err := m.EncryptionPasswordRotationStatus(rotTestFolderID)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Active {
		t.Fatal("rotation should be active")
	}
	if state := rotationDeviceStatus(status, device1); state != config.EncryptionPasswordRotationStateUnseen {
		t.Fatalf("expected unseen, got %q", state)
	}

	oldToken := protocol.PasswordToken(m.keyGen, rotTestFolderID, rotTestOldPassword)
	newToken := protocol.PasswordToken(m.keyGen, rotTestFolderID, rotTestNewPassword)

	// An old password token is accepted and accounted for.
	if err := checkEncryptionToken(t, m.model, device1, rotTestOldPassword, oldToken); err != nil {
		t.Fatalf("old token should be accepted: %v", err)
	}
	status, _ = m.EncryptionPasswordRotationStatus(rotTestFolderID)
	if state := rotationDeviceStatus(status, device1); state != config.EncryptionPasswordRotationStateOld {
		t.Fatalf("expected old, got %q", state)
	}

	// A new password token is accepted and moves the device to rotated.
	if err := checkEncryptionToken(t, m.model, device1, rotTestOldPassword, newToken); err != nil {
		t.Fatalf("new token should be accepted: %v", err)
	}
	status, _ = m.EncryptionPasswordRotationStatus(rotTestFolderID)
	if state := rotationDeviceStatus(status, device1); state != config.EncryptionPasswordRotationStateNew {
		t.Fatalf("expected new, got %q", state)
	}

	// The only device has rotated, so completion succeeds.
	if err := m.CompleteEncryptionPasswordRotation(rotTestFolderID); err != nil {
		t.Fatal(err)
	}
	fcfg, _ := m.cfg.Folder(rotTestFolderID)
	if fcfg.EncryptionPasswordRotation != nil {
		t.Fatal("rotation should be cleared after completion")
	}
	dev, _ := fcfg.Device(device1)
	if dev.EncryptionPassword != rotTestNewPassword {
		t.Errorf("password should be %q, got %q", rotTestNewPassword, dev.EncryptionPassword)
	}

	// After completion the old token is rejected with a distinct error, an
	// unrelated token gets the generic mismatch, and the new token works.
	if err := checkEncryptionToken(t, m.model, device1, rotTestNewPassword, oldToken); !errors.Is(err, errEncryptionPasswordOutdated) {
		t.Fatalf("expected errEncryptionPasswordOutdated, got %v", err)
	}
	if err := checkEncryptionToken(t, m.model, device1, rotTestNewPassword, []byte("totally wrong token")); !errors.Is(err, errEncryptionPassword) {
		t.Fatalf("expected errEncryptionPassword, got %v", err)
	}
	if err := checkEncryptionToken(t, m.model, device1, rotTestNewPassword, newToken); err != nil {
		t.Fatalf("new token should be accepted after completion: %v", err)
	}

	// The cluster config announces the new token and password.
	cc, passwords := m.generateClusterConfig(device1)
	if got := ccTokenForDevice(t, cc, device1); !bytes.Equal(got, newToken) {
		t.Error("cluster config should carry the new password token")
	}
	if passwords[rotTestFolderID] != rotTestNewPassword {
		t.Errorf("cluster config password should be %q, got %q", rotTestNewPassword, passwords[rotTestFolderID])
	}
}

func TestEncryptionPasswordRotationCancel(t *testing.T) {
	m := setupEncryptionRotationModel(t)

	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, rotTestNewPassword); err != nil {
		t.Fatal(err)
	}
	oldToken := protocol.PasswordToken(m.keyGen, rotTestFolderID, rotTestOldPassword)
	newToken := protocol.PasswordToken(m.keyGen, rotTestFolderID, rotTestNewPassword)

	if err := checkEncryptionToken(t, m.model, device1, rotTestOldPassword, oldToken); err != nil {
		t.Fatal(err)
	}

	if err := m.CancelEncryptionPasswordRotation(rotTestFolderID); err != nil {
		t.Fatal(err)
	}
	fcfg, _ := m.cfg.Folder(rotTestFolderID)
	if fcfg.EncryptionPasswordRotation != nil {
		t.Fatal("rotation should be cleared after cancel")
	}
	status, err := m.EncryptionPasswordRotationStatus(rotTestFolderID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Active {
		t.Fatal("rotation should not be active after cancel")
	}

	// Cancelling again is a state error.
	if err := m.CancelEncryptionPasswordRotation(rotTestFolderID); !errors.Is(err, ErrEncryptionPasswordRotationNotActive) {
		t.Fatalf("expected not active error, got %v", err)
	}

	// After cancel, new tokens are rejected and old tokens work.
	if err := checkEncryptionToken(t, m.model, device1, rotTestOldPassword, newToken); !errors.Is(err, errEncryptionPassword) {
		t.Fatalf("expected errEncryptionPassword for new token after cancel, got %v", err)
	}
	if err := checkEncryptionToken(t, m.model, device1, rotTestOldPassword, oldToken); err != nil {
		t.Fatalf("old token should be accepted after cancel: %v", err)
	}
}

func TestEncryptionPasswordRotationCompletionPrecondition(t *testing.T) {
	m := setupEncryptionRotationModel(t)
	setFolderEncryptionPasswords(t, m.cfg, map[protocol.DeviceID]string{
		device2: rotTestOldPassword,
	})

	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, rotTestNewPassword); err != nil {
		t.Fatal(err)
	}
	newToken := protocol.PasswordToken(m.keyGen, rotTestFolderID, rotTestNewPassword)

	// Nothing has rotated yet.
	if err := m.CompleteEncryptionPasswordRotation(rotTestFolderID); !errors.Is(err, ErrEncryptionPasswordRotationIncomplete) {
		t.Fatalf("expected incomplete error, got %v", err)
	}

	// Only device1 rotated: still incomplete.
	if err := checkEncryptionToken(t, m.model, device1, rotTestOldPassword, newToken); err != nil {
		t.Fatal(err)
	}
	if err := m.CompleteEncryptionPasswordRotation(rotTestFolderID); !errors.Is(err, ErrEncryptionPasswordRotationIncomplete) {
		t.Fatalf("expected incomplete error after one device, got %v", err)
	}

	// Both devices rotated: completion succeeds and both passwords are new.
	if err := checkEncryptionToken(t, m.model, device2, rotTestOldPassword, newToken); err != nil {
		t.Fatal(err)
	}
	status, _ := m.EncryptionPasswordRotationStatus(rotTestFolderID)
	if state := rotationDeviceStatus(status, device1); state != config.EncryptionPasswordRotationStateNew {
		t.Errorf("device1: %q", state)
	}
	if state := rotationDeviceStatus(status, device2); state != config.EncryptionPasswordRotationStateNew {
		t.Errorf("device2: %q", state)
	}
	if err := m.CompleteEncryptionPasswordRotation(rotTestFolderID); err != nil {
		t.Fatal(err)
	}
	fcfg, _ := m.cfg.Folder(rotTestFolderID)
	for _, dev := range []protocol.DeviceID{device1, device2} {
		d, _ := fcfg.Device(dev)
		if d.EncryptionPassword != rotTestNewPassword {
			t.Errorf("device %v password should be %q, got %q", dev, rotTestNewPassword, d.EncryptionPassword)
		}
	}

	// Completing again is a state error.
	if err := m.CompleteEncryptionPasswordRotation(rotTestFolderID); !errors.Is(err, ErrEncryptionPasswordRotationNotActive) {
		t.Fatalf("expected not active error, got %v", err)
	}
}

func TestEncryptionPasswordRotationClusterConfigPerDevice(t *testing.T) {
	m := setupEncryptionRotationModel(t)
	setFolderEncryptionPasswords(t, m.cfg, map[protocol.DeviceID]string{
		device2: rotTestOldPassword,
	})
	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, rotTestNewPassword); err != nil {
		t.Fatal(err)
	}

	oldToken := protocol.PasswordToken(m.keyGen, rotTestFolderID, rotTestOldPassword)
	newToken := protocol.PasswordToken(m.keyGen, rotTestFolderID, rotTestNewPassword)

	// device1 stays on the old password, device2 moves to the new one.
	if err := checkEncryptionToken(t, m.model, device1, rotTestOldPassword, oldToken); err != nil {
		t.Fatal(err)
	}
	if err := checkEncryptionToken(t, m.model, device2, rotTestOldPassword, newToken); err != nil {
		t.Fatal(err)
	}

	cc1, passwords1 := m.generateClusterConfig(device1)
	if got := ccTokenForDevice(t, cc1, device1); !bytes.Equal(got, oldToken) {
		t.Error("device1 should get the old token")
	}
	if passwords1[rotTestFolderID] != rotTestOldPassword {
		t.Errorf("device1 should get the old password, got %q", passwords1[rotTestFolderID])
	}

	cc2, passwords2 := m.generateClusterConfig(device2)
	if got := ccTokenForDevice(t, cc2, device2); !bytes.Equal(got, newToken) {
		t.Error("device2 should get the new token")
	}
	if passwords2[rotTestFolderID] != rotTestNewPassword {
		t.Errorf("device2 should get the new password, got %q", passwords2[rotTestFolderID])
	}
}

func TestEncryptionPasswordRotationPersisted(t *testing.T) {
	m := setupEncryptionRotationModel(t)
	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, rotTestNewPassword); err != nil {
		t.Fatal(err)
	}
	newToken := protocol.PasswordToken(m.keyGen, rotTestFolderID, rotTestNewPassword)
	if err := checkEncryptionToken(t, m.model, device1, rotTestOldPassword, newToken); err != nil {
		t.Fatal(err)
	}

	// The bookkeeping lives in the (persisted) configuration: save it to
	// disk and load it again, as happens on restart.
	path := filepath.Join(t.TempDir(), "config.xml")
	saved := config.Wrap(path, m.cfg.RawCopy(), myID, events.NoopLogger)
	if err := saved.Save(); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := config.Load(path, myID, events.NoopLogger)
	if err != nil {
		t.Fatal(err)
	}
	fcfg, ok := loaded.Folder(rotTestFolderID)
	if !ok {
		t.Fatal("folder missing after load")
	}
	if fcfg.EncryptionPasswordRotation == nil {
		t.Fatal("rotation missing after load")
	}
	if fcfg.EncryptionPasswordRotation.NewPassword != rotTestNewPassword {
		t.Errorf("new password not persisted: %q", fcfg.EncryptionPasswordRotation.NewPassword)
	}
	if state := fcfg.EncryptionPasswordRotation.DeviceState(device1); state != config.EncryptionPasswordRotationStateNew {
		t.Errorf("device1 should be recorded as new after load, got %q", state)
	}
}

func TestEncryptionPasswordRotationStatuses(t *testing.T) {
	m := setupEncryptionRotationModel(t)
	if err := m.StartEncryptionPasswordRotation(rotTestFolderID, rotTestNewPassword); err != nil {
		t.Fatal(err)
	}
	statuses := m.EncryptionPasswordRotationStatuses()
	status, ok := statuses[rotTestFolderID]
	if !ok {
		t.Fatal("folder missing from statuses")
	}
	if !status.Active || len(status.Devices) != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}

	if _, err := m.EncryptionPasswordRotationStatus("does-not-exist"); !errors.Is(err, ErrFolderMissing) {
		t.Fatalf("expected folder missing, got %v", err)
	}
}
