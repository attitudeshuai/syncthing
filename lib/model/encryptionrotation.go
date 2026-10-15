// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

// Errors returned by the encryption password rotation operations. These are
// intentionally distinct from the connection time encryption errors in
// model.go as they describe the rotation state machine itself.
var (
	ErrEncryptionPasswordRotationActive            = errors.New("an encryption password rotation is already in progress for this folder")
	ErrEncryptionPasswordRotationNotActive         = errors.New("no encryption password rotation in progress for this folder")
	ErrEncryptionPasswordRotationNotEncrypted      = errors.New("folder is not shared encrypted with any device")
	ErrEncryptionPasswordRotationEmptyPassword     = errors.New("new encryption password must not be empty")
	ErrEncryptionPasswordRotationSamePassword      = errors.New("new encryption password must differ from the current one")
	ErrEncryptionPasswordRotationMultiplePasswords = errors.New("encryption password differs between the folder's devices; make them equal before rotating it")
	ErrEncryptionPasswordRotationIncomplete        = errors.New("encryption password rotation cannot be completed: not all devices have connected with the new password")
)

// EncryptionPasswordRotationDeviceStatus is the rotation observation state
// of a single paired device.
type EncryptionPasswordRotationDeviceStatus struct {
	DeviceID protocol.DeviceID                      `json:"deviceID"`
	State    config.EncryptionPasswordRotationState `json:"state"`
}

// EncryptionPasswordRotationStatus is the externally visible rotation state
// of one folder.
type EncryptionPasswordRotationStatus struct {
	FolderID  string                                   `json:"folderID"`
	Active    bool                                     `json:"active"`
	StartedAt time.Time                                `json:"startedAt,omitempty"`
	Devices   []EncryptionPasswordRotationDeviceStatus `json:"devices"`
}

// StartEncryptionPasswordRotation declares a new encryption password for the
// given folder and enters the transition period. During the transition both
// the current and the new password are accepted on incoming connections, and
// per-device usage is accounted for. Returns an error if a rotation is
// already in progress or the folder is not encrypted for any device.
func (m *model) StartEncryptionPasswordRotation(folderID, newPassword string) error {
	fcfg, ok := m.cfg.Folder(folderID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrFolderMissing, folderID)
	}
	if _, err := encryptionRotationDevicePasswords(fcfg); err != nil {
		return err
	}
	if fcfg.EncryptionPasswordRotation != nil {
		return ErrEncryptionPasswordRotationActive
	}
	if newPassword == "" {
		return ErrEncryptionPasswordRotationEmptyPassword
	}

	startedAt := time.Now().Truncate(time.Second)
	var actionErr error
	w, err := m.cfg.Modify(func(cfg *config.Configuration) {
		fi, idx, ok := cfg.Folder(folderID)
		if !ok {
			actionErr = fmt.Errorf("%w: %s", ErrFolderMissing, folderID)
			return
		}
		currentPassword, err := encryptionRotationDevicePasswords(fi)
		if err != nil {
			actionErr = err
			return
		}
		if fi.EncryptionPasswordRotation != nil {
			actionErr = ErrEncryptionPasswordRotationActive
			return
		}
		if newPassword == currentPassword {
			actionErr = ErrEncryptionPasswordRotationSamePassword
			return
		}
		fi.EncryptionPasswordRotation = &config.EncryptionPasswordRotation{
			NewPassword: newPassword,
			StartedAt:   startedAt,
		}
		cfg.Folders[idx] = fi
	})
	if err != nil {
		return err
	}
	if actionErr != nil {
		return actionErr
	}
	w.Wait()

	slog.Info("Started encryption password rotation", fcfg.LogAttr())
	m.evLogger.Log(events.EncryptionPasswordRotationStarted, map[string]any{
		"folder":    folderID,
		"startedAt": startedAt,
	})
	return nil
}

// CompleteEncryptionPasswordRotation finishes the rotation: the new password
// becomes the current one and the old password stops being accepted. It is a
// precondition that every paired device with an encryption password has
// connected at least once with a token derived from the new password.
func (m *model) CompleteEncryptionPasswordRotation(folderID string) error {
	fcfg, ok := m.cfg.Folder(folderID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrFolderMissing, folderID)
	}
	if _, err := encryptionRotationDevicePasswords(fcfg); err != nil {
		return err
	}
	if fcfg.EncryptionPasswordRotation == nil {
		return ErrEncryptionPasswordRotationNotActive
	}

	oldToken := protocol.PasswordToken(m.keyGen, folderID, fcfg.EncryptionPasswordDevices()[0].EncryptionPassword)

	var actionErr error
	w, err := m.cfg.Modify(func(cfg *config.Configuration) {
		fi, idx, ok := cfg.Folder(folderID)
		if !ok {
			actionErr = fmt.Errorf("%w: %s", ErrFolderMissing, folderID)
			return
		}
		if fi.EncryptionPasswordRotation == nil {
			actionErr = ErrEncryptionPasswordRotationNotActive
			return
		}
		encDevices := fi.EncryptionPasswordDevices()
		if len(encDevices) == 0 {
			actionErr = ErrEncryptionPasswordRotationNotEncrypted
			return
		}
		var pending []string
		for _, dev := range encDevices {
			if !slices.Contains(fi.EncryptionPasswordRotation.RotatedDevices, dev.DeviceID) {
				pending = append(pending, dev.DeviceID.Short().String())
			}
		}
		if len(pending) > 0 {
			slices.Sort(pending)
			actionErr = fmt.Errorf("%w: %s", ErrEncryptionPasswordRotationIncomplete, strings.Join(pending, ", "))
			return
		}

		newPassword := fi.EncryptionPasswordRotation.NewPassword
		for i := range fi.Devices {
			if fi.Devices[i].EncryptionPassword != "" {
				fi.Devices[i].EncryptionPassword = newPassword
			}
		}
		fi.EncryptionPasswordRotation = nil
		cfg.Folders[idx] = fi
	})
	if err != nil {
		return err
	}
	if actionErr != nil {
		return actionErr
	}
	w.Wait()

	m.setSupersededEncryptionToken(folderID, oldToken)

	slog.Info("Completed encryption password rotation", fcfg.LogAttr())
	m.evLogger.Log(events.EncryptionPasswordRotationCompleted, map[string]any{
		"folder": folderID,
	})
	return nil
}

// CancelEncryptionPasswordRotation aborts an in-progress rotation: the
// pending new password is discarded and only the current (old) password
// remains accepted.
func (m *model) CancelEncryptionPasswordRotation(folderID string) error {
	fcfg, ok := m.cfg.Folder(folderID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrFolderMissing, folderID)
	}
	if fcfg.EncryptionPasswordRotation == nil {
		return ErrEncryptionPasswordRotationNotActive
	}

	var actionErr error
	w, err := m.cfg.Modify(func(cfg *config.Configuration) {
		fi, idx, ok := cfg.Folder(folderID)
		if !ok {
			actionErr = fmt.Errorf("%w: %s", ErrFolderMissing, folderID)
			return
		}
		if fi.EncryptionPasswordRotation == nil {
			actionErr = ErrEncryptionPasswordRotationNotActive
			return
		}
		fi.EncryptionPasswordRotation = nil
		cfg.Folders[idx] = fi
	})
	if err != nil {
		return err
	}
	if actionErr != nil {
		return actionErr
	}
	w.Wait()

	m.deleteSupersededEncryptionToken(folderID)

	// Make sure peers that were on the new password get a cluster-config
	// announcing the current one again (and get rejected until reverted).
	m.sendClusterConfig(fcfg.DeviceIDs())

	slog.Info("Cancelled encryption password rotation", fcfg.LogAttr())
	m.evLogger.Log(events.EncryptionPasswordRotationCancelled, map[string]any{
		"folder": folderID,
	})
	return nil
}

// EncryptionPasswordRotationStatus returns the rotation state of one folder.
// An error is returned for unknown folders or folders that are not encrypted
// for any device.
func (m *model) EncryptionPasswordRotationStatus(folderID string) (EncryptionPasswordRotationStatus, error) {
	fcfg, ok := m.cfg.Folder(folderID)
	if !ok {
		return EncryptionPasswordRotationStatus{}, fmt.Errorf("%w: %s", ErrFolderMissing, folderID)
	}
	if len(fcfg.EncryptionPasswordDevices()) == 0 && fcfg.EncryptionPasswordRotation == nil {
		return EncryptionPasswordRotationStatus{}, ErrEncryptionPasswordRotationNotEncrypted
	}
	return encryptionPasswordRotationStatusFor(fcfg), nil
}

// EncryptionPasswordRotationStatuses returns the rotation state for all
// folders that are encrypted for at least one device (including those without
// an active rotation).
func (m *model) EncryptionPasswordRotationStatuses() map[string]EncryptionPasswordRotationStatus {
	folders := m.cfg.FolderList()
	res := make(map[string]EncryptionPasswordRotationStatus, len(folders))
	for _, fcfg := range folders {
		if len(fcfg.EncryptionPasswordDevices()) == 0 && fcfg.EncryptionPasswordRotation == nil {
			continue
		}
		res[fcfg.ID] = encryptionPasswordRotationStatusFor(fcfg)
	}
	return res
}

func encryptionPasswordRotationStatusFor(fcfg config.FolderConfiguration) EncryptionPasswordRotationStatus {
	status := EncryptionPasswordRotationStatus{
		FolderID: fcfg.ID,
		Active:   fcfg.EncryptionPasswordRotation != nil,
	}
	if fcfg.EncryptionPasswordRotation != nil {
		status.StartedAt = fcfg.EncryptionPasswordRotation.StartedAt
		encDevices := fcfg.EncryptionPasswordDevices()
		slices.SortFunc(encDevices, func(a, b config.FolderDeviceConfiguration) int {
			return a.DeviceID.Compare(b.DeviceID)
		})
		status.Devices = make([]EncryptionPasswordRotationDeviceStatus, 0, len(encDevices))
		for _, dev := range encDevices {
			status.Devices = append(status.Devices, EncryptionPasswordRotationDeviceStatus{
				DeviceID: dev.DeviceID,
				State:    fcfg.EncryptionPasswordRotation.DeviceState(dev.DeviceID),
			})
		}
	}
	return status
}

// encryptionRotationDevicePasswords returns the current encryption password
// of the folder (which must be identical for all encrypted devices) and
// validates that the folder is locally the encrypting side with at least one
// encrypted device.
func encryptionRotationDevicePasswords(fcfg config.FolderConfiguration) (string, error) {
	if fcfg.Type == config.FolderTypeReceiveEncrypted {
		return "", ErrEncryptionPasswordRotationNotEncrypted
	}
	encDevices := fcfg.EncryptionPasswordDevices()
	if len(encDevices) == 0 {
		return "", ErrEncryptionPasswordRotationNotEncrypted
	}
	password := encDevices[0].EncryptionPassword
	for _, dev := range encDevices[1:] {
		if dev.EncryptionPassword != password {
			return "", ErrEncryptionPasswordRotationMultiplePasswords
		}
	}
	return password, nil
}

// recordEncryptionPasswordRotationDevice accounts for the fact that the
// given device presented a token derived from the pending new password
// (useNew true) or the current password (useNew false) during a rotation.
// It returns true when the device moved to the new password and a cluster
// config should be resent so that the connection switches over. A no-op when
// no rotation is in progress or the state did not change.
func (m *model) recordEncryptionPasswordRotationDevice(folderID string, deviceID protocol.DeviceID, useNew bool) bool {
	fcfg, ok := m.cfg.Folder(folderID)
	if !ok || fcfg.EncryptionPasswordRotation == nil {
		return false
	}
	rot := fcfg.EncryptionPasswordRotation
	if useNew && slices.Contains(rot.RotatedDevices, deviceID) {
		// Already recorded as using the new password.
		return false
	}
	if !useNew && slices.Contains(rot.OldDevices, deviceID) && !slices.Contains(rot.RotatedDevices, deviceID) {
		// Already recorded as using the current password.
		return false
	}

	var changed bool
	w, err := m.cfg.Modify(func(cfg *config.Configuration) {
		fi, idx, ok := cfg.Folder(folderID)
		if !ok || fi.EncryptionPasswordRotation == nil {
			return
		}
		changed = fi.EncryptionPasswordRotation.RecordDevice(deviceID, useNew)
		cfg.Folders[idx] = fi
	})
	if err != nil {
		slog.Warn("Failed to persist encryption password rotation state", slogutil.Error(err), fcfg.LogAttr(), deviceID.LogAttr())
		return false
	}
	if !changed {
		return false
	}
	w.Wait()

	if useNew {
		slog.Info("Device authenticated with the new encryption password", fcfg.LogAttr(), deviceID.LogAttr())
	} else {
		slog.Debug("Device still authenticating with the current encryption password", fcfg.LogAttr(), deviceID.LogAttr())
	}
	return useNew
}

func (m *model) setSupersededEncryptionToken(folderID string, token []byte) {
	m.mut.Lock()
	m.folderSupersededPasswordTokens[folderID] = token
	m.mut.Unlock()
}

func (m *model) deleteSupersededEncryptionToken(folderID string) {
	m.mut.Lock()
	delete(m.folderSupersededPasswordTokens, folderID)
	m.mut.Unlock()
}

func (m *model) supersededEncryptionToken(folderID string) ([]byte, bool) {
	m.mut.RLock()
	token, ok := m.folderSupersededPasswordTokens[folderID]
	m.mut.RUnlock()
	return token, ok
}
