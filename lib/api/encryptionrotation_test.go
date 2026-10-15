// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/model"
	modelmocks "github.com/syncthing/syncthing/lib/model/mocks"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestGetEncryptionRotation(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	s := &service{model: m}

	expected := model.EncryptionPasswordRotationStatus{
		FolderID: "f1",
		Active:   true,
		Devices: []model.EncryptionPasswordRotationDeviceStatus{
			{DeviceID: protocol.DeviceID{0x1}, State: config.EncryptionPasswordRotationStateNew},
		},
	}
	m.EncryptionPasswordRotationStatusReturns(expected, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/rest/folder/encryptionrotation?folder=f1", nil)
	s.getEncryptionRotation(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d, body: %s", rec.Code, rec.Body.String())
	}
	var got model.EncryptionPasswordRotationStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.FolderID != "f1" || !got.Active || len(got.Devices) != 1 || got.Devices[0].State != config.EncryptionPasswordRotationStateNew {
		t.Fatalf("unexpected response: %+v", got)
	}
	if m.EncryptionPasswordRotationStatusCallCount() != 1 {
		t.Fatal("expected one status call")
	}
	if m.EncryptionPasswordRotationStatusArgsForCall(0) != "f1" {
		t.Error("unexpected folder argument")
	}

	// Without a folder parameter the collection of all statuses is
	// returned.
	m.EncryptionPasswordRotationStatusesReturns(map[string]model.EncryptionPasswordRotationStatus{
		"f1": expected,
	})
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/rest/folder/encryptionrotation", nil)
	s.getEncryptionRotation(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	var all map[string]model.EncryptionPasswordRotationStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("expected one folder status, got %d", len(all))
	}

	// An unknown folder maps to 404.
	m.EncryptionPasswordRotationStatusReturns(model.EncryptionPasswordRotationStatus{}, model.ErrFolderMissing)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/rest/folder/encryptionrotation?folder=nope", nil)
	s.getEncryptionRotation(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}

	// A non-encrypted folder maps to 400.
	m.EncryptionPasswordRotationStatusReturns(model.EncryptionPasswordRotationStatus{}, model.ErrEncryptionPasswordRotationNotEncrypted)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/rest/folder/encryptionrotation?folder=plain", nil)
	s.getEncryptionRotation(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestPostEncryptionRotationStart(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	s := &service{model: m}
	m.StartEncryptionPasswordRotationReturns(nil)

	body := bytes.NewBufferString(`{"folder":"f1","password":"pw"}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rest/folder/encryptionrotation/start", body)
	s.postEncryptionRotationStart(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d, body: %s", rec.Code, rec.Body.String())
	}
	folder, password := m.StartEncryptionPasswordRotationArgsForCall(0)
	if folder != "f1" || password != "pw" {
		t.Fatalf("unexpected args: %q %q", folder, password)
	}

	// Missing folder is a bad request.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rest/folder/encryptionrotation/start", bytes.NewBufferString(`{"password":"pw"}`))
	s.postEncryptionRotationStart(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	// State precondition errors map to conflict.
	m.StartEncryptionPasswordRotationReturns(model.ErrEncryptionPasswordRotationActive)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rest/folder/encryptionrotation/start?folder=f1&password=pw2", nil)
	s.postEncryptionRotationStart(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestPostEncryptionRotationCompleteAndCancel(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	s := &service{model: m}

	m.CompleteEncryptionPasswordRotationReturns(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rest/folder/encryptionrotation/complete?folder=f1", nil)
	s.postEncryptionRotationComplete(rec, req)
	if rec.Code != http.StatusOK || m.CompleteEncryptionPasswordRotationArgsForCall(0) != "f1" {
		t.Fatalf("complete failed: %d %s", rec.Code, rec.Body.String())
	}

	m.CancelEncryptionPasswordRotationReturns(nil)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rest/folder/encryptionrotation/cancel?folder=f1", nil)
	s.postEncryptionRotationCancel(rec, req)
	if rec.Code != http.StatusOK || m.CancelEncryptionPasswordRotationArgsForCall(0) != "f1" {
		t.Fatalf("cancel failed: %d %s", rec.Code, rec.Body.String())
	}

	// Incomplete completion maps to conflict.
	m.CompleteEncryptionPasswordRotationReturns(fmt.Errorf("%w: device XYZ", model.ErrEncryptionPasswordRotationIncomplete))
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rest/folder/encryptionrotation/complete?folder=f1", nil)
	s.postEncryptionRotationComplete(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}

	// Missing folder parameter is a bad request.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/rest/folder/encryptionrotation/cancel", nil)
	s.postEncryptionRotationCancel(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
