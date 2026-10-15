// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/syncthing/syncthing/lib/model"
)

// encryptionRotationRequest is the JSON body accepted by the rotation
// endpoints. Query parameters take precedence over body fields.
type encryptionRotationRequest struct {
	Folder   string `json:"folder"`
	Password string `json:"password"`
}

func (s *service) getEncryptionRotation(w http.ResponseWriter, r *http.Request) {
	folder := r.URL.Query().Get("folder")
	if folder != "" {
		status, err := s.model.EncryptionPasswordRotationStatus(folder)
		if err != nil {
			httpStatus := http.StatusBadRequest
			if errors.Is(err, model.ErrFolderMissing) {
				httpStatus = http.StatusNotFound
			}
			http.Error(w, err.Error(), httpStatus)
			return
		}
		sendJSON(w, status)
		return
	}
	sendJSON(w, s.model.EncryptionPasswordRotationStatuses())
}

func (s *service) postEncryptionRotationStart(w http.ResponseWriter, r *http.Request) {
	req := encryptionRotationRequest{}
	if r.ContentLength != 0 {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	folder := r.URL.Query().Get("folder")
	if folder == "" {
		folder = req.Folder
	}
	password := r.URL.Query().Get("password")
	if password == "" {
		password = req.Password
	}
	if folder == "" {
		http.Error(w, "missing required folder parameter", http.StatusBadRequest)
		return
	}
	if err := s.model.StartEncryptionPasswordRotation(folder, password); err != nil {
		encryptionRotationHTTPError(w, err)
		return
	}
}

func (s *service) postEncryptionRotationComplete(w http.ResponseWriter, r *http.Request) {
	folder := r.URL.Query().Get("folder")
	if folder == "" {
		http.Error(w, "missing required folder parameter", http.StatusBadRequest)
		return
	}
	if err := s.model.CompleteEncryptionPasswordRotation(folder); err != nil {
		encryptionRotationHTTPError(w, err)
		return
	}
}

func (s *service) postEncryptionRotationCancel(w http.ResponseWriter, r *http.Request) {
	folder := r.URL.Query().Get("folder")
	if folder == "" {
		http.Error(w, "missing required folder parameter", http.StatusBadRequest)
		return
	}
	if err := s.model.CancelEncryptionPasswordRotation(folder); err != nil {
		encryptionRotationHTTPError(w, err)
		return
	}
}

func encryptionRotationHTTPError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, model.ErrFolderMissing):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, model.ErrEncryptionPasswordRotationActive),
		errors.Is(err, model.ErrEncryptionPasswordRotationNotActive),
		errors.Is(err, model.ErrEncryptionPasswordRotationIncomplete):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}
