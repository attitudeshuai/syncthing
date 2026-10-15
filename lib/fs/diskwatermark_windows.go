// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// deviceKey returns a key that uniquely identifies the volume backing the
// given path. Drive letters and UNC volume names are used directly as the
// physical disk identity.
func deviceKey(path string) (string, error) {
	if _, err := os.Stat(path); err != nil {
		return "", err
	}
	volume := filepath.VolumeName(path)
	if volume == "" {
		return "", fmt.Errorf("unable to determine volume for %q", path)
	}
	return "vol:" + strings.ToUpper(volume), nil
}

func fallbackDiskName(path string) string {
	if volume := filepath.VolumeName(path); volume != "" {
		return volume
	}
	return path
}
