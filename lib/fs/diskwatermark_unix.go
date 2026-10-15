// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !windows

package fs

import (
	"fmt"
	"os"
	"syscall"
)

// deviceKey returns a key that uniquely identifies the physical device
// backing the given path, based on the device number returned by stat.
// Folders on the same mount point share the same device number.
func deviceKey(path string) (string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("unable to determine device for %q", path)
	}
	return fmt.Sprintf("dev:%v", stat.Dev), nil
}

func fallbackDiskName(path string) string {
	return path
}
