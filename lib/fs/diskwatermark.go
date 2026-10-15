// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"path/filepath"
	"runtime"
	"strings"

	"github.com/shirou/gopsutil/v4/disk"
)

// DiskIdentity identifies the physical disk backing a folder path. Key is
// stable for a given physical disk within a running process and equal for
// all folders on the same disk. Name is a human readable mount point or
// volume name.
type DiskIdentity struct {
	Key  string
	Name string
}

// DiskMount describes a mounted filesystem. It is the portable subset of
// gopsutil's disk.PartitionStat used for mount point name resolution.
type DiskMount struct {
	Device     string
	MountPoint string
}

// DiskMounts enumerates the current filesystem mounts. This is a relatively
// expensive operation and is intended to be called once per watermark check
// and shared across all folders.
func DiskMounts() ([]DiskMount, error) {
	parts, err := disk.Partitions(false)
	if err != nil {
		return nil, err
	}
	mounts := make([]DiskMount, 0, len(parts))
	for _, p := range parts {
		mounts = append(mounts, DiskMount{Device: p.Device, MountPoint: p.Mountpoint})
	}
	return mounts, nil
}

// IdentifyDisk resolves the physical disk identity for the given path. The
// path must exist and be accessible, otherwise an error is returned and the
// caller should fall back to the regular folder health check. mounts may be
// nil, in which case the path itself is used as the display name.
func IdentifyDisk(path string, mounts []DiskMount) (DiskIdentity, error) {
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return DiskIdentity{}, err
	}

	key, err := deviceKey(abs)
	if err != nil {
		return DiskIdentity{}, err
	}

	name := matchMountPoint(abs, mounts)
	if name == "" {
		name = fallbackDiskName(abs)
	}

	return DiskIdentity{Key: key, Name: name}, nil
}

// matchMountPoint returns the longest mount point that contains the path.
func matchMountPoint(path string, mounts []DiskMount) string {
	longest := ""
	for _, m := range mounts {
		mp := m.MountPoint
		if mp == "" {
			continue
		}
		if !pathContains(mp, path) {
			continue
		}
		if len(mp) > len(longest) {
			longest = mp
		}
	}
	return longest
}

// pathContains reports whether root is the path itself or a parent of it.
func pathContains(root, path string) bool {
	if runtime.GOOS == "windows" {
		// Windows paths and mount points are case insensitive.
		root = strings.ToLower(filepath.Clean(root))
		path = strings.ToLower(filepath.Clean(path))
	} else {
		root = filepath.Clean(root)
		path = filepath.Clean(path)
	}
	if root == path {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." {
		return err == nil
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}
