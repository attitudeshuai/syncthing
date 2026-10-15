// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestMatchMountPoint(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix style mount points")
	}

	mounts := []DiskMount{
		{MountPoint: "/"},
		{MountPoint: "/data"},
		{MountPoint: "/data/backups"},
		{MountPoint: "/other"},
		// Pseudo filesystems must not match regular folder paths.
		{MountPoint: "/proc"},
	}

	cases := []struct {
		path string
		want string
	}{
		{"/data/a/b", "/data"},
		{"/data", "/data"},
		{"/data/backups/2026/x", "/data/backups"},
		{"/home/foo", "/"},
		{"/proc/1", "/proc"},
		{"/elsewhere", "/"},
	}

	for _, tc := range cases {
		if got := matchMountPoint(tc.path, mounts); got != tc.want {
			t.Errorf("matchMountPoint(%q)=%q want %q", tc.path, got, tc.want)
		}
	}
}

func TestMatchMountPointWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("windows only")
	}

	mounts := []DiskMount{
		{MountPoint: `C:\`},
		{MountPoint: `D:\`},
	}

	cases := []struct {
		path string
		want string
	}{
		{`d:\folders\a`, `D:\`},
		{`C:\Users\foo`, `C:\`},
		{`c:\users\FOO`, `C:\`},
		{`E:\foo`, ``},
	}

	for _, tc := range cases {
		if got := matchMountPoint(tc.path, mounts); got != tc.want {
			t.Errorf("matchMountPoint(%q)=%q want %q", tc.path, got, tc.want)
		}
	}
}

func TestIdentifyDisk(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}

	id1, err := IdentifyDisk(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if id1.Key == "" || id1.Name == "" {
		t.Fatalf("empty identity: %+v", id1)
	}

	id2, err := IdentifyDisk(nested, nil)
	if err != nil {
		t.Fatal(err)
	}
	if id1.Key != id2.Key {
		t.Errorf("same disk should share key: %q != %q", id1.Key, id2.Key)
	}

	if _, err := IdentifyDisk(filepath.Join(root, "does-not-exist"), nil); err == nil {
		t.Error("nonexistent path should fail")
	}
}
