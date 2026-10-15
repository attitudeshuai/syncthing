// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"io"
	"slices"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
)

func setupIgnoreTestFolder(t *testing.T) (*testModel, fs.Filesystem) {
	t.Helper()

	w, cancel := newConfigWrapper(defaultCfg)
	t.Cleanup(cancel)
	ffs := w.FolderList()[0].Filesystem()
	m := setupModel(t, w)
	t.Cleanup(func() { cleanupModel(m) })

	must(t, ffs.MkdirAll(config.DefaultMarkerName, 0o644))
	writeFile(t, ffs, "alpha", []byte("alpha"))
	writeFile(t, ffs, "beta", []byte("beta"))
	must(t, ffs.MkdirAll("somedir", 0o755))
	writeFile(t, ffs, "somedir/gamma", []byte("gamma"))

	// Make ignore changes reload unconditionally.
	folderIgnoresAlwaysReload(t, m, defaultFolderConfig)

	// ScanFolders is blocking; afterwards everything is indexed.
	m.ScanFolders()

	return m, ffs
}

func readIgnoreFile(t *testing.T, ffs fs.Filesystem) string {
	t.Helper()
	fd, err := ffs.Open(".stignore")
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	bs, err := io.ReadAll(fd)
	if err != nil {
		t.Fatal(err)
	}
	return string(bs)
}

func TestValidateIgnoresImpact(t *testing.T) {
	m, ffs := setupIgnoreTestFolder(t)

	// No ignore file exists yet.
	if _, err := ffs.Stat(".stignore"); !fs.IsNotExist(err) {
		t.Fatal("precondition: .stignore should not exist")
	}

	// Adding a rule ignores exactly alpha, which is already indexed.
	v, err := m.ValidateIgnores("default", []string{"alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if !v.Valid || len(v.Errors) != 0 {
		t.Errorf("content should be valid: %+v", v.Errors)
	}
	if v.Impact.NewlyIgnoredCount != 1 {
		t.Errorf("want 1 newly ignored, got %d", v.Impact.NewlyIgnoredCount)
	}
	if !slices.Contains(v.Impact.NewlyIgnoredSamples, "alpha") {
		t.Errorf("want alpha in samples: %v", v.Impact.NewlyIgnoredSamples)
	}
	if v.Impact.NewlyAllowedCount != 0 {
		t.Errorf("want no newly allowed, got %d", v.Impact.NewlyAllowedCount)
	}

	// Validation must not have written anything.
	if _, err := ffs.Stat(".stignore"); !fs.IsNotExist(err) {
		t.Fatal("validation created .stignore")
	}

	// A directory rule affects the directory and everything below it.
	v, err = m.ValidateIgnores("default", []string{"somedir"})
	if err != nil {
		t.Fatal(err)
	}
	if v.Impact.NewlyIgnoredCount != 2 {
		t.Errorf("want somedir and somedir/gamma newly ignored, got %d (%v)", v.Impact.NewlyIgnoredCount, v.Impact.NewlyIgnoredSamples)
	}

	// Put an ignore file on disk and make the live matcher reload it.
	writeFile(t, ffs, ".stignore", []byte("alpha\n"))
	if _, _, err := m.LoadIgnores("default"); err != nil {
		t.Fatal(err)
	}

	// Empty candidate rules un-ignore alpha (still indexed).
	v, err = m.ValidateIgnores("default", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Impact.NewlyAllowedCount != 1 {
		t.Errorf("want 1 newly allowed, got %d", v.Impact.NewlyAllowedCount)
	}
	if !slices.Contains(v.Impact.NewlyAllowedSamples, "alpha") {
		t.Errorf("want alpha in samples: %v", v.Impact.NewlyAllowedSamples)
	}
	if v.Impact.NewlyIgnoredCount != 0 {
		t.Errorf("want no newly ignored, got %d", v.Impact.NewlyIgnoredCount)
	}

	// Invalid content reports the bad line, is not valid, and does not
	// overwrite the existing ignore file.
	v, err = m.ValidateIgnores("default", []string{"bad["})
	if err != nil {
		t.Fatal(err)
	}
	if v.Valid || len(v.Errors) != 1 {
		t.Fatalf("want one error and invalid result, got %+v", v)
	}
	if v.Errors[0].Line != 1 || v.Errors[0].File != ".stignore" {
		t.Errorf("error should locate line 1 of .stignore: %+v", v.Errors[0])
	}
	if got := readIgnoreFile(t, ffs); got != "alpha\n" {
		t.Errorf("validation changed .stignore: %q", got)
	}

	// Unknown folder.
	if _, err := m.ValidateIgnores("doesnotexist", nil); err == nil {
		t.Error("expected error for unknown folder")
	}
}

func TestDiagnoseIgnores(t *testing.T) {
	m, ffs := setupIgnoreTestFolder(t)
	writeFile(t, ffs, ".stignore", []byte("alpha\n"))
	if _, _, err := m.LoadIgnores("default"); err != nil {
		t.Fatal(err)
	}

	d, err := m.DiagnoseIgnores("default", "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if !d.Ignored {
		t.Error("alpha should be ignored")
	}
	if len(d.Chain) != 1 {
		t.Fatalf("want single hit, got %+v", d.Chain)
	}
	hit := d.Chain[0]
	if !hit.Effective || hit.Text != "alpha" || hit.Source != ".stignore" || hit.Line != 1 || hit.FromInclude {
		t.Errorf("unexpected hit: %+v", hit)
	}

	d, err = m.DiagnoseIgnores("default", "beta")
	if err != nil {
		t.Fatal(err)
	}
	if d.Ignored || len(d.Chain) != 0 {
		t.Errorf("beta should sync with empty chain: %+v", d)
	}

	if _, err := m.DiagnoseIgnores("default", ""); err == nil {
		t.Error("empty path should error")
	}
	if _, err := m.DiagnoseIgnores("default", "../escape"); err == nil {
		t.Error("upward traversal should error")
	}
	if _, err := m.DiagnoseIgnores("doesnotexist", "x"); err == nil {
		t.Error("unknown folder should error")
	}
}
