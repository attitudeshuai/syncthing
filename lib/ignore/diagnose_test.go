// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package ignore

import (
	"bytes"
	"path/filepath"
	"testing"
)

func parseForDiagnosis(t *testing.T, content string) *Matcher {
	t.Helper()
	m := New(newTestFS())
	if err := m.Parse(bytes.NewBufferString(content), ".stignore"); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDiagnoseIncludesProvenance(t *testing.T) {
	m := New(newTestFS())
	if err := m.Load(".stignore"); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		path       string
		source     string
		line       int
		text       string
		fromIncl   bool
		ignored    bool
		exists     bool
		isDir      bool
		canSkipDir bool
	}{
		{"bfile", ".stignore", 2, "bfile", false, true, false, false, true},
		{"dir1/cfile", ".stignore", 3, "dir1/cfile", false, true, false, false, true},
		{"dir2/dfile", "excludes", 1, "dir2/dfile", true, true, false, false, true},
		{"dir3", "further-excludes", 1, "dir3", true, true, true, true, true},
		{"dir3/afile", "further-excludes", 1, "dir3", true, true, false, false, true},
	}

	for _, tc := range cases {
		d := m.Diagnose(filepath.FromSlash(tc.path))
		if d.Ignored != tc.ignored {
			t.Errorf("%s: ignored %v want %v", tc.path, d.Ignored, tc.ignored)
		}
		if d.SkipDir != tc.canSkipDir {
			t.Errorf("%s: skipDir %v want %v", tc.path, d.SkipDir, tc.canSkipDir)
		}
		if d.PathExists != tc.exists {
			t.Errorf("%s: pathExists %v want %v", tc.path, d.PathExists, tc.exists)
		}
		if d.IsDir != tc.isDir {
			t.Errorf("%s: isDir %v want %v", tc.path, d.IsDir, tc.isDir)
		}
		if len(d.Chain) != 1 {
			t.Fatalf("%s: chain %v, want single effective rule", tc.path, d.Chain)
		}
		hit := d.Chain[0]
		if !hit.Effective {
			t.Errorf("%s: first hit must be effective", tc.path)
		}
		if hit.Source != tc.source || hit.Line != tc.line || hit.Text != tc.text || hit.FromInclude != tc.fromIncl {
			t.Errorf("%s: hit %+v want source %q line %d text %q include %v", tc.path, hit, tc.source, tc.line, tc.text, tc.fromIncl)
		}
	}
}

func TestDiagnoseNegationAndFirstMatch(t *testing.T) {
	// Syncthing resolves on the FIRST matching rule; the negation must come
	// before the broad rule to take effect.
	m := parseForDiagnosis(t, "!keep\n*\n")

	d := m.Diagnose("keep")
	if d.Ignored {
		t.Errorf("keep should be un-ignored: %+v", d)
	}
	if len(d.Chain) != 2 {
		t.Fatalf("chain %v, want two hits", d.Chain)
	}
	if !d.Chain[0].Effective || !d.Chain[0].Negated || d.Chain[0].Text != "!keep" {
		t.Errorf("effective rule should be !keep: %+v", d.Chain[0])
	}
	if d.Chain[0].Line != 1 {
		t.Errorf("!keep should be on line 1: %+v", d.Chain[0])
	}
	if d.Chain[1].Effective || d.Chain[1].Text != "*" || d.Chain[1].Negated {
		t.Errorf("second hit should be the shadowed *: %+v", d.Chain[1])
	}

	// Broad match without a preceding negation stays ignored, even with a
	// negation later (it never gets consulted).
	m2 := parseForDiagnosis(t, "i*2\n!ign2\n")
	d2 := m2.Diagnose("ign2")
	if !d2.Ignored {
		t.Errorf("ign2 should stay ignored (first match wins): %+v", d2)
	}
	if len(d2.Chain) != 2 || !d2.Chain[0].Effective || d2.Chain[0].Text != "i*2" {
		t.Errorf("effective rule should be i*2: %+v", d2.Chain)
	}
	if d2.Chain[1].Effective || !d2.Chain[1].Negated {
		t.Errorf("later !ign2 should be reported as non-effective: %+v", d2.Chain[1])
	}
}

func TestDiagnoseCaseFold(t *testing.T) {
	m := parseForDiagnosis(t, "(?i)foo\n")

	for _, p := range []string{"foo", "FOO", "Foo"} {
		d := m.Diagnose(p)
		if !d.Ignored {
			t.Errorf("%s should be ignored", p)
		}
		if !d.CaseFolded {
			t.Errorf("%s: verdict should be marked case folded", p)
		}
		if len(d.Chain) != 1 || !d.Chain[0].CaseFolded {
			t.Errorf("%s: chain hit should be case folded: %+v", p, d.Chain)
		}
	}

	d := m.Diagnose("bar")
	if d.Ignored || len(d.Chain) != 0 {
		t.Errorf("bar should sync with empty chain: ignored=%v chain=%v", d.Ignored, d.Chain)
	}
}

func TestDiagnoseNoRuleMeansSync(t *testing.T) {
	m := parseForDiagnosis(t, "foo\n")
	d := m.Diagnose("bar")
	if d.Ignored || d.SkipDir || d.Reason != "" {
		t.Errorf("unexpected verdict %+v", d)
	}
	if len(d.Chain) != 0 {
		t.Errorf("chain should be empty: %+v", d.Chain)
	}
}

func TestDiagnoseSpecialFiles(t *testing.T) {
	m := parseForDiagnosis(t, "foo\n")

	d := m.Diagnose(".stfolder")
	if !d.Ignored || !d.SkipDir || d.Reason != "internal" {
		t.Errorf(".stfolder: %+v", d)
	}
	if len(d.Chain) != 0 {
		t.Errorf("internal files must not produce rule hits: %+v", d.Chain)
	}

	d = m.Diagnose(".")
	if d.Ignored {
		t.Errorf("folder root should not be ignored: %+v", d)
	}
}

func TestDiagnoseMatchesMatchSemantics(t *testing.T) {
	m := New(newTestFS())
	if err := m.Load(".stignore"); err != nil {
		t.Fatal(err)
	}

	paths := []string{
		"afile", "bfile", "cfile", "dfile", "efile", "ffile",
		"dir1", filepath.Join("dir1", "cfile"), filepath.Join("dir1", "dfile"),
		filepath.Join("dir1", "efile"), filepath.Join("dir1", "ffile"),
		"dir2", filepath.Join("dir2", "dfile"), filepath.Join("dir2", "efile"),
		"dir3", filepath.Join("dir3", "afile"),
		"lost+found", "whatever", ".",
	}

	for _, p := range paths {
		want := m.Match(p)
		d := m.Diagnose(p)
		if d.Ignored != want.IsIgnored() {
			t.Errorf("%s: ignored %v, Match says %v (%v)", p, d.Ignored, want.IsIgnored(), want)
		}
		if d.SkipDir != want.CanSkipDir() {
			t.Errorf("%s: skipDir %v, Match says %v (%v)", p, d.SkipDir, want.CanSkipDir(), want)
		}
		if d.CaseFolded != want.IsCaseFolded() {
			t.Errorf("%s: caseFolded %v, Match says %v (%v)", p, d.CaseFolded, want.IsCaseFolded(), want)
		}
		if d.Deletable != want.IsDeletable() {
			t.Errorf("%s: deletable %v, Match says %v (%v)", p, d.Deletable, want.IsDeletable(), want)
		}
	}
}

func TestDiagnoseSkipDirWithNegation(t *testing.T) {
	// A negation disables wholesale directory skipping from that point on.
	m := parseForDiagnosis(t, "!keep\ndir\n")

	d := m.Diagnose("dir")
	if !d.Ignored {
		t.Fatal("dir should be ignored")
	}
	if d.SkipDir {
		t.Errorf("dir must not be skippable after a negation pattern: %+v", d)
	}
	// "dir" doesn't match !keep, so the single hit is the dir rule.
	if len(d.Chain) != 1 || !d.Chain[0].Effective || d.Chain[0].Text != "dir" {
		t.Errorf("effective hit should be dir: %+v", d.Chain)
	}
}
