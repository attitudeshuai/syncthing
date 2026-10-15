// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package ignore

import (
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/rand"
)

func newValidateFS(t *testing.T, files map[string]string) fs.Filesystem {
	t.Helper()
	ffs := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32)+"?content=true&nostfolder=true")
	for name, content := range files {
		if err := fs.WriteFile(ffs, name, []byte(content), 0o666); err != nil {
			t.Fatal(err)
		}
	}
	return ffs
}

func issueAt(errs []Issue, file string, line int) *Issue {
	for i := range errs {
		if errs[i].File == file && errs[i].Line == line {
			return &errs[i]
		}
	}
	return nil
}

func TestValidateValidContent(t *testing.T) {
	ffs := newValidateFS(t, nil)
	matcher, errs, warns := Validate(ffs, strings.NewReader("foo\n!bar\n"), ".stignore")

	if len(errs) != 0 {
		t.Errorf("unexpected errors: %+v", errs)
	}
	if len(warns) != 0 {
		t.Errorf("unexpected warnings: %+v", warns)
	}
	if !matcher.Match("foo").IsIgnored() {
		t.Error("trial matcher should match the good pattern")
	}
	if matcher.Match("bar").IsIgnored() {
		t.Error("negation pattern should un-ignore")
	}
}

func TestValidateCollectsPerLineErrors(t *testing.T) {
	ffs := newValidateFS(t, nil)
	content := "good\n\nbad[\n"
	matcher, errs, _ := Validate(ffs, strings.NewReader(content), ".stignore")

	if len(errs) != 1 {
		t.Fatalf("want one error, got %+v", errs)
	}
	if errs[0].File != ".stignore" || errs[0].Line != 3 || errs[0].Message == "" {
		t.Errorf("error should point at line 3 of .stignore: %+v", errs[0])
	}
	// Good patterns are still available for trial matching.
	if !matcher.Match("good").IsIgnored() {
		t.Error("good pattern should have been parsed despite the later error")
	}
}

func TestValidateMissingInclude(t *testing.T) {
	ffs := newValidateFS(t, nil)
	_, errs, _ := Validate(ffs, strings.NewReader("#include nope\n"), ".stignore")

	if len(errs) != 1 {
		t.Fatalf("want one error, got %+v", errs)
	}
	if errs[0].File != ".stignore" || errs[0].Line != 1 {
		t.Errorf("missing include should be pinned to the include line: %+v", errs[0])
	}
	if !strings.Contains(errs[0].Message, "nope") {
		t.Errorf("message should name the include file: %q", errs[0].Message)
	}
}

func TestValidateIncludeCycle(t *testing.T) {
	// The same file is referenced through two differently spelled lines,
	// so textual de-duplication doesn't pre-empt the cycle detection.
	ffs := newValidateFS(t, map[string]string{
		"a": "#include b\n",
		"b": "#include ./a\n",
	})
	_, errs, warns := Validate(ffs, strings.NewReader("#include a\n"), ".stignore")

	if len(errs) == 0 {
		t.Fatalf("include cycle should be reported (warnings: %+v)", warns)
	}
	found := false
	for _, e := range errs {
		if e.File == "b" && e.Line == 1 && strings.Contains(e.Message, "include") {
			found = true
		}
	}
	if !found {
		t.Errorf("cycle error should point at the include line in b: %+v", errs)
	}
}

func TestValidateErrorInsideIncludedFile(t *testing.T) {
	ffs := newValidateFS(t, map[string]string{
		"inc": "good\nbad[\n",
	})
	_, errs, _ := Validate(ffs, strings.NewReader("#include inc\n"), ".stignore")

	if e := issueAt(errs, "inc", 2); e == nil {
		t.Errorf("bad line in included file should be located there, got: %+v", errs)
	}
	if issueAt(errs, ".stignore", 1) != nil {
		t.Errorf("include line itself should not be errored when the target loaded: %+v", errs)
	}
}

func TestValidateDuplicateWarning(t *testing.T) {
	ffs := newValidateFS(t, nil)
	_, errs, warns := Validate(ffs, strings.NewReader("foo\n\nfoo\n"), ".stignore")

	if len(errs) != 0 {
		t.Errorf("duplicates are not errors: %+v", errs)
	}
	if len(warns) != 1 || warns[0].Line != 3 || !strings.Contains(warns[0].Message, "duplicate") {
		t.Errorf("duplicate should warn at line 3: %+v", warns)
	}
}

func TestValidateDoesNotMutateExistingMatcher(t *testing.T) {
	ffs := newValidateFS(t, map[string]string{
		".stignore": "foo\n",
	})
	current := New(ffs)
	if err := current.Load(".stignore"); err != nil {
		t.Fatal(err)
	}

	// Validate with different content; its transient matcher is discarded,
	// the loaded one must remain untouched.
	Validate(ffs, strings.NewReader("bar\n"), ".stignore")
	if !current.Match("foo").IsIgnored() {
		t.Error("validation changed the current matcher (foo)")
	}
	if current.Match("bar").IsIgnored() {
		t.Error("validation leaked new patterns into the current matcher (bar)")
	}
}

func TestValidateStrictModeUnchanged(t *testing.T) {
	// Strict parsing still aborts at the first error and does not collect.
	ffs := newValidateFS(t, nil)
	m := New(ffs)
	err := m.Parse(strings.NewReader("bad[\n"), ".stignore")
	if err == nil || !IsParseError(err) {
		t.Errorf("strict parse should still fail with a ParseError, got %v", err)
	}
}
