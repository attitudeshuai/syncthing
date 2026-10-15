// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package ignore

import (
	"io"

	"github.com/syncthing/syncthing/lib/fs"
)

// Issue is a single parse error or warning located at a specific line of a
// specific ignore file (the root file or an #include-d file).
type Issue struct {
	File    string `json:"file"`
	Line    int    `json:"line"`
	Message string `json:"message"`
}

// issueCollector gathers parse errors and warnings while parsing, instead of
// aborting at the first error. A nil *issueCollector selects the strict
// (legacy) behavior of the parser.
type issueCollector struct {
	errors   []Issue
	warnings []Issue
}

func (c *issueCollector) addError(file string, line int, msg string) {
	c.errors = append(c.errors, Issue{File: file, Line: line, Message: msg})
}

func (c *issueCollector) addWarning(file string, line int, msg string) {
	c.warnings = append(c.warnings, Issue{File: file, Line: line, Message: msg})
}

func (c *issueCollector) hasErrors() bool {
	return len(c.errors) > 0
}

// Validate parses ignore content without modifying any existing matcher.
// Included files are resolved and read from filesystem as during a normal
// load, but nothing is written to disk and no scan is triggered.
//
// It returns a temporary matcher holding the patterns that were parsed
// successfully (suitable for read-only trial matching), all per-line parse
// errors (including those located in included files), and per-line warnings.
// The content is considered valid only when no errors were found, regardless
// of how many patterns still parsed successfully.
func Validate(filesystem fs.Filesystem, content io.Reader, file string) (*Matcher, []Issue, []Issue) {
	matcher := New(filesystem)
	coll := &issueCollector{}

	lines, patterns, _ := parseIgnoreFile(filesystem, content, file, file, false, matcher.changeDetector, make(map[string]struct{}), coll)
	matcher.lines = lines
	matcher.patterns = patterns
	matcher.curHash = hashPatterns(patterns)

	return matcher, coll.errors, coll.warnings
}
