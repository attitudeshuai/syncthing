// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package ignore

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gobwas/glob"
	"golang.org/x/text/unicode/norm"

	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore/ignoreresult"
	"github.com/syncthing/syncthing/lib/osutil"
)

const escapePrefix = "#escape"

var defaultEscapeChar = '\\'

func init() {
	if os.PathSeparator == defaultEscapeChar {
		// The pipe character (|) is not allowed in filenames on Windows
		defaultEscapeChar = '|'
	}
}

// A ParseError signifies an error with contents of an ignore file,
// including I/O errors on included files. An I/O error on the root level
// ignore file is not a ParseError.
type ParseError struct {
	inner error
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error: %v", e.inner)
}

func (e *ParseError) Unwrap() error {
	return e.inner
}

func IsParseError(err error) bool {
	var e *ParseError
	return errors.As(err, &e)
}

func parseError(err error) error {
	if err == nil {
		return nil
	}
	return &ParseError{err}
}

type Pattern struct {
	pattern string
	match   glob.Glob
	result  ignoreresult.R
	src     ruleSource
}

// ruleSource records where a pattern came from. Multiple Pattern entries may
// share the same source, as a single source line expands into several
// patterns.
type ruleSource struct {
	// file is the ignore file the rule was read from, as referenced from
	// the folder (".stignore" for the root file, otherwise the path of the
	// #include-d file).
	file string
	// line is the 1-based line number in file, counting blank and comment
	// lines as they appear on disk.
	line int
	// text is the rule text exactly as written on that line, before any
	// escape character translation.
	text string
	// viaInclude is true when the rule was read from an #include-d file
	// rather than the root ignore file.
	viaInclude bool
}

func (p Pattern) String() string {
	ret := p.pattern
	if !p.result.IsIgnored() {
		ret = "!" + ret
	}
	if p.result.IsCaseFolded() {
		ret = "(?i)" + ret
	}
	if p.result.IsDeletable() {
		ret = "(?d)" + ret
	}
	return ret
}

func (p Pattern) allowsSkippingIgnoredDirs() bool {
	if p.result.IsIgnored() {
		return true
	}
	if p.pattern == "" || p.pattern[0] != '/' {
		return false
	}
	// A "/**" at the end is allowed and doesn't have any bearing on the
	// below checks; remove it before checking.
	pattern := strings.TrimSuffix(p.pattern, "/**")
	if len(pattern) == 0 {
		return true
	}
	if strings.Contains(pattern[1:], "/") {
		return false
	}
	// Double asterisk everywhere in the path except at the end is bad
	return !strings.Contains(strings.TrimSuffix(pattern, "**"), "**")
}

// The ChangeDetector is responsible for determining if files have changed
// on disk. It gets told to Remember() files (name and modtime) and will
// then get asked if a file has been Seen() (i.e., Remember() has been
// called on it) and if any of the files have Changed(). To forget all
// files, call Reset().
type ChangeDetector interface {
	Remember(fs fs.Filesystem, name string, modtime time.Time)
	Seen(fs fs.Filesystem, name string) bool
	Changed() bool
	Reset()
}

type Matcher struct {
	fs             fs.Filesystem
	lines          []string  // exact lines read from .stignore
	patterns       []Pattern // patterns including those from included files
	curHash        string
	changeDetector ChangeDetector
	mut            sync.Mutex
}

// An Option can be passed to New()
type Option func(*Matcher)

// WithChangeDetector sets a custom ChangeDetector. The default is to simply
// use the on disk modtime for comparison.
func WithChangeDetector(cd ChangeDetector) Option {
	return func(m *Matcher) {
		m.changeDetector = cd
	}
}

func New(fs fs.Filesystem, opts ...Option) *Matcher {
	m := &Matcher{
		fs: fs,
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.changeDetector == nil {
		m.changeDetector = newModtimeChecker()
	}
	return m
}

// Load and parse a file. The returned error may be of type *ParseError in
// which case a file was loaded from disk but the patterns could not be
// parsed. In this case the contents of the file are nonetheless available
// in the Lines() method.
func (m *Matcher) Load(file string) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	if m.changeDetector.Seen(m.fs, file) && !m.changeDetector.Changed() {
		return nil
	}

	fd, info, err := loadIgnoreFile(m.fs, file)
	if err != nil {
		m.parseLocked(&bytes.Buffer{}, file)
		return err
	}
	defer fd.Close()

	m.changeDetector.Reset()

	err = m.parseLocked(fd, file)
	// If we failed to parse, don't cache, as next time Load is called
	// we'll pretend it's all good.
	if err == nil {
		m.changeDetector.Remember(m.fs, file, info.ModTime())
	}
	return err
}

// Load and parse an io.Reader. See Load() for notes on the returned error.
func (m *Matcher) Parse(r io.Reader, file string) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.parseLocked(r, file)
}

func (m *Matcher) parseLocked(r io.Reader, file string) error {
	lines, patterns, err := parseIgnoreFile(m.fs, r, file, file, false, m.changeDetector, make(map[string]struct{}), nil)
	// Error is saved and returned at the end. We process the patterns
	// (possibly blank) anyway.

	m.lines = lines

	newHash := hashPatterns(patterns)
	if newHash == m.curHash {
		// We've already loaded exactly these patterns.
		return err
	}

	m.curHash = newHash
	m.patterns = patterns

	return err
}

// Match matches the patterns plus temporary and internal files.
//
// The "file" parameter must be in the OS' native unicode format (NFD on macos,
// NFC everywhere else). This is always the case in real usage in syncthing, as
// we ensure native unicode normalisation on all entry points (scanning and from
// protocol) - so no need to normalize when calling this, except e.g. in tests.
func (m *Matcher) Match(file string) ignoreresult.R {
	switch {
	case fs.IsTemporary(file):
		return ignoreresult.IgnoreAndSkip

	case fs.IsInternal(file):
		return ignoreresult.IgnoreAndSkip

	case file == ".":
		return ignoreresult.NotIgnored
	}

	m.mut.Lock()
	defer m.mut.Unlock()

	if len(m.patterns) == 0 {
		return ignoreresult.NotIgnored
	}

	// Change backslashes to slashes (on Windows only)
	file = filepath.ToSlash(file)

	// Check all the patterns for a match. Track whether the patterns so far
	// allow skipping matched directories or not. As soon as we hit an
	// exclude pattern (with some exceptions), we can't skip directories
	// anymore.
	var lowercaseFile string
	canSkipDir := true
	for _, pattern := range m.patterns {
		if canSkipDir && !pattern.allowsSkippingIgnoredDirs() {
			canSkipDir = false
		}

		res := pattern.result
		if canSkipDir {
			res = res.WithSkipDir()
		}
		if pattern.result.IsCaseFolded() {
			if lowercaseFile == "" {
				lowercaseFile = strings.ToLower(file)
			}
			if pattern.match.Match(lowercaseFile) {
				return res
			}
		} else if pattern.match.Match(file) {
			return res
		}
	}

	// Default to not matching.
	return ignoreresult.NotIgnored
}

// Lines return a list of the unprocessed lines in .stignore at last load
func (m *Matcher) Lines() []string {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.lines
}

// Patterns return a list of the loaded patterns, as they've been parsed
func (m *Matcher) Patterns() []string {
	m.mut.Lock()
	defer m.mut.Unlock()

	patterns := make([]string, len(m.patterns))
	for i, pat := range m.patterns {
		patterns[i] = pat.String()
	}
	return patterns
}

func (m *Matcher) String() string {
	return fmt.Sprintf("Matcher/%v@%p", m.Patterns(), m)
}

func (m *Matcher) Hash() string {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.curHash
}

func hashPatterns(patterns []Pattern) string {
	h := sha256.New()
	for _, pat := range patterns {
		h.Write([]byte(pat.String()))
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func loadIgnoreFile(ffs fs.Filesystem, file string) (fs.File, fs.FileInfo, error) {
	fd, err := ffs.OpenFile(file, fs.OptReadOnly|fs.OptFollow, 0o666)
	if err != nil {
		return fd, nil, err
	}

	info, err := fd.Stat()
	if err != nil {
		fd.Close()
	}

	return fd, info, err
}

func loadParseIncludeFile(filesystem fs.Filesystem, file, sourceFile string, cd ChangeDetector, linesSeen map[string]struct{}, coll *issueCollector) ([]Pattern, error) {
	// Allow escaping the folders filesystem.
	// TODO: Deprecate, somehow?
	if filesystem.Type() == fs.FilesystemTypeBasic {
		uri := filesystem.URI()
		joined := filepath.Join(uri, file)
		if !fs.IsParent(joined, uri) {
			filesystem = fs.NewFilesystem(filesystem.Type(), filepath.Dir(joined))
			file = filepath.Base(joined)
		}
	}

	if cd.Seen(filesystem, file) {
		return nil, errors.New("multiple include")
	}

	fd, info, err := loadIgnoreFile(filesystem, file)
	if err != nil {
		// isNotExist is considered "ok" in a sense of that a folder doesn't have to act
		// upon it. This is because it is allowed for .stignore to not exist. However,
		// included ignore files are not allowed to be missing and these errors should be
		// acted upon on. So we don't preserve the error chain here and manually set an
		// error instead, if the file is missing.
		if fs.IsNotExist(err) {
			err = errors.New("file not found")
		}
		return nil, err
	}
	defer fd.Close()

	cd.Remember(filesystem, file, info.ModTime())

	_, patterns, err := parseIgnoreFile(filesystem, fd, file, sourceFile, true, cd, linesSeen, coll)
	if coll != nil && err != nil {
		// The issues have already been recorded inside the included file by
		// the collector; signal that to the caller instead of making it
		// report the failure a second time.
		return patterns, errIssuesCollected
	}
	return patterns, err
}

func parseLine(line string) ([]Pattern, error) {
	// We use native normalization internally, thus the patterns must match
	// that to avoid false negative matches.
	line = nativeUnicodeNorm(line)

	pattern := Pattern{
		result: ignoreresult.Ignored,
	}

	// Allow prefixes to be specified in any order, but only once.
	var seenPrefix [3]bool

	for {
		if strings.HasPrefix(line, "!") && !seenPrefix[0] {
			seenPrefix[0] = true
			line = line[1:]
			pattern.result = pattern.result.ToggleIgnored()
		} else if strings.HasPrefix(line, "(?i)") && !seenPrefix[1] {
			seenPrefix[1] = true
			pattern.result = pattern.result.WithFoldCase()
			line = line[4:]
		} else if strings.HasPrefix(line, "(?d)") && !seenPrefix[2] {
			seenPrefix[2] = true
			pattern.result = pattern.result.WithDeletable()
			line = line[4:]
		} else {
			break
		}
	}

	if line == "" {
		return nil, parseError(errors.New("missing pattern"))
	}

	if pattern.result.IsCaseFolded() {
		line = strings.ToLower(line)
	}

	pattern.pattern = line

	var err error
	if strings.HasPrefix(line, "/") {
		// Pattern is rooted in the current dir only
		pattern.match, err = glob.Compile(line[1:], '/')
		return []Pattern{pattern}, parseError(err)
	}
	patterns := make([]Pattern, 2)
	if strings.HasPrefix(line, "**/") {
		// Add the pattern as is, and without **/ so it matches in current dir
		pattern.match, err = glob.Compile(line, '/')
		if err != nil {
			return nil, parseError(err)
		}
		patterns[0] = pattern

		line = line[3:]
		if line == "" {
			// Pattern was exactly "**/", already covered by patterns[0].
			return patterns[:1], nil
		}
		pattern.pattern = line
		pattern.match, err = glob.Compile(line, '/')
		if err != nil {
			return nil, parseError(err)
		}
		patterns[1] = pattern
		return patterns, nil
	}
	// Path name or pattern, add it so it matches files both in
	// current directory and subdirs.
	pattern.match, err = glob.Compile(line, '/')
	if err != nil {
		return nil, parseError(err)
	}
	patterns[0] = pattern

	line = "**/" + line
	pattern.pattern = line
	pattern.match, err = glob.Compile(line, '/')
	if err != nil {
		return nil, parseError(err)
	}
	patterns[1] = pattern
	return patterns, nil
}

func nativeUnicodeNorm(s string) string {
	if build.IsDarwin || build.IsIOS {
		return norm.NFD.String(s)
	}
	return norm.NFC.String(s)
}

// errIssuesCollected is returned (in collection mode only) to signal that one
// or more issues were recorded in the collector while parsing. It must never
// reach the public API; strict mode always returns the first concrete error.
var errIssuesCollected = errors.New("issues collected while parsing ignore file")

func parseIgnoreFile(filesystem fs.Filesystem, fd io.Reader, currentFile, sourceFile string, viaInclude bool, cd ChangeDetector, linesSeen map[string]struct{}, coll *issueCollector) ([]string, []Pattern, error) {
	var patterns []Pattern

	addPattern := func(line, rawLine string, lineNo int) error {
		newPatterns, err := parseLine(line)
		if err != nil {
			return fmt.Errorf("invalid pattern %q in ignore file: %w", line, err)
		}
		src := ruleSource{
			file:       sourceFile,
			line:       lineNo,
			text:       rawLine,
			viaInclude: viaInclude,
		}
		for i := range newPatterns {
			newPatterns[i].src = src
		}
		patterns = append(patterns, newPatterns...)
		return nil
	}

	scanner := bufio.NewScanner(fd)
	var lines []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	escapeChar := defaultEscapeChar

	var err error
	escapePrefixSeen := false
	includedPatterns := 0
	for idx, rawLine := range lines {
		// 1-based line number as it appears in the file on disk.
		lineNo := idx + 1
		line := rawLine

		if strings.HasPrefix(line, escapePrefix) {
			var escapeErr error
			switch {
			case escapePrefixSeen:
				escapeErr = errors.New("mutiple #escape= lines found in ignore file")
			case len(patterns)-includedPatterns > 0:
				escapeErr = errors.New("#escape= line found after patterns in ignore file")
			default:
				escapePrefixSeen = true
				trimmed := strings.TrimSpace(strings.TrimPrefix(line, escapePrefix))
				before, esc, ok := strings.Cut(trimmed, "=")
				if ok && before == "" {
					esc = strings.TrimSpace(esc)
					// avoids allocation of a new slice.
					if utf8.RuneCountInString(esc) == 1 {
						escapeChar, _ = utf8.DecodeRuneInString(esc)
						continue
					}
				}
				escapeErr = fmt.Errorf("failed to parse #escape= line in ignore file: %q", line)
			}
			if coll == nil {
				return nil, nil, escapeErr
			}
			coll.addError(sourceFile, lineNo, escapeErr.Error())
			continue
		}

		if _, ok := linesSeen[line]; ok {
			// Blank lines and comments are silently deduplicated in strict
			// mode as well; a repeated actual rule is worth a warning.
			if coll != nil && line != "" && !strings.HasPrefix(line, "//") {
				coll.addWarning(sourceFile, lineNo, fmt.Sprintf("duplicate rule %q, ignored", line))
			}
			continue
		}
		linesSeen[line] = struct{}{}
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "//"):
			continue
		}

		if escapeChar != '\\' {
			// ToSlash changes backslashes to forward slashes on Windows only,
			// so we only need to do this, if escapeChar is not a backslash.
			// If escapeChar is a backslash, then the user is using forward
			// slashes for path separators, and we leave backslashes alone.
			line = filepath.ToSlash(line)
			// Replace all escapeChars with backslashes
			line = strings.ReplaceAll(line, string(escapeChar), `\`)
			// Now restore double escapeChars to actually escape the escapeChar.
			line = strings.ReplaceAll(line, `\\`, `\`+string(escapeChar))
		}

		switch {
		case strings.HasPrefix(line, "#include"):
			fields := strings.SplitN(line, " ", 2)
			if len(fields) != 2 {
				err = parseError(errors.New("failed to parse #include line: no file?"))
				break
			}

			includeRel := strings.TrimSpace(fields[1])
			if includeRel == "" {
				err = parseError(errors.New("failed to parse #include line: no file?"))
				break
			}

			includeFile := filepath.Join(filepath.Dir(currentFile), includeRel)
			var includePatterns []Pattern
			includePatterns, incErr := loadParseIncludeFile(filesystem, includeFile, includeFile, cd, linesSeen, coll)
			switch {
			case incErr == nil || coll != nil && errors.Is(incErr, errIssuesCollected):
				// Successful, or issues within the included file were
				// already recorded there. In the latter case we still
				// keep whatever patterns parsed successfully.
				patterns = append(patterns, includePatterns...)
				includedPatterns += len(includePatterns)
			case coll != nil:
				// Load level failure (missing file, include cycle): pin it
				// to the #include line in the current file.
				coll.addError(sourceFile, lineNo, fmt.Sprintf("failed to load include file %s: %v", includeFile, incErr))
			default:
				// Wrap the error, as if the include does not exist, we get a
				// IsNotExists(err) == true error, which we use to check
				// existence of the .stignore file, and just end up assuming
				// there is none, rather than a broken include.
				err = parseError(fmt.Errorf("failed to load include file %s: %w", includeFile, incErr))
			}
		case strings.HasSuffix(line, "/**"):
			err = addPattern(line, rawLine, lineNo)
		case strings.HasSuffix(line, "/"):
			err = addPattern(line+"**", rawLine, lineNo)
		default:
			err = addPattern(line, rawLine, lineNo)
			if err == nil {
				err = addPattern(line+"/**", rawLine, lineNo)
			}
		}
		if err != nil {
			if coll == nil {
				return lines, nil, err
			}
			coll.addError(sourceFile, lineNo, err.Error())
			continue
		}
	}

	if coll != nil && coll.hasErrors() {
		return lines, patterns, errIssuesCollected
	}
	return lines, patterns, nil
}

// WriteIgnores is a convenience function to avoid code duplication
func WriteIgnores(filesystem fs.Filesystem, path string, content []string) error {
	if len(content) == 0 {
		err := filesystem.Remove(path)
		if fs.IsNotExist(err) {
			return nil
		}
		return err
	}

	fd, err := osutil.CreateAtomicFilesystem(filesystem, path)
	if err != nil {
		return err
	}

	wr := osutil.LineEndingsWriter(fd)
	for _, line := range content {
		fmt.Fprintln(wr, line)
	}

	if err := fd.Close(); err != nil {
		return err
	}
	filesystem.Hide(path)

	return nil
}

type modtimeCheckerKey struct {
	fs   fs.Filesystem
	name string
}

// modtimeChecker is the default implementation of ChangeDetector
type modtimeChecker struct {
	modtimes map[modtimeCheckerKey]time.Time
}

func newModtimeChecker() *modtimeChecker {
	return &modtimeChecker{
		modtimes: map[modtimeCheckerKey]time.Time{},
	}
}

func (c *modtimeChecker) Remember(fs fs.Filesystem, name string, modtime time.Time) {
	c.modtimes[modtimeCheckerKey{fs, name}] = modtime
}

func (c *modtimeChecker) Seen(fs fs.Filesystem, name string) bool {
	_, ok := c.modtimes[modtimeCheckerKey{fs, name}]
	return ok
}

func (c *modtimeChecker) Reset() {
	c.modtimes = map[modtimeCheckerKey]time.Time{}
}

func (c *modtimeChecker) Changed() bool {
	for key, modtime := range c.modtimes {
		info, err := key.fs.Stat(key.name)
		if err != nil {
			return true
		}
		if !info.ModTime().Equal(modtime) {
			return true
		}
	}

	return false
}
