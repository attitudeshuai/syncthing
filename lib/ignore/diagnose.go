// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package ignore

import (
	"path/filepath"
	"strings"

	"github.com/syncthing/syncthing/lib/fs"
)

// RuleHit describes one source rule that matched the diagnosed path.
type RuleHit struct {
	// Line is the 1-based line number of the rule in Source.
	Line int `json:"line"`
	// Text is the rule as written on that line.
	Text string `json:"text"`
	// Source is the ignore file the rule was read from (".stignore" or an
	// included file).
	Source string `json:"source"`
	// FromInclude is true when the rule was introduced through an #include.
	FromInclude bool `json:"fromInclude"`
	// Negated is true for rules with a "!" prefix.
	Negated bool `json:"negated"`
	// CaseFolded is true for rules marked "(?i)".
	CaseFolded bool `json:"caseFolded"`
	// Deletable is true for rules marked "(?d)".
	Deletable bool `json:"deletable"`
	// Effective marks the single rule that actually decides the verdict.
	// Rules are resolved on first match, so later matching rules are
	// reported with Effective=false even though they also match the path.
	Effective bool `json:"effective"`
}

// Diagnosis is the verdict for one path together with the chain of rules
// that led to it. When no rule matches, Chain is empty and Ignored is
// false, meaning the path is synced.
type Diagnosis struct {
	// Ignored is the final verdict: true when the path is ignored, false
	// when it is synced.
	Ignored bool `json:"ignored"`
	// SkipDir is true when a directory at this path would be skipped
	// wholesale (descended into), as computed by the matcher.
	SkipDir bool `json:"skipDir"`
	// CaseFolded is true when the deciding match used case folding.
	CaseFolded bool `json:"caseFolded"`
	// Deletable is true when the deciding rule marks the path deletable.
	Deletable bool `json:"deletable"`
	// Reason names a built-in non-pattern reason for the verdict
	// ("temporary" or "internal"); empty when patterns decided.
	Reason string `json:"reason,omitempty"`
	// PathExists reports whether the path could be stat'd on the folder
	// filesystem.
	PathExists bool `json:"pathExists"`
	// IsDir reports whether the existing path is a directory.
	IsDir bool `json:"isDir"`
	// Chain is the ordered list of source rules matching the path; the
	// first entry (if any) is the effective one.
	Chain []RuleHit `json:"chain"`
}

// Diagnose determines whether the given path is ignored using exactly the
// same matching logic as Match, without changing any matcher state, and
// additionally records the source rules that matched. The path is expected
// relative to the folder root, in the OS' native unicode normalization (as
// produced by scanning).
func (m *Matcher) Diagnose(file string) Diagnosis {
	var d Diagnosis
	d.Chain = []RuleHit{}

	// Normalize like the scan entry points do, so that results match what
	// the scanner sees. Match itself assumes already-normalized input.
	normFile := nativeUnicodeNorm(file)
	m.describeFile(normFile, &d)

	switch {
	case fs.IsTemporary(file):
		d.Ignored = true
		d.SkipDir = true
		d.Reason = "temporary"
		return d
	case fs.IsInternal(file):
		d.Ignored = true
		d.SkipDir = true
		d.Reason = "internal"
		return d
	case file == ".":
		return d
	}

	// Change backslashes to slashes (on Windows only), same as Match.
	matchPath := filepath.ToSlash(normFile)

	m.mut.Lock()
	defer m.mut.Unlock()

	var lowercasePath string
	canSkipDir := true
	effectiveAssigned := false

	for _, p := range m.patterns {
		if canSkipDir && !p.allowsSkippingIgnoredDirs() {
			canSkipDir = false
		}

		matched := false
		if p.result.IsCaseFolded() {
			if lowercasePath == "" {
				lowercasePath = strings.ToLower(matchPath)
			}
			matched = p.match.Match(lowercasePath)
		} else {
			matched = p.match.Match(matchPath)
		}
		if !matched {
			continue
		}

		if len(d.Chain) > 0 && d.Chain[len(d.Chain)-1].Source == p.src.file && d.Chain[len(d.Chain)-1].Line == p.src.line {
			// Further expansion of the same source line; already reported.
			continue
		}

		hit := RuleHit{
			Line:        p.src.line,
			Text:        p.src.text,
			Source:      p.src.file,
			FromInclude: p.src.viaInclude,
			Negated:     !p.result.IsIgnored(),
			CaseFolded:  p.result.IsCaseFolded(),
			Deletable:   p.result.IsDeletable(),
			Effective:   !effectiveAssigned,
		}
		d.Chain = append(d.Chain, hit)

		if !effectiveAssigned {
			effectiveAssigned = true
			res := p.result
			if canSkipDir {
				res = res.WithSkipDir()
			}
			d.Ignored = res.IsIgnored()
			d.SkipDir = res.CanSkipDir()
			d.CaseFolded = res.IsCaseFolded()
			d.Deletable = res.IsDeletable()
		}
	}

	return d
}

// describeFile fills in the path existence/type fields by stat'ing the
// filesystem; failures just leave the defaults.
func (m *Matcher) describeFile(normPath string, d *Diagnosis) {
	if info, err := m.fs.Stat(filepath.FromSlash(normPath)); err == nil {
		d.PathExists = true
		d.IsDir = info.IsDir()
	}
}
