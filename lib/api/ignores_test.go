// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/model"
	modelmocks "github.com/syncthing/syncthing/lib/model/mocks"
)

func TestDBIgnoresMatch(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	m.DiagnoseIgnoresReturns(ignore.Diagnosis{
		Ignored: true,
		SkipDir: true,
		Chain: []ignore.RuleHit{{
			Line:      2,
			Text:      "foo",
			Source:    ".stignore",
			Effective: true,
		}},
	}, nil)

	baseURL := startHTTPWithModel(t, apiCfg, m, 0)

	resp := httpGet(baseURL+"/rest/db/ignores/match?folder=default&path=foo", "", "", testAPIKey, "", nil, t)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	var res dbIgnoresMatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	if res.Folder != "default" || res.Path != "foo" {
		t.Errorf("unexpected request echo: %+v", res)
	}
	if !res.Ignored || !res.SkipDir || len(res.Chain) != 1 {
		t.Errorf("unexpected diagnosis: %+v", res)
	}
	if chain := res.Chain[0]; chain.Text != "foo" || chain.Source != ".stignore" || chain.Line != 2 || !chain.Effective {
		t.Errorf("unexpected chain entry: %+v", chain)
	}
}

func TestDBIgnoresValidate(t *testing.T) {
	t.Parallel()

	m := new(modelmocks.Model)
	m.ValidateIgnoresReturns(model.IgnoreValidation{
		Valid:    false,
		Errors:   []ignore.Issue{{File: ".stignore", Line: 1, Message: "parse error: bad pattern"}},
		Warnings: []ignore.Issue{},
		Impact: model.IgnoreImpact{
			NewlyIgnoredCount:   2,
			NewlyIgnoredSamples: []string{"a", "b"},
		},
	}, nil)

	baseURL := startHTTPWithModel(t, apiCfg, m, 0)

	resp := httpRequest(http.MethodPost, baseURL+"/rest/db/ignores/validate?folder=default",
		map[string][]string{"ignore": {"bad["}}, "", "", testAPIKey, "", "", "", nil, t)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}

	var res dbIgnoresValidateResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	if res.Valid || len(res.Errors) != 1 {
		t.Errorf("unexpected validation: %+v", res)
	}
	if res.Errors[0].Line != 1 || res.Errors[0].File != ".stignore" {
		t.Errorf("unexpected error: %+v", res.Errors[0])
	}
	if res.Impact.NewlyIgnoredCount != 2 || len(res.Impact.NewlyIgnoredSamples) != 2 {
		t.Errorf("unexpected impact: %+v", res.Impact)
	}

	// Validation must never write ignores.
	if m.SetIgnoresCallCount() != 0 {
		t.Error("validation must not call SetIgnores")
	}
}
