// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

import (
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
)

func TestDiskWatermarkConfigurationValidate(t *testing.T) {
	cases := []struct {
		name string
		dwc  DiskWatermarkConfiguration
		ok   bool
	}{
		{
			name: "disabled is valid with unset watermarks",
			dwc:  DiskWatermarkConfiguration{Enabled: false},
			ok:   true,
		},
		{
			name: "defaults",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 1, Unit: "%"},
				ResumeFree:     Size{Value: 3, Unit: "%"},
				CheckIntervalS: 30,
			},
			ok: true,
		},
		{
			name: "absolute sizes",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 1, Unit: "G"},
				ResumeFree:     Size{Value: 2, Unit: "G"},
				CheckIntervalS: 1,
			},
			ok: true,
		},
		{
			name: "equal watermarks are allowed",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 5, Unit: "%"},
				ResumeFree:     Size{Value: 5, Unit: "%"},
				CheckIntervalS: 10,
			},
			ok: true,
		},
		{
			name: "hundred percent is allowed",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 100, Unit: "%"},
				ResumeFree:     Size{Value: 100, Unit: "%"},
				CheckIntervalS: 10,
			},
			ok: true,
		},
		{
			name: "zero min free",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 0, Unit: "%"},
				ResumeFree:     Size{Value: 3, Unit: "%"},
				CheckIntervalS: 30,
			},
			ok: false,
		},
		{
			name: "negative resume free",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 1, Unit: "%"},
				ResumeFree:     Size{Value: -1, Unit: "%"},
				CheckIntervalS: 30,
			},
			ok: false,
		},
		{
			name: "percentage above 100",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 1, Unit: "%"},
				ResumeFree:     Size{Value: 101, Unit: "%"},
				CheckIntervalS: 30,
			},
			ok: false,
		},
		{
			name: "mismatched kinds",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 1, Unit: "%"},
				ResumeFree:     Size{Value: 3, Unit: "G"},
				CheckIntervalS: 30,
			},
			ok: false,
		},
		{
			name: "resume below min",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 5, Unit: "%"},
				ResumeFree:     Size{Value: 4, Unit: "%"},
				CheckIntervalS: 30,
			},
			ok: false,
		},
		{
			name: "zero interval",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 1, Unit: "%"},
				ResumeFree:     Size{Value: 3, Unit: "%"},
				CheckIntervalS: 0,
			},
			ok: false,
		},
		{
			name: "unknown absolute unit",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 1, Unit: "X"},
				ResumeFree:     Size{Value: 3, Unit: "X"},
				CheckIntervalS: 30,
			},
			ok: false,
		},
		{
			name: "nonsense percentage unit",
			dwc: DiskWatermarkConfiguration{
				Enabled:        true,
				MinFree:        Size{Value: 1, Unit: "K%"},
				ResumeFree:     Size{Value: 3, Unit: "K%"},
				CheckIntervalS: 30,
			},
			ok: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.dwc.Validate()
			if tc.ok && err != nil {
				t.Errorf("unexpected error: %v", err)
			} else if !tc.ok && err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

func TestDiskWatermarkConfigurationRejectedByWrapper(t *testing.T) {
	w := wrap("/dev/null", New(device1), device1)
	defer w.stop()

	before := w.RawCopy()

	_, err := w.Modify(func(cfg *Configuration) {
		cfg.Options.DiskWatermark = DiskWatermarkConfiguration{
			Enabled:        true,
			MinFree:        Size{Value: 5, Unit: "%"},
			ResumeFree:     Size{Value: 4, Unit: "%"},
			CheckIntervalS: 30,
		}
	})
	if err == nil {
		t.Fatal("invalid watermark config should have been rejected")
	}

	after := w.RawCopy()
	if after.Options.DiskWatermark.Enabled {
		t.Fatal("invalid watermark config should not have been committed")
	}
	if after.Version != before.Version {
		t.Fatal("config should not have changed")
	}
}

func TestFreeSpaceCompare(t *testing.T) {
	cases := []struct {
		name      string
		watermark Size
		usage     fs.Usage
		cmp       int
		wantErr   bool
	}{
		{"zero watermark is exceeded", Size{}, fs.Usage{Free: 0, Total: 100}, 1, false},
		{"absolute below", Size{Value: 100, Unit: ""}, fs.Usage{Free: 99, Total: 1000}, -1, false},
		{"absolute equal", Size{Value: 100, Unit: ""}, fs.Usage{Free: 100, Total: 1000}, 0, false},
		{"absolute above", Size{Value: 100, Unit: ""}, fs.Usage{Free: 101, Total: 1000}, 1, false},
		{"percentage below", Size{Value: 10, Unit: "%"}, fs.Usage{Free: 99, Total: 1000}, -1, false},
		{"percentage equal", Size{Value: 10, Unit: "%"}, fs.Usage{Free: 100, Total: 1000}, 0, false},
		{"percentage above", Size{Value: 10, Unit: "%"}, fs.Usage{Free: 101, Total: 1000}, 1, false},
		{"percentage unknown total", Size{Value: 10, Unit: "%"}, fs.Usage{Free: 100, Total: 0}, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			below, err := FreeSpaceBelow(tc.watermark, tc.usage)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			above, err := FreeSpaceAbove(tc.watermark, tc.usage)
			if err != nil {
				t.Fatal(err)
			}
			var cmp int
			switch {
			case below:
				cmp = -1
			case above:
				cmp = 1
			default:
				cmp = 0
			}
			if cmp != tc.cmp {
				t.Errorf("cmp=%d want %d", cmp, tc.cmp)
			}
		})
	}
}
