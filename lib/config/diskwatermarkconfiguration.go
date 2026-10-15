// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

import (
	"fmt"
	"math"
	"strings"
)

// DiskWatermarkConfiguration configures the global data-disk free space
// watermark protection. When enabled, the free space on the disks backing
// running folders is checked periodically. Pulls that write data to a disk
// whose free space is below MinFree are suspended until the free space is
// above ResumeFree again.
type DiskWatermarkConfiguration struct {
	// Enabled controls whether the watermark protection is active. It
	// defaults to disabled.
	Enabled bool `json:"enabled" xml:"enabled,attr" default:"false"`
	// MinFree is the low watermark. When free space drops below it, pulls
	// to the affected disk get suspended.
	MinFree Size `json:"minFree" xml:"minFree" default:"1 %"`
	// ResumeFree is the high/recovery watermark. Suspended pulls resume
	// only once free space is above it. It must be at least MinFree and
	// use the same kind of unit (percentage or absolute size).
	ResumeFree Size `json:"resumeFree" xml:"resumeFree" default:"3 %"`
	// CheckIntervalS is how often the disks are checked, in seconds.
	CheckIntervalS int `json:"checkIntervalS" xml:"checkIntervalS" default:"30"`
}

// Validate checks the watermark configuration. It only returns an error when
// the protection is enabled, so a disabled configuration with unset
// watermarks remains valid.
func (dwc DiskWatermarkConfiguration) Validate() error {
	if !dwc.Enabled {
		return nil
	}

	if err := validateWatermarkSize("minFree", dwc.MinFree); err != nil {
		return err
	}
	if err := validateWatermarkSize("resumeFree", dwc.ResumeFree); err != nil {
		return err
	}

	if dwc.MinFree.Percentage() != dwc.ResumeFree.Percentage() {
		return fmt.Errorf("disk watermark: minFree %v and resumeFree %v must be of the same kind (both percentages or both absolute sizes)", dwc.MinFree, dwc.ResumeFree)
	}

	if dwc.ResumeFree.BaseValue() < dwc.MinFree.BaseValue() {
		return fmt.Errorf("disk watermark: resumeFree %v must not be below minFree %v", dwc.ResumeFree, dwc.MinFree)
	}

	if dwc.CheckIntervalS < 1 {
		return fmt.Errorf("disk watermark: checkIntervalS must be at least 1 second, got %d", dwc.CheckIntervalS)
	}

	return nil
}

func validateWatermarkSize(name string, s Size) error {
	if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
		return fmt.Errorf("disk watermark: %s has an invalid value %q", name, s)
	}
	if s.Value <= 0 {
		return fmt.Errorf("disk watermark: %s must be greater than zero, got %v", name, s)
	}

	unit := strings.TrimSpace(s.Unit)
	if s.Percentage() {
		// The size parser accepts oddities such as "200K%", but a
		// percentage watermark only makes sense as a plain percentage.
		if unit != "%" {
			return fmt.Errorf("disk watermark: %s has an invalid unit %q", name, s.Unit)
		}
		if s.Value > 100 {
			return fmt.Errorf("disk watermark: %s percentage must not exceed 100, got %v", name, s)
		}
		return nil
	}

	// Absolute sizes may be plain bytes or use one of the SI prefixes
	// accepted by Size.BaseValue; an unknown prefix would otherwise be
	// silently interpreted as bytes.
	if unit != "" {
		switch strings.ToUpper(unit[:1]) {
		case "K", "M", "G", "T":
		default:
			return fmt.Errorf("disk watermark: %s has an invalid unit %q", name, s.Unit)
		}
	}

	return nil
}
