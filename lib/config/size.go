// Copyright (C) 2017 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/syncthing/syncthing/lib/fs"
)

type Size struct {
	Value float64 `json:"value" xml:",chardata"`
	Unit  string  `json:"unit" xml:"unit,attr"`
}

func ParseSize(s string) (Size, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Size{}, nil
	}

	var num, unit string
	for i := 0; i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.' || s[i] == ','); i++ {
		num = s[:i+1]
	}
	i := len(num)
	for i < len(s) && s[i] == ' ' {
		i++
	}
	unit = s[i:]

	val, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return Size{}, err
	}

	return Size{val, unit}, nil
}

func (s Size) BaseValue() float64 {
	unitPrefix := s.Unit
	if len(unitPrefix) > 1 {
		unitPrefix = unitPrefix[:1]
	}

	mult := 1.0
	switch unitPrefix {
	case "k", "K":
		mult = 1000
	case "m", "M":
		mult = 1000 * 1000
	case "g", "G":
		mult = 1000 * 1000 * 1000
	case "t", "T":
		mult = 1000 * 1000 * 1000 * 1000
	}

	return s.Value * mult
}

func (s Size) Percentage() bool {
	return strings.Contains(s.Unit, "%")
}

func (s Size) String() string {
	return fmt.Sprintf("%v %s", s.Value, s.Unit)
}

func (s *Size) ParseDefault(str string) error {
	sz, err := ParseSize(str)
	*s = sz
	return err
}

// CheckFreeSpace checks that the free space does not fall below the minimum required free space.
func CheckFreeSpace(minFree Size, usage fs.Usage) error {
	val := minFree.BaseValue()
	if val <= 0 {
		return nil
	}

	if minFree.Percentage() {
		freePct := (float64(usage.Free) / float64(usage.Total)) * 100
		if freePct < val {
			return fmt.Errorf("current %.2f %% < required %v", freePct, minFree)
		}
	} else if float64(usage.Free) < val {
		return fmt.Errorf("current %sB < required %v", formatSI(usage.Free), minFree)
	}

	return nil
}

// checkAvailableSpace checks that the free space does not fall below the minimum
// required free space, considering additional required space for a future operation.
func checkAvailableSpace(req uint64, minFree Size, usage fs.Usage) error {
	if usage.Free < req {
		return fmt.Errorf("current %sB < required %sB", formatSI(usage.Free), formatSI(req))
	}
	usage.Free -= req
	return CheckFreeSpace(minFree, usage)
}

// FreeSpaceBelow reports whether the available free space is strictly below
// the given watermark. A non-positive watermark never trips. Percentage
// watermarks require the total size to be known.
func FreeSpaceBelow(watermark Size, usage fs.Usage) (bool, error) {
	cmp, err := compareFreeSpace(watermark, usage)
	if err != nil {
		return false, err
	}
	return cmp < 0, nil
}

// FreeSpaceAbove reports whether the available free space is strictly above
// the given watermark. A non-positive watermark is always exceeded.
// Percentage watermarks require the total size to be known.
func FreeSpaceAbove(watermark Size, usage fs.Usage) (bool, error) {
	cmp, err := compareFreeSpace(watermark, usage)
	if err != nil {
		return false, err
	}
	return cmp > 0, nil
}

// compareFreeSpace compares the free space against the watermark and returns
// -1 when below, 0 when equal and 1 when above.
func compareFreeSpace(watermark Size, usage fs.Usage) (int, error) {
	val := watermark.BaseValue()
	if val <= 0 {
		return 1, nil
	}

	var current float64
	if watermark.Percentage() {
		if usage.Total == 0 {
			return 0, fmt.Errorf("can't evaluate percentage watermark %v without total disk size", watermark)
		}
		current = float64(usage.Free) / float64(usage.Total) * 100
	} else {
		current = float64(usage.Free)
	}

	switch {
	case current < val:
		return -1, nil
	case current > val:
		return 1, nil
	default:
		return 0, nil
	}
}

func formatSI(b uint64) string {
	switch {
	case b < 1000:
		return fmt.Sprintf("%d ", b)
	case b < 1000*1000:
		return fmt.Sprintf("%.1f K", float64(b)/1000)
	case b < 1000*1000*1000:
		return fmt.Sprintf("%.1f M", float64(b)/(1000*1000))
	case b < 1000*1000*1000*1000:
		return fmt.Sprintf("%.1f G", float64(b)/(1000*1000*1000))
	default:
		return fmt.Sprintf("%.1f T", float64(b)/(1000*1000*1000*1000))
	}
}
