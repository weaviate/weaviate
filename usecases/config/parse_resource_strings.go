//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// parseResourceEnv rejects rather than defaulting, which would silently behave as unset.
func parseResourceEnv(envName, value string) (int64, error) {
	bytes, err := parseResourceString(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %q: %w", envName, value, err)
	}
	return bytes, nil
}

// MaxQueryBitmapBufsMemory is a coarse, deliberately conservative stand-in for a
// bound on what the buffer pool actually allocates, not a statement about how
// much memory is sensible to give it. The budget sizes one channel slot per
// pooled buffer it affords. At the default QUERY_BITMAP_BUFS_MAX_BUF_SIZE of
// 32MiB, math.MaxInt64 builds 709362340502 slots across all classes, ~5.16TiB
// of backing array, before the pool holds a single buffer. The real cliff is
// deep in math.MaxInt64 territory rather than here: at that same 32MiB, 1024TiB
// still only reaches ~661MiB of slots. Both figures assume that buffer size and
// grow as it shrinks: at 1MiB+1, the smallest one that builds an in-memory
// class, they are ~64TiB and ~8GiB. Everything between this ceiling and that
// cliff is refused for want of a bound on the derived allocation, which is
// tracked separately. It applies to the budget alone; see parseBitmapBufsSize.
const MaxQueryBitmapBufsMemory = 1 << 40

// parseBitmapBufsSize accepts any byte count, 0 included: a size that affords no
// in-memory class leaves the buffer pool its sync tiers, which warns and serves,
// so it degrades the pool rather than the node. No ceiling applies here because
// the budget bounds the allocation whatever this size is: the slots allocated
// come to at most the budget divided by the smallest class built, so they peak
// in the small direction, just above the 1MiB sync ceiling, not the large one.
// At 1MiB+1 with the budget at its 1TiB ceiling that is 1048575 slots, ~8MiB of
// backing array. A larger size only enumerates more classes, and the budget
// breaks that enumeration as soon as they stop fitting, so it yields more
// classes of a smaller limit each, never one that explodes. Only the unlimited
// sentinel is refused, because math.MaxInt64 is not a buffer size.
func parseBitmapBufsSize(envName, value string) (int, error) {
	bytes, err := parseResourceEnv(envName, value)
	if err != nil {
		return 0, err
	}
	if bytes == math.MaxInt64 {
		return 0, fmt.Errorf("%s: %q is not a size: this variable sizes a buffer pool, "+
			"so give a byte count such as 128MiB", envName, value)
	}
	return int(bytes), nil
}

// parseBitmapBufsMemory adds the budget-only ceiling to parseBitmapBufsSize.
func parseBitmapBufsMemory(envName, value string) (int, error) {
	bytes, err := parseBitmapBufsSize(envName, value)
	if err != nil {
		return 0, err
	}
	if bytes > MaxQueryBitmapBufsMemory {
		return 0, fmt.Errorf("%s: %q must be at most %d bytes, got %d",
			envName, value, int64(MaxQueryBitmapBufsMemory), bytes)
	}
	return bytes, nil
}

// parseResourceString takes a string like "1024", "1KiB", "43TiB" and converts it to an integer number of bytes.
func parseResourceString(resource string) (int64, error) {
	resource = strings.TrimSpace(resource)

	if strings.EqualFold(resource, "unlimited") || strings.EqualFold(resource, "nolimit") {
		return math.MaxInt64, nil
	}

	// Find where the digits end
	lastDigit := len(resource)
	for i, r := range resource {
		if r < '0' || r > '9' {
			lastDigit = i
			break
		}
	}

	// Split the numeric part and the unit
	number, unit := resource[:lastDigit], resource[lastDigit:]
	unit = strings.TrimSpace(unit) // Clean up any surrounding whitespace
	value, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return 0, err
	}

	unitMultipliers := map[string]int64{
		"":    1, // No unit means bytes
		"B":   1,
		"KiB": 1024,
		"MiB": 1024 * 1024,
		"GiB": 1024 * 1024 * 1024,
		"TiB": 1024 * 1024 * 1024 * 1024,
		"KB":  1000,
		"MB":  1000 * 1000,
		"GB":  1000 * 1000 * 1000,
		"TB":  1000 * 1000 * 1000 * 1000,
	}
	multiplier, exists := unitMultipliers[unit]
	if !exists {
		return 0, fmt.Errorf("invalid or unsupported unit %q", unit)
	}

	if value > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("%q overflows int64", resource)
	}
	return value * multiplier, nil
}
