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

// MaxQueryBitmapBufsSize bounds QUERY_BITMAP_BUFS_MAX_MEMORY and
// QUERY_BITMAP_BUFS_MAX_BUF_SIZE. Both size an in-process buffer pool whose
// per-size-class buffer counts are derived by dividing the value, so an
// unbounded setting asks the runtime for channels with ~10^11 slots at boot.
// No machine has more than 1 TiB to give a bitmap pool, so a larger value is
// a typo and is better rejected than acted on.
const MaxQueryBitmapBufsSize = 1 << 40

func parseBitmapBufsSize(envName, value string) (int, error) {
	bytes, err := parseResourceString(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envName, err)
	}
	if bytes <= 0 || bytes > MaxQueryBitmapBufsSize {
		return 0, fmt.Errorf("%s: must be between 1 and %d bytes, got %d",
			envName, int64(MaxQueryBitmapBufsSize), bytes)
	}
	return int(bytes), nil
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
		return 0, fmt.Errorf("invalid or unsupported unit")
	}

	if value > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("%q overflows int64", resource)
	}
	return value * multiplier, nil
}
