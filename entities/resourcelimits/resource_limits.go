//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package resourcelimits

import (
	"fmt"
	"strconv"
	"strings"
)

func ParseMemLimit(goMemLimit string) (int64, error) {
	val, err := func() (int64, error) {
		switch {
		case strings.HasSuffix(goMemLimit, "GiB"):
			limit, err := strconv.ParseInt(strings.TrimSuffix(goMemLimit, "GiB"), 10, 64)
			if err != nil {
				return 0, err
			}
			return limit * 1024 * 1024 * 1024, nil
		case strings.HasSuffix(goMemLimit, "MiB"):
			limit, err := strconv.ParseInt(strings.TrimSuffix(goMemLimit, "MiB"), 10, 64)
			if err != nil {
				return 0, err
			}
			return limit * 1024 * 1024, nil
		default:
			// Assume bytes if no unit is specified
			return strconv.ParseInt(goMemLimit, 10, 64)
		}
	}()
	if err != nil {
		return 0, fmt.Errorf("invalid memory limit format. Acceptable values are: XXXGiB, YYYMiB or ZZZ in bytes without a unit. Got: %s: %w", goMemLimit, err)
	}
	return val, nil
}
