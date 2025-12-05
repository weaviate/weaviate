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

package concurrency

import (
	"math"
	"runtime"
)

// Use runtime.GOMAXPROCS instead of runtime.NumCPU because NumCPU returns
// the physical CPU cores. However, in a containerization context, that might
// not be what we want. The physical node could have 128 cores, but we could
// be cgroup-limited to 2 cores. In that case, we want 2 to be our limit, not
// 128. It isn't guaranteed that MAXPROCS reflects the cgroup limit, but at
// least there is a chance that it was set correctly. If not, it defaults to
// NumCPU anyway, so we're not any worse off.
var (
	GOMAXPROCS   = runtime.GOMAXPROCS(0)
	GOMAXPROCSx2 = GOMAXPROCS * 2
	GOMAXPROCS_2 = max(GOMAXPROCS/2, 1)

	SROAR_MERGE = GOMAXPROCS_2
)

func NoMoreThanGOMAXPROCS(conc int) int {
	if conc > GOMAXPROCS || conc <= 0 {
		return GOMAXPROCS
	}
	return conc
}

// TimesGOMAXPROCS calculate number of gorutines based on GOMAXPROCS and given factor.
// Negative factors are interpreted as fractions. Result is rounded down, min returned result is 1.
// Examples for factors:
// * -3: GOMAXPROCS/3
// * -2: GOMAXPROCS/2
// * -1, 0, 1: GOMAXPROCS
// * 2: GOMAXPROCS*2
// * 3: GOMAXPROCS*3
func TimesGOMAXPROCS(factor int) int {
	return timesGOMAXPROCS(factor, GOMAXPROCS)
}

func timesGOMAXPROCS(factor int, gomaxprocs int) int {
	if factor >= -1 && factor <= 1 {
		return gomaxprocs
	}
	if factor > 1 {
		return gomaxprocs * factor
	}
	if n := gomaxprocs / -factor; n > 0 {
		return n
	}
	return 1
}

// TimesFloatGOMAXPROCS calculate number of gorutines based on GOMAXPROCS and given factor greater or equal to 0.
func TimesFloatGOMAXPROCS(factor float64) int {
	return timesFloatGOMAXPROCS(factor, GOMAXPROCS)
}

func timesFloatGOMAXPROCS(factor float64, gomaxprocs int) int {
	if factor <= 0 {
		return gomaxprocs
	}

	return int(math.Max(1, math.Round(factor*float64(gomaxprocs))))
}

func FractionOf(original, factor int) int {
	if factor <= 0 {
		return original
	}

	result := original / factor
	if result < 1 {
		return 1
	}
	return result
}
