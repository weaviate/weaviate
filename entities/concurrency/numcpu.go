//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package concurrency

import "runtime"

// Use runtime.GOMAXPROCS instead of runtime.NumCPU because NumCPU returns
// the physical CPU cores. However, in a containerization context, that might
// not be what we want. The physical node could have 128 cores, but we could
// be cgroup-limited to 2 cores. In that case, we want 2 to be our limit, not
// 128. It isn't guaranteed that MAXPROCS reflects the cgroup limit, but at
// least there is a chance that it was set correctly. If not, it defaults to
// NumCPU anyway, so we're not any worse off.
var (
	NUMCPU   = runtime.GOMAXPROCS(0)
	NUMCPUx2 = NUMCPU * 2
	NUMCPU_2 = NUMCPU / 2
)

func init() {
	if NUMCPU_2 == 0 {
		NUMCPU_2 = 1
	}
}

func NoMoreThanNUMCPU(conc int) int {
	if conc > NUMCPU || conc <= 0 {
		return NUMCPU
	}
	return conc
}

// TimesNUMCPU calculate number of gorutines based on NUMCPU (gomaxprocs) and given factor.
// Negative factors are interpreted as fractions. Result is rounded down, min returned result is 1.
// Examples for factors:
// * -3: NUMCPU/3
// * -2: NUMCPU/2
// * -1, 0, 1: NUMCPU
// * 2: NUMCPU*2
// * 3: NUMCPU*3
func TimesNUMCPU(factor int) int {
	return timesNUMCPU(factor, NUMCPU)
}

func timesNUMCPU(factor int, numcpu int) int {
	if factor >= -1 && factor <= 1 {
		return numcpu
	}
	if factor > 1 {
		return numcpu * factor
	}
	if n := numcpu / -factor; n > 0 {
		return n
	}
	return 1
}
