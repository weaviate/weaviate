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

package lsmkv

import (
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace         = "replace"
	StrategySetCollection   = "setcollection"
	StrategyMapCollection   = "mapcollection"
	StrategyRoaringSet      = "roaringset"
	StrategyRoaringSetRange = "roaringsetrange"
	StrategyInverted        = "inverted"
)

func SegmentStrategyFromString(in string) segmentindex.Strategy {
	switch in {
	case StrategyReplace:
		return segmentindex.StrategyReplace
	case StrategySetCollection:
		return segmentindex.StrategySetCollection
	case StrategyMapCollection:
		return segmentindex.StrategyMapCollection
	case StrategyRoaringSet:
		return segmentindex.StrategyRoaringSet
	case StrategyRoaringSetRange:
		return segmentindex.StrategyRoaringSetRange
	case StrategyInverted:
		return segmentindex.StrategyInverted
	default:
		panic("unsupported strategy")
	}
}

func IsExpectedStrategy(strategy string, expectedStrategies ...string) bool {
	if len(expectedStrategies) == 0 {
		expectedStrategies = []string{
			StrategyReplace,
			StrategySetCollection,
			StrategyMapCollection,
			StrategyRoaringSet,
			StrategyRoaringSetRange,
			StrategyInverted,
		}
	}

	for _, s := range expectedStrategies {
		if s == strategy {
			return true
		}
	}
	return false
}

func CheckExpectedStrategy(strategy string, expectedStrategies ...string) error {
	if IsExpectedStrategy(strategy, expectedStrategies...) {
		return nil
	}
	if len(expectedStrategies) == 1 {
		return fmt.Errorf("strategy %q expected, got %q", expectedStrategies[0], strategy)
	}
	return fmt.Errorf("one of strategies %v expected, got %q", expectedStrategies, strategy)
}

func MustBeExpectedStrategy(strategy string, expectedStrategies ...string) {
	if err := CheckExpectedStrategy(strategy, expectedStrategies...); err != nil {
		panic(err)
	}
}

func CheckStrategyRoaringSet(strategy string) error {
	return CheckExpectedStrategy(strategy, StrategyRoaringSet)
}

func CheckStrategyRoaringSetRange(strategy string) error {
	return CheckExpectedStrategy(strategy, StrategyRoaringSetRange)
}

func DefaultSearchableStrategy(useInvertedSearchable bool) string {
	if useInvertedSearchable {
		return StrategyInverted
	}
	return StrategyMapCollection
}
