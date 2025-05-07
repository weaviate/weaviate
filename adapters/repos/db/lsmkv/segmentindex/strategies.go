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

package segmentindex

import "fmt"

type Strategy uint16

const (
	StrategyReplace Strategy = iota
	StrategySetCollection
	StrategyMapCollection
	StrategyRoaringSet
	StrategyRoaringSetRange
	StrategyInverted
)

// consistent labels with adapters/repos/db/lsmkv/strategies.go
func (s Strategy) String() string {
	switch s {
	case StrategyReplace:
		return "replace"
	case StrategySetCollection:
		return "setcollection"
	case StrategyMapCollection:
		return "mapcollection"
	case StrategyRoaringSet:
		return "roaringset"
	case StrategyRoaringSetRange:
		return "roaringsetrange"
	default:
		return "n/a"
	}
}

func IsExpectedStrategy(strategy Strategy, expectedStrategies ...Strategy) bool {
	if len(expectedStrategies) == 0 {
		expectedStrategies = []Strategy{
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

func CheckExpectedStrategy(strategy Strategy, expectedStrategies ...Strategy) error {
	if IsExpectedStrategy(strategy, expectedStrategies...) {
		return nil
	}
	if len(expectedStrategies) == 1 {
		return fmt.Errorf("strategy %v expected, got %v", expectedStrategies[0], strategy)
	}
	return fmt.Errorf("one of strategies %v expected, got %v", expectedStrategies, strategy)
}

func MustBeExpectedStrategy(strategy Strategy, expectedStrategies ...Strategy) {
	if err := CheckExpectedStrategy(strategy, expectedStrategies...); err != nil {
		panic(err)
	}
}
