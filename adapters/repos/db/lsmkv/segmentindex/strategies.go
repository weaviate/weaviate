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

package segmentindex

import (
	"fmt"
	"slices"
	"strings"
)

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
	case StrategyInverted:
		return "inverted"
	default:
		return "n/a"
	}
}

var allStrategies = []Strategy{
	StrategyReplace,
	StrategySetCollection,
	StrategyMapCollection,
	StrategyRoaringSet,
	StrategyRoaringSetRange,
	StrategyInverted,
}

func IsExpectedStrategy(strategy Strategy, expectedStrategies ...Strategy) bool {
	if len(expectedStrategies) == 0 {
		expectedStrategies = allStrategies
	}
	return slices.Contains(expectedStrategies, strategy)
}

// With no expectedStrategies, CheckExpectedStrategy accepts any known strategy.
func CheckExpectedStrategy(strategy Strategy, expectedStrategies ...Strategy) error {
	// slices.Contains would put this over the inlining budget
	for _, e := range expectedStrategies {
		if strategy == e {
			return nil
		}
	}
	return strategyMismatch(strategy, expectedStrategies)
}

func checkStrategy(strategy, want Strategy) error {
	if strategy == want {
		return nil
	}
	return strategyMismatch(strategy, []Strategy{want})
}

// strategyMismatch holds the slow path so the checks above fit the inlining budget.
func strategyMismatch(strategy Strategy, expectedStrategies []Strategy) error {
	if len(expectedStrategies) == 0 {
		if slices.Contains(allStrategies, strategy) {
			return nil
		}
		expectedStrategies = allStrategies
	}
	if len(expectedStrategies) == 1 {
		return fmt.Errorf("strategy %s expected, got %s", expectedStrategies[0], strategy)
	}
	// the slice is joined rather than passed to Errorf: an Errorf argument would
	// make every caller's variadic slice escape to the heap
	return fmt.Errorf("one of strategies [%s] expected, got %s",
		joinStrategies(expectedStrategies), strategy)
}

func joinStrategies(strategies []Strategy) string {
	labels := make([]string, len(strategies))
	for i, s := range strategies {
		labels[i] = s.String()
	}
	return strings.Join(labels, " ")
}

func MustBeExpectedStrategy(strategy Strategy, expectedStrategies ...Strategy) {
	if err := CheckExpectedStrategy(strategy, expectedStrategies...); err != nil {
		panic(err)
	}
}

func CheckStrategyRoaringSet(strategy Strategy) error {
	return checkStrategy(strategy, StrategyRoaringSet)
}
