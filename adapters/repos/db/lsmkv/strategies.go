//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace       = "replace"
	StrategySetCollection = "setcollection"
	StrategyMapCollection = "mapcollection"
)

type SegmentStrategy uint16

const (
	SegmentStrategyReplace SegmentStrategy = iota
	SegmentStrategySetCollection
	SegmentStrategyMapCollection
)

func SegmentStrategyFromString(in string) SegmentStrategy {
	switch in {
	case StrategyReplace:
		return SegmentStrategyReplace
	case StrategySetCollection:
		return SegmentStrategySetCollection
	case StrategyMapCollection:
		return SegmentStrategyMapCollection
	default:
		panic("unsupported strategy")
	}
}
