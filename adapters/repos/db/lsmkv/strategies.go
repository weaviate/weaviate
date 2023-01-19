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

import (
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace       = "replace"
	StrategySetCollection = "setcollection"
	StrategyMapCollection = "mapcollection"
	StrategyRoaringSet    = "roaringset"
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
	default:
		panic("unsupported strategy")
	}
}
