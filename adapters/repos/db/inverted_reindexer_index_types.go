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

package db

import "github.com/weaviate/weaviate/adapters/repos/db/lsmkv"

type PropertyIndexType uint8

const (
	IndexTypePropValue PropertyIndexType = iota + 1
	IndexTypePropLength
	IndexTypePropNull
	IndexTypeHashPropValue
	IndexTypeHashPropLength
	IndexTypeHashPropNull
)

func IsSupportedPropertyIndexType(indexType PropertyIndexType) bool {
	switch indexType {
	case IndexTypePropValue,
		IndexTypePropLength,
		IndexTypePropNull,
		IndexTypeHashPropValue,
		IndexTypeHashPropLength,
		IndexTypeHashPropNull:
		return true
	default:
		return false
	}
}

func CheckSupportedPropertyIndexType(indexType PropertyIndexType) {
	if !IsSupportedPropertyIndexType(indexType) {
		panic("unsupported property index type")
	}
}

// Some index types are supported by specific strategies only
// Method ensures both index type and strategy work together
func IsIndexTypeSupportedByStrategy(indexType PropertyIndexType, strategy string) bool {
	switch indexType {
	case IndexTypeHashPropValue,
		IndexTypeHashPropLength,
		IndexTypeHashPropNull:
		return lsmkv.IsExpectedStrategy(strategy, lsmkv.StrategyReplace)
	case IndexTypePropLength,
		IndexTypePropNull:
		return lsmkv.IsExpectedStrategy(strategy, lsmkv.StrategySetCollection, lsmkv.StrategyRoaringSet)
	case IndexTypePropValue:
		return lsmkv.IsExpectedStrategy(strategy, lsmkv.StrategySetCollection, lsmkv.StrategyRoaringSet, lsmkv.StrategyMapCollection)
	}
	return false
}
