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

package common

const (
	// DefaultSearchByDistInitialLimit :
	// the initial limit of 100 here is an
	// arbitrary decision, and can be tuned
	// as needed
	DefaultSearchByDistInitialLimit = 100

	// DefaultSearchByDistLimitMultiplier :
	// the decision to increase the limit in
	// multiples of 10 here is an arbitrary
	// decision, and can be tuned as needed
	DefaultSearchByDistLimitMultiplier = 10
)

type SearchByDistParams struct {
	offset             int
	limit              int
	totalLimit         int
	maximumSearchLimit int64
}

func NewSearchByDistParams(
	offset int,
	limit int,
	totalLimit int,
	maximumSearchLimit int64,
) *SearchByDistParams {
	return &SearchByDistParams{
		offset:             offset,
		limit:              limit,
		totalLimit:         totalLimit,
		maximumSearchLimit: maximumSearchLimit,
	}
}

func (params *SearchByDistParams) TotalLimit() int {
	return params.totalLimit
}

func (params *SearchByDistParams) MaximumSearchLimit() int64 {
	return params.maximumSearchLimit
}

func (params *SearchByDistParams) OffsetCapacity(ids []uint64) int {
	if l := len(ids); l < params.offset {
		return l
	}
	return params.offset
}

func (params *SearchByDistParams) TotalLimitCapacity(ids []uint64) int {
	if l := len(ids); l < params.totalLimit {
		return l
	}
	return params.totalLimit
}

func (params *SearchByDistParams) Iterate() {
	params.offset = params.totalLimit
	params.limit *= DefaultSearchByDistLimitMultiplier
	params.totalLimit = params.offset + params.limit
}

func (params *SearchByDistParams) MaxLimitReached() bool {
	if params.maximumSearchLimit < 0 {
		return false
	}

	return int64(params.totalLimit) > params.maximumSearchLimit
}
