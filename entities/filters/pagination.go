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

package filters

const (
	// LimitFlagSearchByDist indicates that the
	// vector search should be conducted by
	// distance, without limit
	LimitFlagSearchByDist int = iota - 2

	// LimitFlagNotSet indicates that no limit
	// was provided by the client
	LimitFlagNotSet
)

type Pagination struct {
	Offset  int
	Limit   int
	Autocut int
}

// ExtractPaginationFromArgs gets the limit key out of a map. Not specific to
// GQL, but can be used from GQL
func ExtractPaginationFromArgs(args map[string]interface{}) (*Pagination, error) {
	offset, offsetOk := args["offset"]
	if !offsetOk {
		offset = 0
	}

	limit, limitOk := args["limit"]
	if !limitOk || limit.(int) < 0 {
		limit = LimitFlagNotSet
	}

	autocut, autocutOk := args["autocut"]
	if !autocutOk {
		autocut = 0 // disabled
	}

	if !offsetOk && !limitOk && !autocutOk {
		return nil, nil
	}

	return &Pagination{
		Offset:  offset.(int),
		Limit:   limit.(int),
		Autocut: autocut.(int),
	}, nil
}
