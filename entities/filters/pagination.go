//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package filters

// Pagination for now only contains a limit parameter, but might be extended in
// the future
type Pagination struct {
	Offset int
	Limit  int
}

// ExtractPaginationFromArgs gets the limit key out of a map. Not specific to
// GQL, but can be used from GQL
func ExtractPaginationFromArgs(args map[string]interface{}) (*Pagination, error) {
	offset, offsetOk := args["offset"]
	if !offsetOk {
		offset = 0
	}

	limit, limitOk := args["limit"]
	if !limitOk {
		limit = -1
	}

	if !offsetOk && !limitOk {
		return nil, nil
	}

	return &Pagination{
		Offset: offset.(int),
		Limit:  limit.(int),
	}, nil
}
