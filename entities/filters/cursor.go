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

type Cursor struct {
	After string `json:"after"`
	Limit int    `json:"limit"`
}

// ExtractCursorFromArgs gets the limit key out of a map. Not specific to
// GQL, but can be used from GQL
func ExtractCursorFromArgs(args map[string]interface{}) (*Cursor, error) {
	after, afterOk := args["after"]

	limit, limitOk := args["limit"]
	if !limitOk || limit.(int) < 0 {
		limit = LimitFlagNotSet
	}

	if !afterOk && !limitOk || after == nil {
		return nil, nil
	}

	return &Cursor{
		After: after.(string),
		Limit: limit.(int),
	}, nil
}
