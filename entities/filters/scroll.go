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

package filters

type Scroll struct {
	After string
	Limit int
}

// ExtractScrollFromArgs gets the limit key out of a map. Not specific to
// GQL, but can be used from GQL
func ExtractScrollFromArgs(args map[string]interface{}) (*Scroll, error) {
	after, afterOk := args["after"]

	limit, limitOk := args["limit"]
	if !limitOk || limit.(int) < 0 {
		limit = LimitFlagNotSet
	}

	if !afterOk && !limitOk || after == nil {
		return nil, nil
	}

	return &Scroll{
		After: after.(string),
		Limit: limit.(int),
	}, nil
}
