/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
package common_resolver

import (
	"errors"
)

type Pagination struct {
	First int
	After int
}

func ExtractPaginationFromArgs(args map[string]interface{}) (*Pagination, error) {
	afterVal, afterOk := args["after"]
	firstVal, firstOk := args["first"]

	if firstOk && afterOk {
		after := afterVal.(int)
		first := firstVal.(int)
		return &Pagination{
			After: after,
			First: first,
		}, nil
	}

	if firstOk || afterOk {
		return nil, errors.New("after and first must both be specified")
	}

	return nil, nil
}
