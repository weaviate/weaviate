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
