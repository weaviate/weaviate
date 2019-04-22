package filters

// Pagination for now only contains a limit paramter, but might be extended in
// the future
type Pagination struct {
	Limit int
}

// ExtractPaginationFromArgs gets the limit key out of a map. Not specific to
// GQL, but can be used from GQL
func ExtractPaginationFromArgs(args map[string]interface{}) (*Pagination, error) {
	limit, ok := args["limit"]
	if !ok {
		return nil, nil
	}

	return &Pagination{
		Limit: limit.(int),
	}, nil
}
