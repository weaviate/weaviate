package esvector

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/semi-technologies/weaviate/entities/filters"
)

func queryFromFilter(filters *filters.LocalFilter) (map[string]interface{}, error) {

	if filters == nil {
		return map[string]interface{}{
			"match_all": map[string]interface{}{},
		}, nil
	}

	q := map[string]interface{}{
		"bool": map[string]interface{}{
			"filter": []map[string]interface{}{
				map[string]interface{}{
					"term": map[string]interface{}{
						filters.Root.On.Property.String(): map[string]interface{}{
							"value": filters.Root.Value.Value,
						},
					},
				},
			},
		},
	}

	spew.Dump(q)
	return q, nil
}
