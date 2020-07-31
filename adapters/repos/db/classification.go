//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// TODO: why is this logic in the persistence package? This is business-logic,
// move out of here!
func (db *DB) GetUnclassified(ctx context.Context, kind kind.Kind, class string, properties []string, filter *libfilters.LocalFilter) ([]search.Result, error) {

	countFilters := make([]libfilters.Clause, len(properties))
	for i, prop := range properties {
		countFilters[i] = libfilters.Clause{
			Operator: libfilters.OperatorEqual,
			Value: &libfilters.Value{
				Type:  schema.DataTypeInt,
				Value: 0,
			},
			On: &libfilters.Path{
				Class:    schema.ClassName(class),
				Property: schema.PropertyName(prop),
			},
		}
	}

	var countRootClause libfilters.Clause
	if len(countFilters) == 1 {
		countRootClause = countFilters[0]
	} else {
		countRootClause = libfilters.Clause{
			Operands: countFilters,
			Operator: libfilters.OperatorAnd,
		}
	}

	var rootFilter = &libfilters.LocalFilter{}

	if filter == nil {
		rootFilter.Root = &countRootClause
	} else {
		rootFilter.Root = &libfilters.Clause{
			Operator: libfilters.OperatorAnd, // so we can AND the refcount requirements and whatever custom filters, the user has
			Operands: []libfilters.Clause{*filter.Root, countRootClause},
		}
	}

	res, err := db.ClassSearch(ctx, traverser.GetParams{
		ClassName: class,
		Filters:   rootFilter,
		Kind:      kind,
		Pagination: &libfilters.Pagination{
			Limit: 10000, // TODO: gh-1219 increase
		},
	})

	return res, err

}

func (db *DB) AggregateNeighbors(ctx context.Context, vector []float32, kind kind.Kind, class string, properties []string, k int, filter *libfilters.LocalFilter) ([]classification.NeighborRef, error) {
	panic("not implemented") // TODO: Implement
}
