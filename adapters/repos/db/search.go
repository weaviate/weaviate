//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (db *DB) Aggregate(ctx context.Context, params traverser.AggregateParams) (*aggregation.Result, error) {
	panic("not implemented") // TODO: Implement
}

func (db *DB) ClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error) {
	panic("not implemented")
}
func (db *DB) VectorClassSearch(ctx context.Context, params traverser.GetParams) ([]search.Result, error) {
	panic("not implemented")
}
func (db *DB) VectorSearch(ctx context.Context, vector []float32, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
	panic("not implemented")
}
