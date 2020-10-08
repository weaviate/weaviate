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

package esvector

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// NoOpRepo does nothing
type NoOpRepo struct{}

// VectorClassSearch panics
func (r *NoOpRepo) VectorClassSearch(ctx context.Context, kind kind.Kind,
	className string, vector []float32, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
	panic("no op repo: not implemented")
}

// VectorSearch panics
func (r *NoOpRepo) VectorSearch(ctx context.Context, vector []float32, limit int,
	filters *filters.LocalFilter) ([]search.Result, error) {
	panic("no op repo: not implemented")
}
func (r *NoOpRepo) ThingByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties) (*search.Result, error) {
	panic("no op repo: not implemented")
}

func (r *NoOpRepo) ActionByID(ctx context.Context, id strfmt.UUID, props traverser.SelectProperties) (*search.Result, error) {
	panic("no op repo: not implemented")
}

func (r *NoOpRepo) ThingSearch(ctx context.Context, limit int, filters *filters.LocalFilter) (search.Results, error) {
	panic("no op repo: not implemented")
}

func (r *NoOpRepo) ActionSearch(ctx context.Context, limit int, filters *filters.LocalFilter) (search.Results, error) {
	panic("no op repo: not implemented")
}

func (r *NoOpRepo) Aggregate(ctx context.Context, params traverser.AggregateParams) (*aggregation.Result, error) {
	panic("no op repo: not implemented")
}

// PutThing does nothing, but doesn't error either
func (r *NoOpRepo) PutThing(ctx context.Context,
	concept *models.Thing, vector []float32) error {
	return nil
}

func (r *NoOpRepo) BatchPutThings(ctx context.Context, batch kinds.BatchThings) (kinds.BatchThings, error) {
	return nil, nil
}

//DeleteThing does nothing, but doesn't error either
func (r *NoOpRepo) DeleteThing(ctx context.Context, className string, id strfmt.UUID) error {
	return nil
}

// PutAction does nothing, but doesn't error either
func (r *NoOpRepo) PutAction(ctx context.Context,
	concept *models.Action, vector []float32) error {
	return nil
}

func (r *NoOpRepo) BatchPutActions(ctx context.Context, batch kinds.BatchActions) (kinds.BatchActions, error) {
	return nil, nil
}

//DeleteAction does nothing, but doesn't error either
func (r *NoOpRepo) DeleteAction(ctx context.Context, className string, id strfmt.UUID) error {
	return nil
}

func (r *NoOpRepo) SetSchemaGetter(sg schema.SchemaGetter) {
}

func (r *NoOpRepo) InitCacheIndexing(size int, waitOnIdle, waitOnBusy time.Duration) {
}

func (r *NoOpRepo) WaitForStartup(time.Duration) error {
	return nil
}

// NewNoOpRepo for when vector indexing is not desired
func NewNoOpRepo() *NoOpRepo {
	return &NoOpRepo{}
}
