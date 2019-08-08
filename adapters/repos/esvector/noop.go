//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package esvector

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// NoOpRepo does nothing
type NoOpRepo struct{}

// VectorClassSearch panics
func (r *NoOpRepo) VectorClassSearch(ctx context.Context, kind kind.Kind,
	className string, vector []float32, limit int,
	filters *filters.LocalFilter) ([]traverser.VectorSearchResult, error) {
	panic("no op repo: not implemented")
}

// VectorSearch panics
func (r *NoOpRepo) VectorSearch(ctx context.Context, vector []float32, limit int,
	filters *filters.LocalFilter) ([]traverser.VectorSearchResult, error) {
	panic("no op repo: not implemented")
}

// PutThing does nothing, but doesn't error either
func (r *NoOpRepo) PutThing(ctx context.Context,
	concept *models.Thing, vector []float32) error {
	return nil
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

//DeleteAction does nothing, but doesn't error either
func (r *NoOpRepo) DeleteAction(ctx context.Context, className string, id strfmt.UUID) error {
	return nil
}

// NewNoOpRepo for when vector indexing is not desired
func NewNoOpRepo() *NoOpRepo {
	return &NoOpRepo{}
}
