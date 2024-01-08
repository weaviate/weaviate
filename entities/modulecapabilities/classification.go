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

package modulecapabilities

import (
	"context"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

type VectorClassSearchParams struct {
	Filters    *filters.LocalFilter
	Pagination *filters.Pagination
	ClassName  string
	Properties []string
}

type VectorClassSearchRepo interface {
	VectorClassSearch(ctx context.Context, params VectorClassSearchParams) ([]search.Result, error)
}

type ClassifyParams struct {
	Schema            schema.Schema
	Params            models.Classification
	Filters           Filters
	UnclassifiedItems []search.Result
	VectorRepo        VectorClassSearchRepo
}

type Filters interface {
	Source() *filters.LocalFilter
	Target() *filters.LocalFilter
	TrainingSet() *filters.LocalFilter
}

type Writer interface {
	Start()
	Store(item search.Result) error
	Stop() WriterResults
}

type WriterResults interface {
	SuccessCount() int64
	ErrorCount() int64
	Err() error
}

type ClassifyItemFn func(item search.Result, itemIndex int,
	params models.Classification, filters Filters, writer Writer) error

type Classifier interface {
	Name() string
	ClassifyFn(params ClassifyParams) (ClassifyItemFn, error)
	ParseClassifierSettings(params *models.Classification) error
}

type ClassificationProvider interface {
	Classifiers() []Classifier
}
