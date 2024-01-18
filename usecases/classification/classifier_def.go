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

package classification

import (
	libfilters "github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

type Filters interface {
	Source() *libfilters.LocalFilter
	Target() *libfilters.LocalFilter
	TrainingSet() *libfilters.LocalFilter
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
