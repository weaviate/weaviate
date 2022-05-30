//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package classification

import (
	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
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
