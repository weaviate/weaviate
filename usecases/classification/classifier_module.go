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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/search"
)

type moduleWriter struct {
	writer Writer
}

func (w *moduleWriter) Start() {
	w.writer.Start()
}

func (w *moduleWriter) Store(item search.Result) error {
	return w.writer.Store(item)
}

func (w *moduleWriter) Stop() modulecapabilities.WriterResults {
	res := w.writer.Stop()
	return batchWriterResults{
		successCount: res.SuccessCount(),
		errorCount:   res.ErrorCount(),
		err:          res.Err(),
	}
}

type moduleClassification struct {
	classifyItemFn modulecapabilities.ClassifyItemFn
}

func (c *moduleClassification) classifyFn(item search.Result, itemIndex int,
	params models.Classification, filters Filters, writer Writer,
) error {
	return c.classifyItemFn(item, itemIndex, params, filters, &moduleWriter{writer})
}
