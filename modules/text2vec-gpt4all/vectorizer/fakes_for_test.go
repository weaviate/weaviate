//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package vectorizer

import (
	"context"

	"github.com/weaviate/weaviate/modules/text2vec-gpt4all/ent"
)

type fakeClient struct {
	lastInput string
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text string,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	return &ent.VectorizationResult{
		Vector:     []float32{0, 1, 2, 3},
		Dimensions: 4,
		Text:       text,
	}, nil
}

type fakeSettings struct {
	skippedProperty    string
	vectorizeClassName bool
	excludedProperty   string
}

func (f *fakeSettings) PropertyIndexed(propName string) bool {
	return f.skippedProperty != propName
}

func (f *fakeSettings) VectorizePropertyName(propName string) bool {
	return f.excludedProperty != propName
}

func (f *fakeSettings) VectorizeClassName() bool {
	return f.vectorizeClassName
}
