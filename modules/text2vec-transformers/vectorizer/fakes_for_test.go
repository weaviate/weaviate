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

package vectorizer

import (
	"context"

	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/ent"
)

type fakeClient struct {
	lastInput  string
	lastConfig ent.VectorizationConfig
}

func (c *fakeClient) VectorizeObject(ctx context.Context,
	text string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vector:     []float32{0, 1, 2, 3},
		Dimensions: 4,
		Text:       text,
	}, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	text string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	return c.VectorizeObject(ctx, text, cfg)
}

type fakeSettings struct {
	skippedProperty    string
	vectorizeClassName bool
	excludedProperty   string
	poolingStrategy    string
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

func (f *fakeSettings) PoolingStrategy() string {
	return f.poolingStrategy
}
