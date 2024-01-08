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

package vectorizer

import (
	"context"

	"github.com/weaviate/weaviate/modules/text2vec-palm/ent"
)

type fakeClient struct {
	lastInput  []string
	lastConfig ent.VectorizationConfig
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text []string, cfg ent.VectorizationConfig, titlePropertyValue string,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vectors:    [][]float32{{0, 1, 2, 3}},
		Dimensions: 4,
		Texts:      text,
	}, nil
}

func (c *fakeClient) VectorizeQuery(ctx context.Context,
	text []string, cfg ent.VectorizationConfig,
) (*ent.VectorizationResult, error) {
	c.lastInput = text
	c.lastConfig = cfg
	return &ent.VectorizationResult{
		Vectors:    [][]float32{{0.1, 1.1, 2.1, 3.1}},
		Dimensions: 4,
		Texts:      text,
	}, nil
}

type fakeSettings struct {
	skippedProperty    string
	vectorizeClassName bool
	excludedProperty   string
	apiEndpoint        string
	projectID          string
	endpointID         string
	truncateType       string
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

func (f *fakeSettings) Truncate() string {
	return f.truncateType
}

func (f *fakeSettings) ApiEndpoint() string {
	return f.truncateType
}

func (f *fakeSettings) ProjectID() string {
	return f.truncateType
}

func (f *fakeSettings) ModelID() string {
	return f.truncateType
}

func (f *fakeSettings) TitleProperty() string {
	return ""
}
