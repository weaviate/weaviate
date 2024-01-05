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

	txt2vecmodels "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
)

type fakeClient struct {
	lastInput []string
}

func (c *fakeClient) VectorForCorpi(ctx context.Context, corpi []string, overrides map[string]string) ([]float32, []txt2vecmodels.InterpretationSource, error) {
	c.lastInput = corpi
	return []float32{0, 1, 2, 3}, nil, nil
}

func (c *fakeClient) VectorForWord(ctx context.Context, word string) ([]float32, error) {
	c.lastInput = []string{word}
	return []float32{3, 2, 1, 0}, nil
}

func (c *fakeClient) NearestWordsByVector(ctx context.Context,
	vector []float32, n int, k int,
) ([]string, []float32, error) {
	return []string{"word1", "word2"}, []float32{0.1, 0.2}, nil
}

func (c *fakeClient) IsWordPresent(ctx context.Context, word string) (bool, error) {
	return true, nil
}

type fakeIndexCheck struct {
	skippedProperty    string
	vectorizeClassName bool
	excludedProperty   string
}

func (f *fakeIndexCheck) PropertyIndexed(propName string) bool {
	return f.skippedProperty != propName
}

func (f *fakeIndexCheck) VectorizePropertyName(propName string) bool {
	return f.excludedProperty != propName
}

func (f *fakeIndexCheck) VectorizeClassName() bool {
	return f.vectorizeClassName
}
