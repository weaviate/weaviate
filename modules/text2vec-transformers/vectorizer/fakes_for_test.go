package vectorizer

import (
	"context"

	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/ent"
)

type fakeClient struct {
	lastInput string
}

func (c *fakeClient) Vectorize(ctx context.Context,
	text string) (*ent.VectorizationResult, error) {
	c.lastInput = text
	return &ent.VectorizationResult{
		Vector:     []float32{0, 1, 2, 3},
		Dimensions: 4,
		Text:       text,
	}, nil
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
