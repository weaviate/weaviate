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
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-voyageai/ent"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

type Vectorizer struct {
	client           Client
	objectVectorizer *objectsvectorizer.ObjectVectorizer
}

func New(client Client) *Vectorizer {
	return &Vectorizer{
		client:           client,
		objectVectorizer: objectsvectorizer.New(),
	}
}

type Client interface {
	Vectorize(ctx context.Context, input []string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, error)
	VectorizeQuery(ctx context.Context, input []string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Model() string
	Truncate() bool
	BaseURL() string
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, error) {
	icheck := NewClassSettings(cfg)
	text := v.objectVectorizer.Texts(ctx, object, icheck)
	// vectorize text
	res, err := v.client.Vectorize(ctx, []string{text}, v.getVectorizationConfig(cfg))
	if err != nil {
		return nil, err
	}
	if len(res.Vectors) == 0 {
		return nil, fmt.Errorf("no vectors generated")
	}

	if len(res.Vectors) > 1 {
		return libvectorizer.CombineVectors(res.Vectors), nil
	}
	return res.Vectors[0], nil
}

func (v *Vectorizer) getVectorizationConfig(cfg moduletools.ClassConfig) ent.VectorizationConfig {
	settings := NewClassSettings(cfg)
	return ent.VectorizationConfig{
		Model:    settings.Model(),
		BaseURL:  settings.BaseURL(),
		Truncate: settings.Truncate(),
	}
}
