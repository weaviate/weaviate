//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package vectorizer

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-aws/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
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
		config ent.VectorizationConfig) (*modulecomponents.VectorizationResult[[]float32], error)
	VectorizeQuery(ctx context.Context, input []string,
		config ent.VectorizationConfig) (*modulecomponents.VectorizationResult[[]float32], error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	Service() string
	Region() string
	Model() string
	Endpoint() string
	TargetModel() string
	TargetVariant() string
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	vec, err := v.object(ctx, object, cfg)
	return vec, nil, err
}

func (v *Vectorizer) object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig,
) ([]float32, error) {
	icheck := NewClassSettings(cfg)
	text, isEmpty := v.objectVectorizer.Texts(ctx, object, icheck)
	if isEmpty {
		// don't vectorize empty text
		return nil, nil
	}

	res, err := v.client.Vectorize(ctx, []string{text}, ent.VectorizationConfig{
		Service:       icheck.Service(),
		Region:        icheck.Region(),
		Model:         icheck.Model(),
		Endpoint:      icheck.Endpoint(),
		TargetModel:   icheck.TargetModel(),
		TargetVariant: icheck.TargetVariant(),
	})
	if err != nil {
		return nil, err
	}
	if len(res.Vector) == 0 {
		return nil, errors.New("empty vector")
	}
	return res.Vector[0], nil
}
