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
	"github.com/weaviate/weaviate/modules/text2vec-palm/ent"
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
		config ent.VectorizationConfig, titlePropertyValue string) (*ent.VectorizationResult, error)
	VectorizeQuery(ctx context.Context, input []string,
		config ent.VectorizationConfig) (*ent.VectorizationResult, error)
}

// IndexCheck returns whether a property of a class should be indexed
type ClassSettings interface {
	PropertyIndexed(property string) bool
	VectorizePropertyName(propertyName string) bool
	VectorizeClassName() bool
	ApiEndpoint() string
	ProjectID() string
	ModelID() string
	TitleProperty() string
}

func (v *Vectorizer) Object(ctx context.Context, object *models.Object,
	comp moduletools.VectorizablePropsComparator, cfg moduletools.ClassConfig,
) error {
	vec, err := v.object(ctx, object.Class, comp, cfg)
	if err != nil {
		return err
	}

	// TODO[named-vectors]: move the vectorizer code to use a generic method to
	// object = v.objectVectorizer.AddVectorToObject(object, vec, settings)
	object.Vector = vec
	v.objectVectorizer.AddVectorToObject(object, vec, nil, cfg)
	return nil
}

func (v *Vectorizer) object(ctx context.Context, className string,
	comp moduletools.VectorizablePropsComparator, cfg moduletools.ClassConfig,
) ([]float32, error) {
	icheck := NewClassSettings(cfg)

	corpi, titlePropertyValue, vector := v.objectVectorizer.TextsOrVectorWithTitleProperty(ctx,
		className, comp, icheck, icheck.TitleProperty())
	if vector != nil {
		// dont' re-vectorize
		return vector, nil
	}
	// vectorize text
	res, err := v.client.Vectorize(ctx, []string{corpi}, ent.VectorizationConfig{
		ApiEndpoint: icheck.ApiEndpoint(),
		ProjectID:   icheck.ProjectID(),
		Model:       icheck.ModelID(),
	}, titlePropertyValue)
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
