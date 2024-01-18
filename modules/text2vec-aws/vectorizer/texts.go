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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-aws/ent"
	libvectorizer "github.com/weaviate/weaviate/usecases/vectorizer"
)

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	settings := NewClassSettings(cfg)
	vectors := make([][]float32, len(inputs))
	for i := range inputs {
		res, err := v.client.VectorizeQuery(ctx, []string{inputs[i]}, ent.VectorizationConfig{
			Service:       settings.Service(),
			Region:        settings.Region(),
			Model:         settings.Model(),
			Endpoint:      settings.Endpoint(),
			TargetModel:   settings.TargetModel(),
			TargetVariant: settings.TargetVariant(),
		})
		if err != nil {
			return nil, errors.Wrap(err, "remote client vectorize")
		}
		vectors[i] = res.Vector
	}

	return libvectorizer.CombineVectors(vectors), nil
}
