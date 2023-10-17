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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/modules/text2vec-transformers/ent"
)

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	settings ClassSettings,
) ([]float32, error) {
	vectors := make([][]float32, len(inputs))
	for i := range inputs {
		res, err := v.client.VectorizeQuery(ctx, inputs[i], ent.VectorizationConfig{
			PoolingStrategy: settings.PoolingStrategy(),
		})
		if err != nil {
			return nil, errors.Wrap(err, "remote client vectorize")
		}
		vectors[i] = res.Vector
	}

	return v.CombineVectors(vectors), nil
}
