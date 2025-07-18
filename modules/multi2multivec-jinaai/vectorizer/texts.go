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
	"github.com/weaviate/weaviate/entities/moduletools"
)

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	cfg moduletools.ClassConfig,
) ([][]float32, error) {
	if len(inputs) != 1 {
		return nil, errors.Errorf("only 1 query can be vectorized, passed %v queries", len(inputs))
	}
	res, err := v.client.VectorizeQuery(ctx, inputs, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "remote client vectorize")
	}
	if len(inputs) != len(res.Vector) {
		return nil, errors.New("inputs are not equal to vectors returned")
	}
	return res.Vector[0], nil
}
