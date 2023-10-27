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
	"github.com/weaviate/weaviate/modules/text2vec-jinaai/ent"
)

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	settings ClassSettings,
) ([]float32, error) {
	res, err := v.client.VectorizeQuery(ctx, inputs, ent.VectorizationConfig{
		Type:         settings.Type(),
		Model:        settings.Model(),
		ResourceName: settings.ResourceName(),
		BaseURL:      settings.BaseURL(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "remote client vectorize")
	}

	if len(res.Vector) > 1 {
		return v.CombineVectors(res.Vector), nil
	}
	return res.Vector[0], nil
}
