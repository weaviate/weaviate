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
)

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	settings ClassSettings,
) ([]float32, error) {
	res, err := v.client.Vectorize(ctx, inputs, []string{}, []string{}, []string{}, []string{}, []string{}, []string{})
	if err != nil {
		return nil, errors.Wrap(err, "remote client vectorize")
	}
	if len(inputs) != len(res.TextVectors) {
		return nil, errors.New("inputs are not equal to vectors returned")
	}
	return v.CombineVectors(res.TextVectors), nil
}
