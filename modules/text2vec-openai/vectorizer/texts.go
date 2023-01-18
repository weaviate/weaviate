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
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"
)

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	settings ClassSettings,
) ([]float32, error) {
	res, err := v.client.VectorizeQuery(ctx, v.joinSentences(inputs), ent.VectorizationConfig{
		Type:         settings.Type(),
		Model:        settings.Model(),
		ModelVersion: settings.ModelVersion(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "remote client vectorize")
	}

	return res.Vector, nil
}

func (v *Vectorizer) joinSentences(input []string) string {
	if len(input) == 1 {
		return input[0]
	}

	b := &strings.Builder{}
	for i, sent := range input {
		if i > 0 {
			if v.endsWithPunctuation(input[i-1]) {
				b.WriteString(" ")
			} else {
				b.WriteString(". ")
			}
		}
		b.WriteString(sent)
	}

	return b.String()
}

func (v *Vectorizer) endsWithPunctuation(sent string) bool {
	if len(sent) == 0 {
		// treat an empty string as if it ended with punctuation so we don't add
		// additional punctuation
		return true
	}

	lastChar := sent[len(sent)-1]
	switch lastChar {
	case '.', ',', '?', '!':
		return true

	default:
		return false
	}
}
