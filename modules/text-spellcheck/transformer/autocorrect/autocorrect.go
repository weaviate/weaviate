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

package autocorrect

import (
	"context"
	"strings"

	"github.com/weaviate/weaviate/modules/text-spellcheck/ent"
)

type spellCheckClient interface {
	Check(ctx context.Context, text []string) (*ent.SpellCheckResult, error)
}

type AutocorrectTransformer struct {
	spellCheckClient spellCheckClient
}

func New(spellCheckClient spellCheckClient) *AutocorrectTransformer {
	return &AutocorrectTransformer{spellCheckClient}
}

func (t *AutocorrectTransformer) Transform(in []string) ([]string, error) {
	spellCheckResult, err := t.spellCheckClient.Check(context.Background(), in)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(in))
	changes := spellCheckResult.Changes
	for i, txt := range spellCheckResult.Text {
		didYouMean := txt
		for _, change := range changes {
			didYouMean = strings.ReplaceAll(strings.ToLower(didYouMean), change.Original, change.Correction)
		}
		result[i] = didYouMean
	}
	return result, nil
}
