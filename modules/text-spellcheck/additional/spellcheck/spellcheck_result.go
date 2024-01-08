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

package spellcheck

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	spellcheckmodels "github.com/weaviate/weaviate/modules/text-spellcheck/additional/models"
	"github.com/weaviate/weaviate/modules/text-spellcheck/ent"
)

func (p *SpellCheckProvider) findSpellCheck(ctx context.Context,
	in []search.Result, params *Params, limit *int,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	if len(in) > 0 {
		name, texts, err := p.paramHelper.getTexts(argumentModuleParams)
		if err != nil {
			return in, errors.New("cannot get texts")
		}
		spellCheckAdditionalProperty, err := p.performSpellCheck(ctx, name, texts)
		if err != nil {
			return in, err
		}
		for i := range in {
			ap := in[i].AdditionalProperties
			if ap == nil {
				ap = models.AdditionalProperties{}
			}
			ap["spellCheck"] = spellCheckAdditionalProperty
			in[i].AdditionalProperties = ap
		}
	}
	return in, nil
}

func (p *SpellCheckProvider) performSpellCheck(ctx context.Context, name string, texts []string) ([]*spellcheckmodels.SpellCheck, error) {
	if len(texts) == 0 {
		return []*spellcheckmodels.SpellCheck{}, nil
	}
	spellCheckResult, err := p.spellCheck.Check(ctx, texts)
	if err != nil {
		return nil, err
	}
	return p.getSpellCheckAdditionalProperty(name, spellCheckResult), nil
}

func (p *SpellCheckProvider) getSpellCheckAdditionalProperty(name string, spellCheckResult *ent.SpellCheckResult) []*spellcheckmodels.SpellCheck {
	spellCheck := []*spellcheckmodels.SpellCheck{}
	for i, t := range spellCheckResult.Text {
		spellCheck = append(spellCheck, p.getSpellCheckAdditionalPropertyObject(t, p.getSpellCheckLocation(name, i), spellCheckResult))
	}
	return spellCheck
}

func (p *SpellCheckProvider) getSpellCheckLocation(name string, i int) string {
	if name == "nearText" {
		return fmt.Sprintf("nearText.concepts[%v]", i)
	}
	return "ask.question"
}

func (p *SpellCheckProvider) getSpellCheckAdditionalPropertyObject(originalText, location string, spellCheckResult *ent.SpellCheckResult) *spellcheckmodels.SpellCheck {
	didYouMean := originalText
	changes := []spellcheckmodels.SpellCheckChange{}
	for _, change := range spellCheckResult.Changes {
		if strings.Contains(strings.ToLower(didYouMean), change.Original) {
			didYouMean = strings.ReplaceAll(strings.ToLower(didYouMean), change.Original, change.Correction)
			change := spellcheckmodels.SpellCheckChange{
				Original:  change.Original,
				Corrected: change.Correction,
			}
			changes = append(changes, change)
		}
	}
	return &spellcheckmodels.SpellCheck{
		OriginalText:        originalText,
		DidYouMean:          didYouMean,
		Location:            location,
		NumberOfCorrections: len(changes),
		Changes:             changes,
	}
}
