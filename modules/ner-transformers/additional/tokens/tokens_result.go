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

package tokens

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/ner-transformers/ent"
)

func (p *TokenProvider) findTokens(ctx context.Context,
	in []search.Result, params *Params,
) ([]search.Result, error) {
	if len(in) > 0 {

		if len(in) == 0 {
			return nil, nil
		}

		if params == nil {
			return nil, fmt.Errorf("no params provided")
		}

		properties := params.GetProperties()

		// check if user parameter values are valid
		if len(properties) == 0 {
			return in, errors.New("no properties provided")
		}

		for i := range in { // for each result of the general GraphQL Query
			ap := in[i].AdditionalProperties
			if ap == nil {
				ap = models.AdditionalProperties{}
			}

			// check if the schema of the GraphQL data object contains the properties and they are text or string values
			textProperties := map[string]string{}
			schema := in[i].Object().Properties.(map[string]interface{})
			for property, value := range schema {
				if p.containsProperty(property, properties) {
					if valueString, ok := value.(string); ok && len(valueString) > 0 {
						textProperties[property] = valueString
					}
				}
			}

			certainty := params.GetCertainty()
			limit := params.GetLimit()
			tokensList := []ent.TokenResult{}

			// for each text property result, call the NER function and add to additional result
			for property, value := range textProperties {

				if limit != nil && len(tokensList) > *limit {
					break
				}

				tokens, err := p.ner.GetTokens(ctx, property, value)
				if err != nil {
					return in, err
				}

				tokens = cutOffByCertainty(tokens, certainty)

				tokensList = append(tokensList, tokens...)
			}

			if limit != nil && len(tokensList) > *limit {
				ap["tokens"] = tokensList[:*limit]
			} else {
				ap["tokens"] = tokensList
			}

			in[i].AdditionalProperties = ap
		}
	}
	return in, nil
}

func cutOffByCertainty(tokens []ent.TokenResult, certainty *float64) []ent.TokenResult {
	minCertainty := 0.0
	if certainty != nil {
		minCertainty = *certainty
	}
	a := 0
	for _, x := range tokens {
		if x.Certainty >= minCertainty {
			tokens[a] = x
			a++
		}
	}
	tokens = tokens[:a]

	return tokens
}

func (p *TokenProvider) containsProperty(property string, properties []string) bool {
	if len(properties) == 0 {
		return true
	}
	for i := range properties {
		if properties[i] == property {
			return true
		}
	}
	return false
}
