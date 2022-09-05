//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package summary

import (
	"context"
	"errors"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/modules/sum-transformers/ent"
)

func (p *SummaryProvider) findSummary(ctx context.Context,
	in []search.Result, params *Params,
) ([]search.Result, error) {
	if len(in) == 0 {
		return nil, nil
	} else {
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

			summaryList := []ent.SummaryResult{}

			// for each text property result, call the SUM function and add to additional result
			for property, value := range textProperties {
				summary, err := p.sum.GetSummary(ctx, property, value)
				if err != nil {
					return in, err
				}

				summaryList = append(summaryList, summary...)
			}

			ap["summary"] = summaryList

			in[i].AdditionalProperties = ap
		}
	}
	return in, nil
}

func (p *SummaryProvider) containsProperty(property string, properties []string) bool {
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
