/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	contextionary "github.com/creativesoftwarefdn/weaviate/contextionary/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/filters"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

func parseWhere(args map[string]interface{}, kind kind.Kind) (*kinds.FetchSearch, error) {
	// the structure is already guaranteed by graphQL, we can therefore make
	// plenty of assertions without having to check. If required fields are not
	// set, graphQL will error before already and we won't get here.
	where := args["where"].(map[string]interface{})
	classMap := where["class"].(map[string]interface{})
	classKeywords := extractKeywords(classMap["keywords"])

	propertiesRaw := where["properties"].([]interface{})
	properties := make([]kinds.FetchSearchProperty, len(propertiesRaw), len(propertiesRaw))

	for i, prop := range propertiesRaw {
		propertiesMap := prop.(map[string]interface{})
		propertiesKeywords := extractKeywords(propertiesMap["keywords"])
		search := contextionary.SearchParams{
			SearchType: contextionary.SearchTypeProperty,
			Name:       propertiesMap["name"].(string),
			Certainty:  float32(propertiesMap["certainty"].(float64)),
			Keywords:   propertiesKeywords,
			Kind:       kind,
		}

		match, err := extractMatch(propertiesMap)
		if err != nil {
			return nil, fmt.Errorf("could not extract operator and matching value: %s", err)
		}

		properties[i] = kinds.FetchSearchProperty{
			Search: search,
			Match:  match,
		}
	}

	return &kinds.FetchSearch{
		Class: contextionary.SearchParams{
			SearchType: contextionary.SearchTypeClass,
			Name:       classMap["name"].(string),
			Certainty:  float32(classMap["certainty"].(float64)),
			Keywords:   classKeywords,
			Kind:       kind,
		},
		Properties: properties,
	}, nil
}

func extractKeywords(kw interface{}) models.SemanticSchemaKeywords {
	if kw == nil {
		return nil
	}

	asSlice := kw.([]interface{})
	result := make(models.SemanticSchemaKeywords, len(asSlice), len(asSlice))
	for i, keyword := range asSlice {
		keywordMap := keyword.(map[string]interface{})
		result[i] = &models.SemanticSchemaKeywordsItems0{
			Keyword: keywordMap["value"].(string),
			Weight:  float32(keywordMap["weight"].(float64)),
		}
	}

	return result
}

func extractMatch(prop map[string]interface{}) (kinds.FetchPropertyMatch, error) {
	operator, err := parseOperator(prop["operator"].(string))
	if err != nil {
		return kinds.FetchPropertyMatch{}, fmt.Errorf("could not parse operator: %s", err)
	}

	value, err := common_filters.ParseValue(prop)
	if err != nil {
		return kinds.FetchPropertyMatch{}, fmt.Errorf("could not parse value: %s", err)
	}

	return kinds.FetchPropertyMatch{
		Operator: operator,
		Value:    value,
	}, nil
}

func parseOperator(op string) (filters.Operator, error) {
	switch op {
	case "Equal":
		return filters.OperatorEqual, nil
	case "NotEqual":
		return filters.OperatorNotEqual, nil
	case "LessThan":
		return filters.OperatorLessThan, nil
	case "LessThanEqual":
		return filters.OperatorLessThanEqual, nil
	case "GreaterThan":
		return filters.OperatorGreaterThan, nil
	case "GreaterThanEqual":
		return filters.OperatorGreaterThanEqual, nil
	}

	return -1, fmt.Errorf("unknown operator '%s'", op)
}
