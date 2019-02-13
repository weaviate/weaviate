package fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/models"
)

type whereFilter struct {
	class      contextionary.SearchParams
	properties []whereProperty
}

type whereProperty struct {
	search contextionary.SearchParams
	match  PropertyMatch
}

func parseWhere(args map[string]interface{}, kind kind.Kind) (*whereFilter, error) {
	// the structure is already guaranteed by graphQL, we can therefore make
	// plenty of assertions without having to check. If required fields are not
	// set, graphQL will error before already and we won't get here.
	where := args["where"].(map[string]interface{})
	classMap := where["class"].(map[string]interface{})
	classKeywords := extractKeywords(classMap["keywords"])

	propertiesRaw := where["properties"].([]interface{})
	properties := make([]whereProperty, len(propertiesRaw), len(propertiesRaw))

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

		properties[i] = whereProperty{
			search: search,
			match:  match,
		}
	}

	return &whereFilter{
		class: contextionary.SearchParams{
			SearchType: contextionary.SearchTypeClass,
			Name:       classMap["name"].(string),
			Certainty:  float32(classMap["certainty"].(float64)),
			Keywords:   classKeywords,
			Kind:       kind,
		},
		properties: properties,
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

func extractMatch(prop map[string]interface{}) (PropertyMatch, error) {
	operator, err := parseOperator(prop["operator"].(string))
	if err != nil {
		return PropertyMatch{}, fmt.Errorf("could not parse operator: %s", err)
	}

	value, err := common_filters.ParseValue(prop)
	if err != nil {
		return PropertyMatch{}, fmt.Errorf("could not parse value: %s", err)
	}

	return PropertyMatch{
		Operator: operator,
		Value:    value,
	}, nil
}

func parseOperator(op string) (common_filters.Operator, error) {

	switch op {
	case "Equal":
		return common_filters.OperatorEqual, nil
	case "NotEqual":
		return common_filters.OperatorNotEqual, nil
	case "LessThan":
		return common_filters.OperatorLessThan, nil
	case "LessThanEqual":
		return common_filters.OperatorLessThanEqual, nil
	case "GreaterThan":
		return common_filters.OperatorGreaterThan, nil
	case "GreaterThanEqual":
		return common_filters.OperatorGreaterThanEqual, nil
	}

	return -1, fmt.Errorf("unknown operator '%s'", op)
}
