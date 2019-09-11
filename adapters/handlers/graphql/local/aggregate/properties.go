//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/common"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/models"
)

func numericPropertyFields(class *models.Class, property *models.Property, prefix string) *graphql.Object {
	getMetaIntFields := graphql.Fields{
		"sum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sSum", prefix, class.Class, property.Name),
			Description: descriptions.LocalAggregateSum,
			Type:        graphql.Float,
			Resolve:     makeResolveFieldAggregator("sum"),
		},
		"minimum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMinimum", prefix, class.Class, property.Name),
			Description: descriptions.LocalAggregateMin,
			Type:        graphql.Float,
			Resolve:     makeResolveFieldAggregator("minimum"),
		},
		"maximum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMaximum", prefix, class.Class, property.Name),
			Description: descriptions.LocalAggregateMax,
			Type:        graphql.Float,
			Resolve:     makeResolveFieldAggregator("maximum"),
		},
		"mean": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMean", prefix, class.Class, property.Name),
			Description: descriptions.LocalAggregateMean,
			Type:        graphql.Float,
			Resolve:     makeResolveFieldAggregator("mean"),
		},
		"mode": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMode", prefix, class.Class, property.Name),
			Description: descriptions.LocalAggregateMode,
			Type:        graphql.Float,
			Resolve:     makeResolveFieldAggregator("mode"),
		},
		"median": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMedian", prefix, class.Class, property.Name),
			Description: descriptions.LocalAggregateMedian,
			Type:        graphql.Float,
			Resolve:     makeResolveFieldAggregator("median"),
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.LocalAggregateCount,
			Type:        graphql.Int,
			Resolve:     makeResolveFieldAggregator("count"),
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaIntFields,
		Description: descriptions.LocalAggregatePropertyObject,
	})
}

func nonNumericPropertyFields(class *models.Class,
	property *models.Property, prefix string) *graphql.Object {
	getMetaPointingFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%sCount", prefix, class.Class),
			Description: descriptions.LocalAggregateCount,
			Type:        graphql.Int,
			Resolve:     makeResolveFieldAggregator("count"),
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaPointingFields,
		Description: descriptions.LocalAggregatePropertyObject,
	})
}

func booleanPropertyFields(class *models.Class,
	property *models.Property, prefix string) *graphql.Object {
	getMetaPointingFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.AggregatePropertyCount,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"totalTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalTrue", prefix, class.Class, property.Name),
			Description: descriptions.AggregateClassPropertyTotalTrue,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"percentageTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageTrue", prefix, class.Class, property.Name),
			Description: descriptions.AggregateClassPropertyPercentageTrue,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
		"totalFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalFalse", prefix, class.Class, property.Name),
			Description: descriptions.AggregateClassPropertyTotalFalse,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"percentageFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageFalse", prefix, class.Class, property.Name),
			Description: descriptions.AggregateClassPropertyPercentageFalse,
			Type:        graphql.Float,
			Resolve:     common.JSONNumberResolver,
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaPointingFields,
		Description: descriptions.LocalAggregatePropertyObject,
	})
}

func stringPropertyFields(class *models.Class,
	property *models.Property, prefix string) *graphql.Object {
	getAggregatePointingFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%sCount", prefix, class.Class),
			Description: descriptions.AggregatePropertyCount,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("%s%sTopOccurrences", prefix, class.Class),
			Description: descriptions.AggregatePropertyTopOccurrences,
			Type:        graphql.NewList(stringTopOccurrences(class, property, prefix)),
			Args: graphql.FieldConfigArgument{
				"first": &graphql.ArgumentConfig{
					Description: descriptions.First,
					Type:        graphql.Int,
				},
				"after": &graphql.ArgumentConfig{
					Description: descriptions.After,
					Type:        graphql.Int,
				},
			},
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getAggregatePointingFields,
		Description: descriptions.LocalAggregatePropertyObject,
	})
}

func stringTopOccurrences(class *models.Class,
	property *models.Property, prefix string) *graphql.Object {
	getAggregateAggregatePointingFields := graphql.Fields{
		"value": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTopOccurrencesValue", prefix, class.Class, property.Name),
			Description: descriptions.AggregatePropertyTopOccurrencesValue,
			Type:        graphql.String,
		},
		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTopOccurrencesOccurs", prefix, class.Class, property.Name),
			Description: descriptions.AggregatePropertyTopOccurrencesOccurs,
			Type:        graphql.Int,
			Resolve:     common.JSONNumberResolver,
		},
	}

	getAggregateAggregatePointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sTopOccurrencesObj", prefix, class.Class, property.Name),
		Fields:      getAggregateAggregatePointingFields,
		Description: descriptions.AggregatePropertyTopOccurrences,
	}

	return graphql.NewObject(getAggregateAggregatePointing)
}

func groupedByProperty(class *models.Class) *graphql.Object {
	classProperties := graphql.Fields{
		"path": &graphql.Field{
			Description: descriptions.LocalAggregateGroupedByGroupedByPath,
			Type:        graphql.NewList(graphql.String),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				switch typed := p.Source.(type) {
				case aggregation.GroupedBy:
					return typed.Path, nil
				case map[string]interface{}:
					return typed["path"], nil
				default:
					return nil, fmt.Errorf("groupedBy field %s: unsupported type %T", "path", p.Source)
				}
			},
		},
		"value": &graphql.Field{
			Description: descriptions.LocalAggregateGroupedByGroupedByValue,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				switch typed := p.Source.(type) {
				case aggregation.GroupedBy:
					return typed.Value, nil
				case map[string]interface{}:
					return typed["value"], nil
				default:
					return nil, fmt.Errorf("groupedBy field %s: unsupported type %T", "value", p.Source)
				}
			},
		},
	}

	classPropertiesObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("LocalAggregate%sGroupedByObj", class.Class),
		Fields:      classProperties,
		Description: descriptions.LocalAggregateGroupedByObj,
	})

	return classPropertiesObj
}

func makeResolveFieldAggregator(aggregator string) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		switch typed := p.Source.(type) {
		case aggregation.Property:
			return typed.NumericalAggregations[aggregator], nil
		case map[string]interface{}:
			return typed[aggregator], nil
		default:
			return nil, fmt.Errorf("aggregator %s, unsupported type %T", aggregator, p.Source)
		}
	}
}
