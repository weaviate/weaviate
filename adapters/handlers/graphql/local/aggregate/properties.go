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

package aggregate

import (
	"fmt"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
)

func numericPropertyFields(class *models.Class, property *models.Property, prefix string) *graphql.Object {
	getMetaIntFields := graphql.Fields{
		"sum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sSum", prefix, class.Class, property.Name),
			Description: descriptions.AggregateSum,
			Type:        graphql.Float,
			Resolve:     makeResolveNumericFieldAggregator("sum"),
		},
		"minimum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMinimum", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMin,
			Type:        graphql.Float,
			Resolve:     makeResolveNumericFieldAggregator("minimum"),
		},
		"maximum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMaximum", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMax,
			Type:        graphql.Float,
			Resolve:     makeResolveNumericFieldAggregator("maximum"),
		},
		"mean": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMean", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMean,
			Type:        graphql.Float,
			Resolve:     makeResolveNumericFieldAggregator("mean"),
		},
		"mode": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMode", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMode,
			Type:        graphql.Float,
			Resolve:     makeResolveNumericFieldAggregator("mode"),
		},
		"median": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMedian", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMedian,
			Type:        graphql.Float,
			Resolve:     makeResolveNumericFieldAggregator("median"),
		},
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.AggregateCount,
			Type:        graphql.Int,
			Resolve:     makeResolveNumericFieldAggregator("count"),
		},
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sType", prefix, class.Class, property.Name),
			Description: descriptions.AggregateCount,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				prop, ok := p.Source.(aggregation.Property)
				if !ok {
					return nil, fmt.Errorf("numerical: type: expected aggregation.Property, got %T", p.Source)
				}

				return prop.SchemaType, nil
			},
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaIntFields,
		Description: descriptions.AggregatePropertyObject,
	})
}

func datePropertyFields(class *models.Class,
	property *models.Property, prefix string,
) *graphql.Object {
	getMetaDateFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%sCount", prefix, class.Class),
			Description: descriptions.AggregateCount,
			Type:        graphql.Int,
			Resolve:     makeResolveDateFieldAggregator("count"),
		},
		"minimum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMinimum", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMin,
			Type:        graphql.String,
			Resolve:     makeResolveDateFieldAggregator("minimum"),
		},
		"maximum": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMaximum", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMax,
			Type:        graphql.String,
			Resolve:     makeResolveDateFieldAggregator("maximum"),
		},
		"mode": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMode", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMode,
			Type:        graphql.String,
			Resolve:     makeResolveDateFieldAggregator("mode"),
		},
		"median": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sMedian", prefix, class.Class, property.Name),
			Description: descriptions.AggregateMedian,
			Type:        graphql.String,
			Resolve:     makeResolveDateFieldAggregator("median"),
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaDateFields,
		Description: descriptions.AggregatePropertyObject,
	})
}

func referencePropertyFields(class *models.Class,
	property *models.Property, prefix string,
) *graphql.Object {
	getMetaPointingFields := graphql.Fields{
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%sType", prefix, class.Class),
			Description: descriptions.AggregatePropertyType,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				prop, ok := p.Source.(aggregation.Property)
				if !ok {
					return nil, fmt.Errorf("ref property type: expected aggregation.Property, got %T",
						p.Source)
				}

				return prop.SchemaType, nil
			},
		},
		"pointingTo": &graphql.Field{
			Name:        fmt.Sprintf("%s%sPointingTo", prefix, class.Class),
			Description: descriptions.AggregateClassPropertyPointingTo,
			Type:        graphql.NewList(graphql.String),
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				ref, err := extractReferenceAggregation(p.Source)
				if err != nil {
					return nil, fmt.Errorf("ref property pointingTo: %v", err)
				}

				return ref.PointingTo, nil
			},
			DeprecationReason: "Experimental, the format will change",
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaPointingFields,
		Description: descriptions.AggregatePropertyObject,
	})
}

func extractReferenceAggregation(source interface{}) (*aggregation.Reference, error) {
	property, ok := source.(aggregation.Property)
	if !ok {
		return nil, fmt.Errorf("expected aggregation.Property, got %T", source)
	}

	if property.Type != aggregation.PropertyTypeReference {
		return nil, fmt.Errorf("expected property to be of type reference, got %s", property.Type)
	}

	return &property.ReferenceAggregation, nil
}

func booleanPropertyFields(class *models.Class,
	property *models.Property, prefix string,
) *graphql.Object {
	getMetaPointingFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sCount", prefix, class.Class, property.Name),
			Description: descriptions.AggregatePropertyCount,
			Type:        graphql.Int,
			Resolve:     booleanResolver(func(b aggregation.Boolean) interface{} { return b.Count }),
		},
		"totalTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalTrue", prefix, class.Class, property.Name),
			Description: descriptions.AggregateClassPropertyTotalTrue,
			Type:        graphql.Int,
			Resolve:     booleanResolver(func(b aggregation.Boolean) interface{} { return b.TotalTrue }),
		},
		"percentageTrue": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageTrue", prefix, class.Class, property.Name),
			Description: descriptions.AggregateClassPropertyPercentageTrue,
			Type:        graphql.Float,
			Resolve:     booleanResolver(func(b aggregation.Boolean) interface{} { return b.PercentageTrue }),
		},
		"totalFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTotalFalse", prefix, class.Class, property.Name),
			Description: descriptions.AggregateClassPropertyTotalFalse,
			Type:        graphql.Int,
			Resolve:     booleanResolver(func(b aggregation.Boolean) interface{} { return b.TotalFalse }),
		},
		"percentageFalse": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sPercentageFalse", prefix, class.Class, property.Name),
			Description: descriptions.AggregateClassPropertyPercentageFalse,
			Type:        graphql.Float,
			Resolve:     booleanResolver(func(b aggregation.Boolean) interface{} { return b.PercentageFalse }),
		},
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sType", prefix, class.Class, property.Name),
			Description: descriptions.AggregateCount,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				prop, ok := p.Source.(aggregation.Property)
				if !ok {
					return nil, fmt.Errorf("boolean: type: expected aggregation.Property, got %T", p.Source)
				}

				return prop.SchemaType, nil
			},
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getMetaPointingFields,
		Description: descriptions.AggregatePropertyObject,
	})
}

type booleanExtractorFunc func(aggregation.Boolean) interface{}

func booleanResolver(extractor booleanExtractorFunc) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		boolean, err := extractBooleanAggregation(p.Source)
		if err != nil {
			return nil, fmt.Errorf("boolean: %v", err)
		}

		return extractor(*boolean), nil
	}
}

func extractBooleanAggregation(source interface{}) (*aggregation.Boolean, error) {
	property, ok := source.(aggregation.Property)
	if !ok {
		return nil, fmt.Errorf("expected aggregation.Property, got %T", source)
	}

	if property.Type != aggregation.PropertyTypeBoolean {
		return nil, fmt.Errorf("expected property to be of type boolean, got %s", property.Type)
	}

	return &property.BooleanAggregation, nil
}

func stringPropertyFields(class *models.Class,
	property *models.Property, prefix string,
) *graphql.Object {
	getAggregatePointingFields := graphql.Fields{
		"count": &graphql.Field{
			Name:        fmt.Sprintf("%s%sCount", prefix, class.Class),
			Description: descriptions.AggregatePropertyCount,
			Type:        graphql.Int,
			Resolve: textResolver(func(text aggregation.Text) (interface{}, error) {
				return text.Count, nil
			}),
		},
		"type": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sType", prefix, class.Class, property.Name),
			Description: descriptions.AggregateCount,
			Type:        graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				prop, ok := p.Source.(aggregation.Property)
				if !ok {
					return nil, fmt.Errorf("text type: expected aggregation.Property, got %T", p.Source)
				}

				return prop.SchemaType, nil
			},
		},
		"topOccurrences": &graphql.Field{
			Name:        fmt.Sprintf("%s%sTopOccurrences", prefix, class.Class),
			Description: descriptions.AggregatePropertyTopOccurrences,
			Type:        graphql.NewList(stringTopOccurrences(class, property, prefix)),
			Resolve: textResolver(func(text aggregation.Text) (interface{}, error) {
				list := make([]interface{}, len(text.Items))
				for i, to := range text.Items {
					list[i] = to
				}

				return list, nil
			}),
			Args: graphql.FieldConfigArgument{
				"limit": &graphql.ArgumentConfig{
					Description: descriptions.First,
					Type:        graphql.Int,
				},
			},
		},
	}

	return graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sObj", prefix, class.Class, property.Name),
		Fields:      getAggregatePointingFields,
		Description: descriptions.AggregatePropertyObject,
	})
}

type textExtractorFunc func(aggregation.Text) (interface{}, error)

func textResolver(extractor textExtractorFunc) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		text, err := extractTextAggregation(p.Source)
		if err != nil {
			return nil, fmt.Errorf("text: %v", err)
		}

		return extractor(text)
	}
}

func stringTopOccurrences(class *models.Class,
	property *models.Property, prefix string,
) *graphql.Object {
	getAggregateAggregatePointingFields := graphql.Fields{
		"value": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTopOccurrencesValue", prefix, class.Class, property.Name),
			Description: descriptions.AggregatePropertyTopOccurrencesValue,
			Type:        graphql.String,
			Resolve:     textOccurrenceResolver(func(t aggregation.TextOccurrence) interface{} { return t.Value }),
		},
		"occurs": &graphql.Field{
			Name:        fmt.Sprintf("%s%s%sTopOccurrencesOccurs", prefix, class.Class, property.Name),
			Description: descriptions.AggregatePropertyTopOccurrencesOccurs,
			Type:        graphql.Int,
			Resolve:     textOccurrenceResolver(func(t aggregation.TextOccurrence) interface{} { return t.Occurs }),
		},
	}

	getAggregateAggregatePointing := graphql.ObjectConfig{
		Name:        fmt.Sprintf("%s%s%sTopOccurrencesObj", prefix, class.Class, property.Name),
		Fields:      getAggregateAggregatePointingFields,
		Description: descriptions.AggregatePropertyTopOccurrences,
	}

	return graphql.NewObject(getAggregateAggregatePointing)
}

type textOccurrenceExtractorFunc func(aggregation.TextOccurrence) interface{}

func textOccurrenceResolver(extractor textOccurrenceExtractorFunc) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		textOccurrence, ok := p.Source.(aggregation.TextOccurrence)
		if !ok {
			return nil, fmt.Errorf("textOccurrence: %s: expected aggregation.TextOccurrence, but got %T",
				p.Info.FieldName, p.Source)
		}

		return extractor(textOccurrence), nil
	}
}

func extractTextAggregation(source interface{}) (aggregation.Text, error) {
	property, ok := source.(aggregation.Property)
	if !ok {
		return aggregation.Text{}, fmt.Errorf("expected aggregation.Property, got %T", source)
	}

	if property.Type == aggregation.PropertyTypeNumerical {
		// in this case we can only use count
		return aggregation.Text{
			Count: property.NumericalAggregations["count"].(int),
		}, nil
	}

	if property.Type != aggregation.PropertyTypeText {
		return aggregation.Text{}, fmt.Errorf("expected property to be of type text, got %s (%#v)", property.Type, property)
	}

	return property.TextAggregation, nil
}

func groupedByProperty(class *models.Class) *graphql.Object {
	classProperties := graphql.Fields{
		"path": &graphql.Field{
			Description: descriptions.AggregateGroupedByGroupedByPath,
			Type:        graphql.NewList(graphql.String),
			Resolve:     groupedByResolver(func(g *aggregation.GroupedBy) interface{} { return g.Path }),
		},
		"value": &graphql.Field{
			Description: descriptions.AggregateGroupedByGroupedByValue,
			Type:        graphql.String,
			Resolve:     groupedByResolver(func(g *aggregation.GroupedBy) interface{} { return g.Value }),
		},
	}

	classPropertiesObj := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("Aggregate%sGroupedByObj", class.Class),
		Fields:      classProperties,
		Description: descriptions.AggregateGroupedByObj,
	})

	return classPropertiesObj
}

type groupedByExtractorFunc func(*aggregation.GroupedBy) interface{}

func groupedByResolver(extractor groupedByExtractorFunc) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		groupedBy, ok := p.Source.(*aggregation.GroupedBy)
		if !ok {
			return nil, fmt.Errorf("groupedBy: %s: expected aggregation.GroupedBy, but got %T",
				p.Info.FieldName, p.Source)
		}

		return extractor(groupedBy), nil
	}
}

func makeResolveNumericFieldAggregator(aggregator string) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		num, err := extractNumericAggregation(p.Source)
		if err != nil {
			return nil, fmt.Errorf("numerical aggregator %s: %v", aggregator, err)
		}

		return num[aggregator], nil
	}
}

func extractNumericAggregation(source interface{}) (map[string]interface{}, error) {
	property, ok := source.(aggregation.Property)
	if !ok {
		return nil, fmt.Errorf("expected aggregation.Property, got %T", source)
	}

	if property.Type != aggregation.PropertyTypeNumerical {
		return nil, fmt.Errorf("expected property to be of type numerical, got %s", property.Type)
	}

	return property.NumericalAggregations, nil
}

func makeResolveDateFieldAggregator(aggregator string) func(p graphql.ResolveParams) (interface{}, error) {
	return func(p graphql.ResolveParams) (interface{}, error) {
		date, err := extractDateAggregation(p.Source)
		if err != nil {
			return nil, fmt.Errorf("date aggregator %s: %v", aggregator, err)
		}

		return date[aggregator], nil
	}
}

func extractDateAggregation(source interface{}) (map[string]interface{}, error) {
	property, ok := source.(aggregation.Property)
	if !ok {
		return nil, fmt.Errorf("expected aggregation.Property, got %T", source)
	}

	if property.Type != aggregation.PropertyTypeDate {
		return nil, fmt.Errorf("expected property to be of type date, got %s", property.Type)
	}

	return property.DateAggregations, nil
}
