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

package v1

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/aggregation"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type AggregateReplier struct{}

func NewAggregateReplier() *AggregateReplier {
	return &AggregateReplier{}
}

func (r *AggregateReplier) Aggregate(res interface{}) (*pb.AggregateReply_Result, error) {
	var groups []*pb.AggregateGroup

	if res != nil {
		result, ok := res.(*aggregation.Result)
		if !ok {
			return nil, fmt.Errorf("unexpected aggregate result type: %T", res)
		}

		if len(result.Groups) > 0 {
			groups = make([]*pb.AggregateGroup, len(result.Groups))
			for i := range result.Groups {
				count := int64(result.Groups[i].Count)
				aggregations, err := parseAggregatedProperties(result.Groups[i].Properties)
				if err != nil {
					return nil, fmt.Errorf("aggregations: %w", err)
				}
				groupedBy, err := parseAggregateGroupedBy(result.Groups[i].GroupedBy)
				if err != nil {
					return nil, fmt.Errorf("groupedBy: %w", err)
				}
				groups[i] = &pb.AggregateGroup{
					ObjectsCount: &count,
					Aggregations: aggregations,
					GroupedBy:    groupedBy,
				}
			}
		}
	}

	return &pb.AggregateReply_Result{Groups: groups}, nil
}

func parseAggregateGroupedBy(in *aggregation.GroupedBy) (*pb.AggregateGroup_GroupedBy, error) {
	if in != nil {
		switch val := in.Value.(type) {
		case string:
			return &pb.AggregateGroup_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateGroup_GroupedBy_Text{Text: val},
			}, nil
		case bool:
			return &pb.AggregateGroup_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateGroup_GroupedBy_Boolean{Boolean: val},
			}, nil
		case float64:
			return &pb.AggregateGroup_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateGroup_GroupedBy_Number{Number: val},
			}, nil
		case int64:
			return &pb.AggregateGroup_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateGroup_GroupedBy_Int{Int: val},
			}, nil
		case []string:
			return &pb.AggregateGroup_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateGroup_GroupedBy_Texts{Texts: &pb.TextArray{Values: val}},
			}, nil
		case []bool:
			return &pb.AggregateGroup_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateGroup_GroupedBy_Booleans{Booleans: &pb.BooleanArray{Values: val}},
			}, nil
		case []float64:
			return &pb.AggregateGroup_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateGroup_GroupedBy_Numbers{Numbers: &pb.NumberArray{Values: val}},
			}, nil
		case []int64:
			return &pb.AggregateGroup_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateGroup_GroupedBy_Ints{Ints: &pb.IntArray{Values: val}},
			}, nil
		default:
			return nil, fmt.Errorf("unrecognized grouped by value type: %T", in.Value)
		}
	}
	return nil, nil
}

func parseAggregatedProperties(in map[string]aggregation.Property) (*pb.AggregateGroup_Aggregations, error) {
	var aggregations *pb.AggregateGroup_Aggregations
	if len(in) > 0 {
		propertyAggregations := []*pb.AggregateGroup_Aggregations_Aggregation{}
		for name, property := range in {
			aggregationResult, err := parseAggregationResult(name, property)
			if err != nil {
				return nil, fmt.Errorf("parse aggregation property: %w", err)
			}
			propertyAggregations = append(propertyAggregations, aggregationResult)
		}
		aggregations = &pb.AggregateGroup_Aggregations{
			Aggregations: propertyAggregations,
		}
	}
	return aggregations, nil
}

func parseAggregationResult(propertyName string, property aggregation.Property) (*pb.AggregateGroup_Aggregations_Aggregation, error) {
	switch property.Type {
	case aggregation.PropertyTypeNumerical:
		switch property.SchemaType {
		case "int", "int[]":
			integerAggregation, err := parseIntegerAggregation(property.SchemaType, property.NumericalAggregations)
			if err != nil {
				return nil, fmt.Errorf("parse integer aggregation: %w", err)
			}
			return &pb.AggregateGroup_Aggregations_Aggregation{
				Property:    propertyName,
				Aggregation: &pb.AggregateGroup_Aggregations_Aggregation_Int{Int: integerAggregation},
			}, nil
		default:
			numericalAggregation, err := parseNumericalAggregation(property.SchemaType, property.NumericalAggregations)
			if err != nil {
				return nil, fmt.Errorf("parse numerical aggregation: %w", err)
			}
			return &pb.AggregateGroup_Aggregations_Aggregation{
				Property:    propertyName,
				Aggregation: &pb.AggregateGroup_Aggregations_Aggregation_Number_{Number: numericalAggregation},
			}, nil
		}
	case aggregation.PropertyTypeText:
		textAggregation := parseTextAggregation(property.SchemaType, property.TextAggregation)
		return &pb.AggregateGroup_Aggregations_Aggregation{
			Property:    propertyName,
			Aggregation: &pb.AggregateGroup_Aggregations_Aggregation_Text_{Text: textAggregation},
		}, nil
	case aggregation.PropertyTypeBoolean:
		booleanAggregation := parseBooleanAggregation(property.SchemaType, property.BooleanAggregation)
		return &pb.AggregateGroup_Aggregations_Aggregation{
			Property:    propertyName,
			Aggregation: &pb.AggregateGroup_Aggregations_Aggregation_Boolean_{Boolean: booleanAggregation},
		}, nil
	case aggregation.PropertyTypeDate:
		dateAggregation, err := parseDateAggregation(property.SchemaType, property.DateAggregations)
		if err != nil {
			return nil, fmt.Errorf("parse date aggregation: %w", err)
		}
		return &pb.AggregateGroup_Aggregations_Aggregation{
			Property:    propertyName,
			Aggregation: &pb.AggregateGroup_Aggregations_Aggregation_Date_{Date: dateAggregation},
		}, nil
	case aggregation.PropertyTypeReference:
		referenceAggregation := parseReferenceAggregation(property.SchemaType, property.ReferenceAggregation)
		return &pb.AggregateGroup_Aggregations_Aggregation{
			Property:    propertyName,
			Aggregation: &pb.AggregateGroup_Aggregations_Aggregation_Reference_{Reference: referenceAggregation},
		}, nil
	default:
		return nil, fmt.Errorf("unknown property type: %s", property.Type)
	}
}

func parseNumericalAggregation(schemaType string, in map[string]interface{}) (*pb.AggregateGroup_Aggregations_Aggregation_Number, error) {
	var number *pb.AggregateGroup_Aggregations_Aggregation_Number
	if len(in) > 0 {
		number = &pb.AggregateGroup_Aggregations_Aggregation_Number{}
		number.Type = &schemaType
		for name, value := range in {
			switch val := value.(type) {
			case float64:
				switch name {
				case aggregation.CountAggregator.String():
					number.Count = ptInt64(val)
				case aggregation.MeanAggregator.String():
					number.Mean = &val
				case aggregation.MedianAggregator.String():
					number.Median = &val
				case aggregation.ModeAggregator.String():
					number.Mode = &val
				case aggregation.MaximumAggregator.String():
					number.Maximum = &val
				case aggregation.MinimumAggregator.String():
					number.Minimum = &val
				case aggregation.SumAggregator.String():
					number.Sum = &val
				default:
					return nil, fmt.Errorf("unknown numerical value aggregation type: %s", name)
				}
			default:
				return nil, fmt.Errorf("unknown numerical value type: %T", value)
			}
		}
	}
	return number, nil
}

func parseIntegerAggregation(schemaType string, in map[string]interface{}) (*pb.AggregateGroup_Aggregations_Aggregation_Integer, error) {
	var number *pb.AggregateGroup_Aggregations_Aggregation_Integer
	if len(in) > 0 {
		number = &pb.AggregateGroup_Aggregations_Aggregation_Integer{}
		number.Type = &schemaType
		for name, value := range in {
			switch val := value.(type) {
			case float64:
				switch name {
				case aggregation.CountAggregator.String():
					number.Count = ptInt64(val)
				case aggregation.MeanAggregator.String():
					number.Mean = &val
				case aggregation.MedianAggregator.String():
					number.Median = &val
				case aggregation.ModeAggregator.String():
					number.Mode = ptInt64(val)
				case aggregation.MaximumAggregator.String():
					number.Maximum = ptInt64(val)
				case aggregation.MinimumAggregator.String():
					number.Minimum = ptInt64(val)
				case aggregation.SumAggregator.String():
					number.Sum = ptInt64(val)
				default:
					return nil, fmt.Errorf("unknown integer value aggregation type: %s", name)
				}
			default:
				return nil, fmt.Errorf("unknown integer value type: %T", value)
			}
		}
	}
	return number, nil
}

func parseTextAggregation(schemaType string, in aggregation.Text) *pb.AggregateGroup_Aggregations_Aggregation_Text {
	var topOccurences *pb.AggregateGroup_Aggregations_Aggregation_Text_TopOccurrences
	if len(in.Items) > 0 {
		items := make([]*pb.AggregateGroup_Aggregations_Aggregation_Text_TopOccurrences_TopOccurrence, len(in.Items))
		for i := range in.Items {
			items[i] = &pb.AggregateGroup_Aggregations_Aggregation_Text_TopOccurrences_TopOccurrence{
				Value:  in.Items[i].Value,
				Occurs: int64(in.Items[i].Occurs),
			}
		}
		topOccurences = &pb.AggregateGroup_Aggregations_Aggregation_Text_TopOccurrences{Items: items}
	}
	return &pb.AggregateGroup_Aggregations_Aggregation_Text{
		Count:         ptInt64(in.Count),
		Type:          &schemaType,
		TopOccurences: topOccurences,
	}
}

func parseBooleanAggregation(schemaType string, in aggregation.Boolean) *pb.AggregateGroup_Aggregations_Aggregation_Boolean {
	// TODO: check if it was requested
	return &pb.AggregateGroup_Aggregations_Aggregation_Boolean{
		Count:           ptInt64(in.Count),
		Type:            &schemaType,
		TotalTrue:       ptInt64(in.TotalTrue),
		TotalFalse:      ptInt64(in.TotalFalse),
		PercentageTrue:  &in.PercentageTrue,
		PercentageFalse: &in.PercentageFalse,
	}
}

func parseDateAggregation(schemaType string, in map[string]interface{}) (*pb.AggregateGroup_Aggregations_Aggregation_Date, error) {
	var date *pb.AggregateGroup_Aggregations_Aggregation_Date
	if len(in) > 0 {
		date = &pb.AggregateGroup_Aggregations_Aggregation_Date{}
		date.Type = &schemaType
		for name, value := range in {
			switch val := value.(type) {
			case int64:
				if name == aggregation.CountAggregator.String() {
					date.Count = &val
				}
			case string:
				switch name {
				case aggregation.MedianAggregator.String():
					date.Median = &val
				case aggregation.ModeAggregator.String():
					date.Mode = &val
				case aggregation.MaximumAggregator.String():
					date.Maximum = &val
				case aggregation.MinimumAggregator.String():
					date.Minimum = &val
				default:
					return nil, fmt.Errorf("unknown date value aggregation type: %s", name)
				}
			default:
				return nil, fmt.Errorf("unknown date value type: %T", value)
			}
		}
	}
	return date, nil
}

func parseReferenceAggregation(schemaType string, in aggregation.Reference) *pb.AggregateGroup_Aggregations_Aggregation_Reference {
	return &pb.AggregateGroup_Aggregations_Aggregation_Reference{
		Type:       &schemaType,
		PointingTo: in.PointingTo,
	}
}

func ptInt64[T int | float64](in T) *int64 {
	out := int64(in)
	return &out
}
