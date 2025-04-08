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
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type AggregateReplier struct {
	authorizedGetDataTypeOfProp func(string) (string, error)
}

func NewAggregateReplier(authorizedGetClass classGetterWithAuthzFunc, params *aggregation.Params) *AggregateReplier {
	return &AggregateReplier{
		authorizedGetDataTypeOfProp: func(propName string) (string, error) {
			class, err := authorizedGetClass(string(params.ClassName))
			if err != nil {
				return "", fmt.Errorf("get class: %w", err)
			}
			schemaProp, err := schema.GetPropertyByName(class, propName)
			if err != nil {
				return "", fmt.Errorf("get property by name: %w", err)
			}
			return string(schema.DataType(schemaProp.DataType[0])), nil
		},
	}
}

func (r *AggregateReplier) Aggregate(res interface{}, isGroupby bool) (*pb.AggregateReply, error) {
	var groups []*pb.AggregateReply_Group

	if res != nil {
		result, ok := res.(*aggregation.Result)
		if !ok {
			return nil, fmt.Errorf("unexpected aggregate result type: %T", res)
		}

		if !isGroupby {
			if len(result.Groups) == 0 {
				return &pb.AggregateReply{}, fmt.Errorf("no groups found in aggregate result")
			}
			group := result.Groups[0]
			count := int64(group.Count)
			aggregations, err := r.parseAggregatedProperties(group.Properties)
			if err != nil {
				return nil, fmt.Errorf("aggregations: %w", err)
			}
			return &pb.AggregateReply{Result: &pb.AggregateReply_SingleResult{SingleResult: &pb.AggregateReply_Single{
				ObjectsCount: &count,
				Aggregations: aggregations,
			}}}, nil
		}

		if len(result.Groups) > 0 {
			groups = make([]*pb.AggregateReply_Group, len(result.Groups))
			for i := range result.Groups {
				count := int64(result.Groups[i].Count)
				aggregations, err := r.parseAggregatedProperties(result.Groups[i].Properties)
				if err != nil {
					return nil, fmt.Errorf("aggregations: %w", err)
				}
				groupedBy, err := r.parseAggregateGroupedBy(result.Groups[i].GroupedBy)
				if err != nil {
					return nil, fmt.Errorf("groupedBy: %w", err)
				}
				groups[i] = &pb.AggregateReply_Group{
					ObjectsCount: &count,
					Aggregations: aggregations,
					GroupedBy:    groupedBy,
				}
			}
			return &pb.AggregateReply{Result: &pb.AggregateReply_GroupedResults{GroupedResults: &pb.AggregateReply_Grouped{Groups: groups}}}, nil
		}
	}
	return &pb.AggregateReply{}, nil
}

func (r *AggregateReplier) parseAggregateGroupedBy(in *aggregation.GroupedBy) (*pb.AggregateReply_Group_GroupedBy, error) {
	if in != nil {
		switch val := in.Value.(type) {
		case string:
			return &pb.AggregateReply_Group_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateReply_Group_GroupedBy_Text{Text: val},
			}, nil
		case bool:
			return &pb.AggregateReply_Group_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateReply_Group_GroupedBy_Boolean{Boolean: val},
			}, nil
		case float64:
			return &pb.AggregateReply_Group_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateReply_Group_GroupedBy_Number{Number: val},
			}, nil
		case int64:
			return &pb.AggregateReply_Group_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateReply_Group_GroupedBy_Int{Int: val},
			}, nil
		case []string:
			return &pb.AggregateReply_Group_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateReply_Group_GroupedBy_Texts{Texts: &pb.TextArray{Values: val}},
			}, nil
		case []bool:
			return &pb.AggregateReply_Group_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateReply_Group_GroupedBy_Booleans{Booleans: &pb.BooleanArray{Values: val}},
			}, nil
		case []float64:
			return &pb.AggregateReply_Group_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateReply_Group_GroupedBy_Numbers{Numbers: &pb.NumberArray{Values: val}},
			}, nil
		case []int64:
			return &pb.AggregateReply_Group_GroupedBy{
				Path:  in.Path,
				Value: &pb.AggregateReply_Group_GroupedBy_Ints{Ints: &pb.IntArray{Values: val}},
			}, nil
		default:
			return nil, fmt.Errorf("unrecognized grouped by value type: %T", in.Value)
		}
	}
	return nil, nil
}

func (r *AggregateReplier) parseAggregatedProperties(in map[string]aggregation.Property) (*pb.AggregateReply_Aggregations, error) {
	var aggregations *pb.AggregateReply_Aggregations
	if len(in) > 0 {
		propertyAggregations := []*pb.AggregateReply_Aggregations_Aggregation{}
		for name, property := range in {
			aggregationResult, err := r.parseAggregationResult(name, property)
			if err != nil {
				return nil, fmt.Errorf("parse aggregation property: %w", err)
			}
			propertyAggregations = append(propertyAggregations, aggregationResult)
		}
		aggregations = &pb.AggregateReply_Aggregations{
			Aggregations: propertyAggregations,
		}
	}
	return aggregations, nil
}

func (r *AggregateReplier) parseAggregationResult(propertyName string, property aggregation.Property) (*pb.AggregateReply_Aggregations_Aggregation, error) {
	switch property.Type {
	case aggregation.PropertyTypeNumerical:
		dataType, err := r.authorizedGetDataTypeOfProp(propertyName)
		if err != nil {
			return nil, fmt.Errorf("get data type of property: %w", err)
		}
		switch dataType {
		case "int", "int[]":
			integerAggregation, err := parseIntegerAggregation(property.SchemaType, property.NumericalAggregations)
			if err != nil {
				return nil, fmt.Errorf("parse integer aggregation: %w", err)
			}
			return &pb.AggregateReply_Aggregations_Aggregation{
				Property:    propertyName,
				Aggregation: &pb.AggregateReply_Aggregations_Aggregation_Int{Int: integerAggregation},
			}, nil
		default:
			numericalAggregation, err := parseNumericalAggregation(property.SchemaType, property.NumericalAggregations)
			if err != nil {
				return nil, fmt.Errorf("parse numerical aggregation: %w", err)
			}
			return &pb.AggregateReply_Aggregations_Aggregation{
				Property:    propertyName,
				Aggregation: &pb.AggregateReply_Aggregations_Aggregation_Number_{Number: numericalAggregation},
			}, nil
		}
	case aggregation.PropertyTypeText:
		textAggregation := parseTextAggregation(property.SchemaType, property.TextAggregation)
		return &pb.AggregateReply_Aggregations_Aggregation{
			Property:    propertyName,
			Aggregation: &pb.AggregateReply_Aggregations_Aggregation_Text_{Text: textAggregation},
		}, nil
	case aggregation.PropertyTypeBoolean:
		booleanAggregation := parseBooleanAggregation(property.SchemaType, property.BooleanAggregation)
		return &pb.AggregateReply_Aggregations_Aggregation{
			Property:    propertyName,
			Aggregation: &pb.AggregateReply_Aggregations_Aggregation_Boolean_{Boolean: booleanAggregation},
		}, nil
	case aggregation.PropertyTypeDate:
		dateAggregation, err := parseDateAggregation(property.SchemaType, property.DateAggregations)
		if err != nil {
			return nil, fmt.Errorf("parse date aggregation: %w", err)
		}
		return &pb.AggregateReply_Aggregations_Aggregation{
			Property:    propertyName,
			Aggregation: &pb.AggregateReply_Aggregations_Aggregation_Date_{Date: dateAggregation},
		}, nil
	case aggregation.PropertyTypeReference:
		referenceAggregation := parseReferenceAggregation(property.SchemaType, property.ReferenceAggregation)
		return &pb.AggregateReply_Aggregations_Aggregation{
			Property:    propertyName,
			Aggregation: &pb.AggregateReply_Aggregations_Aggregation_Reference_{Reference: referenceAggregation},
		}, nil
	default:
		return nil, fmt.Errorf("unknown property type: %s", property.Type)
	}
}

func parseNumericalAggregation(schemaType string, in map[string]interface{}) (*pb.AggregateReply_Aggregations_Aggregation_Number, error) {
	var number *pb.AggregateReply_Aggregations_Aggregation_Number
	if len(in) > 0 {
		number = &pb.AggregateReply_Aggregations_Aggregation_Number{}
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

func parseIntegerAggregation(schemaType string, in map[string]interface{}) (*pb.AggregateReply_Aggregations_Aggregation_Integer, error) {
	var number *pb.AggregateReply_Aggregations_Aggregation_Integer
	if len(in) > 0 {
		number = &pb.AggregateReply_Aggregations_Aggregation_Integer{}
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

func parseTextAggregation(schemaType string, in aggregation.Text) *pb.AggregateReply_Aggregations_Aggregation_Text {
	var topOccurences *pb.AggregateReply_Aggregations_Aggregation_Text_TopOccurrences
	if len(in.Items) > 0 {
		items := make([]*pb.AggregateReply_Aggregations_Aggregation_Text_TopOccurrences_TopOccurrence, len(in.Items))
		for i := range in.Items {
			items[i] = &pb.AggregateReply_Aggregations_Aggregation_Text_TopOccurrences_TopOccurrence{
				Value:  in.Items[i].Value,
				Occurs: int64(in.Items[i].Occurs),
			}
		}
		topOccurences = &pb.AggregateReply_Aggregations_Aggregation_Text_TopOccurrences{Items: items}
	}
	return &pb.AggregateReply_Aggregations_Aggregation_Text{
		Count:         ptInt64(in.Count),
		Type:          &schemaType,
		TopOccurences: topOccurences,
	}
}

func parseBooleanAggregation(schemaType string, in aggregation.Boolean) *pb.AggregateReply_Aggregations_Aggregation_Boolean {
	// TODO: check if it was requested
	return &pb.AggregateReply_Aggregations_Aggregation_Boolean{
		Count:           ptInt64(in.Count),
		Type:            &schemaType,
		TotalTrue:       ptInt64(in.TotalTrue),
		TotalFalse:      ptInt64(in.TotalFalse),
		PercentageTrue:  &in.PercentageTrue,
		PercentageFalse: &in.PercentageFalse,
	}
}

func parseDateAggregation(schemaType string, in map[string]interface{}) (*pb.AggregateReply_Aggregations_Aggregation_Date, error) {
	var date *pb.AggregateReply_Aggregations_Aggregation_Date
	if len(in) > 0 {
		date = &pb.AggregateReply_Aggregations_Aggregation_Date{}
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

func parseReferenceAggregation(schemaType string, in aggregation.Reference) *pb.AggregateReply_Aggregations_Aggregation_Reference {
	return &pb.AggregateReply_Aggregations_Aggregation_Reference{
		Type:       &schemaType,
		PointingTo: in.PointingTo,
	}
}

func ptInt64[T int | float64](in T) *int64 {
	out := int64(in)
	return &out
}
