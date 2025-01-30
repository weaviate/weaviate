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
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type AggregateParser struct {
	authorizedGetClass func(string) (*models.Class, error)
}

func NewAggregateParser(authorizedGetClass func(string) (*models.Class, error)) *AggregateParser {
	return &AggregateParser{
		authorizedGetClass: authorizedGetClass,
	}
}

func (p *AggregateParser) Aggregate(req *pb.AggregateRequest) (*aggregation.Params, error) {
	params := &aggregation.Params{}
	class, err := p.authorizedGetClass(req.Collection)
	if err != nil {
		return nil, err
	}

	params.ClassName = schema.ClassName(class.Class)
	params.Tenant = req.Tenant

	if req.ObjectLimit != nil {
		objectLimit := int(*req.ObjectLimit)
		params.ObjectLimit = &objectLimit
	}

	if req.GroupBy != nil {
		params.GroupBy = &filters.Path{
			Class:    schema.ClassName(req.GroupBy.Collection),
			Property: schema.PropertyName(req.GroupBy.Property),
		}
	}
	if req.Limit != nil {
		limit := int(*req.Limit)
		params.Limit = &limit
	}

	params.IncludeMetaCount = req.ObjectsCount

	if len(req.Aggregations) > 0 {
		properties := make([]aggregation.ParamProperty, len(req.Aggregations))
		for i := range req.Aggregations {
			properties[i] = aggregation.ParamProperty{
				Name:        schema.PropertyName(req.Aggregations[i].Property),
				Aggregators: parseAggregations(req.Aggregations[i]),
			}
		}
		params.Properties = properties
	}

	if req.Filters != nil {
		clause, err := ExtractFilters(req.Filters, p.authorizedGetClass, req.Collection)
		if err != nil {
			return nil, fmt.Errorf("extract filters: %w", err)
		}
		filter := &filters.LocalFilter{Root: &clause}
		if err := filters.ValidateFilters(p.authorizedGetClass, filter); err != nil {
			return nil, fmt.Errorf("validate filters: %w", err)
		}
		params.Filters = filter
	}

	return params, nil
}

func parseAggregations(in *pb.AggregateRequest_Aggregation) []aggregation.Aggregator {
	switch a := in.GetAggregation().(type) {
	case *pb.AggregateRequest_Aggregation_Int:
		var aggregators []aggregation.Aggregator
		if a.Int.Count {
			aggregators = append(aggregators, aggregation.CountAggregator)
		}
		if a.Int.Type {
			aggregators = append(aggregators, aggregation.TypeAggregator)
		}
		if a.Int.Mean {
			aggregators = append(aggregators, aggregation.MeanAggregator)
		}
		if a.Int.Median {
			aggregators = append(aggregators, aggregation.MedianAggregator)
		}
		if a.Int.Mode {
			aggregators = append(aggregators, aggregation.ModeAggregator)
		}
		if a.Int.Maximum {
			aggregators = append(aggregators, aggregation.MaximumAggregator)
		}
		if a.Int.Minimum {
			aggregators = append(aggregators, aggregation.MinimumAggregator)
		}
		if a.Int.Sum {
			aggregators = append(aggregators, aggregation.SumAggregator)
		}
		return aggregators
	case *pb.AggregateRequest_Aggregation_Number_:
		var aggregators []aggregation.Aggregator
		if a.Number.Count {
			aggregators = append(aggregators, aggregation.CountAggregator)
		}
		if a.Number.Type {
			aggregators = append(aggregators, aggregation.TypeAggregator)
		}
		if a.Number.Mean {
			aggregators = append(aggregators, aggregation.MeanAggregator)
		}
		if a.Number.Median {
			aggregators = append(aggregators, aggregation.MedianAggregator)
		}
		if a.Number.Mode {
			aggregators = append(aggregators, aggregation.ModeAggregator)
		}
		if a.Number.Maximum {
			aggregators = append(aggregators, aggregation.MaximumAggregator)
		}
		if a.Number.Minimum {
			aggregators = append(aggregators, aggregation.MinimumAggregator)
		}
		if a.Number.Sum {
			aggregators = append(aggregators, aggregation.SumAggregator)
		}
		return aggregators
	case *pb.AggregateRequest_Aggregation_Text_:
		var aggregators []aggregation.Aggregator
		if a.Text.Count {
			aggregators = append(aggregators, aggregation.CountAggregator)
		}
		if a.Text.Type {
			aggregators = append(aggregators, aggregation.TypeAggregator)
		}
		if a.Text.TopOccurences {
			if a.Text.TopOccurencesLimit != nil {
				limit := int(*a.Text.TopOccurencesLimit)
				aggregators = append(aggregators, aggregation.NewTopOccurrencesAggregator(&limit))
			} else {
				aggregators = append(aggregators, aggregation.TotalTrueAggregator)
			}
		}
		return aggregators
	case *pb.AggregateRequest_Aggregation_Boolean_:
		var aggregators []aggregation.Aggregator
		if a.Boolean.Count {
			aggregators = append(aggregators, aggregation.CountAggregator)
		}
		if a.Boolean.Type {
			aggregators = append(aggregators, aggregation.TypeAggregator)
		}
		if a.Boolean.TotalTrue {
			aggregators = append(aggregators, aggregation.TotalTrueAggregator)
		}
		if a.Boolean.TotalFalse {
			aggregators = append(aggregators, aggregation.TotalFalseAggregator)
		}
		if a.Boolean.PercentageTrue {
			aggregators = append(aggregators, aggregation.PercentageTrueAggregator)
		}
		if a.Boolean.PercentageFalse {
			aggregators = append(aggregators, aggregation.PercentageFalseAggregator)
		}
		return aggregators
	case *pb.AggregateRequest_Aggregation_Date_:
		var aggregators []aggregation.Aggregator
		if a.Date.Count {
			aggregators = append(aggregators, aggregation.CountAggregator)
		}
		if a.Date.Type {
			aggregators = append(aggregators, aggregation.TypeAggregator)
		}
		if a.Date.Median {
			aggregators = append(aggregators, aggregation.MedianAggregator)
		}
		if a.Date.Mode {
			aggregators = append(aggregators, aggregation.ModeAggregator)
		}
		if a.Date.Maximum {
			aggregators = append(aggregators, aggregation.MaximumAggregator)
		}
		if a.Date.Minimum {
			aggregators = append(aggregators, aggregation.MinimumAggregator)
		}
		return aggregators
	case *pb.AggregateRequest_Aggregation_Reference_:
		var aggregators []aggregation.Aggregator
		if a.Reference.Type {
			aggregators = append(aggregators, aggregation.TypeAggregator)
		}
		if a.Reference.PointingTo {
			aggregators = append(aggregators, aggregation.PointingToAggregator)
		}
		return aggregators
	default:
		return nil
	}
}
