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

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/byteops"
)

type AggregateParser struct {
	authorizedGetClass classGetterWithAuthzFunc
}

func NewAggregateParser(authorizedGetClass classGetterWithAuthzFunc) *AggregateParser {
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
		clause, err := ExtractFilters(req.Filters, p.authorizedGetClass, req.Collection, req.Tenant)
		if err != nil {
			return nil, fmt.Errorf("extract filters: %w", err)
		}
		filter := &filters.LocalFilter{Root: &clause}
		if err := filters.ValidateFilters(p.authorizedGetClass, filter); err != nil {
			return nil, fmt.Errorf("validate filters: %w", err)
		}
		params.Filters = filter
	}

	// targetCombination is not supported with Aggregate queries
	targetVectors, _, _, err := extractTargetVectorsForAggregate(req, class)
	if err != nil {
		return nil, fmt.Errorf("extract target vectors: %w", err)
	}

	switch search := req.GetSearch().(type) {
	case *pb.AggregateRequest_NearVector:
		if nv := search.NearVector; nv != nil {
			params.NearVector, _, err = parseNearVec(nv, targetVectors, class, nil)
			if err != nil {
				return nil, fmt.Errorf("parse near vector: %w", err)
			}

			// The following business logic should not sit in the API. However, it is
			// also part of the GraphQL API, so we need to duplicate it in order to get
			// the same behavior
			if nv.Distance != nil && nv.Certainty != nil {
				return nil, fmt.Errorf("near_vector: cannot provide distance and certainty")
			}

			if nv.Certainty != nil {
				params.NearVector.Certainty = *nv.Certainty
			}

			if nv.Distance != nil {
				params.NearVector.Distance = *nv.Distance
				params.NearVector.WithDistance = true
			}
		}
	case *pb.AggregateRequest_NearObject:
		if no := search.NearObject; no != nil {
			if no.Id == "" {
				return nil, fmt.Errorf("near_object: id is required")
			}
			params.NearObject = &searchparams.NearObject{
				ID:            no.Id,
				TargetVectors: targetVectors,
			}

			// The following business logic should not sit in the API. However, it is
			// also part of the GraphQL API, so we need to duplicate it in order to get
			// the same behavior
			if no.Distance != nil && no.Certainty != nil {
				return nil, fmt.Errorf("near_object: cannot provide distance and certainty")
			}

			if no.Certainty != nil {
				params.NearObject.Certainty = *no.Certainty
			}

			if no.Distance != nil {
				params.NearObject.Distance = *no.Distance
				params.NearObject.WithDistance = true
			}
		}
	case *pb.AggregateRequest_NearText:
		if nt := search.NearText; nt != nil {
			limit := 0
			if params.ObjectLimit != nil {
				limit = *params.ObjectLimit
			}
			nearText, err := extractNearText(params.ClassName.String(), limit, nt, targetVectors)
			if err != nil {
				return nil, err
			}
			if params.ModuleParams == nil {
				params.ModuleParams = make(map[string]interface{})
			}
			params.ModuleParams["nearText"] = nearText
		}
	case *pb.AggregateRequest_NearImage:
		if ni := search.NearImage; ni != nil {
			nearImageOut, err := parseNearImage(ni, targetVectors)
			if err != nil {
				return nil, err
			}

			if params.ModuleParams == nil {
				params.ModuleParams = make(map[string]interface{})
			}
			params.ModuleParams["nearImage"] = nearImageOut
		}
	case *pb.AggregateRequest_NearAudio:
		if na := search.NearAudio; na != nil {
			nearAudioOut, err := parseNearAudio(na, targetVectors)
			if err != nil {
				return nil, err
			}

			if params.ModuleParams == nil {
				params.ModuleParams = make(map[string]interface{})
			}
			params.ModuleParams["nearAudio"] = nearAudioOut
		}
	case *pb.AggregateRequest_NearVideo:
		if nv := search.NearVideo; nv != nil {
			nearVideoOut, err := parseNearVideo(nv, targetVectors)
			if err != nil {
				return nil, err
			}

			if params.ModuleParams == nil {
				params.ModuleParams = make(map[string]interface{})
			}
			params.ModuleParams["nearVideo"] = nearVideoOut
		}
	case *pb.AggregateRequest_NearDepth:
		if nd := search.NearDepth; nd != nil {
			nearDepthOut, err := parseNearDepth(nd, targetVectors)
			if err != nil {
				return nil, err
			}

			if params.ModuleParams == nil {
				params.ModuleParams = make(map[string]interface{})
			}
			params.ModuleParams["nearDepth"] = nearDepthOut
		}
	case *pb.AggregateRequest_NearThermal:
		if nt := search.NearThermal; nt != nil {
			nearThermalOut, err := parseNearThermal(nt, targetVectors)
			if err != nil {
				return nil, err
			}

			if params.ModuleParams == nil {
				params.ModuleParams = make(map[string]interface{})
			}
			params.ModuleParams["nearThermal"] = nearThermalOut
		}
	case *pb.AggregateRequest_NearImu:
		if ni := search.NearImu; ni != nil {
			nearIMUOut, err := parseNearIMU(ni, targetVectors)
			if err != nil {
				return nil, err
			}
			if params.ModuleParams == nil {
				params.ModuleParams = make(map[string]interface{})
			}
			params.ModuleParams["nearIMU"] = nearIMUOut
		}
	case *pb.AggregateRequest_Hybrid:
		if hs := search.Hybrid; hs != nil {
			fusionType := common_filters.HybridFusionDefault
			if hs.FusionType == pb.Hybrid_FUSION_TYPE_RANKED {
				fusionType = common_filters.HybridRankedFusion
			} else if hs.FusionType == pb.Hybrid_FUSION_TYPE_RELATIVE_SCORE {
				fusionType = common_filters.HybridRelativeScoreFusion
			}

			var vector models.Vector
			// vectors has precedent for being more efficient
			if len(hs.Vectors) > 0 {
				switch len(hs.Vectors) {
				case 1:
					vector, err = extractVector(hs.Vectors[0])
					if err != nil {
						return nil, fmt.Errorf("hybrid: %w", err)
					}
				default:
					return nil, fmt.Errorf("hybrid: only 1 vector supported, found %d vectors", len(hs.Vectors))
				}
			} else if len(hs.VectorBytes) > 0 {
				vector = byteops.Fp32SliceFromBytes(hs.VectorBytes)
			} else if len(hs.Vector) > 0 {
				vector = hs.Vector
			}

			var distance float32
			withDistance := false
			if hs.Threshold != nil {
				withDistance = true
				switch hs.Threshold.(type) {
				case *pb.Hybrid_VectorDistance:
					distance = hs.Threshold.(*pb.Hybrid_VectorDistance).VectorDistance
				default:
					return nil, fmt.Errorf("unknown value type %v", hs.Threshold)
				}
			}

			limit := 0
			if params.ObjectLimit != nil {
				limit = *params.ObjectLimit
			}
			nearTxt, err := extractNearText(params.ClassName.String(), limit, search.Hybrid.NearText, targetVectors)
			if err != nil {
				return nil, err
			}
			nearVec := search.Hybrid.NearVector

			params.Hybrid = &searchparams.HybridSearch{
				Query:           hs.Query,
				Properties:      schema.LowercaseFirstLetterOfStrings(hs.Properties),
				Vector:          vector,
				Alpha:           float64(hs.Alpha),
				FusionAlgorithm: fusionType,
				TargetVectors:   targetVectors,
				Distance:        distance,
				WithDistance:    withDistance,
			}

			if hs.Bm25SearchOperator != nil {
				if hs.Bm25SearchOperator.MinimumOrTokensMatch != nil {
					params.Hybrid.MinimumOrTokensMatch = int(*hs.Bm25SearchOperator.MinimumOrTokensMatch)
				}
				params.Hybrid.SearchOperator = hs.Bm25SearchOperator.Operator.String()
			}

			if nearVec != nil {
				params.Hybrid.NearVectorParams, _, err = parseNearVec(nearVec, targetVectors, class, nil)
				if err != nil {
					return nil, err
				}

				params.Hybrid.TargetVectors = params.Hybrid.NearVectorParams.TargetVectors
				if nearVec.Distance != nil {
					params.Hybrid.NearVectorParams.Distance = *nearVec.Distance
					params.Hybrid.NearVectorParams.WithDistance = true
				}
				if nearVec.Certainty != nil {
					params.Hybrid.NearVectorParams.Certainty = *nearVec.Certainty
				}
			}

			if nearTxt != nil {
				params.Hybrid.NearTextParams = &searchparams.NearTextParams{
					Values:        nearTxt.Values,
					Limit:         nearTxt.Limit,
					Certainty:     nearTxt.Certainty,
					Distance:      nearTxt.Distance,
					WithDistance:  nearTxt.WithDistance,
					MoveAwayFrom:  searchparams.ExploreMove{Force: nearTxt.MoveAwayFrom.Force, Values: nearTxt.MoveAwayFrom.Values},
					MoveTo:        searchparams.ExploreMove{Force: nearTxt.MoveTo.Force, Values: nearTxt.MoveTo.Values},
					TargetVectors: targetVectors,
				}
			}
		}
	case nil:
		// do nothing, search is not set
	default:
		return nil, fmt.Errorf("unrecognized search: %T", req.GetSearch())
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

func extractTargetVectorsForAggregate(req *pb.AggregateRequest, class *models.Class) ([]string, *dto.TargetCombination, bool, error) {
	var targetVectors []string
	var targets *pb.Targets
	vectorSearch := false

	extract := func(targets *pb.Targets, targetVectors *[]string) ([]string, *pb.Targets, bool) {
		if targets != nil {
			return targets.TargetVectors, targets, true
		} else {
			return *targetVectors, nil, true
		}
	}
	if hs := req.GetHybrid(); hs != nil {
		targetVectors, targets, vectorSearch = extract(hs.Targets, &hs.TargetVectors)
	}
	if na := req.GetNearAudio(); na != nil {
		targetVectors, targets, vectorSearch = extract(na.Targets, &na.TargetVectors)
	}
	if nd := req.GetNearDepth(); nd != nil {
		targetVectors, targets, vectorSearch = extract(nd.Targets, &nd.TargetVectors)
	}
	if ni := req.GetNearImage(); ni != nil {
		targetVectors, targets, vectorSearch = extract(ni.Targets, &ni.TargetVectors)
	}
	if ni := req.GetNearImu(); ni != nil {
		targetVectors, targets, vectorSearch = extract(ni.Targets, &ni.TargetVectors)
	}
	if no := req.GetNearObject(); no != nil {
		targetVectors, targets, vectorSearch = extract(no.Targets, &no.TargetVectors)
	}
	if nt := req.GetNearText(); nt != nil {
		targetVectors, targets, vectorSearch = extract(nt.Targets, &nt.TargetVectors)
	}
	if nt := req.GetNearThermal(); nt != nil {
		targetVectors, targets, vectorSearch = extract(nt.Targets, &nt.TargetVectors)
	}
	if nv := req.GetNearVector(); nv != nil {
		targetVectors, targets, vectorSearch = extract(nv.Targets, &nv.TargetVectors)
	}
	if nv := req.GetNearVideo(); nv != nil {
		targetVectors, targets, vectorSearch = extract(nv.Targets, &nv.TargetVectors)
	}

	var combination *dto.TargetCombination
	if targets != nil {
		var err error
		if combination, err = extractTargets(targets); err != nil {
			return nil, nil, false, err
		}
	} else if len(targetVectors) > 1 {
		// here weights need to be added if the default combination requires it
		combination = &dto.TargetCombination{Type: dto.DefaultTargetCombinationType}
	}

	if vectorSearch && len(targetVectors) == 0 && !modelsext.ClassHasLegacyVectorIndex(class) {
		if len(class.VectorConfig) > 1 {
			return nil, nil, false, fmt.Errorf("class %s has multiple vectors, but no target vectors were provided", class.Class)
		} else if len(class.VectorConfig) == 1 {
			for targetVector := range class.VectorConfig {
				targetVectors = append(targetVectors, targetVector)
			}
		}
	}

	if vectorSearch {
		for _, target := range targetVectors {
			if _, ok := class.VectorConfig[target]; !ok {
				return nil, nil, false, fmt.Errorf("class %s does not have named vector %v configured. Available named vectors %v", class.Class, target, class.VectorConfig)
			}
		}
	}

	return targetVectors, combination, vectorSearch, nil
}
