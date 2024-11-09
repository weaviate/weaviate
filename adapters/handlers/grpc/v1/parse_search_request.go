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

	"github.com/weaviate/weaviate/entities/schema/configvalidation"
	"github.com/weaviate/weaviate/usecases/config"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/byteops"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearAudio"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearDepth"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImage"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearImu"
	nearText2 "github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearText"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearThermal"
	"github.com/weaviate/weaviate/usecases/modulecomponents/arguments/nearVideo"
)

func searchParamsFromProto(req *pb.SearchRequest, getClass func(string) *models.Class, config *config.Config) (dto.GetParams, error) {
	out := dto.GetParams{}
	class := getClass(req.Collection)
	if class == nil {
		return dto.GetParams{}, fmt.Errorf("could not find class %s in schema", req.Collection)
	}

	out.ClassName = req.Collection
	out.ReplicationProperties = extractReplicationProperties(req.ConsistencyLevel)

	out.Tenant = req.Tenant

	targetVectors, targetCombination, vectorSearch, err := extractTargetVectors(req, class)
	if err != nil {
		return dto.GetParams{}, errors.Wrap(err, "extract target vectors")
	}
	out.TargetVectorCombination = targetCombination

	if req.Metadata != nil {
		addProps, err := extractAdditionalPropsFromMetadata(class, req.Metadata, targetVectors, vectorSearch)
		if err != nil {
			return dto.GetParams{}, errors.Wrap(err, "extract additional props")
		}
		out.AdditionalProperties = addProps
	}

	out.Properties, err = extractPropertiesRequest(req.Properties, getClass, req.Collection, req.Uses_123Api, targetVectors, vectorSearch)
	if err != nil {
		return dto.GetParams{}, errors.Wrap(err, "extract properties request")
	}
	if len(out.Properties) == 0 {
		out.AdditionalProperties.NoProps = true
	}

	if bm25 := req.Bm25Search; bm25 != nil {
		out.KeywordRanking = &searchparams.KeywordRanking{Query: bm25.Query, Properties: schema.LowercaseFirstLetterOfStrings(bm25.Properties), Type: "bm25", AdditionalExplanations: out.AdditionalProperties.ExplainScore}
	}

	if nv := req.NearVector; nv != nil {
		out.NearVector, err = parseNearVec(nv, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}

		// The following business logic should not sit in the API. However, it is
		// also part of the GraphQL API, so we need to duplicate it in order to get
		// the same behavior
		if nv.Distance != nil && nv.Certainty != nil {
			return out, fmt.Errorf("near_vector: cannot provide distance and certainty")
		}

		if nv.Certainty != nil {
			out.NearVector.Certainty = *nv.Certainty
		}

		if nv.Distance != nil {
			out.NearVector.Distance = *nv.Distance
			out.NearVector.WithDistance = true
		}
	}

	if no := req.NearObject; no != nil {
		if no.Id == "" {
			return dto.GetParams{}, fmt.Errorf("near_object: id is required")
		}
		out.NearObject = &searchparams.NearObject{
			ID:            no.Id,
			TargetVectors: targetVectors,
		}

		// The following business logic should not sit in the API. However, it is
		// also part of the GraphQL API, so we need to duplicate it in order to get
		// the same behavior
		if no.Distance != nil && no.Certainty != nil {
			return out, fmt.Errorf("near_object: cannot provide distance and certainty")
		}

		if no.Certainty != nil {
			out.NearObject.Certainty = *no.Certainty
		}

		if no.Distance != nil {
			out.NearObject.Distance = *no.Distance
			out.NearObject.WithDistance = true
		}
	}

	if ni := req.NearImage; ni != nil {
		nearImageOut, err := parseNearImage(ni, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}

		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearImage"] = nearImageOut
	}

	if na := req.NearAudio; na != nil {
		nearAudioOut, err := parseNearAudio(na, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}

		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearAudio"] = nearAudioOut
	}

	if nv := req.NearVideo; nv != nil {
		nearVideoOut, err := parseNearVideo(nv, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}

		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearVideo"] = nearVideoOut
	}

	if nd := req.NearDepth; nd != nil {
		nearDepthOut, err := parseNearDepth(nd, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}

		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearDepth"] = nearDepthOut
	}

	if nt := req.NearThermal; nt != nil {
		nearThermalOut, err := parseNearThermal(nt, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}

		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearThermal"] = nearThermalOut
	}

	if ni := req.NearImu; ni != nil {
		nearIMUOut, err := parseNearIMU(ni, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}
		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearIMU"] = nearIMUOut
	}

	out.Pagination = &filters.Pagination{Offset: int(req.Offset), Autocut: int(req.Autocut)}
	if req.Limit > 0 {
		out.Pagination.Limit = int(req.Limit)
	} else {
		out.Pagination.Limit = int(config.QueryDefaults.Limit)
	}

	// Hybrid search now has the ability to run subsearches using the real nearvector and neartext searches.  So we need to extract those settings the same way we prepare for the real searches.
	if hs := req.HybridSearch; hs != nil {
		fusionType := common_filters.HybridFusionDefault
		if hs.FusionType == pb.Hybrid_FUSION_TYPE_RANKED {
			fusionType = common_filters.HybridRankedFusion
		} else if hs.FusionType == pb.Hybrid_FUSION_TYPE_RELATIVE_SCORE {
			fusionType = common_filters.HybridRelativeScoreFusion
		}

		var vector []float32
		// bytes vector has precedent for being more efficient
		if len(hs.VectorBytes) > 0 {
			vector = byteops.Float32FromByteVector(hs.VectorBytes)
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
				return dto.GetParams{}, fmt.Errorf("unknown value type %v", hs.Threshold)
			}
		}

		nearTxt, err := extractNearText(out.ClassName, out.Pagination.Limit, req.HybridSearch.NearText, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}
		nearVec := req.HybridSearch.NearVector

		out.HybridSearch = &searchparams.HybridSearch{
			Query:           hs.Query,
			Properties:      schema.LowercaseFirstLetterOfStrings(hs.Properties),
			Vector:          vector,
			Alpha:           float64(hs.Alpha),
			FusionAlgorithm: fusionType,
			TargetVectors:   targetVectors,
			Distance:        distance,
			WithDistance:    withDistance,
		}

		if nearVec != nil {
			out.HybridSearch.NearVectorParams, err = parseNearVec(nearVec, targetVectors)
			if err != nil {
				return dto.GetParams{}, err
			}

			if nearVec.Distance != nil {
				out.HybridSearch.NearVectorParams.Distance = *nearVec.Distance
				out.HybridSearch.NearVectorParams.WithDistance = true
			}
			if nearVec.Certainty != nil {
				out.HybridSearch.NearVectorParams.Certainty = *nearVec.Certainty
			}
		}

		if nearTxt != nil {
			out.HybridSearch.NearTextParams = &searchparams.NearTextParams{
				Values:        nearTxt.Values,
				Limit:         nearTxt.Limit,
				MoveAwayFrom:  searchparams.ExploreMove{Force: nearTxt.MoveAwayFrom.Force, Values: nearTxt.MoveAwayFrom.Values},
				MoveTo:        searchparams.ExploreMove{Force: nearTxt.MoveTo.Force, Values: nearTxt.MoveTo.Values},
				TargetVectors: targetVectors,
			}
		}
	}

	var nearText *nearText2.NearTextParams
	if req.NearText != nil {
		nearText, err = extractNearText(out.ClassName, out.Pagination.Limit, req.NearText, targetVectors)
		if err != nil {
			return dto.GetParams{}, err
		}
		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearText"] = nearText
	}

	if req.Generative != nil {
		if out.AdditionalProperties.ModuleParams == nil {
			out.AdditionalProperties.ModuleParams = make(map[string]interface{})
		}
		out.AdditionalProperties.ModuleParams["generate"] = extractGenerative(req)
	}

	if req.Rerank != nil {
		if out.AdditionalProperties.ModuleParams == nil {
			out.AdditionalProperties.ModuleParams = make(map[string]interface{})
		}
		out.AdditionalProperties.ModuleParams["rerank"] = extractRerank(req)
	}

	if len(req.After) > 0 {
		out.Cursor = &filters.Cursor{After: req.After, Limit: out.Pagination.Limit}
	}

	if req.Filters != nil {
		clause, err := extractFilters(req.Filters, getClass, req.Collection)
		if err != nil {
			return dto.GetParams{}, err
		}
		filter := &filters.LocalFilter{Root: &clause}
		if err := filters.ValidateFilters(getClass, filter); err != nil {
			return dto.GetParams{}, err
		}
		out.Filters = filter
	}

	if len(req.SortBy) > 0 {
		if req.NearText != nil || req.NearVideo != nil || req.NearAudio != nil || req.NearImage != nil || req.NearObject != nil || req.NearVector != nil || req.HybridSearch != nil || req.Bm25Search != nil || req.Generative != nil {
			return dto.GetParams{}, errors.New("sorting cannot be combined with search")
		}
		out.Sort = extractSorting(req.SortBy)
	}

	if req.GroupBy != nil {
		groupBy, err := extractGroupBy(req.GroupBy, &out)
		if err != nil {
			return dto.GetParams{}, err
		}
		out.AdditionalProperties.Group = true

		out.GroupBy = groupBy
	}

	if out.HybridSearch != nil && out.HybridSearch.NearTextParams != nil && out.HybridSearch.NearVectorParams != nil {
		return dto.GetParams{}, errors.New("cannot combine nearText and nearVector in hybrid search")
	}
	if out.HybridSearch != nil && out.HybridSearch.NearTextParams != nil && out.HybridSearch.Vector != nil {
		return dto.GetParams{}, errors.New("cannot combine nearText and query in hybrid search")
	}
	if out.HybridSearch != nil && out.HybridSearch.NearVectorParams != nil && out.HybridSearch.Vector != nil {
		return dto.GetParams{}, errors.New("cannot combine nearVector and vector in hybrid search")
	}
	return out, nil
}

func extractGroupBy(groupIn *pb.GroupBy, out *dto.GetParams) (*searchparams.GroupBy, error) {
	if len(groupIn.Path) != 1 {
		return nil, fmt.Errorf("groupby path can only have one entry, received %v", groupIn.Path)
	}

	var additionalGroupProperties []search.SelectProperty
	for _, prop := range out.Properties {
		additionalGroupHitProp := search.SelectProperty{Name: prop.Name}
		additionalGroupHitProp.Refs = append(additionalGroupHitProp.Refs, prop.Refs...)
		additionalGroupHitProp.IsPrimitive = prop.IsPrimitive
		additionalGroupHitProp.IsObject = prop.IsObject
		additionalGroupProperties = append(additionalGroupProperties, additionalGroupHitProp)

	}

	groupOut := &searchparams.GroupBy{
		Property:        groupIn.Path[0],
		ObjectsPerGroup: int(groupIn.ObjectsPerGroup),
		Groups:          int(groupIn.NumberOfGroups),
		Properties:      additionalGroupProperties,
	}

	// add the property in case it was not requested as return prop - otherwise it is not resolved
	if out.Properties.FindProperty(groupOut.Property) == nil {
		out.Properties = append(out.Properties, search.SelectProperty{Name: groupOut.Property, IsPrimitive: true})
	}
	out.AdditionalProperties.NoProps = false

	return groupOut, nil
}

func extractTargetVectors(req *pb.SearchRequest, class *models.Class) ([]string, *dto.TargetCombination, bool, error) {
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
	if hs := req.HybridSearch; hs != nil {
		targetVectors, targets, vectorSearch = extract(hs.Targets, &hs.TargetVectors)
	}
	if na := req.NearAudio; na != nil {
		targetVectors, targets, vectorSearch = extract(na.Targets, &na.TargetVectors)
	}
	if nd := req.NearDepth; nd != nil {
		targetVectors, targets, vectorSearch = extract(nd.Targets, &nd.TargetVectors)
	}
	if ni := req.NearImage; ni != nil {
		targetVectors, targets, vectorSearch = extract(ni.Targets, &ni.TargetVectors)
	}
	if ni := req.NearImu; ni != nil {
		targetVectors, targets, vectorSearch = extract(ni.Targets, &ni.TargetVectors)
	}
	if no := req.NearObject; no != nil {
		targetVectors, targets, vectorSearch = extract(no.Targets, &no.TargetVectors)
	}
	if nt := req.NearText; nt != nil {
		targetVectors, targets, vectorSearch = extract(nt.Targets, &nt.TargetVectors)
	}
	if nt := req.NearThermal; nt != nil {
		targetVectors, targets, vectorSearch = extract(nt.Targets, &nt.TargetVectors)
	}
	if nv := req.NearVector; nv != nil {
		targetVectors, targets, vectorSearch = extract(nv.Targets, &nv.TargetVectors)
	}
	if nv := req.NearVideo; nv != nil {
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

	if vectorSearch && len(targetVectors) == 0 {
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

func extractTargets(in *pb.Targets) (*dto.TargetCombination, error) {
	if in == nil {
		return nil, nil
	}

	var combinationType dto.TargetCombinationType
	weights := make(map[string]float32, len(in.Weights))
	switch in.Combination {
	case pb.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE:
		combinationType = dto.Average
		for _, target := range in.TargetVectors {
			weights[target] = 1.0 / float32(len(in.TargetVectors))
		}
	case pb.CombinationMethod_COMBINATION_METHOD_TYPE_SUM:
		combinationType = dto.Sum
		for _, target := range in.TargetVectors {
			weights[target] = 1.0
		}
	case pb.CombinationMethod_COMBINATION_METHOD_TYPE_MIN:
		combinationType = dto.Minimum
	case pb.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL:
		if len(in.Weights) != len(in.TargetVectors) {
			return nil, fmt.Errorf("number of weights (%d) does not match number of targets (%d)", len(in.Weights), len(in.TargetVectors))
		}
		combinationType = dto.ManualWeights
		for k, v := range in.Weights {
			weights[k] = v
		}
	case pb.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE:
		combinationType = dto.RelativeScore
		for k, v := range in.Weights {
			weights[k] = v
		}
	case pb.CombinationMethod_COMBINATION_METHOD_UNSPECIFIED:
		combinationType = dto.DefaultTargetCombinationType
	default:
		return nil, fmt.Errorf("unknown combination method %v", in.Combination)
	}
	return &dto.TargetCombination{Weights: weights, Type: combinationType}, nil
}

func extractSorting(sortIn []*pb.SortBy) []filters.Sort {
	sortOut := make([]filters.Sort, len(sortIn))
	for i := range sortIn {
		order := "asc"
		if !sortIn[i].Ascending {
			order = "desc"
		}
		sortOut[i] = filters.Sort{Order: order, Path: sortIn[i].Path}
	}
	return sortOut
}

func extractGenerative(req *pb.SearchRequest) *generate.Params {
	generative := generate.Params{}
	if req.Generative.SingleResponsePrompt != "" {
		generative.Prompt = &req.Generative.SingleResponsePrompt
	}
	if req.Generative.GroupedResponseTask != "" {
		generative.Task = &req.Generative.GroupedResponseTask
	}
	if len(req.Generative.GroupedProperties) > 0 {
		generative.Properties = req.Generative.GroupedProperties
	}
	return &generative
}

func extractRerank(req *pb.SearchRequest) *rank.Params {
	rerank := rank.Params{
		Property: &req.Rerank.Property,
	}
	if req.Rerank.Query != nil {
		rerank.Query = req.Rerank.Query
	}
	return &rerank
}

func extractNearText(classname string, limit int, nearTextIn *pb.NearTextSearch, targetVectors []string) (*nearText2.NearTextParams, error) {
	if nearTextIn == nil {
		return nil, nil
	}

	moveAwayOut, err := extractNearTextMove(classname, nearTextIn.MoveAway)
	if err != nil {
		return &nearText2.NearTextParams{}, err
	}
	moveToOut, err := extractNearTextMove(classname, nearTextIn.MoveTo)
	if err != nil {
		return &nearText2.NearTextParams{}, err
	}

	nearText := &nearText2.NearTextParams{
		Values:        nearTextIn.Query,
		Limit:         limit,
		MoveAwayFrom:  moveAwayOut,
		MoveTo:        moveToOut,
		TargetVectors: targetVectors,
	}

	if nearTextIn.Certainty != nil {
		nearText.Certainty = *nearTextIn.Certainty
	}
	if nearTextIn.Distance != nil {
		nearText.Distance = *nearTextIn.Distance
		nearText.WithDistance = true
	}
	return nearText, nil
}

func extractNearTextMove(classname string, Move *pb.NearTextSearch_Move) (nearText2.ExploreMove, error) {
	var moveAwayOut nearText2.ExploreMove

	if moveAwayReq := Move; moveAwayReq != nil {
		moveAwayOut.Force = moveAwayReq.Force
		if len(moveAwayReq.Uuids) > 0 {
			moveAwayOut.Objects = make([]nearText2.ObjectMove, len(moveAwayReq.Uuids))
			for i, objUUid := range moveAwayReq.Uuids {
				uuidFormat, err := uuid.Parse(objUUid)
				if err != nil {
					return moveAwayOut, err
				}
				moveAwayOut.Objects[i] = nearText2.ObjectMove{
					ID:     objUUid,
					Beacon: crossref.NewLocalhost(classname, strfmt.UUID(uuidFormat.String())).String(),
				}
			}
		}

		moveAwayOut.Values = moveAwayReq.Concepts
	}
	return moveAwayOut, nil
}

func extractPropertiesRequest(reqProps *pb.PropertiesRequest, getClass func(string) *models.Class, className string, usesNewDefaultLogic bool, targetVectors []string, vectorSearch bool) ([]search.SelectProperty, error) {
	props := make([]search.SelectProperty, 0)

	if reqProps == nil {
		// No properties selected at all, return all non-ref properties.
		// Ignore blobs to not overload the response
		nonRefProps, err := getAllNonRefNonBlobProperties(getClass, className)
		if err != nil {
			return nil, errors.Wrap(err, "get all non ref non blob properties")
		}
		return nonRefProps, nil
	}

	if !usesNewDefaultLogic {
		// Old stubs being used, use deprecated method
		return extractPropertiesRequestDeprecated(reqProps, getClass, className, targetVectors, vectorSearch)
	}

	if reqProps.ReturnAllNonrefProperties {
		// No non-ref return properties selected, return all non-ref properties.
		// Ignore blobs to not overload the response
		returnProps, err := getAllNonRefNonBlobProperties(getClass, className)
		if err != nil {
			return nil, errors.Wrap(err, "get all non ref non blob properties")
		}
		props = append(props, returnProps...)
	} else if len(reqProps.NonRefProperties) > 0 {
		// Non-ref properties are selected, return only those specified
		// This catches the case where users send an empty list of non ref properties as their request,
		// i.e. they want no non-ref properties
		for _, prop := range reqProps.NonRefProperties {
			props = append(props, search.SelectProperty{
				Name:        schema.LowercaseFirstLetter(prop),
				IsPrimitive: true,
				IsObject:    false,
			})
		}
	}

	if len(reqProps.RefProperties) > 0 {
		class := getClass(className)
		if class == nil {
			return nil, fmt.Errorf("could not find class %s in schema", className)
		}

		for _, prop := range reqProps.RefProperties {
			normalizedRefPropName := schema.LowercaseFirstLetter(prop.ReferenceProperty)
			schemaProp, err := schema.GetPropertyByName(class, normalizedRefPropName)
			if err != nil {
				return nil, err
			}

			var linkedClassName string
			if len(schemaProp.DataType) == 1 {
				// use datatype of the reference property to get the name of the linked class
				linkedClassName = schemaProp.DataType[0]
			} else {
				linkedClassName = prop.TargetCollection
				if linkedClassName == "" {
					return nil, fmt.Errorf(
						"multi target references from collection %v and property %v with need an explicit"+
							"linked collection. Available linked collections are %v",
						className, prop.ReferenceProperty, schemaProp.DataType)
				}
			}
			var refProperties []search.SelectProperty
			var addProps additional.Properties
			if prop.Properties != nil {
				refProperties, err = extractPropertiesRequest(prop.Properties, getClass, linkedClassName, usesNewDefaultLogic, targetVectors, vectorSearch)
				if err != nil {
					return nil, errors.Wrap(err, "extract properties request")
				}
			}
			linkedClass := getClass(linkedClassName)
			if prop.Metadata != nil {
				addProps, err = extractAdditionalPropsFromMetadata(linkedClass, prop.Metadata, targetVectors, vectorSearch)
				if err != nil {
					return nil, errors.Wrap(err, "extract additional props for refs")
				}
			}

			if prop.Properties == nil {
				refProperties, err = getAllNonRefNonBlobProperties(getClass, linkedClassName)
				if err != nil {
					return nil, errors.Wrap(err, "get all non ref non blob properties")
				}
			}
			if len(refProperties) == 0 && isIdOnlyRequest(prop.Metadata) {
				// This is a pure-ID query without any properties or additional metadata.
				// Indicate this to the DB, so it can optimize accordingly
				addProps.NoProps = true
			}

			props = append(props, search.SelectProperty{
				Name:        normalizedRefPropName,
				IsPrimitive: false,
				IsObject:    false,
				Refs: []search.SelectClass{{
					ClassName:            linkedClassName,
					RefProperties:        refProperties,
					AdditionalProperties: addProps,
				}},
			})
		}
	}

	if len(reqProps.ObjectProperties) > 0 {
		props = append(props, extractNestedProperties(reqProps.ObjectProperties)...)
	}

	return props, nil
}

func extractPropertiesRequestDeprecated(reqProps *pb.PropertiesRequest, getClass func(string) *models.Class, className string, targetVectors []string, vectorSearch bool) ([]search.SelectProperty, error) {
	if reqProps == nil {
		return nil, nil
	}
	props := make([]search.SelectProperty, 0)
	if len(reqProps.NonRefProperties) > 0 {
		for _, prop := range reqProps.NonRefProperties {
			props = append(props, search.SelectProperty{
				Name:        schema.LowercaseFirstLetter(prop),
				IsPrimitive: true,
				IsObject:    false,
			})
		}
	}

	if len(reqProps.RefProperties) > 0 {
		class := getClass(className)
		if class == nil {
			return []search.SelectProperty{}, fmt.Errorf("could not find class %s in schema", className)
		}

		for _, prop := range reqProps.RefProperties {
			normalizedRefPropName := schema.LowercaseFirstLetter(prop.ReferenceProperty)
			schemaProp, err := schema.GetPropertyByName(class, normalizedRefPropName)
			if err != nil {
				return nil, err
			}

			var linkedClassName string
			if len(schemaProp.DataType) == 1 {
				// use datatype of the reference property to get the name of the linked class
				linkedClassName = schemaProp.DataType[0]
			} else {
				linkedClassName = prop.TargetCollection
				if linkedClassName == "" {
					return nil, fmt.Errorf(
						"multi target references from collection %v and property %v with need an explicit"+
							"linked collection. Available linked collections are %v",
						className, prop.ReferenceProperty, schemaProp.DataType)
				}
			}
			var refProperties []search.SelectProperty
			var addProps additional.Properties
			if prop.Properties != nil {
				refProperties, err = extractPropertiesRequestDeprecated(prop.Properties, getClass, linkedClassName, targetVectors, vectorSearch)
				if err != nil {
					return nil, errors.Wrap(err, "extract properties request")
				}
			}
			linkedClass := getClass(linkedClassName)
			if prop.Metadata != nil {
				addProps, err = extractAdditionalPropsFromMetadata(linkedClass, prop.Metadata, targetVectors, vectorSearch)
				if err != nil {
					return nil, errors.Wrap(err, "extract additional props for refs")
				}
			}

			if prop.Properties == nil {
				refProperties, err = getAllNonRefNonBlobProperties(getClass, linkedClassName)
				if err != nil {
					return nil, errors.Wrap(err, "get all non ref non blob properties")
				}
			}
			if len(refProperties) == 0 && isIdOnlyRequest(prop.Metadata) {
				// This is a pure-ID query without any properties or additional metadata.
				// Indicate this to the DB, so it can optimize accordingly
				addProps.NoProps = true
			}

			props = append(props, search.SelectProperty{
				Name:        normalizedRefPropName,
				IsPrimitive: false,
				IsObject:    false,
				Refs: []search.SelectClass{{
					ClassName:            linkedClassName,
					RefProperties:        refProperties,
					AdditionalProperties: addProps,
				}},
			})
		}
	}

	if len(reqProps.ObjectProperties) > 0 {
		props = append(props, extractNestedProperties(reqProps.ObjectProperties)...)
	}

	return props, nil
}

func extractNestedProperties(props []*pb.ObjectPropertiesRequest) []search.SelectProperty {
	selectProps := make([]search.SelectProperty, 0)
	for _, prop := range props {
		nestedProps := make([]search.SelectProperty, 0)
		if len(prop.PrimitiveProperties) > 0 {
			for _, primitive := range prop.PrimitiveProperties {
				nestedProps = append(nestedProps, search.SelectProperty{
					Name:        schema.LowercaseFirstLetter(primitive),
					IsPrimitive: true,
					IsObject:    false,
				})
			}
		}
		if len(prop.ObjectProperties) > 0 {
			nestedProps = append(nestedProps, extractNestedProperties(prop.ObjectProperties)...)
		}
		selectProps = append(selectProps, search.SelectProperty{
			Name:        schema.LowercaseFirstLetter(prop.PropName),
			IsPrimitive: false,
			IsObject:    true,
			Props:       nestedProps,
		})
	}
	return selectProps
}

func extractAdditionalPropsFromMetadata(class *models.Class, prop *pb.MetadataRequest, targetVectors []string, vectorSearch bool) (additional.Properties, error) {
	props := additional.Properties{
		Vector:             prop.Vector,
		ID:                 prop.Uuid,
		CreationTimeUnix:   prop.CreationTimeUnix,
		LastUpdateTimeUnix: prop.LastUpdateTimeUnix,
		Distance:           prop.Distance,
		Score:              prop.Score,
		ExplainScore:       prop.ExplainScore,
		IsConsistent:       prop.IsConsistent,
		Vectors:            prop.Vectors,
	}

	if vectorSearch && configvalidation.CheckCertaintyCompatibility(class, targetVectors) != nil {
		props.Certainty = false
	} else {
		props.Certainty = prop.Certainty
	}

	// return all named vectors if vector is true
	if prop.Vector && len(class.VectorConfig) > 0 {
		props.Vectors = make([]string, 0, len(class.VectorConfig))
		for vectorName := range class.VectorConfig {
			props.Vectors = append(props.Vectors, vectorName)
		}

	}

	return props, nil
}

func isIdOnlyRequest(metadata *pb.MetadataRequest) bool {
	// could also use reflect here but this is more explicit
	return (metadata != nil &&
		metadata.Uuid &&
		!metadata.Vector &&
		!metadata.CreationTimeUnix &&
		!metadata.LastUpdateTimeUnix &&
		!metadata.Distance &&
		!metadata.Certainty &&
		!metadata.Score &&
		!metadata.ExplainScore &&
		!metadata.IsConsistent)
}

func getAllNonRefNonBlobProperties(getClass func(string) *models.Class, className string) ([]search.SelectProperty, error) {
	var props []search.SelectProperty
	class := getClass(className)
	if class == nil {
		return []search.SelectProperty{}, fmt.Errorf("could not find class %s in schema", className)
	}

	for _, prop := range class.Properties {
		dt, err := schema.GetPropertyDataType(class, prop.Name)
		if err != nil {
			return []search.SelectProperty{}, errors.Wrap(err, "get property data type")
		}
		if *dt == schema.DataTypeCRef || *dt == schema.DataTypeBlob {
			continue
		}
		if *dt == schema.DataTypeObject || *dt == schema.DataTypeObjectArray {
			nested, err := schema.GetPropertyByName(class, prop.Name)
			if err != nil {
				return []search.SelectProperty{}, errors.Wrap(err, "get nested property by name")
			}
			nestedProps, err := getAllNonRefNonBlobNestedProperties(&Property{Property: nested})
			if err != nil {
				return []search.SelectProperty{}, errors.Wrap(err, "get all non ref non blob nested properties")
			}
			props = append(props, search.SelectProperty{
				Name:        prop.Name,
				IsPrimitive: false,
				IsObject:    true,
				Props:       nestedProps,
			})
		} else {
			props = append(props, search.SelectProperty{
				Name:        prop.Name,
				IsPrimitive: true,
			})
		}
	}
	return props, nil
}

func getAllNonRefNonBlobNestedProperties[P schema.PropertyInterface](property P) ([]search.SelectProperty, error) {
	var props []search.SelectProperty
	for _, prop := range property.GetNestedProperties() {
		dt, err := schema.GetNestedPropertyDataType(property, prop.Name)
		if err != nil {
			return []search.SelectProperty{}, errors.Wrap(err, "get nested property data type")
		}
		if *dt == schema.DataTypeCRef || *dt == schema.DataTypeBlob {
			continue
		}
		if *dt == schema.DataTypeObject || *dt == schema.DataTypeObjectArray {
			nested, err := schema.GetNestedPropertyByName(property, prop.Name)
			if err != nil {
				return []search.SelectProperty{}, errors.Wrap(err, "get nested property by name")
			}
			nestedProps, err := getAllNonRefNonBlobNestedProperties(&NestedProperty{NestedProperty: nested})
			if err != nil {
				return []search.SelectProperty{}, errors.Wrap(err, "get all non ref non blob nested properties")
			}
			props = append(props, search.SelectProperty{
				Name:        prop.Name,
				IsPrimitive: false,
				IsObject:    true,
				Props:       nestedProps,
			})
		} else {
			props = append(props, search.SelectProperty{
				Name:        prop.Name,
				IsPrimitive: true,
			})
		}
	}
	return props, nil
}

func parseNearImage(n *pb.NearImageSearch, targetVectors []string) (*nearImage.NearImageParams, error) {
	out := &nearImage.NearImageParams{
		Image:         n.Image,
		TargetVectors: targetVectors,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if n.Distance != nil && n.Certainty != nil {
		return nil, fmt.Errorf("near_image: cannot provide distance and certainty")
	}

	if n.Certainty != nil {
		out.Certainty = *n.Certainty
	}

	if n.Distance != nil {
		out.Distance = *n.Distance
		out.WithDistance = true
	}

	return out, nil
}

func parseNearAudio(n *pb.NearAudioSearch, targetVectors []string) (*nearAudio.NearAudioParams, error) {
	out := &nearAudio.NearAudioParams{
		Audio:         n.Audio,
		TargetVectors: targetVectors,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if n.Distance != nil && n.Certainty != nil {
		return nil, fmt.Errorf("near_audio: cannot provide distance and certainty")
	}

	if n.Certainty != nil {
		out.Certainty = *n.Certainty
	}

	if n.Distance != nil {
		out.Distance = *n.Distance
		out.WithDistance = true
	}

	return out, nil
}

func parseNearVideo(n *pb.NearVideoSearch, targetVectors []string) (*nearVideo.NearVideoParams, error) {
	out := &nearVideo.NearVideoParams{
		Video:         n.Video,
		TargetVectors: targetVectors,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if n.Distance != nil && n.Certainty != nil {
		return nil, fmt.Errorf("near_video: cannot provide distance and certainty")
	}

	if n.Certainty != nil {
		out.Certainty = *n.Certainty
	}

	if n.Distance != nil {
		out.Distance = *n.Distance
		out.WithDistance = true
	}

	return out, nil
}

func parseNearDepth(n *pb.NearDepthSearch, targetVectors []string) (*nearDepth.NearDepthParams, error) {
	out := &nearDepth.NearDepthParams{
		Depth:         n.Depth,
		TargetVectors: targetVectors,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if n.Distance != nil && n.Certainty != nil {
		return nil, fmt.Errorf("near_depth: cannot provide distance and certainty")
	}

	if n.Certainty != nil {
		out.Certainty = *n.Certainty
	}

	if n.Distance != nil {
		out.Distance = *n.Distance
		out.WithDistance = true
	}

	return out, nil
}

func parseNearThermal(n *pb.NearThermalSearch, targetVectors []string) (*nearThermal.NearThermalParams, error) {
	out := &nearThermal.NearThermalParams{
		Thermal:       n.Thermal,
		TargetVectors: targetVectors,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if n.Distance != nil && n.Certainty != nil {
		return nil, fmt.Errorf("near_thermal: cannot provide distance and certainty")
	}

	if n.Certainty != nil {
		out.Certainty = *n.Certainty
	}

	if n.Distance != nil {
		out.Distance = *n.Distance
		out.WithDistance = true
	}

	return out, nil
}

func parseNearIMU(n *pb.NearIMUSearch, targetVectors []string) (*nearImu.NearIMUParams, error) {
	out := &nearImu.NearIMUParams{
		IMU:           n.Imu,
		TargetVectors: targetVectors,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if n.Distance != nil && n.Certainty != nil {
		return nil, fmt.Errorf("near_imu: cannot provide distance and certainty")
	}

	if n.Certainty != nil {
		out.Certainty = *n.Certainty
	}

	if n.Distance != nil {
		out.Distance = *n.Distance
		out.WithDistance = true
	}

	return out, nil
}

func parseNearVec(nv *pb.NearVector, targetVectors []string) (*searchparams.NearVector, error) {
	var vector []float32
	// bytes vector has precedent for being more efficient
	if len(nv.VectorBytes) > 0 {
		vector = byteops.Float32FromByteVector(nv.VectorBytes)
	} else if len(nv.Vector) > 0 {
		vector = nv.Vector
	}

	if vector != nil && nv.VectorPerTarget != nil {
		return nil, fmt.Errorf("near_vector: either vector or VectorPerTarget must be provided, not both")
	}

	targetVectorsTmp := targetVectors
	if len(targetVectors) == 0 {
		targetVectorsTmp = []string{""}
	}

	targetsPerVector := make(map[string][]float32, len(targetVectorsTmp))
	if vector != nil {
		for _, target := range targetVectorsTmp {
			targetsPerVector[target] = vector
		}
	} else if nv.VectorPerTarget != nil {
		if len(nv.VectorPerTarget) != len(targetVectorsTmp) {
			return nil, fmt.Errorf("near_vector: vector per target must be provided for all targets")
		}
		for _, target := range targetVectorsTmp {
			if vec, ok := nv.VectorPerTarget[target]; ok {
				targetsPerVector[target] = byteops.Float32FromByteVector(vec)
			} else {
				return nil, fmt.Errorf("near_vector: vector for target %s is required", target)
			}
		}
	} else {
		return nil, fmt.Errorf("near_vector: vector is required")
	}

	return &searchparams.NearVector{
		VectorPerTarget: targetsPerVector,
		TargetVectors:   targetVectors,
	}, nil
}
