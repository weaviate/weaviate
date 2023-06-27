//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/weaviate/weaviate/entities/schema"
	schemaManager "github.com/weaviate/weaviate/usecases/schema"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/search"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/searchparams"
	pb "github.com/weaviate/weaviate/grpc"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/traverser"
	"google.golang.org/grpc"
)

func CreateGRPCServer(state *state.State) *GRPCServer {
	s := grpc.NewServer()

	pb.RegisterWeaviateServer(s, &Server{
		traverser: state.Traverser,
		authComposer: composer.New(
			state.ServerConfig.Config.Authentication,
			state.APIKey, state.OIDC),
		allowAnonymousAccess: state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
		schemaManager:        state.SchemaManager,
	})

	return &GRPCServer{s}
}

func StartAndListen(s *GRPCServer, state *state.State) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d",
		state.ServerConfig.Config.GRPC.Port))
	if err != nil {
		return err
	}
	state.Logger.WithField("action", "grpc_startup").
		Infof("grpc server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

type GRPCServer struct {
	*grpc.Server
}

type Server struct {
	pb.UnimplementedWeaviateServer
	traverser            *traverser.Traverser
	authComposer         composer.TokenFunc
	allowAnonymousAccess bool
	schemaManager        *schemaManager.Manager
}

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	before := time.Now()

	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}

	searchParams, err := searchParamsFromProto(req)
	if err != nil {
		return nil, fmt.Errorf("extract params: %w", err)
	}

	if err := s.validateClassAndProperty(searchParams); err != nil {
		return nil, err
	}

	res, err := s.traverser.GetClass(ctx, principal, searchParams)
	if err != nil {
		return nil, err
	}

	return searchResultsToProto(res, before, searchParams)
}

func (s *Server) validateClassAndProperty(searchParams dto.GetParams) error {
	scheme := s.schemaManager.GetSchemaSkipAuth()
	class, err := schema.GetClassByName(scheme.Objects, searchParams.ClassName)
	if err != nil {
		return err
	}

	for _, prop := range searchParams.Properties {
		_, err := schema.GetPropertyByName(class, prop.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

func searchResultsToProto(res []any, start time.Time, searchParams dto.GetParams) (*pb.SearchReply, error) {
	tookSeconds := float64(time.Since(start)) / float64(time.Second)
	out := &pb.SearchReply{
		Took:    float32(tookSeconds),
		Results: make([]*pb.SearchResult, len(res)),
	}

	for i, raw := range res {
		asMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		props, err := extractPropertiesAnswer(asMap, searchParams.Properties, searchParams.ClassName)
		if err != nil {
			continue
		}

		additionalProps, err := extractAdditionalProps(asMap, searchParams)
		if err != nil {
			continue
		}

		result := &pb.SearchResult{
			Properties:           props,
			AdditionalProperties: additionalProps,
		}

		out.Results[i] = result
	}

	return out, nil
}

func extractAdditionalProps(asMap map[string]any, searchParams dto.GetParams) (*pb.ResultAdditionalProps, error) {
	err := errors.New("could not extract additional prop")
	additionalProps := &pb.ResultAdditionalProps{}
	if searchParams.AdditionalProperties.ID {
		idRaw, ok := asMap["id"]
		if !ok {
			return nil, err
		}

		idStrfmt, ok := idRaw.(strfmt.UUID)
		if !ok {
			return nil, err
		}
		additionalProps.Id = idStrfmt.String()
	}
	_, ok := asMap["_additional"]
	if !ok {
		return additionalProps, nil
	}

	additionalPropertiesMap := asMap["_additional"].(map[string]interface{})

	// additional properties are only present for certain searches/configs => don't return an error if not available
	if searchParams.AdditionalProperties.Vector {
		vector, ok := additionalPropertiesMap["vector"]
		if ok {
			vectorfmt, ok2 := vector.([]float32)
			if ok2 {
				additionalProps.Vector = vectorfmt
			}
		}
	}

	if searchParams.AdditionalProperties.Certainty {
		additionalProps.CertaintyPresent = false
		certainty, ok := additionalPropertiesMap["certainty"]
		if ok {
			certaintyfmt, ok2 := certainty.(float32)
			if ok2 {
				additionalProps.Certainty = certaintyfmt
				additionalProps.CertaintyPresent = true
			}
		}
	}

	if searchParams.AdditionalProperties.Distance {
		additionalProps.DistancePresent = false
		distance, ok := additionalPropertiesMap["distance"]
		if ok {
			distancefmt, ok2 := distance.(float32)
			if ok2 {
				additionalProps.Distance = distancefmt
				additionalProps.DistancePresent = true
			}
		}
	}

	if searchParams.AdditionalProperties.CreationTimeUnix {
		additionalProps.CreationTimeUnixPresent = false
		creationtime, ok := additionalPropertiesMap["creationTimeUnix"]
		if ok {
			creationtimefmt, ok2 := creationtime.(int64)
			if ok2 {
				additionalProps.CreationTimeUnix = creationtimefmt
				additionalProps.CreationTimeUnixPresent = true
			}
		}
	}

	if searchParams.AdditionalProperties.LastUpdateTimeUnix {
		additionalProps.LastUpdateTimeUnixPresent = false
		lastUpdateTime, ok := additionalPropertiesMap["lastUpdateTimeUnix"]
		if ok {
			lastUpdateTimefmt, ok2 := lastUpdateTime.(int64)
			if ok2 {
				additionalProps.LastUpdateTimeUnix = lastUpdateTimefmt
				additionalProps.LastUpdateTimeUnixPresent = true
			}
		}
	}

	if searchParams.AdditionalProperties.ExplainScore {
		additionalProps.ExplainScorePresent = false
		explainScore, ok := additionalPropertiesMap["explainScore"]
		if ok {
			explainScorefmt, ok2 := explainScore.(string)
			if ok2 {
				additionalProps.ExplainScore = explainScorefmt
				additionalProps.ExplainScorePresent = true
			}
		}
	}

	if searchParams.AdditionalProperties.Score {
		additionalProps.ScorePresent = false
		score, ok := additionalPropertiesMap["score"]
		if ok {
			scorefmt, ok2 := score.(float32)
			if ok2 {
				additionalProps.Score = scorefmt
				additionalProps.ScorePresent = true
			}
		}
	}

	return additionalProps, nil
}

func extractPropertiesAnswer(results map[string]interface{}, properties search.SelectProperties, class string) (*pb.ResultProperties, error) {
	props := pb.ResultProperties{}
	nonRefProps := make(map[string]interface{}, 0)
	refProps := make([]*pb.ReturnRefProperties, 0)
	for _, prop := range properties {
		propRaw, ok := results[prop.Name]
		if !ok {
			continue
		}
		if prop.IsPrimitive {
			nonRefProps[prop.Name] = propRaw
			continue
		}
		refs, ok := propRaw.([]interface{})
		if !ok {
			continue
		}
		extractedRefProps := make([]*pb.ResultProperties, 0, len(refs))
		for _, ref := range refs {
			refLocal, ok := ref.(search.LocalRef)
			if !ok {
				continue
			}
			extractedRefProp, err := extractPropertiesAnswer(refLocal.Fields, prop.Refs[0].RefProperties, refLocal.Class)
			if err != nil {
				continue
			}
			extractedRefProps = append(extractedRefProps, extractedRefProp)
		}

		refProp := pb.ReturnRefProperties{PropName: prop.Name, Properties: extractedRefProps}
		refProps = append(refProps, &refProp)
	}
	if len(nonRefProps) > 0 {
		newStruct, err := structpb.NewStruct(nonRefProps)
		if err != nil {
			return nil, errors.Wrap(err, "creating ref-prop struct")
		}
		props.NonRefProperties = newStruct
	}
	if len(refProps) > 0 {
		props.RefProps = refProps
	}

	props.ClassName = class

	return &props, nil
}

func extractPropertiesRequest(reqProps *pb.Properties) []search.SelectProperty {
	var props []search.SelectProperty
	if reqProps == nil {
		return props
	}
	if reqProps.NonRefProperties != nil && len(reqProps.NonRefProperties) > 0 {
		for _, prop := range reqProps.NonRefProperties {
			props = append(props, search.SelectProperty{
				Name:        prop,
				IsPrimitive: true,
			})
		}
	}

	if reqProps.RefProperties != nil && len(reqProps.RefProperties) > 0 {
		for _, prop := range reqProps.RefProperties {
			props = append(props, search.SelectProperty{
				Name:        prop.ReferenceProperty,
				IsPrimitive: false,
				Refs: []search.SelectClass{{
					ClassName:     prop.LinkedClass,
					RefProperties: extractPropertiesRequest(prop.LinkedProperties),
				}},
			})
		}
	}

	return props
}

func searchParamsFromProto(req *pb.SearchRequest) (dto.GetParams, error) {
	out := dto.GetParams{}
	out.ClassName = req.ClassName

	out.Properties = extractPropertiesRequest(req.Properties)

	if len(out.Properties) == 0 {
		// This is a pure-ID query without any props. Indicate this to the DB, so
		// it can optimize accordingly
		out.AdditionalProperties.NoProps = true
	}

	explainScore := false
	if req.AdditionalProperties != nil {
		out.AdditionalProperties.ID = req.AdditionalProperties.Uuid
		out.AdditionalProperties.Vector = req.AdditionalProperties.Vector
		out.AdditionalProperties.Distance = req.AdditionalProperties.Distance
		out.AdditionalProperties.LastUpdateTimeUnix = req.AdditionalProperties.LastUpdateTimeUnix
		out.AdditionalProperties.CreationTimeUnix = req.AdditionalProperties.Distance
		out.AdditionalProperties.Score = req.AdditionalProperties.Score
		out.AdditionalProperties.Certainty = req.AdditionalProperties.Certainty
		explainScore = req.AdditionalProperties.ExplainScore
	}

	if hs := req.HybridSearch; hs != nil {
		out.HybridSearch = &searchparams.HybridSearch{Query: hs.Query, Properties: hs.Properties, Vector: hs.Vector, Alpha: float64(hs.Alpha)}
	}

	if bm25 := req.Bm25Search; bm25 != nil {
		out.KeywordRanking = &searchparams.KeywordRanking{Query: bm25.Query, Properties: bm25.Properties, Type: "bm25", AdditionalExplanations: explainScore}
	}

	if nv := req.NearVector; nv != nil {
		out.NearVector = &searchparams.NearVector{
			Vector: nv.Vector,
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
		out.NearObject = &searchparams.NearObject{
			ID: req.NearObject.Id,
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

	out.Pagination = &filters.Pagination{}
	if req.Limit > 0 {
		out.Pagination.Limit = int(req.Limit)
	} else {
		// TODO: align default with other APIs
		out.Pagination.Limit = 10
	}

	return out, nil
}
