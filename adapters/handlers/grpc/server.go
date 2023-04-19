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
	"strings"
	"time"

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

func StartAndListen(port int, state *state.State) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterWeaviateServer(s, &Server{
		traverser: state.Traverser,
		authComposer: composer.New(
			state.ServerConfig.Config.Authentication,
			state.APIKey, state.OIDC),
		allowAnonymousAccess: state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
	})
	state.Logger.WithField("action", "grpc_startup").
		Infof("grpc server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

type Server struct {
	pb.UnimplementedWeaviateServer
	traverser            *traverser.Traverser
	authComposer         composer.TokenFunc
	allowAnonymousAccess bool
}

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	before := time.Now()

	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}

	searchParams := searchParamsFromProto(req)
	res, err := s.traverser.GetClass(ctx, principal, searchParams)
	if err != nil {
		return nil, err
	}

	return searchResultsToProto(res, before, searchParams), nil
}

func searchResultsToProto(res []any, start time.Time, searchParams dto.GetParams) *pb.SearchReply {
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

		props := make(map[string]interface{})
		for _, prop := range searchParams.Properties {
			propRaw, ok := asMap[prop.Name]
			if !ok {
				continue
			}
			props[prop.Name] = propRaw
		}

		newStruct, err := structpb.NewStruct(props)
		if err != nil {
			continue
		}

		result := &pb.SearchResult{
			Properties:           newStruct,
			AdditionalProperties: &pb.AdditionalProps{},
		}

		if searchParams.AdditionalProperties.ID {
			idRaw, ok := asMap["id"]
			if !ok {
				continue
			}

			idStrfmt, ok := idRaw.(strfmt.UUID)
			if !ok {
				continue
			}
			result.AdditionalProperties.Id = idStrfmt.String()
		}

		out.Results[i] = result
	}

	return out
}

func searchParamsFromProto(req *pb.SearchRequest) dto.GetParams {
	out := dto.GetParams{}
	out.ClassName = req.ClassName

	if req.Properties != nil && len(req.Properties) > 0 {
		for _, prop := range req.Properties {
			isPrimitive := strings.Contains(prop, "...")

			// Todo: Ref Props
			out.Properties = append(out.Properties, search.SelectProperty{
				Name:        prop,
				IsPrimitive: isPrimitive,
			})
		}
	}

	if req.AdditionalProperties != nil {
		for _, addProp := range req.AdditionalProperties {
			if addProp == "id" {
				out.AdditionalProperties.ID = true
			}
		}
	}

	if req.NearVector != nil {
		out.NearVector = &searchparams.NearVector{
			Vector: req.NearVector.Vector,
		}
	}

	if req.NearObject != nil {
		out.NearObject = &searchparams.NearObject{
			ID: req.NearObject.Id,
		}
	}

	out.Pagination = &filters.Pagination{}
	if req.Limit > 0 {
		out.Pagination.Limit = int(req.Limit)
	} else {
		// TODO: align default with other APIs
		out.Pagination.Limit = 10
	}

	return out
}
