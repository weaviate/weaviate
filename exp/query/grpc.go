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

package query

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/traverser"
)

// GRPC transport on top of the query.API.
type GRPC struct {
	api *API
	log logrus.FieldLogger

	// Needed to extrat `filters` from the payload.
	schema SchemaQuerier

	// TODO(kavi): This should go away once we split v1.WeaviateServer into composable v1.Searcher
	pb.UnimplementedWeaviateServer

	// parser is used to parse/decode grpc request types from gRPC wire transport.
	parser *v1.Parser

	// replier is used to marshal/encode search results into gRPC wire transport
	replier *v1.Replier

	// getClass is used in encoding/decoding when need class info for different object fields
	getClass func(name string) *models.Class
}

func NewGRPC(api *API, schema SchemaQuerier, getClass func(name string) *models.Class, log logrus.FieldLogger) *GRPC {
	return &GRPC{
		api:      api,
		log:      log,
		schema:   schema,
		parser:   v1.NewParser(false, getClass),
		getClass: getClass,
		replier:  v1.NewReplier(false, false, false, nil, log),
	}
}

func (g *GRPC) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	start := time.Now()

	params, err := g.decode(req)
	if err != nil {
		return nil, err
	}

	res, err := g.api.Search(ctx, params)
	if err != nil {
		return nil, err
	}

	schema, err := g.schema.Schema(ctx)
	if err != nil {
		return nil, err
	}

	return g.encode(res, params, start, schema)
}

func (g *GRPC) encode(res []search.Result, params dto.GetParams, searchStart time.Time, schema schema.Schema) (*pb.SearchReply, error) {
	var xres []interface{}

	// []search.Result -> []interface{} as needed by grpc.Replier
	var exp traverser.Explorer

	exp.

	return g.replier.Search(xres, searchStart, params, schema)
}

func (g *GRPC) decode(req *pb.SearchRequest) (dto.GetParams, error) {
	return g.parser.Search(req, maxQueryObjectsLimit)
}

// TODO(kavi): This is duplicated code from `traverser.Explorer`.
func (g *GRPC) toReponse(ctx context.Context, input []search.Result, searchVector []float32, params dto.GetParams) ([]interface{}, error) {
	output := make([]interface{}, 0, len(input))
	results, err := g.toResponseWithType(ctx, input, searchVector, params)
	if err != nil {
		return nil, err
	}

	if params.GroupBy != nil {
		for _, result := range results {
			wrapper := map[string]interface{}{}
			wrapper["_additional"] = result.AdditionalProperties
			output = append(output, wrapper)
		}
	} else {
		for _, result := range results {
			output = append(output, result.Schema)
		}
	}
	return output, nil
}

func (g *GRPC) toResponseWithType(ctx, input, searchVector, params) ([]search.Result, error) {}

// func requestFromProto(req *pb.SearchRequest, getClass func(string) *models.Class) (*SearchRequest, error) {
// 	sr := &SearchRequest{
// 		Collection: req.Collection,
// 		Tenant:     req.Tenant,
// 		Limit:      int(req.Limit),
// 	}
// 	if req.NearText != nil {
// 		sr.NearText = req.NearText.Query
// 		if req.NearText.Certainty != nil {
// 			sr.Certainty = *req.NearText.Certainty
// 		}
// 	}
// 	if req.Filters != nil {
// 		filter, err := v1.ExtractFilters(req.Filters, getClass, req.Collection)
// 		if err != nil {
// 			return nil, err
// 		}
// 		sr.Filters = &filters.LocalFilter{Root: &filter}
// 	}
// 	return sr, nil
// }

// func toProtoResponse(res *SearchResponse) *protocol.SearchReply {
// 	var resp protocol.SearchReply

// 	// TODO(kavi): copy rest of the fields accordingly.
// 	for _, v := range res.Results {
// 		props := protocol.Properties{
// 			Fields: make(map[string]*protocol.Value),
// 		}
// 		objprops := v.Obj.Object.Properties.(map[string]interface{})
// 		for prop, val := range objprops {
// 			props.Fields[prop] = &protocol.Value{
// 				Kind: &protocol.Value_StringValue{
// 					StringValue: val.(string),
// 				},
// 			}
// 		}

// 		resp.Results = append(resp.Results, &protocol.SearchResult{
// 			Metadata: &protocol.MetadataResult{
// 				Id:        v.Obj.ID().String(),
// 				Certainty: float32(v.Certainty),
// 			},
// 			Properties: &protocol.PropertiesResult{
// 				TargetCollection: v.Obj.Object.Class,
// 				NonRefProps:      &props,
// 			},
// 		})

// 	}
// 	return &resp
// }
