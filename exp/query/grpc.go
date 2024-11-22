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
	"fmt"

	"github.com/sirupsen/logrus"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// GRPC transport on top of the query.API.
type GRPC struct {
	api *API
	log logrus.FieldLogger

	// Needed to extrat `filters` from the payload.
	schema SchemaQuerier

	// TODO(kavi): This should go away once we split v1.WeaviateServer into composable v1.Searcher
	protocol.UnimplementedWeaviateServer
}

func NewGRPC(api *API, schema SchemaQuerier, log logrus.FieldLogger) *GRPC {
	return &GRPC{
		api:    api,
		log:    log,
		schema: schema,
	}
}

func (g *GRPC) Search(ctx context.Context, req *protocol.SearchRequest) (*protocol.SearchReply, error) {
	class, err := g.schema.Collection(ctx, req.Collection)
	if err != nil {
		return nil, fmt.Errorf("search: failed to get collection %q: %w", req.Collection, err)
	}

	getClass := func(name string) *models.Class {
		return class
	}

	parsed, err := requestFromProto(req, getClass)
	if err != nil {
		return nil, err
	}
	parsed.Class = class

	res, err := g.api.Search(ctx, parsed)
	if err != nil {
		return nil, err
	}

	return toProtoResponse(res), nil
}

func requestFromProto(req *protocol.SearchRequest, getClass func(string) *models.Class) (*SearchRequest, error) {
	sr := &SearchRequest{
		Collection: req.Collection,
		Tenant:     req.Tenant,
		Limit:      int(req.Limit),
	}
	if req.NearText != nil {
		sr.NearText = req.NearText.Query
		if req.NearText.Certainty != nil {
			sr.Certainty = *req.NearText.Certainty
		}
	}
	if req.Filters != nil {
		filter, err := v1.ExtractFilters(req.Filters, getClass, req.Collection)
		if err != nil {
			return nil, err
		}
		sr.Filters = &filters.LocalFilter{Root: &filter}
	}
	return sr, nil
}

func toProtoResponse(res *SearchResponse) *protocol.SearchReply {
	var resp protocol.SearchReply

	// TODO(kavi): copy rest of the fields accordingly.
	for _, v := range res.Results {
		props := protocol.Properties{
			Fields: make(map[string]*protocol.Value),
		}
		objprops := v.Obj.Object.Properties.(map[string]interface{})
		for prop, val := range objprops {
			props.Fields[prop] = &protocol.Value{
				Kind: &protocol.Value_StringValue{
					StringValue: val.(string),
				},
			}
		}

		resp.Results = append(resp.Results, &protocol.SearchResult{
			Metadata: &protocol.MetadataResult{
				Id:        v.Obj.ID().String(),
				Certainty: float32(v.Certainty),
			},
			Properties: &protocol.PropertiesResult{
				TargetCollection: v.Obj.Object.Class,
				NonRefProps:      &props,
			},
		})

	}
	return &resp
}
