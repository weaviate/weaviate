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
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/generative"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
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

func NewGRPC(api *API, schema SchemaQuerier, log logrus.FieldLogger) *GRPC {
	getClass := func(name string) *models.Class {
		// Terrible idea. Come back later
		c, _ := schema.Collection(context.Background(), name)
		return c
	}

	parser := v1.NewParser(false, getClass)
	replier := v1.NewReplier(false, false, false, generative.NewParser(false), log)

	return &GRPC{
		api:      api,
		log:      log,
		schema:   schema,
		parser:   parser,
		getClass: getClass,
		replier:  replier,
	}
}

func (g *GRPC) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	start := time.Now()

	params, err := g.decode(req)
	if err != nil {
		return nil, err
	}

	res, vectors, err := g.api.Search(ctx, params)
	if err != nil {
		return nil, err
	}

	schema, err := g.schema.Schema(ctx)
	if err != nil {
		return nil, err
	}

	return g.encode(res, vectors, params, start, schema)
}

func (g *GRPC) encode(res []search.Result, vectors []float32, params dto.GetParams, searchStart time.Time, schema *schema.Schema) (*pb.SearchReply, error) {
	resp, err := g.api.explorer.SearchResultsToGetResponse(context.Background(), res, vectors, params)
	if err != nil {
		return nil, err
	}
	return g.replier.Search(resp, searchStart, params, *schema)
}

func (g *GRPC) decode(req *pb.SearchRequest) (dto.GetParams, error) {
	return g.parser.Search(req, maxQueryObjectsLimit)
}
