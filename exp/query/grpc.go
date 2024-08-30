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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// GRPC transport on top of the query.API.
type GRPC struct {
	api *API
	log logrus.FieldLogger

	// TODO(kavi): This should go away once we split v1.WeaviateServer into composable v1.Searcher
	protocol.UnimplementedWeaviateServer
}

func NewGRPC(api *API, log logrus.FieldLogger) *GRPC {
	return &GRPC{
		api: api,
		log: log,
	}
}

func (g *GRPC) Search(ctx context.Context, req *protocol.SearchRequest) (*protocol.SearchReply, error) {
	return g.api.svc.Search(ctx, req)
}
