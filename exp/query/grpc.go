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
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
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
	// TODO only create class if not exists/not changed? replace with go client and do not hardcode the url
	r, err := http.Get(fmt.Sprintf("http://localhost:8080/v1/schema/%s", req.Collection))
	if err != nil {
		panic(err)
	}
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	cc := models.Class{}
	json.Unmarshal(body, &cc)
	classToCreate := models.Class{
		Class:              cc.Class,
		MultiTenancyConfig: cc.MultiTenancyConfig,
		// TODO fill in the other relevant parts of the class here?
	}
	_, _, err = g.api.schema.AddClass(context.TODO(), &models.Principal{}, &classToCreate)
	if err != nil {
		panic(err)
	}
	_, err = g.api.schema.AddTenants(context.TODO(), &models.Principal{}, req.Collection, []*models.Tenant{{Name: req.Tenant, ActivityStatus: "ACTIVE"}})
	if err != nil {
		panic(err)
	}
	// TODO download objects from s3 here?
	return g.api.svc.Search(ctx, req)
}
