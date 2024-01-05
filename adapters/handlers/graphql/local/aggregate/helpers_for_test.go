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

package aggregate

import (
	"context"
	"fmt"

	testhelper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {
}

type mockResolver struct {
	testhelper.MockResolver
}

func newMockResolver(cfg config.Config) *mockResolver {
	field, err := Build(&testhelper.CarSchema, cfg, nil)
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mockLog := &mockRequestsLog{}
	mocker := &mockResolver{}
	mocker.RootFieldName = "Aggregate"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{
		"Resolver":    Resolver(mocker),
		"RequestsLog": mockLog,
		"Config":      cfg,
	}

	return mocker
}

func (m *mockResolver) Aggregate(ctx context.Context, principal *models.Principal,
	params *aggregation.Params,
) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}
