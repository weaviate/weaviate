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

package vectorizer

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
)

type fakeClassConfig map[string]interface{}

func (cfg fakeClassConfig) Class() map[string]interface{} {
	return cfg
}

func (cfg fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return cfg
}

func (cfg fakeClassConfig) Property(string) map[string]interface{} {
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

type fakeObjectsRepo struct {
	mock.Mock
}

func (r *fakeObjectsRepo) Object(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	addl additional.Properties, tenant string,
) (*search.Result, error) {
	args := r.Called(ctx, class, id, tenant)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*search.Result), args.Error(1)
}
