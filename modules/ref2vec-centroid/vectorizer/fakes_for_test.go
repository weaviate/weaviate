//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package vectorizer

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/stretchr/testify/mock"
)

type fakeClassConfig map[string]interface{}

func (cfg fakeClassConfig) Class() map[string]interface{} {
	return cfg
}

func (cfg fakeClassConfig) Property(string) map[string]interface{} {
	return nil
}

type fakeObjectsRepo struct {
	mock.Mock
}

func (r *fakeObjectsRepo) Object(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	addl additional.Properties,
) (*search.Result, error) {
	args := r.Called(ctx, class, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*search.Result), args.Error(1)
}
