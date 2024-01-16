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

package objects

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func TestQuery(t *testing.T) {
	t.Parallel()
	var (
		cls    = "MyClass"
		m      = newFakeGetManager(schema.Schema{})
		errAny = errors.New("any")
	)
	params := QueryParams{
		Class: cls,
		Limit: ptInt64(10),
	}
	inputs := QueryInput{
		Class: cls,
		Limit: 10,
	}
	tests := []struct {
		class             string
		name              string
		param             QueryParams
		mockedErr         *Error
		authErr           error
		lockErr           error
		wantCode          int
		mockedDBResponse  []search.Result
		wantResponse      []*models.Object
		wantQueryInput    QueryInput
		wantUsageTracking bool
	}{
		{
			name:           "not found",
			class:          cls,
			param:          params,
			mockedErr:      &Error{Code: StatusNotFound},
			wantCode:       StatusNotFound,
			wantQueryInput: inputs,
		},
		{
			name:           "forbidden",
			class:          cls,
			param:          params,
			authErr:        errAny,
			wantCode:       StatusForbidden,
			wantQueryInput: inputs,
		},
		{
			name:           "internal error",
			class:          cls,
			param:          params,
			lockErr:        errAny,
			wantCode:       StatusInternalServerError,
			wantQueryInput: inputs,
		},
		{
			name:  "happy path",
			class: cls,
			param: params,
			mockedDBResponse: []search.Result{
				{
					ClassName: cls,
					Schema: map[string]interface{}{
						"foo": "bar",
					},
					Dims: 3,
					Dist: 0,
				},
			},
			wantResponse: []*models.Object{{
				Class:         cls,
				VectorWeights: map[string]string(nil),
				Properties: map[string]interface{}{
					"foo": "bar",
				},
			}},
			wantQueryInput: inputs,
		},
		{
			name:  "happy path with explicit vector requested",
			class: cls,
			param: QueryParams{
				Class:      cls,
				Limit:      ptInt64(10),
				Additional: additional.Properties{Vector: true},
			},
			mockedDBResponse: []search.Result{
				{
					ClassName: cls,
					Schema: map[string]interface{}{
						"foo": "bar",
					},
					Dims: 3,
				},
			},
			wantResponse: []*models.Object{{
				Class:         cls,
				VectorWeights: map[string]string(nil),
				Properties: map[string]interface{}{
					"foo": "bar",
				},
			}},
			wantQueryInput: QueryInput{
				Class:      cls,
				Limit:      10,
				Additional: additional.Properties{Vector: true},
			},
			wantUsageTracking: true,
		},
		{
			name:           "bad request",
			class:          cls,
			param:          QueryParams{Class: cls, Offset: ptInt64(1), Limit: &m.config.Config.QueryMaximumResults},
			wantCode:       StatusBadRequest,
			wantQueryInput: inputs,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m.authorizer.Err = tc.authErr
			m.locks.Err = tc.lockErr
			if tc.authErr == nil && tc.lockErr == nil {
				m.repo.On("Query", &tc.wantQueryInput).Return(tc.mockedDBResponse, tc.mockedErr).Once()
			}
			if tc.wantUsageTracking {
				m.metrics.On("AddUsageDimensions", cls, "get_rest", "list_include_vector",
					tc.mockedDBResponse[0].Dims)
			}
			res, err := m.Manager.Query(context.Background(), nil, &tc.param)
			code := 0
			if err != nil {
				code = err.Code
			}
			if tc.wantCode != code {
				t.Errorf("case %d expected:%v got:%v", i+1, tc.wantCode, code)
			}

			assert.Equal(t, tc.wantResponse, res)
		})
	}
}
