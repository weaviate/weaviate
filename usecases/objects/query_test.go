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

package objects

import (
	"context"
	"errors"
	"testing"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
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
		class     string
		param     QueryParams
		mockedErr *Error
		authErr   error
		lockErr   error
		wantCode  int
	}{
		{
			class:     cls,
			param:     params,
			mockedErr: &Error{Code: StatusNotFound},
			wantCode:  StatusNotFound,
		},
		{
			class:    cls,
			param:    params,
			authErr:  errAny,
			wantCode: StatusForbidden,
		},
		{
			class:    cls,
			param:    params,
			lockErr:  errAny,
			wantCode: StatusInternalServerError,
		},
		{
			class: cls,
			param: params,
		},
		{
			class:    cls,
			param:    QueryParams{Class: cls, Offset: ptInt64(1), Limit: &m.config.Config.QueryMaximumResults},
			wantCode: StatusBadRequest,
		},
	}
	for i, tc := range tests {
		m.authorizer.Err = tc.authErr
		m.locks.Err = tc.lockErr
		if tc.authErr == nil && tc.lockErr == nil {
			m.repo.On("Query", &inputs).Return([]search.Result{}, tc.mockedErr).Once()
		}
		_, err := m.Manager.Query(context.Background(), nil, &tc.param)
		code := 0
		if err != nil {
			code = err.Code
		}
		if tc.wantCode != code {
			t.Errorf("case %d expected:%v got:%v", i+1, tc.wantCode, code)
		}
	}
}
