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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func Test_HeadObject(t *testing.T) {
	t.Parallel()
	var (
		cls    = "MyClass"
		id     = strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		m      = newFakeGetManager(schema.Schema{})
		errAny = errors.New("any")
	)

	tests := []struct {
		class     string
		mockedOk  bool
		mockedErr error
		authErr   error
		lockErr   error
		wantOK    bool
		wantErr   error
	}{
		{
			mockedOk: true,
			wantOK:   true,
		},
		{
			class:    cls,
			mockedOk: true,
			wantOK:   true,
		},
		{
			class:    cls,
			mockedOk: false,
			wantOK:   false,
		},
		{
			class:     cls,
			mockedOk:  false,
			mockedErr: errAny,
			wantOK:    false,
			wantErr:   ErrServiceInternal,
		},
		{
			class:   cls,
			authErr: errAny,
			wantOK:  false,
			wantErr: ErrAuthorization,
		},
		{
			class:   cls,
			lockErr: errAny,
			wantOK:  false,
			wantErr: ErrServiceInternal,
		},
	}
	for i, tc := range tests {
		m.authorizer.Err = tc.authErr
		m.locks.Err = tc.lockErr
		if tc.authErr == nil && tc.lockErr == nil {
			m.repo.On("Exists", tc.class, id).Return(tc.mockedOk, tc.mockedErr).Once()
		}
		ok, err := m.Manager.HeadObject(context.Background(), nil, tc.class, id)
		if tc.wantOK != ok || !errors.Is(err, tc.wantErr) {
			t.Errorf("case-%d: expected: (%v, %v), got:(%v, %v)", i+1, tc.wantOK, tc.wantErr, ok, err)
		}
	}
}
