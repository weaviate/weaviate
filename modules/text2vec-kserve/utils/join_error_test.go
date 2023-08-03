//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Copyright 2022 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_joinError_Error(t *testing.T) {
	type fields struct {
		errs []error
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "No errors",
			fields: fields{
				errs: []error{},
			},
			want: "[]",
		},
		{
			name: "Single error",
			fields: fields{
				errs: []error{
					errors.New("new error"),
				},
			},
			want: "[new error]",
		},
		{
			name: "Multiple errors",
			fields: fields{
				errs: []error{
					errors.New("error 1"),
					errors.New("error 2 "),
					errors.New("error 3"),
				},
			},
			want: "[error 1,\nerror 2,\nerror 3]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &joinError{
				errs: tt.fields.errs,
			}
			assert.Equal(t, e.Error(), tt.want)
		})
	}
}
