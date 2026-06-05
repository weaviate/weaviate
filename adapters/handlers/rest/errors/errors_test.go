//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestErrPayloadFromSingleErr(t *testing.T) {
	nsPrincipal := &models.Principal{Namespace: "ns"}
	tests := []struct {
		name      string
		principal *models.Principal
		err       error
		want      string
	}{
		{
			name: "nil principal, nil err yields fmt's <nil> placeholder",
			want: "<nil>",
		},
		{
			name: "nil principal leaves message unchanged",
			err:  errors.New("ns:Class not found"),
			want: "ns:Class not found",
		},
		{
			name:      "namespaced principal strips own prefix",
			principal: nsPrincipal,
			err:       errors.New("ns:Class not found"),
			want:      "Class not found",
		},
		{
			name:      "namespaced principal, nil err yields fmt's <nil> placeholder",
			principal: nsPrincipal,
			want:      "<nil>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ErrPayloadFromSingleErr(tt.principal, tt.err)
			require.Len(t, got.Error, 1)
			require.Equal(t, tt.want, got.Error[0].Message)
		})
	}
}
