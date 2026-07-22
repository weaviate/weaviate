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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/namespaces"
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

func TestHTTPStatusForNamespaceErr(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantOK     bool
	}{
		{name: "deleting → 422", err: namespaces.ErrNamespaceDeleting, wantStatus: http.StatusUnprocessableEntity, wantOK: true},
		{name: "not-empty → 422", err: namespaces.ErrNamespaceNotEmpty, wantStatus: http.StatusUnprocessableEntity, wantOK: true},
		{name: "invalid-state → 422", err: namespaces.ErrInvalidState, wantStatus: http.StatusUnprocessableEntity, wantOK: true},
		{name: "namespace-suspended → 422", err: namespaces.ErrNamespaceSuspended, wantStatus: http.StatusUnprocessableEntity, wantOK: true},
		{name: "collection-suspended → 422", err: namespaces.ErrCollectionSuspended, wantStatus: http.StatusUnprocessableEntity, wantOK: true},
		{name: "wrapped suspended still classifies", err: fmt.Errorf("apply: %w", namespaces.ErrNamespaceSuspended), wantStatus: http.StatusUnprocessableEntity, wantOK: true},
		{name: "resuming → 503", err: namespaces.ErrNamespaceResuming, wantStatus: http.StatusServiceUnavailable, wantOK: true},
		{name: "invalid-state-transition → 422", err: namespaces.ErrInvalidStateTransition, wantStatus: http.StatusUnprocessableEntity, wantOK: true},
		{name: "wrapped invalid-state-transition still classifies", err: fmt.Errorf("apply: %w", namespaces.ErrInvalidStateTransition), wantStatus: http.StatusUnprocessableEntity, wantOK: true},
		{name: "mt-disabled is not in the family", err: schema.ErrMTDisabled, wantStatus: 0, wantOK: false},
		{name: "gone is not in the family", err: namespaces.ErrNamespaceGone, wantStatus: 0, wantOK: false},
		{name: "already-exists is not in the family", err: namespaces.ErrAlreadyExists, wantStatus: 0, wantOK: false},
		{name: "bad-request is not in the family", err: namespaces.ErrBadRequest, wantStatus: 0, wantOK: false},
		{name: "untyped error is not in the family", err: errors.New("boom"), wantStatus: 0, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, ok := HTTPStatusForNamespaceErr(tt.err)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantStatus, status)
		})
	}
}
