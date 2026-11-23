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

package validation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

const BEACON = "weaviate://localhost/"

var (
	UuidUpper = "4E5CD755-4F43-44C5-B23C-0C7D6F6C21E6"
	UuidLower = strings.ToLower(UuidUpper)
)

func TestValidationReferencesInObject(t *testing.T) {
	validator := New(fakeExists, &config.WeaviateConfig{}, nil, nil, false, &authorization.DummyAuthorizer{})

	class := &models.Class{
		Class: "From",
		Properties: []*models.Property{
			{Name: "ref", DataType: []string{"To"}},
		},
	}

	obj := &models.Object{
		Class: "From",
		Properties: map[string]interface{}{
			"ref": []interface{}{
				map[string]interface{}{"beacon": BEACON + "To/" + UuidUpper},
			},
		},
	}

	err := validator.properties(context.Background(), class, obj, nil)
	require.Nil(t, err)
	require.Equal(t, obj.Properties.(map[string]interface{})["ref"].(models.MultipleRef)[0].Beacon.String(), BEACON+"To/"+UuidLower)
}

func TestValidationReference(t *testing.T) {
	validator := New(fakeExists, &config.WeaviateConfig{}, nil, nil, false, &authorization.DummyAuthorizer{})

	cref := &models.SingleRef{Beacon: strfmt.URI(BEACON + "To/" + UuidUpper)}
	ref, err := validator.ValidateSingleRef(cref)
	require.Nil(t, err)
	require.Equal(t, ref.TargetID.String(), UuidLower)
}

func TestValidateExistence(t *testing.T) {
	tenantErr := errors.New("has multi-tenancy disabled, but request was with tenant")
	replicasErr := fmt.Errorf("check existence: %w",
		replicaerrors.NewNotEnoughReplicasError(errors.New("2 of 3 replicas down")))
	otherErr := errors.New("connection refused")

	tests := []struct {
		name     string
		retryErr error
		check    func(t *testing.T, err error)
	}{
		{
			name:     "replica shortage on the tenant-less retry is returned",
			retryErr: replicasErr,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, replicaerrors.ErrReplicas)
			},
		},
		{
			name:     "other retry errors return the tenant-scoped error",
			retryErr: otherErr,
			check: func(t *testing.T, err error) {
				require.Equal(t, tenantErr, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tenants []string
			stub := func(_ context.Context, _ string, _ strfmt.UUID, _ *additional.ReplicationProperties, tenant string) (bool, error) {
				tenants = append(tenants, tenant)
				if tenant == "" {
					return false, tt.retryErr
				}
				return false, tenantErr
			}
			validator := New(stub, &config.WeaviateConfig{}, nil, nil, false, &authorization.DummyAuthorizer{})
			ref := &crossref.Ref{Class: "To", TargetID: strfmt.UUID(UuidLower)}

			err := validator.ValidateExistence(context.Background(), ref, "ref", "tenantA")
			tt.check(t, err)
			require.Equal(t, []string{"tenantA", ""}, tenants)
		})
	}
}

// Each lookup needs READ on the scope it reads: a class-less one and the
// tenant-less retry read without a tenant.
func TestValidateExistenceAuthorizesLookupScope(t *testing.T) {
	tenantErr := errors.New("has multi-tenancy disabled, but request was with tenant")
	tests := []struct {
		name        string
		class       string
		denied      string
		wantLookups []string
	}{
		{
			name:        "the tenant-less retry needs READ without a tenant",
			class:       "To",
			denied:      authorization.Objects("To", ""),
			wantLookups: []string{"tenantA"},
		},
		{
			name:   "a class-less lookup needs READ on every collection without a tenant",
			denied: authorization.Objects("", ""),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lookups []string
			stub := func(_ context.Context, _ string, _ strfmt.UUID, _ *additional.ReplicationProperties, tenant string) (bool, error) {
				lookups = append(lookups, tenant)
				if tenant != "" {
					return false, tenantErr
				}
				return true, nil
			}
			authorizer := mocks.NewMockAuthorizer()
			authorizer.Deny(tt.denied)
			validator := New(stub, &config.WeaviateConfig{}, nil, nil, false, authorizer)
			ref := &crossref.Ref{Class: tt.class, TargetID: strfmt.UUID(UuidLower)}

			err := validator.ValidateExistence(context.Background(), ref, "ref", "tenantA")
			require.True(t, errors.As(err, &autherrs.Forbidden{}), "%v", err)
			require.Equal(t, tt.wantLookups, lookups)
		})
	}
}
