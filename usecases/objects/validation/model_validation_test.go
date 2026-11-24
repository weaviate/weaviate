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
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
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
	validator := New(fakeExists, &config.WeaviateConfig{}, nil, nil, &authorization.DummyAuthorizer{}, false)

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
	validator := New(fakeExists, &config.WeaviateConfig{}, nil, nil, &authorization.DummyAuthorizer{}, false)

	cref := &models.SingleRef{Beacon: strfmt.URI(BEACON + "To/" + UuidUpper)}
	ref, err := validator.ValidateSingleRef(cref)
	require.Nil(t, err)
	require.Equal(t, ref.TargetID.String(), UuidLower)
}

func TestValidateExistence(t *testing.T) {
	tenantErr := fmt.Errorf("class To %w", enterrors.ErrTenantForNonMultiTenantClass)
	tenantNotFoundErr := fmt.Errorf("validate tenant: %w", enterrors.ErrTenantNotFound)
	tenantNotActiveErr := fmt.Errorf("validate tenant: %w", enterrors.ErrTenantNotActive)
	replicasErr := fmt.Errorf("check existence: %w",
		replicaerrors.NewNotEnoughReplicasError(errors.New("2 of 3 replicas down")))
	storageErr := errors.New("connection refused")
	retryErr := errors.New("retry failed")

	tests := []struct {
		name        string
		initialErr  error
		retryErr    error
		denyRetry   bool
		wantTenants []string
		wantAuth    int
		check       func(t *testing.T, err error)
	}{
		{
			name:        "replica shortage on the tenant-scoped check is returned without retry",
			initialErr:  replicasErr,
			denyRetry:   true,
			wantTenants: []string{"tenantA"},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, replicaerrors.ErrReplicas)
			},
		},
		{
			name:        "missing tenant is returned without retry",
			initialErr:  tenantNotFoundErr,
			denyRetry:   true,
			wantTenants: []string{"tenantA"},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, enterrors.ErrTenantNotFound)
			},
		},
		{
			name:        "inactive tenant is returned without retry",
			initialErr:  tenantNotActiveErr,
			denyRetry:   true,
			wantTenants: []string{"tenantA"},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, enterrors.ErrTenantNotActive)
			},
		},
		{
			name:        "storage error is returned without retry",
			initialErr:  storageErr,
			denyRetry:   true,
			wantTenants: []string{"tenantA"},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, storageErr)
			},
		},
		{
			name:        "replica shortage on the tenant-less retry is returned",
			initialErr:  tenantErr,
			retryErr:    replicasErr,
			wantTenants: []string{"tenantA", ""},
			wantAuth:    1,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, replicaerrors.ErrReplicas)
			},
		},
		{
			name:        "other retry errors return the tenant-scoped error",
			initialErr:  tenantErr,
			retryErr:    retryErr,
			wantTenants: []string{"tenantA", ""},
			wantAuth:    1,
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
				return false, tt.initialErr
			}
			authorizer := mocks.NewMockAuthorizer()
			if tt.denyRetry {
				authorizer.Deny(authorization.ShardsData("To", "")...)
			}
			validator := New(stub, &config.WeaviateConfig{}, nil, nil, authorizer, false)
			ref := &crossref.Ref{Class: "To", TargetID: strfmt.UUID(UuidLower)}

			err := validator.ValidateExistence(context.Background(), ref, "ref", "tenantA")
			tt.check(t, err)
			require.Equal(t, tt.wantTenants, tenants)
			require.Len(t, authorizer.Calls(), tt.wantAuth)
		})
	}
}
