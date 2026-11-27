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
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

const BEACON = "weaviate://localhost/"

var (
	UuidUpper = "4E5CD755-4F43-44C5-B23C-0C7D6F6C21E6"
	UuidLower = strings.ToLower(UuidUpper)
)

func TestValidationReferencesInObject(t *testing.T) {
	validator := New(fakeExists, &config.WeaviateConfig{}, nil, nil, &authorization.DummyAuthorizer{}, namespacing.Disabled, nil)

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
	validator := New(fakeExists, &config.WeaviateConfig{}, nil, nil, &authorization.DummyAuthorizer{}, namespacing.Disabled, nil)

	cref := &models.SingleRef{Beacon: strfmt.URI(BEACON + "To/" + UuidUpper)}
	ref, err := validator.ValidateSingleRef(cref)
	require.Nil(t, err)
	require.Equal(t, ref.TargetID.String(), UuidLower)
}

func TestValidateExistence(t *testing.T) {
	tenantErr := fmt.Errorf("class To %w", enterrors.ErrTenantForNonMultiTenantClass)
	tenantNotFoundErr := fmt.Errorf("validate tenant: %w", enterrors.ErrTenantNotFound)
	replicasErr := fmt.Errorf("check existence: %w",
		replicaerrors.NewNotEnoughReplicasError(errors.New("2 of 3 replicas down")))
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
			name:        "missing tenant is returned without retry",
			initialErr:  tenantNotFoundErr,
			denyRetry:   true,
			wantTenants: []string{"tenantA"},
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, enterrors.ErrTenantNotFound)
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
			validator := New(stub, &config.WeaviateConfig{}, nil, nil, authorizer, namespacing.Disabled, nil)
			ref := &crossref.Ref{Class: "To", TargetID: strfmt.UUID(UuidLower)}

			err := validator.ValidateExistence(context.Background(), ref, "ref", "tenantA")
			tt.check(t, err)
			require.Equal(t, tt.wantTenants, tenants)
			require.Len(t, authorizer.Calls(), tt.wantAuth)
		})
	}
}

func TestValidateExistenceInProbesEveryTarget(t *testing.T) {
	replicasErr := replicaerrors.NewNotEnoughReplicasError(errors.New("2 of 3 replicas down"))

	tests := []struct {
		name     string
		classes  []string
		answers  map[string]error // a nil answer means the class holds the target
		wantErr  error
		notFound bool
	}{
		{
			name:    "replica shortage before the holding class",
			classes: []string{"Alpha", "Beta"},
			answers: map[string]error{"Alpha": replicasErr, "Beta": nil},
		},
		{
			name:    "replica shortage after the holding class",
			classes: []string{"Beta", "Alpha"},
			answers: map[string]error{"Alpha": replicasErr, "Beta": nil},
		},
		{
			name:    "replica shortage and no holding class",
			classes: []string{"Alpha", "Beta"},
			answers: map[string]error{"Alpha": replicasErr},
			wantErr: replicaerrors.ErrReplicas,
		},
		{
			name:     "no holding class",
			classes:  []string{"Alpha", "Beta"},
			answers:  map[string]error{},
			notFound: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exists := func(_ context.Context, class string, _ strfmt.UUID, _ *additional.ReplicationProperties, _ string) (bool, error) {
				err, ok := tt.answers[class]
				return ok && err == nil, err
			}
			v := New(exists, &config.WeaviateConfig{}, nil, nil, &authorization.DummyAuthorizer{}, namespacing.Disabled, nil)

			err := v.ValidateExistenceIn(context.Background(), &crossref.Ref{TargetID: strfmt.UUID(UuidLower)},
				tt.classes, "ref", "")
			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.notFound:
				require.Error(t, err)
				require.NotErrorIs(t, err, replicaerrors.ErrReplicas)
			default:
				require.NoError(t, err)
			}
		})
	}
}
