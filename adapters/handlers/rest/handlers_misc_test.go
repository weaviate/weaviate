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

package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/meta"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

type mutableLicenseProvider struct {
	state license.State
	calls int
}

func (p *mutableLicenseProvider) Current() license.State {
	p.calls++
	return p.state
}

func TestMetaGetLicense(t *testing.T) {
	expiresAt := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	lastCheckedAt := time.Date(2026, 11, 10, 11, 12, 13, 0, time.UTC)
	gracePeriodEndsAt := lastCheckedAt.Add(7 * 24 * time.Hour)
	cases := []struct {
		name                  string
		licenseState          *license.State
		wantEdition           string
		wantStatus            string
		wantLicenseID         string
		wantExpiresAt         time.Time
		wantLastCheckedAt     time.Time
		wantGracePeriodEndsAt time.Time
		wantClusterMismatch   bool
	}{
		{
			name: "enterprise edition with reporting fields",
			licenseState: &license.State{
				Status: license.StatusValid, LicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ExpiresAt: expiresAt, LastCheckedAt: lastCheckedAt,
				GracePeriodEndsAt: gracePeriodEndsAt, ClusterMismatch: true,
			},
			wantEdition: "enterprise", wantStatus: "valid",
			wantLicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			wantExpiresAt: expiresAt, wantLastCheckedAt: lastCheckedAt,
			wantClusterMismatch: true,
		},
		{
			name:          "enterprise edition with unknown timestamps",
			licenseState:  &license.State{Status: license.StatusUnreachable, LicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			wantEdition:   "enterprise",
			wantStatus:    "unreachable",
			wantLicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		{
			name: "community edition omits enterprise fields",
			licenseState: &license.State{
				Status: license.StatusUnlicensed, LicenseID: "must-not-appear",
				ExpiresAt: expiresAt, LastCheckedAt: lastCheckedAt,
				GracePeriodEndsAt: gracePeriodEndsAt, ClusterMismatch: true,
			},
			wantEdition: "community",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newMetaAPI(tc.licenseState)
			body, raw := requestMeta(t, api)

			require.NotNil(t, body.License)
			require.Equal(t, tc.wantEdition, body.License.Edition)
			require.Equal(t, license.EnterpriseDocsURL, body.License.DocumentationHref)
			require.Equal(t, tc.wantStatus, body.License.Status)
			require.Equal(t, tc.wantLicenseID, body.License.LicenseID)
			require.False(t, body.License.Enforcing)
			require.Equal(t, tc.wantClusterMismatch, body.License.ClusterMismatch)
			requireMetaTimestamp(t, tc.wantExpiresAt, body.License.ExpiresAt)
			requireMetaTimestamp(t, tc.wantLastCheckedAt, body.License.LastCheckedAt)
			requireMetaTimestamp(t, tc.wantGracePeriodEndsAt, body.License.GracePeriodEndsAt)
			for _, field := range []struct {
				key   string
				value *strfmt.DateTime
			}{
				{key: "expiresAt", value: body.License.ExpiresAt},
				{key: "lastCheckedAt", value: body.License.LastCheckedAt},
				{key: "gracePeriodEndsAt", value: body.License.GracePeriodEndsAt},
			} {
				if field.value == nil {
					require.NotContains(t, raw, `"`+field.key+`"`,
						"unknown timestamps must be absent, not serialized as zero values")
				} else {
					require.Contains(t, raw, `"`+field.key+`"`)
				}
			}
			if tc.wantEdition == "community" {
				require.NotContains(t, raw, `"status"`)
				require.NotContains(t, raw, `"licenseId"`)
				require.NotContains(t, raw, `"clusterMismatch"`)
			}
			require.NotContains(t, raw, "wv8.",
				"key material must never appear in the meta response")
		})
	}
}

func TestMetaGetLicenseDynamic(t *testing.T) {
	firstExpiry := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	secondExpiry := firstExpiry.Add(48 * time.Hour)
	lastCheckedAt := time.Date(2026, 11, 10, 11, 12, 13, 0, time.UTC)
	provider := &mutableLicenseProvider{state: license.State{
		Status: license.StatusValid, LicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExpiresAt: firstExpiry,
	}}
	api := newMetaAPI(provider)

	body, _ := requestMeta(t, api)
	require.Equal(t, "valid", body.License.Status)
	requireMetaTimestamp(t, firstExpiry, body.License.ExpiresAt)
	require.False(t, body.License.ClusterMismatch)
	require.Equal(t, 1, provider.calls)

	provider.state = license.State{
		Status: license.StatusValid, LicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ExpiresAt: secondExpiry, LastCheckedAt: lastCheckedAt,
		GracePeriodEndsAt: lastCheckedAt.Add(7 * 24 * time.Hour), ClusterMismatch: true,
	}
	body, _ = requestMeta(t, api)
	require.Equal(t, "valid", body.License.Status)
	requireMetaTimestamp(t, secondExpiry, body.License.ExpiresAt)
	requireMetaTimestamp(t, lastCheckedAt, body.License.LastCheckedAt)
	require.Nil(t, body.License.GracePeriodEndsAt, "valid responses omit the grace deadline")
	require.True(t, body.License.ClusterMismatch)
	require.Equal(t, 2, provider.calls)

	provider.state = license.State{
		Status: license.StatusExpired, LicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		GracePeriodEndsAt: lastCheckedAt.Add(7 * 24 * time.Hour),
	}
	body, raw := requestMeta(t, api)
	require.Equal(t, "expired", body.License.Status)
	require.Nil(t, body.License.ExpiresAt)
	require.Nil(t, body.License.LastCheckedAt)
	requireMetaTimestamp(t, lastCheckedAt.Add(7*24*time.Hour), body.License.GracePeriodEndsAt)
	require.False(t, body.License.ClusterMismatch)
	require.NotContains(t, raw, "expiresAt")
	require.Equal(t, 3, provider.calls)
}

func newMetaAPI(provider license.StateProvider) *operations.WeaviateAPI {
	api := operations.NewWeaviateAPI(nil)
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	setupMiscHandlers(api, &config.WeaviateConfig{}, nil, provider, nil, logger)
	return api
}

func requestMeta(t *testing.T, api *operations.WeaviateAPI) (*models.Meta, string) {
	t.Helper()
	responder := api.MetaMetaGetHandler.Handle(meta.MetaGetParams{}, nil)
	rec := httptest.NewRecorder()
	responder.WriteResponse(rec, runtime.JSONProducer())
	require.Equal(t, http.StatusOK, rec.Code)

	var body models.Meta
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return &body, rec.Body.String()
}

func requireMetaTimestamp(t *testing.T, want time.Time, got *strfmt.DateTime) {
	t.Helper()
	if want.IsZero() {
		require.Nil(t, got)
		return
	}
	require.NotNil(t, got)
	require.True(t, want.Equal(time.Time(*got)), "want %s, got %s", want, got.String())
}
