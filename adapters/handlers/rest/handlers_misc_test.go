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

	"github.com/go-openapi/runtime"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/meta"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

func TestMetaGetLicense(t *testing.T) {
	cases := []struct {
		name          string
		licenseState  *license.State
		wantStatus    string
		wantLicenseID string
	}{
		{
			name:          "licensed",
			licenseState:  &license.State{Status: license.StatusValid, LicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			wantStatus:    "valid",
			wantLicenseID: "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		{
			name:         "unlicensed",
			licenseState: &license.State{Status: license.StatusUnlicensed},
			wantStatus:   "unlicensed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := operations.NewWeaviateAPI(nil)

			logger := logrus.New()
			logger.SetOutput(io.Discard)
			setupMiscHandlers(api, &config.WeaviateConfig{}, nil, tc.licenseState, nil, logger)

			responder := api.MetaMetaGetHandler.Handle(meta.MetaGetParams{}, nil)
			rec := httptest.NewRecorder()
			responder.WriteResponse(rec, runtime.JSONProducer())

			require.Equal(t, http.StatusOK, rec.Code)

			var body models.Meta
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.NotNil(t, body.License)
			require.Equal(t, tc.wantStatus, body.License.Status)
			require.Equal(t, tc.wantLicenseID, body.License.LicenseID)
			require.False(t, body.License.Enforcing)
			require.False(t, body.License.ClusterMismatch)
			for _, key := range []string{"expiresAt", "lastCheckedAt", "graceEndsAt"} {
				require.NotContains(t, rec.Body.String(), key,
					"unknown timestamps must be absent, not serialized as zero values")
			}
			require.NotContains(t, rec.Body.String(), "wv8.",
				"key material must never appear in the meta response")
		})
	}
}
