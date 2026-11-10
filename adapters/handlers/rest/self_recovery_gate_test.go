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
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/wl/selfrecovery"
	srhandlers "github.com/weaviate/weaviate/wl/selfrecovery/handlers"
)

var allLicenseModes = []struct {
	name string
	mode license.Mode
}{
	{name: "off", mode: license.FeatureOff},
	{name: "unlicensed", mode: license.FeatureUnlicensed},
	{name: "licensed", mode: license.FeatureLicensed},
}

func TestSelfRecoveryModeFor(t *testing.T) {
	cases := []struct {
		name     string
		enabled  bool
		licensed bool
		want     license.Mode
	}{
		{name: "flag off without a license", want: license.FeatureOff},
		{name: "flag off with a license", licensed: true, want: license.FeatureOff},
		{name: "flag on without a license", enabled: true, want: license.FeatureUnlicensed},
		{name: "flag on with a license", enabled: true, licensed: true, want: license.FeatureLicensed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config.Config
			cfg.Replication.SelfRecoveryEnabled = tc.enabled
			cfg.WeaviateLicense = tc.licensed

			require.Equal(t, tc.want, selfRecoveryModeFor(cfg))
		})
	}
}

func TestSelfRecoveryFor(t *testing.T) {
	cases := []struct {
		name       string
		mode       license.Mode
		wantBuilds int
		wantNil    bool
		wantStub   bool
		wantInWL   bool
	}{
		{name: "off", mode: license.FeatureOff, wantNil: true},
		{name: "unlicensed", mode: license.FeatureUnlicensed, wantStub: true},
		{name: "a mode outside the three", mode: license.Mode(99), wantStub: true},
		{name: "licensed", mode: license.FeatureLicensed, wantBuilds: 1, wantInWL: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			builds := 0
			orch := selfRecoveryFor(tc.mode, logger, func() db.SelfRecoveryOrchestrator {
				builds++
				return selfrecovery.New(selfrecovery.Config{Logger: logger})
			})
			if orch != nil {
				t.Cleanup(func() { require.NoError(t, orch.Close(context.Background())) })
			}

			require.Equal(t, tc.wantBuilds, builds)
			require.Equal(t, tc.wantNil, orch == nil)
			require.Equal(t, tc.wantInWL, license.DeclaredInWL(orch))
			_, isStub := orch.(db.UnlicensedSelfRecovery)
			require.Equal(t, tc.wantStub, isStub)
			if tc.wantNil {
				return
			}
			require.True(t, orch.Enabled())
			if tc.wantStub {
				require.False(t, orch.SubmitRecovery(context.Background(), "C", "S", false))
				require.False(t, orch.SubmitActivationRecovery(context.Background(), "C", "S"))
			}
		})
	}
}

func TestSelfRecoveryHousekeeping(t *testing.T) {
	for _, tc := range allLicenseModes {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			root := t.TempDir()
			marker := filepath.Join(root, api.SelfRecoveryWipeMarkerName)
			require.NoError(t, os.WriteFile(marker, nil, 0o644))
			for _, dir := range []string{"C/S1", "C/S1" + api.RecoveryFolderSuffix, "C/S2" + api.RecoveryFolderSuffix} {
				require.NoError(t, os.MkdirAll(filepath.Join(root, dir), 0o755))
			}

			selfRecoveryHousekeeping(tc.mode, root, logger)

			require.NoDirExists(t, filepath.Join(root, "C/S1"+api.RecoveryFolderSuffix))
			require.DirExists(t, filepath.Join(root, "C/S1"))
			require.DirExists(t, filepath.Join(root, "C/S2"+api.RecoveryFolderSuffix))
			if tc.mode == license.FeatureLicensed {
				require.FileExists(t, marker)
			} else {
				require.NoFileExists(t, marker)
			}
		})
	}
}

func TestSetupSelfRecoveryDebugHandlers(t *testing.T) {
	refusal := license.Required(selfRecoveryFeature).Error()
	paths := []string{srhandlers.AcceptEmptyPath, srhandlers.RestartPath}
	require.ElementsMatch(t, paths, selfRecoveryDebugPaths[:])
	cases := []struct {
		name       string
		mode       license.Mode
		method     string
		wantWL     bool
		wantStatus int
		wantBody   string
	}{
		{name: "off", mode: license.FeatureOff, method: http.MethodPost, wantStatus: http.StatusNotFound},
		{name: "unlicensed", mode: license.FeatureUnlicensed, method: http.MethodPost, wantStatus: http.StatusForbidden, wantBody: refusal},
		{name: "unlicensed GET", mode: license.FeatureUnlicensed, method: http.MethodGet, wantStatus: http.StatusMethodNotAllowed},
		{name: "licensed", mode: license.FeatureLicensed, method: http.MethodPost, wantWL: true, wantStatus: http.StatusServiceUnavailable, wantBody: "not configured"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			mux := http.NewServeMux()
			wlSetups := 0
			setupSelfRecoveryDebugHandlers(mux, tc.mode, func(mux *http.ServeMux) {
				wlSetups++
				srhandlers.SetupHandlers(mux, logger, nil)
			})

			require.Equal(t, tc.wantWL, wlSetups == 1)
			for _, path := range paths {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(tc.method, path+"?collection=C&shard=S", nil))

				require.Equal(t, tc.wantStatus, rec.Code, path)
				require.Contains(t, rec.Body.String(), tc.wantBody, path)
			}
		})
	}
}

func TestLogUnlicensedSelfRecovery(t *testing.T) {
	for _, tc := range allLicenseModes {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()

			logUnlicensedSelfRecovery(logger, tc.mode)

			if tc.mode != license.FeatureUnlicensed {
				require.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			require.Equal(t, logrus.WarnLevel, entry.Level)
			require.Equal(t, selfRecoveryFeature, entry.Data["feature"])
			require.Contains(t, entry.Message, "no well-formed Weaviate license key")
			require.Contains(t, entry.Message, "still complete")
			require.NotContains(t, selfRecoveryFeature, ":")
		})
	}
}
