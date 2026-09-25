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
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func TestNamespaceModeFor(t *testing.T) {
	cases := []struct {
		name              string
		namespacesEnabled bool
		licensed          bool
		want              license.Mode
	}{
		{name: "namespaces on without a license", namespacesEnabled: true, licensed: false, want: license.FeatureUnlicensed},
		{name: "namespaces off with a license", namespacesEnabled: false, licensed: true, want: license.FeatureOff},
		{name: "namespaces on with a license", namespacesEnabled: true, licensed: true, want: license.FeatureLicensed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config.Config
			cfg.Namespaces.Enabled = tc.namespacesEnabled
			cfg.WeaviateLicense = tc.licensed

			require.Equal(t, tc.want, namespaceModeFor(cfg))
		})
	}
}

func TestNamespaceQualifier(t *testing.T) {
	principal := &models.Principal{Username: "u", Namespace: "customer1"}
	cases := []struct {
		name        string
		mode        license.Mode
		wantType    namespacing.Qualifier
		wantEnabled bool
		wantName    string
		wantErr     error
	}{
		{
			name:        "off passes names through",
			mode:        license.FeatureOff,
			wantType:    namespacing.Disabled,
			wantEnabled: false,
			wantName:    "Movies",
		},
		{
			name:        "unlicensed refuses",
			mode:        license.FeatureUnlicensed,
			wantType:    namespacing.Refusing(nil),
			wantEnabled: true,
			wantErr:     license.ErrRequired,
		},
		{
			name:        "licensed prefixes the caller's namespace",
			mode:        license.FeatureLicensed,
			wantType:    &namespacing.Prefixing{},
			wantEnabled: true,
			wantName:    "customer1:Movies",
		},
		{
			name:        "a mode outside the three refuses",
			mode:        license.Mode(99),
			wantType:    namespacing.Refusing(nil),
			wantEnabled: true,
			wantErr:     license.ErrRequired,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := namespaceQualifier(tc.mode)

			require.IsType(t, tc.wantType, q)
			require.Equal(t, tc.wantEnabled, q.NamespacesEnabled())
			if tc.mode != license.FeatureLicensed {
				require.False(t, license.DeclaredInWL(q), "only FeatureLicensed may wire wl code")
			}
			got, err := q.Qualify(principal, "Movies")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantName, got)
		})
	}
}

func TestLogUnlicensedNamespaces(t *testing.T) {
	cases := []struct {
		name     string
		mode     license.Mode
		wantWarn bool
	}{
		{name: "off", mode: license.FeatureOff, wantWarn: false},
		{name: "unlicensed", mode: license.FeatureUnlicensed, wantWarn: true},
		{name: "licensed", mode: license.FeatureLicensed, wantWarn: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()

			logUnlicensedNamespaces(logger, tc.mode)

			if !tc.wantWarn {
				require.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			require.Equal(t, logrus.WarnLevel, entry.Level)
			require.Equal(t, namespacesFeature, entry.Data["feature"])
			require.Contains(t, entry.Message, "data request")
			require.Contains(t, entry.Message, "batch references")
		})
	}
}
