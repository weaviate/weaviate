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

package license

import (
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	wlnamespaces "github.com/weaviate/weaviate/wl/namespaces"
)

func TestRequired(t *testing.T) {
	err := Required("namespaces")

	require.ErrorIs(t, err, ErrRequired)
	var forbidden authzerrors.Forbidden
	require.ErrorAs(t, err, &forbidden)
	require.Contains(t, err.Error(), "the namespaces feature")
	require.NotContains(t, err.Error(), ":")
}

func TestModeFor(t *testing.T) {
	cases := []struct {
		name           string
		featureEnabled bool
		licensed       bool
		want           Mode
	}{
		{name: "off, unlicensed", featureEnabled: false, licensed: false, want: FeatureOff},
		{name: "off, licensed", featureEnabled: false, licensed: true, want: FeatureOff},
		{name: "on, unlicensed", featureEnabled: true, licensed: false, want: FeatureUnlicensed},
		{name: "on, licensed", featureEnabled: true, licensed: true, want: FeatureLicensed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ModeFor(tc.featureEnabled, tc.licensed))
		})
	}
}

func TestLogUnlicensed(t *testing.T) {
	cases := []struct {
		name     string
		mode     Mode
		wantWarn bool
	}{
		{name: "off", mode: FeatureOff, wantWarn: false},
		{name: "unlicensed", mode: FeatureUnlicensed, wantWarn: true},
		{name: "licensed", mode: FeatureLicensed, wantWarn: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()

			LogUnlicensed(logger, tc.mode, "feat", "the detail")

			if !tc.wantWarn {
				require.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			require.Equal(t, logrus.WarnLevel, entry.Level)
			require.Equal(t, "startup", entry.Data["action"])
			require.Equal(t, "feat", entry.Data["feature"])
			require.Contains(t, entry.Message, "the feat feature is enabled but this node holds no well-formed Weaviate license key")
			require.Contains(t, entry.Message, "the detail")
		})
	}
}

func TestInWL(t *testing.T) {
	cases := []struct {
		pkgPath string
		want    bool
	}{
		{pkgPath: "github.com/weaviate/weaviate/wl", want: true},
		{pkgPath: "github.com/weaviate/weaviate/wl/namespaces", want: true},
		{pkgPath: "github.com/weaviate/weaviate/wlx", want: false},
		{pkgPath: "github.com/weaviate/weaviate/usecases/license", want: false},
		{pkgPath: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.pkgPath, func(t *testing.T) {
			require.Equal(t, tc.want, inWL(tc.pkgPath))
		})
	}
}

func TestDeclaredInWL(t *testing.T) {
	cases := []struct {
		name string
		v    any
		want bool
	}{
		{name: "pointer to a type in wl", v: wlnamespaces.NewPrefixing(), want: true},
		{name: "non-pointer in wl", v: wlnamespaces.Prefixing{}, want: true},
		{name: "pointer to a type outside wl", v: new(Mode), want: false},
		{name: "non-pointer outside wl", v: FeatureOff, want: false},
		{name: "nil", v: nil, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, DeclaredInWL(tc.v))
		})
	}
}
