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

package backupdedupe

import (
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

func TestModeFor(t *testing.T) {
	cases := []struct {
		name     string
		flag     string
		licensed bool
		want     license.Mode
	}{
		{name: "flag off without a license", flag: "", licensed: false, want: license.FeatureOff},
		{name: "flag off with a license", flag: "false", licensed: true, want: license.FeatureOff},
		{name: "flag on without a license", flag: "true", licensed: false, want: license.FeatureUnlicensed},
		{name: "flag on with a license", flag: "true", licensed: true, want: license.FeatureLicensed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BACKUP_DEDUPE_ENABLED", tc.flag)
			var cfg config.Config
			cfg.WeaviateLicense = tc.licensed

			require.Equal(t, tc.want, ModeFor(cfg))
		})
	}
}

func TestNewForMode(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	cases := []struct {
		name         string
		mode         license.Mode
		checkpointer Checkpointer
		wantPlanner  bool
		wantErr      error
	}{
		{name: "off wires no planner", mode: license.FeatureOff, checkpointer: newFakeCheckpointer()},
		{name: "unlicensed wires no planner", mode: license.FeatureUnlicensed, checkpointer: newFakeCheckpointer()},
		{name: "licensed wires the wl planner", mode: license.FeatureLicensed, checkpointer: newFakeCheckpointer(), wantPlanner: true},
		{name: "licensed without a checkpointer wires no planner", mode: license.FeatureLicensed, wantErr: ErrNilCheckpointer},
		{name: "a mode outside the three wires no planner", mode: license.Mode(99), checkpointer: newFakeCheckpointer()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewForMode(tc.mode, Config{Checkpointer: tc.checkpointer, Logger: logger})

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantPlanner, p != nil, "only a planner may be a non-nil interface")
			require.Equal(t, tc.wantPlanner, license.DeclaredInWL(p), "only FeatureLicensed may wire wl code")
		})
	}
}

func TestLogUnlicensed(t *testing.T) {
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

			LogUnlicensed(logger, tc.mode)

			if !tc.wantWarn {
				require.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			require.Equal(t, logrus.WarnLevel, entry.Level)
			require.Equal(t, licenseFeature, entry.Data["feature"])
			require.Contains(t, entry.Message, "dedupeReplicas")
			require.Contains(t, entry.Message, "restores")
			require.NotContains(t, licenseFeature, ":")
		})
	}
}
