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
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

type fakeDedupePlanner struct{}

func (fakeDedupePlanner) PlanDesignatedShards(context.Context, []string, time.Duration,
	map[string]struct{}, map[string]map[string]string, func() bool,
) *backup.DedupePlan {
	return nil
}

func TestDedupeModeFor(t *testing.T) {
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

			require.Equal(t, tc.want, dedupeModeFor(cfg))
		})
	}
}

func TestDedupePlannerFor(t *testing.T) {
	errBuild := errors.New("build failed")
	cases := []struct {
		name        string
		mode        license.Mode
		buildErr    error
		wantBuilds  int
		wantPlanner bool
		wantErr     error
	}{
		{name: "off", mode: license.FeatureOff},
		{name: "unlicensed", mode: license.FeatureUnlicensed},
		{name: "a mode outside the three", mode: license.Mode(99)},
		{name: "licensed", mode: license.FeatureLicensed, wantBuilds: 1, wantPlanner: true},
		{name: "licensed build error", mode: license.FeatureLicensed, buildErr: errBuild, wantBuilds: 1, wantErr: errBuild},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builds := 0
			p, err := dedupePlannerFor(tc.mode, func() (backup.DedupePlanner, error) {
				builds++
				if tc.buildErr != nil {
					return nil, tc.buildErr
				}
				return fakeDedupePlanner{}, nil
			})

			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantBuilds, builds)
			require.Equal(t, tc.wantPlanner, p != nil)
			require.False(t, license.DeclaredInWL(p))
		})
	}
}

func TestLogUnlicensedDedupe(t *testing.T) {
	cases := []struct {
		name     string
		mode     license.Mode
		wantWarn bool
	}{
		{name: "off", mode: license.FeatureOff},
		{name: "unlicensed", mode: license.FeatureUnlicensed, wantWarn: true},
		{name: "licensed", mode: license.FeatureLicensed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()

			logUnlicensedDedupe(logger, tc.mode)

			if !tc.wantWarn {
				require.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			require.Equal(t, logrus.WarnLevel, entry.Level)
			require.Equal(t, backup.DedupeFeature, entry.Data["feature"])
			require.Contains(t, entry.Message, "dedupeReplicas")
			require.Contains(t, entry.Message, "restores")
			require.NotContains(t, backup.DedupeFeature, ":")
		})
	}
}
