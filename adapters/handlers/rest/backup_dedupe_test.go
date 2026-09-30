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
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/wl/backupdedupe"
)

func TestBackupDedupeModeFor(t *testing.T) {
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

			require.Equal(t, tc.want, backupDedupeModeFor(cfg))
		})
	}
}

type stubCheckpointer struct{}

func (stubCheckpointer) ShardReplicas(context.Context, string) (map[string][]string, error) {
	return nil, nil
}

func (stubCheckpointer) IsAsyncReplicationEnabled(context.Context, string) bool { return false }

func (stubCheckpointer) CreateAsyncCheckpoints(context.Context, string, int64, []string) error {
	return nil
}

func (stubCheckpointer) DeleteAsyncCheckpoints(context.Context, string, []string) error { return nil }

func (stubCheckpointer) GetAsyncCheckpointNodeStatuses(context.Context, string, []string) (map[string][]replica.AsyncCheckpointNodeStatus, error) {
	return nil, nil
}

func TestBackupDedupePlanner(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	cases := []struct {
		name         string
		mode         license.Mode
		checkpointer backupdedupe.Checkpointer
		wantPlanner  bool
		wantErr      error
	}{
		{name: "off wires no planner", mode: license.FeatureOff, checkpointer: stubCheckpointer{}},
		{name: "unlicensed wires no planner", mode: license.FeatureUnlicensed, checkpointer: stubCheckpointer{}},
		{name: "licensed wires the wl planner", mode: license.FeatureLicensed, checkpointer: stubCheckpointer{}, wantPlanner: true},
		{name: "licensed without a checkpointer wires no planner", mode: license.FeatureLicensed, wantErr: backupdedupe.ErrNilCheckpointer},
		{name: "a mode outside the three wires no planner", mode: license.Mode(99), checkpointer: stubCheckpointer{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := backupDedupePlanner(tc.mode, tc.checkpointer, logger)

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

func TestLogUnlicensedBackupDedupe(t *testing.T) {
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

			logUnlicensedBackupDedupe(logger, tc.mode)

			if !tc.wantWarn {
				require.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			require.Equal(t, logrus.WarnLevel, entry.Level)
			require.Equal(t, backupDedupeFeature, entry.Data["feature"])
			require.Contains(t, entry.Message, "dedupeReplicas")
			require.Contains(t, entry.Message, "restores")
			require.NotContains(t, backupDedupeFeature, ":")
		})
	}
}
