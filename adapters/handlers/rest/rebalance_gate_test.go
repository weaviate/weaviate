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

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/wl/rebalance"
)

func TestRebalanceModeFor(t *testing.T) {
	cases := []struct {
		name        string
		movement    bool
		rebalance   bool
		licensed    bool
		want        license.Mode
		wantPlanner license.Mode
	}{
		{name: "flags off without a license", want: license.FeatureOff, wantPlanner: license.FeatureUnlicensed},
		{name: "flags off with a license", licensed: true, want: license.FeatureOff, wantPlanner: license.FeatureLicensed},
		{name: "movement off with a license", rebalance: true, licensed: true, want: license.FeatureOff, wantPlanner: license.FeatureLicensed},
		{name: "rebalance off with a license", movement: true, licensed: true, want: license.FeatureOff, wantPlanner: license.FeatureLicensed},
		{name: "flags on without a license", movement: true, rebalance: true, want: license.FeatureUnlicensed, wantPlanner: license.FeatureUnlicensed},
		{name: "flags on with a license", movement: true, rebalance: true, licensed: true, want: license.FeatureLicensed, wantPlanner: license.FeatureLicensed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config.Config
			cfg.Replication.ReplicaMovementEnabled = runtime.NewDynamicValue(tc.movement)
			cfg.Replication.ReplicaRebalanceEnabled = runtime.NewDynamicValue(tc.rebalance)
			cfg.WeaviateLicense = tc.licensed

			require.Equal(t, tc.want, rebalanceModeFor(cfg))
			require.Equal(t, tc.wantPlanner, rebalancePlannerModeFor(cfg))
		})
	}
}

func TestRebalancePlannerFor(t *testing.T) {
	cases := []struct {
		name        string
		mode        license.Mode
		wantPlanner bool
	}{
		{name: "off", mode: license.FeatureOff},
		{name: "unlicensed", mode: license.FeatureUnlicensed},
		{name: "a mode outside the three", mode: license.Mode(99)},
		{name: "licensed", mode: license.FeatureLicensed, wantPlanner: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			builds := 0
			newPlanner := rebalancePlannerFor(tc.mode, func(deps cluster.RebalancePlannerDeps) (cluster.RebalancePlanner, error) {
				builds++
				return rebalance.NewRebalancePlanner(rebalance.RebalancePlannerParams{
					Logger:      logger,
					FSM:         replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry()),
					Proposer:    &cluster.Raft{},
					Drafting:    &cluster.Raft{},
					Clock:       clockwork.NewFakeClock(),
					Registerer:  prometheus.NewPedanticRegistry(),
					Schema:      schema.SchemaReader{},
					CurrentTerm: func() uint64 { return 0 },
					ReadyToPlan: func() bool { return false },
					Enabled:     func() bool { return false },
				})
			})

			require.Zero(t, builds, "the gate leaves the build to the cluster service")
			require.Equal(t, tc.wantPlanner, newPlanner != nil)
			if newPlanner == nil {
				return
			}
			planner, err := newPlanner(cluster.RebalancePlannerDeps{})
			require.NoError(t, err)
			require.True(t, license.DeclaredInWL(planner))
		})
	}
}
