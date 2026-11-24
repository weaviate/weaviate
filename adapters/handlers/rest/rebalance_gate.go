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
	"github.com/sirupsen/logrus"

	replicationHandlers "github.com/weaviate/weaviate/adapters/handlers/rest/replication"
	"github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

const unlicensedRebalanceDetail = "Every rebalance plan create request is refused with 403 and no rebalance planning runs on this node. " +
	"An existing plan can still be read, stopped and deleted."

// rebalanceModeFor pairs the startup values of REPLICA_MOVEMENT_ENABLED and REPLICA_REBALANCE_ENABLED with Config.WeaviateLicense.
// Both flags can change at runtime, so only the startup warning uses this mode.
func rebalanceModeFor(cfg config.Config) license.Mode {
	return license.ModeFor(cfg.Replication.ReplicaMovementEnabled.Get() && cfg.Replication.ReplicaRebalanceEnabled.Get(), cfg.WeaviateLicense)
}

// rebalancePlannerModeFor is never FeatureOff: a licensed node always builds the planner, which reads both flags on every pass.
func rebalancePlannerModeFor(cfg config.Config) license.Mode {
	return license.ModeFor(true, cfg.WeaviateLicense)
}

func logUnlicensedRebalance(logger logrus.FieldLogger, mode license.Mode) {
	license.LogUnlicensed(logger, mode, replicationHandlers.RebalanceFeature, unlicensedRebalanceDetail)
}

// rebalancePlannerFor returns build, the only wl/ code of cluster rebalancing, in FeatureLicensed alone. Every other mode gets nil,
// so the cluster service runs no planner.
func rebalancePlannerFor(mode license.Mode, build func(cluster.RebalancePlannerDeps) (cluster.RebalancePlanner, error),
) func(cluster.RebalancePlannerDeps) (cluster.RebalancePlanner, error) {
	switch mode {
	case license.FeatureLicensed:
		return build
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return nil
}
