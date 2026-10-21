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
	"net/http"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

// selfRecoveryFeature names self-recovery in the license refusal and the startup warning.
const selfRecoveryFeature = "self-recovery"

const unlicensedSelfRecoveryDetail = "No new shard self-recovery starts: a shard missing at startup or on tenant activation " +
	"is created empty and backfilled by async replication. SELF_RECOVERY ops registered by an earlier licensed run still complete. " +
	"POST /debug/self-recovery/restart and /debug/self-recovery/accept-empty are refused with 403."

// selfRecoveryDebugPaths are the operator endpoints an unlicensed node refuses; the licensed ones are registered from wl.
var selfRecoveryDebugPaths = [...]string{"/debug/self-recovery/accept-empty", "/debug/self-recovery/restart"}

// selfRecoveryModeFor is the only code that pairs SELF_RECOVERY_ENABLED with Config.WeaviateLicense.
func selfRecoveryModeFor(cfg config.Config) license.Mode {
	return license.ModeFor(cfg.Replication.SelfRecoveryEnabled, cfg.WeaviateLicense)
}

func logUnlicensedSelfRecovery(logger logrus.FieldLogger, mode license.Mode) {
	license.LogUnlicensed(logger, mode, selfRecoveryFeature, unlicensedSelfRecoveryDetail)
}

// selfRecoveryFor picks the orchestrator the DB hands missing shards to; build, the only wl/ code of self-recovery, runs in FeatureLicensed alone.
func selfRecoveryFor(mode license.Mode, logger logrus.FieldLogger, build func() db.SelfRecoveryOrchestrator) db.SelfRecoveryOrchestrator {
	switch mode {
	case license.FeatureOff:
		return nil
	case license.FeatureLicensed:
		return build()
	case license.FeatureUnlicensed:
	}
	return db.UnlicensedSelfRecovery{Logger: logger.WithField("component", "self_recovery")}
}

// selfRecoveryHousekeeping reclaims orphan "<shard>.recovering/" dirs in every mode and drops a stale wipe-round marker in every mode but FeatureLicensed, whose orchestrator resumes the round from it.
func selfRecoveryHousekeeping(mode license.Mode, rootDataPath string, logger logrus.FieldLogger) {
	if removed, err := db.CleanupOrphanRecoveryDirs(rootDataPath, logger); err != nil {
		logger.Warnf("self-recovery orphan cleanup failed: %v", err)
	} else if len(removed) > 0 {
		logger.WithField("count", len(removed)).Info("self-recovery: removed orphan recovery dirs")
	}
	switch mode {
	case license.FeatureLicensed:
		return
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	db.RemoveStaleSelfRecoveryWipeMarker(rootDataPath, logger)
}

// setupSelfRecoveryDebugHandlers registers the /debug/self-recovery endpoints for mode: none when off, setupWL when licensed and a 403 refusal otherwise.
func setupSelfRecoveryDebugHandlers(mux *http.ServeMux, mode license.Mode, setupWL func(*http.ServeMux)) {
	switch mode {
	case license.FeatureOff:
		return
	case license.FeatureLicensed:
		setupWL(mux)
		return
	case license.FeatureUnlicensed:
	}
	refusal := license.Required(selfRecoveryFeature).Error()
	for _, path := range selfRecoveryDebugPaths {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed; use POST", http.StatusMethodNotAllowed)
				return
			}
			http.Error(w, refusal, http.StatusForbidden)
		})
	}
}
