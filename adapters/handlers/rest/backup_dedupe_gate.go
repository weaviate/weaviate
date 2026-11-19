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

	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

const unlicensedDedupeDetail = "Every backup create request with dedupeReplicas is refused with 403. " +
	"Backups without the option and restores of existing deduplicated backups are not refused."

// dedupeModeFor is the only code that pairs BACKUP_DEDUPE_ENABLED with Config.WeaviateLicense.
func dedupeModeFor(cfg config.Config) license.Mode {
	return license.ModeFor(backup.DedupeEnabled(), cfg.WeaviateLicense)
}

func logUnlicensedDedupe(logger logrus.FieldLogger, mode license.Mode) {
	license.LogUnlicensed(logger, mode, backup.DedupeFeature, unlicensedDedupeDetail)
}

// dedupePlannerFor calls build, the only wl/ code of deduplicated backups, in FeatureLicensed alone; every other mode gets a nil planner.
func dedupePlannerFor(mode license.Mode, build func() (backup.DedupePlanner, error)) (backup.DedupePlanner, error) {
	switch mode {
	case license.FeatureLicensed:
		return build()
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return nil, nil
}
