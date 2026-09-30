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
	"github.com/weaviate/weaviate/wl/backupdedupe"
)

// backupDedupeFeature names deduplicated backups in the license refusal and the startup warning.
const backupDedupeFeature = backup.DedupeFeature

const unlicensedBackupDedupeDetail = "Every backup create request with dedupeReplicas is refused with 403. " +
	"Backups without the option and restores of existing deduplicated backups are not refused."

// backupDedupeModeFor is the only code that pairs BACKUP_DEDUPE_ENABLED with
// Config.WeaviateLicense.
func backupDedupeModeFor(cfg config.Config) license.Mode {
	return license.ModeFor(backup.DedupeEnabled(), cfg.WeaviateLicense)
}

// backupDedupePlanner returns the planner the backup scheduler plans with in
// mode: only FeatureLicensed constructs one, every other mode gets a nil
// interface.
func backupDedupePlanner(mode license.Mode, checkpointer backupdedupe.Checkpointer, logger logrus.FieldLogger) (backup.DedupePlanner, error) {
	switch mode {
	case license.FeatureLicensed:
		p, err := backupdedupe.New(backupdedupe.Config{Checkpointer: checkpointer, Logger: logger})
		if err != nil {
			return nil, err
		}
		return p, nil
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return nil, nil
}

func logUnlicensedBackupDedupe(logger logrus.FieldLogger, mode license.Mode) {
	license.LogUnlicensed(logger, mode, backupDedupeFeature, unlicensedBackupDedupeDetail)
}
