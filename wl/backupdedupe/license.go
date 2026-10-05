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
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/license"
)

// licenseFeature names deduplicated backups in the license refusal and the startup warning.
const licenseFeature = backup.DedupeFeature

const unlicensedDetail = "Every backup create request with dedupeReplicas is refused with 403. " +
	"Backups without the option and restores of existing deduplicated backups are not refused."

// ModeFor is the only code that pairs BACKUP_DEDUPE_ENABLED with Config.WeaviateLicense.
func ModeFor(cfg config.Config) license.Mode {
	return license.ModeFor(backup.DedupeEnabled(), cfg.WeaviateLicense)
}

// NewForMode returns the planner the backup scheduler plans with in mode: only FeatureLicensed constructs one, every other mode gets a nil interface.
func NewForMode(mode license.Mode, cfg Config) (backup.DedupePlanner, error) {
	switch mode {
	case license.FeatureLicensed:
		p, err := New(cfg)
		if err != nil {
			return nil, err
		}
		return p, nil
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return nil, nil
}

// LogUnlicensed warns at startup when dedupeReplicas is enabled on an unlicensed node.
func LogUnlicensed(logger logrus.FieldLogger, mode license.Mode) {
	license.LogUnlicensed(logger, mode, licenseFeature, unlicensedDetail)
}
