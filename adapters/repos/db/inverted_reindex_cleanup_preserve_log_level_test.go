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

package db

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestCleanStaleMigrationDirsAt_PreservedGensLogAtDebug pins that preserving a
// deferred-finalize tracker dir logs at Debug: this runs inside the RAFT
// apply loop, and Info would serialize ~10k lines per property DELETE on a
// 10k-tenant class awaiting restart-finalize.
func TestCleanStaleMigrationDirsAt_PreservedGensLogAtDebug(t *testing.T) {
	lsm := t.TempDir()
	propName := "category"
	indexType := "filterable"

	const preservedGens = 3
	for gen := 1; gen <= preservedGens; gen++ {
		dir := migrationDirWithProps(MigrationDirPrefixEnableFilterable, []string{propName}) + genSuffix(gen)
		mkTrackerDir(t, lsm, dir, "started.mig", "merged.mig", "tidied.mig")
	}

	hookLogger, hook := test.NewNullLogger()
	hookLogger.SetLevel(logrus.DebugLevel)

	cleanStaleMigrationDirsAt(t.Context(), lsm, propName, indexType, hookLogger, nil)

	var infoCount, debugCount int
	for _, e := range hook.AllEntries() {
		switch e.Level {
		case logrus.InfoLevel:
			infoCount++
		case logrus.DebugLevel:
			debugCount++
		default:
		}
	}
	require.Equal(t, 0, infoCount,
		"preserving a deferred-finalize tracker dir must not log at Info inside the RAFT apply loop")
	require.Equal(t, preservedGens, debugCount, "one Debug line per preserved generation")
}
