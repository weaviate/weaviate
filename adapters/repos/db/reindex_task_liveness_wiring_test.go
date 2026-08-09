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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestReindexTaskLivenessLookup_ProductionWiringOrder pins the answer a
// real (unstubbed) lookup gives at each point in startup, and what the
// merged-residue decision does with it.
//
// The order matters and is not obvious: shards loaded eagerly during
// startup initialize before configure_api installs the task-list lookup
// via SetReindexAuditDeps. Their liveness answer is therefore Unknown,
// which decides Leave, and the working dirs wait for a later startup or
// the orphan audit. Refusal is reached only by shards that load after
// the deps land, in practice lazily-loaded and multi-tenant ones.
//
// Every other test in this area injects a liveness stub, so this is the
// only place the production accessor is exercised.
func TestReindexTaskLivenessLookup_ProductionWiringOrder(t *testing.T) {
	shape := rangeableResidue()

	deadBuilder := func() (KnownReindexTaskLookup, error) {
		return func(taskID string, taskVersion uint64) bool { return false }, nil
	}

	t.Run("eager shard init, before the deps land", func(t *testing.T) {
		db := &DB{}
		logger, _ := test.NewNullLogger()

		lookup := db.reindexTaskLivenessLookup()
		require.Equal(t, ReindexTaskLivenessUnknown,
			lookup.Answer(deadTaskID, deadTaskVersion),
			"a shard that initializes before SetReindexAuditDeps has no task list to consult")

		lsmPath := writeMergedResidue(t, shape, true)
		decision := mergedPromotionDecision(
			filepath.Join(lsmPath, ".migrations", shape.dirName), shape.dirName,
			classWith(shape.disagreeing), lookup, logger)
		require.Equal(t, mergedPromotionLeave, decision,
			"unknown liveness must never take the destructive arm")
	})

	t.Run("a shard loaded after the deps land", func(t *testing.T) {
		db := &DB{}
		logger, _ := test.NewNullLogger()
		db.SetReindexAuditDeps(deadBuilder, logger)

		lookup := db.reindexTaskLivenessLookup()
		require.Equal(t, ReindexTaskLivenessDead,
			lookup.Answer(deadTaskID, deadTaskVersion))

		lsmPath := writeMergedResidue(t, shape, true)
		decision := mergedPromotionDecision(
			filepath.Join(lsmPath, ".migrations", shape.dirName), shape.dirName,
			classWith(shape.disagreeing), lookup, logger)
		require.Equal(t, mergedPromotionRefuse, decision,
			"a task proven dead whose migration the schema does not reflect must be refused")
	})

	t.Run("each shard gets a fresh lookup, so the deps are picked up", func(t *testing.T) {
		db := &DB{}
		logger, _ := test.NewNullLogger()

		early := db.reindexTaskLivenessLookup()
		require.Equal(t, ReindexTaskLivenessUnknown, early.Answer(deadTaskID, deadTaskVersion))

		db.SetReindexAuditDeps(deadBuilder, logger)

		require.Equal(t, ReindexTaskLivenessUnknown, early.Answer(deadTaskID, deadTaskVersion),
			"a lookup that already resolved keeps its answer for the shard that took it")
		require.Equal(t, ReindexTaskLivenessDead,
			db.reindexTaskLivenessLookup().Answer(deadTaskID, deadTaskVersion),
			"the next shard to start must see the installed task list")
	})
}
