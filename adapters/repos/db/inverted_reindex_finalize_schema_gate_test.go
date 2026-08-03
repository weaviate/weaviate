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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// retokenizeGateClass builds the single-property text class the schema-gate
// journeys use, at the given tokenization.
func retokenizeGateClass(className, propName, tokenization string) *models.Class {
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: []*models.Property{{
			Name:         propName,
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: tokenization,
		}},
	}
}

// TestFinalize_MergedResidue_SchemaGate pins the merged-recovery gate on a
// FAILED task's merged-but-untidied residue, on byte-identical on-disk
// state: promotion happens only once the schema already reflects the
// target, never from file presence alone (weaviate/weaviate#10675-shape
// divergence otherwise).
func TestFinalize_MergedResidue_SchemaGate(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	cases := []struct {
		name string
		// tokenization the shard is reloaded with.
		reloadTokenization string
		// wantSourceTerms: canonical bucket keeps word-tokenized postings.
		wantSourceTerms bool
	}{
		{
			name:               "schema_still_source_refuses_promotion",
			reloadTokenization: models.PropertyTokenizationWord,
			wantSourceTerms:    true,
		},
		{
			name:               "schema_already_target_promotes",
			reloadTokenization: models.PropertyTokenizationField,
			wantSourceTerms:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "MergedGate_" + uuid.NewString()[:8]
			class := retokenizeGateClass(className, propName, models.PropertyTokenizationWord)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
			sourceFingerprint := fingerprintInvertedBucket(t, shard.store.Bucket(bucketName))
			require.Contains(t, sourceFingerprint, "alpha",
				"sanity: the pre-migration bucket is word-tokenized")

			// Drive the real task to merged: reindex + prepare, no swap.
			preStrategy := shard.store.Bucket(bucketName).Strategy()
			task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
				models.PropertyTokenizationField, preStrategy)
			persistTestRecoveryPayload(t, task, shard.pathLSM(), ReindexTaskPayload{
				MigrationType:      ReindexTypeChangeTokenization,
				Collection:         className,
				Properties:         []string{propName},
				TargetTokenization: models.PropertyTokenizationField,
			})
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))

			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			require.True(t, rt.IsMerged(), "sanity: the task must have reached merged")
			require.False(t, rt.IsTidied(), "sanity: the swap must not have run")

			// The task fails here and is never resumed: no reindexer is
			// wired on the reload, so nothing but finalize touches the
			// residue.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))
			idx.shardReindexer = NewShardReindexerV3Noop()

			reloadClass := retokenizeGateClass(className, propName, tc.reloadTokenization)
			shd2, err := idx.initShard(ctx, shardName, reloadClass, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed")
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			got := fingerprintInvertedBucket(t, shard2.store.Bucket(bucketName))
			if tc.wantSourceTerms {
				require.Equal(t, sourceFingerprint, got,
					"the schema still says %q, so the merged residue of an uncommitted migration "+
						"must NOT replace the canonical bucket — it now serves target-tokenized "+
						"postings the schema cannot address", models.PropertyTokenizationWord)
			} else {
				require.NotContains(t, got, "alpha",
					"the schema already says %q, so the merged residue is the post-flip state "+
						"this node must converge to — promotion was skipped and the bucket is "+
						"still word-tokenized", models.PropertyTokenizationField)
				require.NotEmpty(t, got, "the promoted bucket must not be empty")
			}

			// Either way the residue is consumed: promoted or discarded,
			// never left to be promoted by a later restart against a
			// bucket that has drifted since the task died.
			migsDir := filepath.Join(shard2.pathLSM(), ".migrations")
			entries, _ := os.ReadDir(migsDir)
			require.Empty(t, entries, "the merged tracker must not survive finalize")
			require.NoDirExists(t,
				filepath.Join(shard2.pathLSM(), bucketName+"__retokenize_ingest_1"),
				"the ingest sidecar must not survive finalize")
		})
	}
}

// persistTestRecoveryPayload writes the tracker's payload.mig through the
// same production method [ReindexProvider.persistRecoveryRecord] uses, so
// the gate under test reads a real recovery record rather than a
// hand-placed file.
func persistTestRecoveryPayload(t *testing.T, task *ShardReindexTaskGeneric,
	lsmPath string, payload ReindexTaskPayload,
) {
	t.Helper()
	encoded, err := json.Marshal(reindexRecoveryRecord{
		TaskID:      "gate-test-task",
		TaskVersion: 1,
		UnitID:      "unit-0",
		Payload:     payload,
	})
	require.NoError(t, err)
	require.NoError(t, task.SaveRecoveryPayload(lsmPath, encoded))
}

// TestMergedPromotionAgreesWithSchema covers the decision matrix the
// merged-recovery gate applies per property.
func TestMergedPromotionAgreesWithSchema(t *testing.T) {
	textProp := func(tokenization string) *models.Property {
		return &models.Property{
			Name:         "p",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: tokenization,
		}
	}
	classOf := func(prop *models.Property) *models.Class {
		return &models.Class{Class: "C", Properties: []*models.Property{prop}}
	}

	cases := []struct {
		name    string
		payload ReindexTaskPayload
		class   *models.Class
		want    bool
	}{
		{
			name:    "repair-filterable always promotes",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeRepairFilterable},
			class:   nil,
			want:    true,
		},
		{
			name:    "repair-rangeable always promotes",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeRepairRangeable},
			class:   nil,
			want:    true,
		},
		{
			name:    "rebuild-searchable always promotes",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeRebuildSearchable},
			class:   nil,
			want:    true,
		},
		{
			name:    "change-algorithm always promotes",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeChangeAlgorithm},
			class:   nil,
			want:    true,
		},
		{
			name: "change-tokenization promotes when the schema reached the target",
			payload: ReindexTaskPayload{
				MigrationType:      ReindexTypeChangeTokenization,
				TargetTokenization: models.PropertyTokenizationField,
			},
			class: classOf(textProp(models.PropertyTokenizationField)),
			want:  true,
		},
		{
			name: "change-tokenization refuses while the schema is still the source",
			payload: ReindexTaskPayload{
				MigrationType:      ReindexTypeChangeTokenization,
				TargetTokenization: models.PropertyTokenizationField,
			},
			class: classOf(textProp(models.PropertyTokenizationWord)),
			want:  false,
		},
		{
			name: "change-tokenization-filterable promotes when the schema reached the target",
			payload: ReindexTaskPayload{
				MigrationType:      ReindexTypeChangeTokenizationFilterable,
				TargetTokenization: models.PropertyTokenizationWord,
			},
			class: classOf(textProp(models.PropertyTokenizationWord)),
			want:  true,
		},
		{
			name: "change-tokenization refuses on an empty target tokenization",
			payload: ReindexTaskPayload{
				MigrationType: ReindexTypeChangeTokenization,
			},
			class: classOf(textProp("")),
			want:  false,
		},
		{
			name:    "enable-filterable promotes once the flag is true",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable},
			class: classOf(&models.Property{
				Name: "p", DataType: schema.DataTypeText.PropString(),
				IndexFilterable: ptBool(true),
			}),
			want: true,
		},
		{
			name:    "enable-filterable refuses while the flag is false",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable},
			class: classOf(&models.Property{
				Name: "p", DataType: schema.DataTypeText.PropString(),
				IndexFilterable: ptBool(false),
			}),
			want: false,
		},
		{
			name:    "enable-searchable promotes once the flag is true",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableSearchable},
			class: classOf(&models.Property{
				Name: "p", DataType: schema.DataTypeText.PropString(),
				IndexSearchable: ptBool(true),
			}),
			want: true,
		},
		{
			name:    "enable-searchable refuses while the flag is false",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableSearchable},
			class: classOf(&models.Property{
				Name: "p", DataType: schema.DataTypeText.PropString(),
				IndexSearchable: ptBool(false),
			}),
			want: false,
		},
		{
			name:    "enable-rangeable promotes once the flag is true",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable},
			class: classOf(&models.Property{
				Name: "p", DataType: schema.DataTypeInt.PropString(),
				IndexRangeFilters: ptBool(true),
			}),
			want: true,
		},
		{
			name:    "enable-rangeable refuses while the flag is unset",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable},
			class: classOf(&models.Property{
				Name: "p", DataType: schema.DataTypeInt.PropString(),
			}),
			want: false,
		},
		{
			name:    "refuses when the property is gone from the class",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable},
			class:   &models.Class{Class: "C"},
			want:    false,
		},
		{
			name:    "refuses on a nil class",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable},
			class:   nil,
			want:    false,
		},
		{
			name:    "refuses an unrecognized migration type",
			payload: ReindexTaskPayload{MigrationType: "some-future-strategy"},
			class:   classOf(textProp(models.PropertyTokenizationWord)),
			want:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want,
				mergedPromotionAgreesWithSchema(tc.payload, "p", tc.class))
		})
	}
}

// TestFinalize_MergedResidue_PartialSchemaFlip pins the per-property
// verdict: a crash mid-flip can commit some properties' schema but not
// others, and only the committed ones may promote.
func TestFinalize_MergedResidue_PartialSchemaFlip(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "enable_filterable_flipped_pending_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"),
		[]byte("flipped,pending"), 0o644))
	writeMigrationPayload(t, gen1, ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"flipped", "pending"},
	})

	for _, prop := range []string{"flipped", "pending"} {
		ingest := filepath.Join(lsmPath, "property_"+prop+"__enable_filterable_ingest_1")
		require.NoError(t, os.MkdirAll(ingest, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(ingest, "seg.db"), []byte(prop+"-NEW"), 0o644))
	}

	class := &models.Class{
		Class: "PartialFlip",
		Properties: []*models.Property{
			{
				Name: "flipped", DataType: schema.DataTypeText.PropString(),
				IndexFilterable: ptBool(true),
			},
			{
				Name: "pending", DataType: schema.DataTypeText.PropString(),
				IndexFilterable: ptBool(false),
			},
		},
	}

	logger, _ := test.NewNullLogger()
	require.NoError(t, FinalizeCompletedMigrations(lsmPath, class, logger))

	promoted, err := os.ReadFile(filepath.Join(lsmPath, "property_flipped", "seg.db"))
	require.NoError(t, err, "the property whose schema flag committed must be promoted")
	require.Equal(t, "flipped-NEW", string(promoted))

	require.NoDirExists(t, filepath.Join(lsmPath, "property_pending"),
		"the property whose schema flag never committed must not gain a canonical bucket")
	require.NoDirExists(t, filepath.Join(lsmPath, "property_pending__enable_filterable_ingest_1"),
		"the refused property's ingest sidecar must be discarded — it stops "+
			"receiving mirrored writes the moment the task dies")

	entries, _ := os.ReadDir(migsDir)
	require.Empty(t, entries, "the tracker must not survive a decided generation")
}

// TestFinalize_MergedResidue_RepairRangeableKeepsCanonicalData guards the
// deletion above: repair-rangeable rebuilds an EXISTING index, so its
// canonical bucket must survive even if the payload is misread.
func TestFinalize_MergedResidue_RepairRangeableKeepsCanonicalData(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "filterable_to_rangeable_score_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("score"), 0o644))
	writeMigrationPayload(t, gen1, ReindexTaskPayload{
		MigrationType: ReindexTypeRepairRangeable,
		Properties:    []string{"score"},
	})

	canonical := filepath.Join(lsmPath, helpers.BucketRangeableFromPropNameLSM("score"))
	require.NoError(t, os.MkdirAll(canonical, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(canonical, "seg.db"), []byte("pre-migration"), 0o644))

	ingest := filepath.Join(lsmPath, helpers.BucketRangeableFromPropNameLSM("score")+"__rangeable_ingest_1")
	require.NoError(t, os.MkdirAll(ingest, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(ingest, "seg.db"), []byte("rebuilt"), 0o644))

	class := &models.Class{
		Class:      "RangeRepair",
		Properties: []*models.Property{{Name: "score", DataType: schema.DataTypeInt.PropString()}},
	}

	logger, _ := test.NewNullLogger()
	require.NoError(t, FinalizeCompletedMigrations(lsmPath, class, logger))

	got, err := os.ReadFile(filepath.Join(canonical, "seg.db"))
	require.NoError(t, err, "repair-rangeable must never leave the property without a bucket")
	require.Equal(t, "rebuilt", string(got),
		"repair-rangeable is content-equivalent to its source and promotes regardless "+
			"of the IndexRangeFilters flag")
}

// TestFinalize_PendingPromotionWithoutCanonical_FailsLoudly pins that
// shard init must fail, not let initNonVector create an empty bucket,
// when a pending promotion's canonical dir is missing.
func TestFinalize_PendingPromotionWithoutCanonical_FailsLoudly(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "swapped.mig"))
	touchSentinel(t, filepath.Join(gen1, "tidied.mig"))
	// properties.mig as a directory: present but unreadable as a file,
	// the shape a torn write or operator surgery leaves behind.
	require.NoError(t, os.MkdirAll(filepath.Join(gen1, "properties.mig"), 0o755))

	require.NoError(t, os.MkdirAll(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))

	logger, _ := test.NewNullLogger()
	err := FinalizeCompletedMigrations(lsmPath, nil, logger)
	require.Error(t, err,
		"an unpromotable ingest dir whose canonical dir does not exist must fail shard "+
			"init, not leave initNonVector to create an empty bucket in its place")
	require.ErrorContains(t, err, "property_text_searchable__retokenize_ingest_1")
}

// TestFinalize_UnreadablePropertiesButCanonicalIntact_KeepsTracker pins
// the survivable half: with the canonical dir intact, finalize keeps the
// tracker for retry instead of failing the shard.
func TestFinalize_UnreadablePropertiesButCanonicalIntact_KeepsTracker(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "swapped.mig"))
	touchSentinel(t, filepath.Join(gen1, "tidied.mig"))
	require.NoError(t, os.MkdirAll(filepath.Join(gen1, "properties.mig"), 0o755))

	canonical := filepath.Join(lsmPath, "property_text_searchable")
	require.NoError(t, os.MkdirAll(canonical, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(canonical, "seg.db"), []byte("live"), 0o644))
	require.NoError(t, os.MkdirAll(
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1"), 0o755))

	logger, _ := test.NewNullLogger()
	require.NoError(t, FinalizeCompletedMigrations(lsmPath, nil, logger))

	require.DirExists(t, gen1,
		"a tracker that could not be finalized must survive so the next startup retries; "+
			"removing it orphans the sidecars for good")
	got, err := os.ReadFile(filepath.Join(canonical, "seg.db"))
	require.NoError(t, err)
	require.Equal(t, "live", string(got), "the canonical bucket must be untouched")
}

// TestFinalize_NothingToPromote_PreservesBackupAndFails pins the ordering
// contract inside finalizeMigrationDir: the backup dir holds the pre-swap
// data, so it is removed only once the canonical dir is in place.
//
// The torn state is a tidied tracker whose ingest dir is gone (a partial
// restore, a half-finished operator move) while the canonical dir has
// already been renamed aside by the swap. Removing the backup first and
// then finding nothing to promote deletes the last surviving copy and
// leaves initNonVector to create an empty bucket in its place.
func TestFinalize_NothingToPromote_PreservesBackupAndFails(t *testing.T) {
	lsmPath := t.TempDir()
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "swapped.mig"))
	touchSentinel(t, filepath.Join(gen1, "tidied.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))

	// Backup holds the pre-swap data. No ingest dir, no canonical dir.
	backup := filepath.Join(lsmPath, "property_text_searchable__retokenize_backup_1")
	require.NoError(t, os.MkdirAll(backup, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backup, "seg.db"), []byte("pre-swap"), 0o644))

	logger, _ := test.NewNullLogger()
	err := FinalizeCompletedMigrations(lsmPath, nil, logger)
	require.Error(t, err,
		"with nothing to promote and no canonical dir, finalize must fail the shard rather "+
			"than let initNonVector create an empty bucket")
	require.ErrorContains(t, err, "property_text_searchable")

	got, readErr := os.ReadFile(filepath.Join(backup, "seg.db"))
	require.NoError(t, readErr,
		"the backup dir is the last copy of this property's data and must survive the failure")
	require.Equal(t, "pre-swap", string(got))

	require.DirExists(t, gen1, "the tracker must survive so the next startup retries")
}

// TestFinalize_UnreadableMigrationsDir_FailsShard pins that finalize does
// not shrug off a migrations dir it cannot read. If a completed swap has
// already renamed the canonical dir aside, skipping the scan lets
// initNonVector create an empty bucket at that name while the real data
// waits in an un-promoted ingest dir.
//
// A plain file at the .migrations path reproduces the non-ENOENT read
// failure (ENOTDIR) deterministically, without depending on the test
// process's uid the way a chmod would.
func TestFinalize_UnreadableMigrationsDir_FailsShard(t *testing.T) {
	lsmPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(lsmPath, ".migrations"), []byte("not a dir"), 0o644))

	logger, _ := test.NewNullLogger()
	err := FinalizeCompletedMigrations(lsmPath, nil, logger)
	require.Error(t, err,
		"a migrations dir that cannot be read hides pending promotions, so startup must fail "+
			"rather than continue and risk an empty canonical bucket")
	require.ErrorContains(t, err, ".migrations")
}

// TestFinalize_NoMigrationsDir_IsNoOp keeps the common path honest: no
// .migrations dir at all is how the overwhelming majority of shards start,
// and it must stay a silent success.
func TestFinalize_NoMigrationsDir_IsNoOp(t *testing.T) {
	logger, _ := test.NewNullLogger()
	require.NoError(t, FinalizeCompletedMigrations(t.TempDir(), nil, logger))
}
