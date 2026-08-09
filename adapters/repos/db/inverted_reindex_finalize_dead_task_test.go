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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// Tests for the promotion scoping in FinalizeCompletedMigrations: a
// merged-but-untidied generation may only be promoted when the task
// that produced it is done and the schema reflects it. See
// https://github.com/weaviate/0-weaviate-issues/issues/464.

const (
	deadTaskID      = "task-under-test"
	deadTaskVersion = uint64(7)
)

// residueShape describes one migration's on-disk footprint: the
// namespace's tracker dir plus the bucket dirs it renames between.
type residueShape struct {
	dirName      string
	canonicalDir string
	ingestDir    string
	payload      ReindexTaskPayload
	// agreeing and disagreeing are the two schema states for the
	// property this migration targets.
	agreeing    *models.Property
	disagreeing *models.Property
}

func rangeableResidue() residueShape {
	yes, no := true, false
	return residueShape{
		dirName:      "filterable_to_rangeable_price_1",
		canonicalDir: "property_price_rangeable",
		ingestDir:    "property_price_rangeable__rangeable_ingest_1",
		payload: ReindexTaskPayload{
			MigrationType: ReindexTypeEnableRangeable,
			Collection:    "Products",
			Properties:    []string{"price"},
		},
		agreeing: &models.Property{
			Name: "price", DataType: []string{string(schema.DataTypeInt)},
			IndexRangeFilters: &yes,
		},
		disagreeing: &models.Property{
			Name: "price", DataType: []string{string(schema.DataTypeInt)},
			IndexRangeFilters: &no,
		},
	}
}

func tokenizationResidue() residueShape {
	return residueShape{
		dirName:      "searchable_retokenize_text_1",
		canonicalDir: "property_text_searchable",
		ingestDir:    "property_text_searchable__retokenize_ingest_1",
		payload: ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenization,
			Collection:         "Articles",
			Properties:         []string{"text"},
			TargetTokenization: models.PropertyTokenizationLowercase,
		},
		agreeing: &models.Property{
			Name: "text", DataType: []string{string(schema.DataTypeText)},
			Tokenization: models.PropertyTokenizationLowercase,
		},
		disagreeing: &models.Property{
			Name: "text", DataType: []string{string(schema.DataTypeText)},
			Tokenization: models.PropertyTokenizationWord,
		},
	}
}

// writeMergedResidue lays down the disk state a node has after its
// runtime swap died between markMerged and markTidied: a tracker with
// merged.mig and the task's identity, an ingest dir holding the
// migrated data, and the untouched canonical dir.
func writeMergedResidue(t *testing.T, shape residueShape, withPayload bool) string {
	t.Helper()
	lsmPath := t.TempDir()
	migDir := filepath.Join(lsmPath, ".migrations", shape.dirName)
	require.NoError(t, os.MkdirAll(migDir, 0o755))
	touchSentinel(t, filepath.Join(migDir, "started.mig"))
	touchSentinel(t, filepath.Join(migDir, "merged.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(migDir, "properties.mig"),
		[]byte(shape.payload.Properties[0]), 0o644))
	if withPayload {
		rec, err := json.Marshal(reindexRecoveryRecord{
			TaskID: deadTaskID, TaskVersion: deadTaskVersion,
			UnitID: "unit-0", Payload: shape.payload,
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(migDir, reindexRecoveryPayloadFile), rec, 0o644))
	}

	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, shape.ingestDir), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, shape.ingestDir, "segment.db"), []byte("migrated"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, shape.canonicalDir), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, shape.canonicalDir, "segment.db"), []byte("pre-migration"), 0o644))
	return lsmPath
}

func fixedLiveness(l ReindexTaskLiveness) ReindexTaskLivenessLookup {
	if l == ReindexTaskLivenessUnknown {
		return nil
	}
	return func(taskID string, taskVersion uint64) ReindexTaskLiveness {
		if taskID != deadTaskID || taskVersion != deadTaskVersion {
			return ReindexTaskLivenessUnknown
		}
		return l
	}
}

func classWith(prop *models.Property) *models.Class {
	return &models.Class{Class: "Test", Properties: []*models.Property{prop}}
}

// TestFinalize_MergedResiduePromotionScoping is the decision table:
// task liveness × whether the schema reflects the migration, for a
// rangeable-style and a tokenization-style residue.
func TestFinalize_MergedResiduePromotionScoping(t *testing.T) {
	type want struct {
		canonicalContent string
		ingestExists     bool
		trackerExists    bool
	}
	promoted := want{canonicalContent: "migrated", ingestExists: false, trackerExists: false}
	untouched := want{canonicalContent: "pre-migration", ingestExists: true, trackerExists: true}
	discarded := want{canonicalContent: "pre-migration", ingestExists: false, trackerExists: false}

	cases := []struct {
		name     string
		liveness ReindexTaskLiveness
		agrees   bool
		want     want
	}{
		{"live task, schema agrees", ReindexTaskLivenessLive, true, untouched},
		{"live task, schema disagrees", ReindexTaskLivenessLive, false, untouched},
		{"dead task, schema agrees", ReindexTaskLivenessDead, true, promoted},
		{"dead task, schema disagrees", ReindexTaskLivenessDead, false, discarded},
		{"unknown liveness, schema agrees", ReindexTaskLivenessUnknown, true, promoted},
		{"unknown liveness, schema disagrees", ReindexTaskLivenessUnknown, false, untouched},
	}

	shapes := map[string]residueShape{
		"rangeable":    rangeableResidue(),
		"tokenization": tokenizationResidue(),
	}

	for shapeName, shape := range shapes {
		for _, c := range cases {
			t.Run(shapeName+"/"+c.name, func(t *testing.T) {
				lsmPath := writeMergedResidue(t, shape, true)
				prop := shape.disagreeing
				if c.agrees {
					prop = shape.agreeing
				}
				logger, _ := test.NewNullLogger()

				FinalizeCompletedMigrations(lsmPath, classWith(prop), fixedLiveness(c.liveness), logger)

				got, err := os.ReadFile(filepath.Join(lsmPath, shape.canonicalDir, "segment.db"))
				require.NoError(t, err, "the canonical dir must always survive")
				require.Equal(t, c.want.canonicalContent, string(got))

				_, err = os.Stat(filepath.Join(lsmPath, shape.ingestDir))
				require.Equal(t, c.want.ingestExists, err == nil, "ingest dir presence")

				_, err = os.Stat(filepath.Join(lsmPath, ".migrations", shape.dirName))
				require.Equal(t, c.want.trackerExists, err == nil, "tracker dir presence")
			})
		}
	}
}

// TestFinalize_MergedResidueWithoutPayloadIsPromoted pins that a
// tracker written by a build that predates payload.mig keeps the old
// promote-unconditionally behavior: without a task identity there is no
// proof of death, and refusing would strand data no later startup can
// promote.
func TestFinalize_MergedResidueWithoutPayloadIsPromoted(t *testing.T) {
	shape := tokenizationResidue()
	lsmPath := writeMergedResidue(t, shape, false)
	logger, _ := test.NewNullLogger()

	FinalizeCompletedMigrations(lsmPath, classWith(shape.disagreeing),
		fixedLiveness(ReindexTaskLivenessDead), logger)

	got, err := os.ReadFile(filepath.Join(lsmPath, shape.canonicalDir, "segment.db"))
	require.NoError(t, err)
	require.Equal(t, "migrated", string(got))
}

// TestFinalize_RefusalKeepsABackupWithNoCanonical pins the one dir a
// refusal must never remove: a backup dir whose canonical name is
// absent is the only copy of that property's data (the swap renamed
// old-main away and died before renaming the ingest dir in).
func TestFinalize_RefusalKeepsABackupWithNoCanonical(t *testing.T) {
	shape := tokenizationResidue()
	lsmPath := writeMergedResidue(t, shape, true)
	require.NoError(t, os.RemoveAll(filepath.Join(lsmPath, shape.canonicalDir)))
	backupDir := filepath.Join(lsmPath, "property_text_searchable__retokenize_backup_1")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backupDir, "segment.db"), []byte("only-copy"), 0o644))
	logger, _ := test.NewNullLogger()

	FinalizeCompletedMigrations(lsmPath, classWith(shape.disagreeing),
		fixedLiveness(ReindexTaskLivenessDead), logger)

	got, err := os.ReadFile(filepath.Join(backupDir, "segment.db"))
	require.NoError(t, err, "a backup dir with no canonical beside it must survive a refusal")
	require.Equal(t, "only-copy", string(got))
}

// TestFinalize_RefusalLeavesATidiedGenAlone pins that refusing gen 2
// does not disturb an already-committed gen 1 swap in the same
// namespace: gen 1 still promotes.
func TestFinalize_RefusalLeavesATidiedGenAlone(t *testing.T) {
	shape := tokenizationResidue()
	lsmPath := writeMergedResidue(t, shape, true)
	// Rewrite the residue at gen 2 and put a tidied gen 1 beneath it.
	migsDir := filepath.Join(lsmPath, ".migrations")
	require.NoError(t, os.Rename(filepath.Join(migsDir, "searchable_retokenize_text_1"),
		filepath.Join(migsDir, "searchable_retokenize_text_2")))
	require.NoError(t, os.Rename(filepath.Join(lsmPath, shape.ingestDir),
		filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_2")))

	gen1 := filepath.Join(migsDir, "searchable_retokenize_text_1")
	require.NoError(t, os.MkdirAll(gen1, 0o755))
	touchSentinel(t, filepath.Join(gen1, "swapped.mig"))
	touchSentinel(t, filepath.Join(gen1, "tidied.mig"))
	require.NoError(t, os.WriteFile(filepath.Join(gen1, "properties.mig"), []byte("text"), 0o644))
	gen1Ingest := filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_1")
	require.NoError(t, os.MkdirAll(gen1Ingest, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(gen1Ingest, "segment.db"), []byte("gen-1"), 0o644))

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, classWith(shape.disagreeing),
		fixedLiveness(ReindexTaskLivenessDead), logger)

	got, err := os.ReadFile(filepath.Join(lsmPath, shape.canonicalDir, "segment.db"))
	require.NoError(t, err)
	require.Equal(t, "gen-1", string(got), "the committed gen-1 swap must still finalize")
	_, err = os.Stat(filepath.Join(lsmPath, "property_text_searchable__retokenize_ingest_2"))
	require.True(t, os.IsNotExist(err), "the refused gen-2 ingest dir must be discarded")
}

// TestMergedPromotionAgreesWithSchema covers the per-type agreement
// rule, including the multi-property case where one property lagging
// behind blocks the whole tracker.
func TestMergedPromotionAgreesWithSchema(t *testing.T) {
	yes, no := true, false
	intProp := func(name string, rangeable *bool) *models.Property {
		return &models.Property{
			Name: name, DataType: []string{string(schema.DataTypeInt)},
			IndexRangeFilters: rangeable,
		}
	}

	cases := []struct {
		name    string
		payload ReindexTaskPayload
		class   *models.Class
		want    bool
	}{
		{
			name:    "content-equivalent rewrite needs no schema proof",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeRepairFilterable, Properties: []string{"a"}},
			class:   &models.Class{},
			want:    true,
		},
		{
			name:    "unknown migration type is never confirmed",
			payload: ReindexTaskPayload{MigrationType: "invented-later", Properties: []string{"a"}},
			class:   classWith(intProp("a", &yes)),
			want:    false,
		},
		{
			name:    "enable-rangeable with the flag set",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable, Properties: []string{"a"}},
			class:   classWith(intProp("a", &yes)),
			want:    true,
		},
		{
			name:    "enable-rangeable with the flag unset",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable, Properties: []string{"a"}},
			class:   classWith(intProp("a", &no)),
			want:    false,
		},
		{
			name:    "one lagging property blocks the whole tracker",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable, Properties: []string{"a", "b"}},
			class: &models.Class{Properties: []*models.Property{
				intProp("a", &yes), intProp("b", &no),
			}},
			want: false,
		},
		{
			name:    "property deleted since the task was submitted",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable, Properties: []string{"gone"}},
			class:   classWith(intProp("a", &yes)),
			want:    false,
		},
		{
			name: "change-tokenization reaching the target",
			payload: ReindexTaskPayload{
				MigrationType: ReindexTypeChangeTokenization, Properties: []string{"a"},
				TargetTokenization: models.PropertyTokenizationLowercase,
			},
			class: classWith(&models.Property{
				Name: "a", DataType: []string{string(schema.DataTypeText)},
				Tokenization: models.PropertyTokenizationLowercase,
			}),
			want: true,
		},
		{
			name: "change-tokenization still on the old tokenization",
			payload: ReindexTaskPayload{
				MigrationType: ReindexTypeChangeTokenization, Properties: []string{"a"},
				TargetTokenization: models.PropertyTokenizationLowercase,
			},
			class: classWith(&models.Property{
				Name: "a", DataType: []string{string(schema.DataTypeText)},
				Tokenization: models.PropertyTokenizationWord,
			}),
			want: false,
		},
		{
			// A tracker that names no properties is the whole-collection
			// shape the repair-guidance path also produces. Nothing about
			// it is confirmed, so it must not promote.
			name:    "a tracker naming no properties confirms nothing",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable},
			class:   classWith(intProp("a", &yes)),
			want:    false,
		},
		{
			// A tracker that lost its target would agree with any
			// property whose tokenization is also empty unless the
			// target is checked for itself.
			name: "change-tokenization with no target tokenization",
			payload: ReindexTaskPayload{
				MigrationType: ReindexTypeChangeTokenization, Properties: []string{"a"},
			},
			class: classWith(&models.Property{
				Name: "a", DataType: []string{string(schema.DataTypeText)},
			}),
			want: false,
		},
		{
			name:    "no schema at all",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable, Properties: []string{"a"}},
			class:   nil,
			want:    false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, mergedPromotionAgreesWithSchema(c.payload, c.class))
		})
	}
}

// TestReindexTaskLivenessLookup_UnknownWithoutDeps pins that the
// liveness lookup answers "unknown" rather than "dead" before the
// distributed task list is reachable — the answer that keeps startup
// from deleting on a guess.
func TestReindexTaskLivenessLookup_UnknownWithoutDeps(t *testing.T) {
	var nilShard *Shard
	require.Equal(t, ReindexTaskLivenessUnknown,
		nilShard.reindexTaskLivenessLookup().Answer("t", 1))

	db := &DB{}
	require.Equal(t, ReindexTaskLivenessUnknown,
		db.reindexTaskLivenessLookup().Answer("t", 1))

	db.SetReindexAuditDeps(context.Background(), func(context.Context) (KnownReindexTaskLookup, error) {
		return func(taskID string, taskVersion uint64) bool { return taskID == "live" }, nil
	}, nil)
	lookup := db.reindexTaskLivenessLookup()
	require.Equal(t, ReindexTaskLivenessLive, lookup.Answer("live", 1))
	require.Equal(t, ReindexTaskLivenessDead, lookup.Answer("gone", 1))
	require.Equal(t, ReindexTaskLivenessUnknown, lookup.Answer("", 1))
}
