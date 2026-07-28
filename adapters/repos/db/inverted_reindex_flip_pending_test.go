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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// writeFlipTracker lays down a migration tracker dir with the given sentinels
// and property list, matching what a strategy writes at runtime.
func writeFlipTracker(t *testing.T, lsmPath, dirName, props string, sentinels ...string) string {
	t.Helper()
	migDir := filepath.Join(lsmPath, ".migrations", dirName)
	require.NoError(t, os.MkdirAll(migDir, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(migDir, "properties.mig"), []byte(props), 0o644))
	for _, s := range sentinels {
		require.NoError(t, os.WriteFile(filepath.Join(migDir, s), nil, 0o644))
	}
	return migDir
}

func TestPendingFlipsPersistence(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("round trip", func(t *testing.T) {
		lsmPath := t.TempDir()
		want := []PendingFlip{
			{Prop: "title", IndexType: "filterable"},
			{Prop: "body", IndexType: "searchable", Tokenization: models.PropertyTokenizationField},
		}
		require.NoError(t, writePendingFlips(lsmPath, want))
		require.Equal(t, want, readPendingFlips(lsmPath, logger))
	})

	t.Run("empty set removes the file", func(t *testing.T) {
		lsmPath := t.TempDir()
		require.NoError(t, writePendingFlips(lsmPath, []PendingFlip{{Prop: "title", IndexType: "filterable"}}))
		require.NoError(t, writePendingFlips(lsmPath, nil))
		require.NoFileExists(t, filepath.Join(lsmPath, ".migrations", pendingFlipFile))
		require.Nil(t, readPendingFlips(lsmPath, logger))
	})

	t.Run("no records writes no migrations dir", func(t *testing.T) {
		lsmPath := t.TempDir()
		require.NoError(t, writePendingFlips(lsmPath, nil))
		require.NoDirExists(t, filepath.Join(lsmPath, ".migrations"))
	})

	t.Run("malformed content degrades to no records", func(t *testing.T) {
		lsmPath := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, ".migrations"), 0o777))
		require.NoError(t, os.WriteFile(
			filepath.Join(lsmPath, ".migrations", pendingFlipFile), []byte("{not json"), 0o644))
		require.Nil(t, readPendingFlips(lsmPath, logger))
	})
}

// TestDropPendingFlipRecords pins that a retired record can't re-arm a later restart.
func TestDropPendingFlipRecords(t *testing.T) {
	logger, _ := test.NewNullLogger()
	seed := []PendingFlip{
		{Prop: "title", IndexType: "filterable"},
		{Prop: "title", IndexType: "searchable", Tokenization: models.PropertyTokenizationField},
		{Prop: "body", IndexType: "searchable"},
	}

	tests := []struct {
		name  string
		props []string
		want  []PendingFlip
	}{
		{
			name:  "drops every index type of the flipped property",
			props: []string{"title"},
			want:  []PendingFlip{{Prop: "body", IndexType: "searchable"}},
		},
		{
			name:  "multi-property flip drops all of them",
			props: []string{"title", "body"},
		},
		{
			name:  "unrelated property leaves the set untouched",
			props: []string{"other"},
			want:  seed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			require.NoError(t, writePendingFlips(lsmPath, seed))

			dropPendingFlipRecords(lsmPath, tc.props, logger)
			require.Equal(t, tc.want, readPendingFlips(lsmPath, logger))
		})
	}
}

// TestScanPendingFlips covers the pre-finalize view: marker plus unconsumed trackers.
func TestScanPendingFlips(t *testing.T) {
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name    string
		arrange func(t *testing.T, lsmPath string)
		want    []PendingFlip
	}{
		{
			name:    "nothing on disk",
			arrange: func(t *testing.T, lsmPath string) {},
			want:    nil,
		},
		{
			name: "tidied enable-filterable tracker",
			arrange: func(t *testing.T, lsmPath string) {
				writeFlipTracker(t, lsmPath, "enable_filterable_title_1", "title", "tidied.mig")
			},
			want: []PendingFlip{{Prop: "title", IndexType: "filterable"}},
		},
		{
			name: "merged-but-not-tidied enable-searchable tracker",
			arrange: func(t *testing.T, lsmPath string) {
				writeFlipTracker(t, lsmPath, "enable_searchable_body_1", "body", "merged.mig")
			},
			want: []PendingFlip{{Prop: "body", IndexType: "searchable"}},
		},
		{
			name: "multi-prop tracker yields one record per property",
			arrange: func(t *testing.T, lsmPath string) {
				writeFlipTracker(t, lsmPath, "enable_filterable_subtitle_title_1",
					"subtitle,title", "tidied.mig")
			},
			want: []PendingFlip{
				{Prop: "subtitle", IndexType: "filterable"},
				{Prop: "title", IndexType: "filterable"},
			},
		},
		{
			name: "tracker without a completion sentinel is still in flight",
			arrange: func(t *testing.T, lsmPath string) {
				writeFlipTracker(t, lsmPath, "enable_filterable_title_1", "title", "started.mig")
			},
			want: nil,
		},
		{
			name: "non-enable strategy has no swap-vs-flip window",
			arrange: func(t *testing.T, lsmPath string) {
				writeFlipTracker(t, lsmPath, "searchable_retokenize_title_1", "title", "tidied.mig")
			},
			want: nil,
		},
		{
			name: "tracker without a generation suffix is ignored",
			arrange: func(t *testing.T, lsmPath string) {
				writeFlipTracker(t, lsmPath, "enable_filterable_title", "title", "tidied.mig")
			},
			want: nil,
		},
		{
			name: "target tokenization comes from the recovery payload",
			arrange: func(t *testing.T, lsmPath string) {
				migDir := writeFlipTracker(t, lsmPath, "enable_searchable_body_1", "body", "tidied.mig")
				require.NoError(t, os.WriteFile(filepath.Join(migDir, reindexRecoveryPayloadFile),
					[]byte(`{"payload":{"targetTokenization":"field"}}`), 0o644))
			},
			want: []PendingFlip{
				{Prop: "body", IndexType: "searchable", Tokenization: models.PropertyTokenizationField},
			},
		},
		{
			name: "persisted marker survives a consumed tracker",
			arrange: func(t *testing.T, lsmPath string) {
				require.NoError(t, writePendingFlips(lsmPath,
					[]PendingFlip{{Prop: "title", IndexType: "filterable"}}))
			},
			want: []PendingFlip{{Prop: "title", IndexType: "filterable"}},
		},
		{
			name: "tracker wins over a stale marker for the same tuple",
			arrange: func(t *testing.T, lsmPath string) {
				require.NoError(t, writePendingFlips(lsmPath,
					[]PendingFlip{{Prop: "body", IndexType: "searchable"}}))
				migDir := writeFlipTracker(t, lsmPath, "enable_searchable_body_1", "body", "tidied.mig")
				require.NoError(t, os.WriteFile(filepath.Join(migDir, reindexRecoveryPayloadFile),
					[]byte(`{"payload":{"targetTokenization":"field"}}`), 0o644))
			},
			want: []PendingFlip{
				{Prop: "body", IndexType: "searchable", Tokenization: models.PropertyTokenizationField},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			tc.arrange(t, lsmPath)
			require.ElementsMatch(t, tc.want, scanPendingFlips(lsmPath, logger))
		})
	}
}

// TestLivePendingFlips pins retirement of a record without any explicit hook.
func TestLivePendingFlips(t *testing.T) {
	vTrue, vFalse := true, false
	textProp := func(name string, filterable, searchable *bool, tokenization string) *models.Property {
		return &models.Property{
			Name:            name,
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    tokenization,
			IndexFilterable: filterable,
			IndexSearchable: searchable,
		}
	}

	tests := []struct {
		name       string
		flip       PendingFlip
		prop       *models.Property
		withBucket bool
		wantKept   bool
	}{
		{
			name:       "filterable pending: flag still false and bucket present",
			flip:       PendingFlip{Prop: "title", IndexType: "filterable"},
			prop:       textProp("title", &vFalse, &vFalse, models.PropertyTokenizationWord),
			withBucket: true,
			wantKept:   true,
		},
		{
			name:       "filterable retires once the flip lands",
			flip:       PendingFlip{Prop: "title", IndexType: "filterable"},
			prop:       textProp("title", &vTrue, &vFalse, models.PropertyTokenizationWord),
			withBucket: true,
		},
		{
			name:       "filterable retires once the bucket is deleted",
			flip:       PendingFlip{Prop: "title", IndexType: "filterable"},
			prop:       textProp("title", &vFalse, &vFalse, models.PropertyTokenizationWord),
			withBucket: false,
		},
		{
			name:       "record for a property that left the schema retires",
			flip:       PendingFlip{Prop: "gone", IndexType: "filterable"},
			prop:       textProp("title", &vFalse, &vFalse, models.PropertyTokenizationWord),
			withBucket: true,
		},
		{
			name: "searchable pending while the tokenization has not flipped",
			flip: PendingFlip{
				Prop: "title", IndexType: "searchable",
				Tokenization: models.PropertyTokenizationField,
			},
			prop:       textProp("title", &vFalse, &vTrue, models.PropertyTokenizationWord),
			withBucket: true,
			wantKept:   true,
		},
		{
			name: "searchable retires once flag and tokenization both landed",
			flip: PendingFlip{
				Prop: "title", IndexType: "searchable",
				Tokenization: models.PropertyTokenizationField,
			},
			prop:       textProp("title", &vFalse, &vTrue, models.PropertyTokenizationField),
			withBucket: true,
		},
		{
			name:       "unknown index type retires",
			flip:       PendingFlip{Prop: "title", IndexType: "bogus"},
			prop:       textProp("title", &vFalse, &vFalse, models.PropertyTokenizationWord),
			withBucket: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			if tc.withBucket {
				bucketName, ok := mainBucketForPropertyIndex(tc.flip.Prop, tc.flip.IndexType)
				if ok {
					require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, bucketName), 0o777))
				}
			}
			class := &models.Class{Class: "Test", Properties: []*models.Property{tc.prop}}

			kept := livePendingFlips(lsmPath, []PendingFlip{tc.flip}, class)
			if tc.wantKept {
				require.Equal(t, []PendingFlip{tc.flip}, kept)
				return
			}
			require.Empty(t, kept)
		})
	}
}

// TestEnsureBucketsAreRemoved_PendingFlipSuppressed pins that a still-unflipped
// migration's bucket survives the nonexistent-index sweep.
func TestEnsureBucketsAreRemoved_PendingFlipSuppressed(t *testing.T) {
	vFalse := false

	tests := []struct {
		name       string
		indexType  string
		bucketName string
		prop       *models.Property
	}{
		{
			name:       "filterable",
			indexType:  "filterable",
			bucketName: "property_title",
			prop: &models.Property{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: &vFalse,
			},
		},
		{
			name:       "searchable",
			indexType:  "searchable",
			bucketName: "property_title_searchable",
			prop: &models.Property{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				IndexSearchable: &vFalse,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			indexPath := t.TempDir()
			const shardName = "shard1"
			bucketPath := filepath.Join(indexPath, shardName, "lsm", tc.bucketName)
			class := &models.Class{Class: "Test", Properties: []*models.Property{tc.prop}}
			helper := newPropertyDeleteIndexHelper()

			require.NoError(t, os.MkdirAll(bucketPath, 0o777))
			require.NoError(t, helper.ensureBucketsAreRemovedForNonExistentPropertyIndexes(
				indexPath, shardName, class,
				newPendingFlipLookup([]PendingFlip{{Prop: "title", IndexType: tc.indexType}})))
			require.DirExists(t, bucketPath, "a swapped-but-not-flipped bucket must survive")

			require.NoError(t, helper.ensureBucketsAreRemovedForNonExistentPropertyIndexes(
				indexPath, shardName, class, nil))
			require.NoDirExists(t, bucketPath, "without a pending flip the bucket is unused and goes")
		})
	}
}
