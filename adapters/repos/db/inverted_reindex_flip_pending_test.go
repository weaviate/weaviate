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

// corruptPendingFlipMarker overwrites the marker with content no reader can
// turn into records, the state [readPendingFlips] reports as unreadable.
func corruptPendingFlipMarker(t *testing.T, lsmPath string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, ".migrations"), 0o777))
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, ".migrations", pendingFlipFile), []byte("{not json"), 0o644))
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
		got, unreadable := readPendingFlips(lsmPath, logger)
		require.Equal(t, want, got)
		require.False(t, unreadable)
	})

	t.Run("empty set removes the file", func(t *testing.T) {
		lsmPath := t.TempDir()
		require.NoError(t, writePendingFlips(lsmPath, []PendingFlip{{Prop: "title", IndexType: "filterable"}}))
		require.NoError(t, writePendingFlips(lsmPath, nil))
		require.NoFileExists(t, filepath.Join(lsmPath, ".migrations", pendingFlipFile))
		got, unreadable := readPendingFlips(lsmPath, logger)
		require.Nil(t, got)
		require.False(t, unreadable, "an absent marker proves there is no pending flip")
	})

	t.Run("no records writes no migrations dir", func(t *testing.T) {
		lsmPath := t.TempDir()
		require.NoError(t, writePendingFlips(lsmPath, nil))
		require.NoDirExists(t, filepath.Join(lsmPath, ".migrations"))
	})

	t.Run("malformed content reports unreadable", func(t *testing.T) {
		lsmPath := t.TempDir()
		corruptPendingFlipMarker(t, lsmPath)
		got, unreadable := readPendingFlips(lsmPath, logger)
		require.Nil(t, got)
		require.True(t, unreadable,
			"a present-but-unparseable marker must not read as 'nothing is pending'")

		scanned, scanUnreadable := scanPendingFlips(lsmPath, logger)
		require.Nil(t, scanned)
		require.True(t, scanUnreadable, "scanPendingFlips must forward the flag")
	})

	t.Run("unreadable marker survives a drop", func(t *testing.T) {
		lsmPath := t.TempDir()
		corruptPendingFlipMarker(t, lsmPath)
		dropPendingFlipRecords(lsmPath, []string{"title"}, "filterable", logger)
		_, unreadable := readPendingFlips(lsmPath, logger)
		require.True(t, unreadable, "a drop must not rewrite records it could not read")
	})
}

// TestDropPendingFlipRecords pins that a retired record can't re-arm a later
// restart, and that the drop stays inside the flipping migration's own index
// type — the other one's window is still open.
func TestDropPendingFlipRecords(t *testing.T) {
	logger, _ := test.NewNullLogger()
	titleSearchable := PendingFlip{
		Prop: "title", IndexType: "searchable", Tokenization: models.PropertyTokenizationField,
	}
	seed := []PendingFlip{
		{Prop: "title", IndexType: "filterable"},
		titleSearchable,
		{Prop: "body", IndexType: "searchable"},
	}

	tests := []struct {
		name      string
		props     []string
		indexType string
		want      []PendingFlip
	}{
		{
			name:      "keeps the property's other index type",
			props:     []string{"title"},
			indexType: "filterable",
			want:      []PendingFlip{titleSearchable, {Prop: "body", IndexType: "searchable"}},
		},
		{
			name:      "drops only the flipped index type",
			props:     []string{"title"},
			indexType: "searchable",
			want: []PendingFlip{
				{Prop: "title", IndexType: "filterable"},
				{Prop: "body", IndexType: "searchable"},
			},
		},
		{
			name:      "multi-property flip drops each of them for that type",
			props:     []string{"title", "body"},
			indexType: "searchable",
			want:      []PendingFlip{{Prop: "title", IndexType: "filterable"}},
		},
		{
			name:      "unrelated property leaves the set untouched",
			props:     []string{"other"},
			indexType: "filterable",
			want:      seed,
		},
		{
			name:      "unrelated index type leaves the set untouched",
			props:     []string{"title", "body"},
			indexType: "rangeable",
			want:      seed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			require.NoError(t, writePendingFlips(lsmPath, seed))

			dropPendingFlipRecords(lsmPath, tc.props, tc.indexType, logger)
			got, unreadable := readPendingFlips(lsmPath, logger)
			require.False(t, unreadable)
			require.Equal(t, tc.want, got)
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
			got, unreadable := scanPendingFlips(lsmPath, logger)
			require.False(t, unreadable)
			require.ElementsMatch(t, tc.want, got)
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

// TestLivePendingFlips_PerRecordRetirement pins that retirement is decided per
// record. One property can hold a filterable and a searchable record at once,
// and the schema catching up with one must leave the other armed.
func TestLivePendingFlips_PerRecordRetirement(t *testing.T) {
	vTrue, vFalse := true, false
	filterable := PendingFlip{Prop: "title", IndexType: "filterable"}
	searchable := PendingFlip{
		Prop: "title", IndexType: "searchable",
		Tokenization: models.PropertyTokenizationField,
	}

	tests := []struct {
		name         string
		filterableOn *bool
		searchableOn *bool
		tokenization string
		want         []PendingFlip
	}{
		{
			name:         "neither flip landed: both records survive",
			filterableOn: &vFalse,
			searchableOn: &vFalse,
			tokenization: models.PropertyTokenizationWord,
			want:         []PendingFlip{filterable, searchable},
		},
		{
			name:         "searchable landed: filterable survives alone",
			filterableOn: &vFalse,
			searchableOn: &vTrue,
			tokenization: models.PropertyTokenizationField,
			want:         []PendingFlip{filterable},
		},
		{
			name:         "filterable landed: searchable survives alone",
			filterableOn: &vTrue,
			searchableOn: &vFalse,
			tokenization: models.PropertyTokenizationWord,
			want:         []PendingFlip{searchable},
		},
		{
			name:         "both landed: nothing survives",
			filterableOn: &vTrue,
			searchableOn: &vTrue,
			tokenization: models.PropertyTokenizationField,
			want:         []PendingFlip{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			for _, flip := range []PendingFlip{filterable, searchable} {
				bucketName, ok := mainBucketForPropertyIndex(flip.Prop, flip.IndexType)
				require.True(t, ok)
				require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, bucketName), 0o777))
			}
			class := &models.Class{Class: "Test", Properties: []*models.Property{{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    tc.tokenization,
				IndexFilterable: tc.filterableOn,
				IndexSearchable: tc.searchableOn,
			}}}

			kept := livePendingFlips(lsmPath, []PendingFlip{filterable, searchable}, class)
			require.Equal(t, tc.want, kept)
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
				newPendingFlipShield([]PendingFlip{{Prop: "title", IndexType: tc.indexType}})))
			require.DirExists(t, bucketPath, "a swapped-but-not-flipped bucket must survive")

			require.NoError(t, helper.ensureBucketsAreRemovedForNonExistentPropertyIndexes(
				indexPath, shardName, class, newPendingFlipShield(nil)))
			require.NoDirExists(t, bucketPath, "without a pending flip the bucket is unused and goes")
		})
	}
}

// The zero-value shield is what a caller that never ran scanPendingFlips holds.
// It has to read as "everything is protected": the sweep deletes the only copy
// of a swapped-but-not-flipped migration's data, so "I did not look" must never
// be indistinguishable from "I looked and found none"
// (weaviate/0-weaviate-issues#438).
func TestPendingFlipShield_ZeroValueProtectsEverything(t *testing.T) {
	var unscanned pendingFlipShield
	require.True(t, unscanned.protects("title", "filterable"))
	require.True(t, unscanned.protects("anything", "searchable"))

	scannedEmpty := newPendingFlipShield(nil)
	require.False(t, scannedEmpty.protects("title", "filterable"),
		"a completed scan that found nothing must let the sweep run, or the shield would pin every bucket forever")

	scanned := newPendingFlipShield([]PendingFlip{{Prop: "title", IndexType: "filterable"}})
	require.True(t, scanned.protects("title", "filterable"))
	require.False(t, scanned.protects("title", "searchable"), "the shield is keyed on the tuple, not the property")
}
