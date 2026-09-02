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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// readPersistedPropLengths loads the property length state a restart would
// recover: the tracker file's current on-disk bytes, opened through a fresh
// tracker over a copy of the file. The copy keeps NewJsonShardMetaData (which
// rewrites its path on load) away from the live shard's file.
func readPersistedPropLengths(t *testing.T, fileName, propName string) (sum, count int) {
	t.Helper()

	data, err := os.ReadFile(fileName)
	require.NoError(t, err)

	copyPath := filepath.Join(t.TempDir(), "proplengths_recovered")
	require.NoError(t, os.WriteFile(copyPath, data, 0o666))

	nullLogger, _ := logrustest.NewNullLogger()
	recovered, err := inverted.NewJsonShardMetaData(copyPath, nullLogger)
	require.NoError(t, err)
	defer recovered.Close()

	sum, count, _, err = recovered.PropertyTally(propName)
	require.NoError(t, err)
	return sum, count
}

// TestSingleObjectDelete_PropLengthTrackerNotFlushed pins
// https://github.com/weaviate/weaviate/issues/12891.
//
// deleteObject -> cleanupInvertedIndexOnDelete -> subtractPropLengths ->
// GetPropertyLengthTracker().UnTrackProperty mutates the tracker only in
// memory, outside the LSM store. The single-delete path then flushes the LSM
// WALs (store.WriteWALs) and the vector queue WALs, but never the tracker —
// unlike the batch-delete path, whose flushWALs (shard_write_batch_delete.go)
// does call GetPropertyLengthTracker().Flush(). A crash after an acknowledged
// single-object delete therefore recovers the pre-delete property lengths,
// skewing BM25 length normalization.
//
// The crash boundary is simulated honestly: after the delete returns, the test
// reads what is durable — the tracker file's on-disk state via a fresh tracker
// instance — without calling Flush/Shutdown on the live shard first.
func TestSingleObjectDelete_PropLengthTrackerNotFlushed(t *testing.T) {
	t.Skip("pins https://github.com/weaviate/weaviate/issues/12891 — single-object delete never flushes the PropertyLengthTracker; remove skip when fixing")

	ctx := testCtx()
	className := "PropLenSingleDelete_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	class := &models.Class{
		Class:               className,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{{
			Name:            "text",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWord,
			IndexFilterable: &vFalse,
			IndexSearchable: &vTrue,
		}},
	}

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })

	// distinct word counts per object; all words unique within an object so
	// len(prop.Items) equals the word count
	texts := []string{
		"alpha bravo",                    // 2 terms
		"alpha bravo charlie",            // 3 terms
		"alpha bravo charlie delta echo", // 5 terms
	}
	now := time.Now().UnixMilli()
	objs := make([]*storobj.Object, len(texts))
	for i, text := range texts {
		objs[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 strfmt.UUID(uuid.NewString()),
				Class:              className,
				Properties:         map[string]interface{}{"text": text},
				CreationTimeUnix:   now,
				LastUpdateTimeUnix: now,
			},
		}
		require.NoError(t, shard.PutObject(ctx, objs[i]))
	}

	tracker := shard.GetPropertyLengthTracker()

	// Batch imports flush the tracker (shard_write_batch_objects.go); flush
	// explicitly here so the pre-delete state is durably on disk, exactly as
	// after a completed batch import.
	require.NoError(t, tracker.Flush())

	sum, count, _, err := tracker.PropertyTally("text")
	require.NoError(t, err)
	require.Equal(t, 2+3+5, sum, "precondition: imports tracked")
	require.Equal(t, 3, count, "precondition: imports tracked")

	// single-object delete of the 3-term object
	require.NoError(t, shard.DeleteObject(ctx, objs[1].ID(), time.Now()))

	// the subtraction happened in memory ...
	sum, count, _, err = tracker.PropertyTally("text")
	require.NoError(t, err)
	require.Equal(t, 2+5, sum, "in-memory tally must reflect the delete")
	require.Equal(t, 2, count, "in-memory tally must reflect the delete")

	// ... but a crash right after the acknowledged delete recovers the
	// tracker from disk, where the subtraction must also have landed
	persistedSum, persistedCount := readPersistedPropLengths(t, tracker.FileName(), "text")
	assert.Equal(t, 2+5, persistedSum,
		"persisted prop length sum still holds the pre-delete value: the single-delete path never flushes the PropertyLengthTracker")
	assert.Equal(t, 2, persistedCount,
		"persisted prop length count still holds the pre-delete value: the single-delete path never flushes the PropertyLengthTracker")
}
