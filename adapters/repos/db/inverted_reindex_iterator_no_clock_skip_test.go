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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestUuidObjectsIterator_AnalyzesEveryObjectRegardlessOfTimestamp pins that
// uuidObjectsIteratorAsync must not gate analysis on LastUpdateTimeUnix (a
// coordinator-clock stamp) vs. the locally-captured watermark, which
// misclassifies under multi-node clock skew (weaviate/weaviate#11692).
func TestUuidObjectsIterator_AnalyzesEveryObjectRegardlessOfTimestamp(t *testing.T) {
	ctx := testCtx()
	const propName = "title"

	className := "IterNoClockSkip_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	timestamps := map[string]int64{
		"far past":                    time.Now().Add(-24 * time.Hour).UnixMilli(),
		"now":                         time.Now().UnixMilli(),
		"future (coordinator skewed)": time.Now().Add(24 * time.Hour).UnixMilli(),
		"far future (extreme skew)":   time.Now().Add(365 * 24 * time.Hour).UnixMilli(),
	}
	idToLabel := map[string]string{}
	for label, ts := range timestamps {
		id := uuid.NewString()
		idToLabel[id] = label
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 strfmt.UUID(id),
				Class:              className,
				Properties:         map[string]interface{}{propName: "some searchable text"},
				CreationTimeUnix:   ts,
				LastUpdateTimeUnix: ts,
			},
		}))
	}

	// The production path flushes before creating the on-disk cursor; mirror
	// that so the writes above are visible to CursorOnDisk.
	require.NoError(t, shard.store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())

	propExtraction := storobj.NewPropExtraction()
	propExtraction.Add(propName)

	// breakCh protocol (see asyncReindex): primed with one false, then fed
	// one value per received migrationData.
	breakCh := make(chan bool, 1)
	breakCh <- false

	logger, _ := logrustest.NewNullLogger()
	_, mdCh := uuidObjectsIteratorAsync(logger, shard, nil, (&UuidKeyParser{}).FromBytes,
		propExtraction, breakCh, nil)

	analyzed := map[string]bool{}
	for md := range mdCh {
		if md == nil {
			break
		}
		require.NoError(t, md.err)
		if label, ok := idToLabel[md.key.String()]; ok {
			analyzed[label] = len(md.props) > 0
		}
		breakCh <- false
	}

	require.Len(t, analyzed, len(timestamps), "every written object must be scanned")
	for label, wasAnalyzed := range analyzed {
		assert.Truef(t, wasAnalyzed,
			"object with %s timestamp was scanned but NOT analyzed (empty props) — "+
				"the backfill iterator is making a wall-clock skip decision again; "+
				"under multi-node clock skew that silently loses unmirrored writes "+
				"(weaviate/weaviate#11692)", label)
	}
}
