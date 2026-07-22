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

package compressionhelpers_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// rq4CapturingCommitLogger captures the RQData written by PersistCompression
// so tests can feed it back through the restore path.
type rq4CapturingCommitLogger struct {
	rqData *compression.RQData
}

func (l *rq4CapturingCommitLogger) AddPQCompression(compression.PQData) error   { return nil }
func (l *rq4CapturingCommitLogger) AddSQCompression(compression.SQData) error   { return nil }
func (l *rq4CapturingCommitLogger) AddBRQCompression(compression.BRQData) error { return nil }

func (l *rq4CapturingCommitLogger) AddRQCompression(data compression.RQData) error {
	l.rqData = &data
	return nil
}

// A quantizer restored from the persisted commit-log data must be
// byte-identical to the original: same codes and same distance estimates.
// This is what guarantees that an index reloaded after a restart searches the
// codes it wrote before the restart.
func TestRQ4PersistRestoreRoundTrip(t *testing.T) {
	rng := newRNG(20260722)
	for _, d := range []int{2, 64, 100, 768, 1536} {
		t.Run(fmt.Sprintf("d%d", d), func(t *testing.T) {
			for _, m := range allMetrics() {
				rq := compressionhelpers.NewFourBitRotationalQuantizer(d, rng.Uint64(), m)

				logger := &rq4CapturingCommitLogger{}
				rq.PersistCompression(logger)
				require.NotNil(t, logger.rqData)
				data := logger.rqData
				assert.Equal(t, uint32(4), data.Bits)
				assert.Equal(t, uint32(d), data.InputDim)

				restored, err := compressionhelpers.RestoreFourBitRotationalQuantizer(
					int(data.InputDim), int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
					data.Rotation.Swaps, data.Rotation.Signs, m)
				require.NoError(t, err)

				q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
				cx := rq.Encode(x)
				assert.Equal(t, cx, restored.Encode(x), "restored quantizer must produce identical codes")

				want, err := rq.NewDistancer(q).Distance(cx)
				require.NoError(t, err)
				got, err := restored.NewDistancer(q).Distance(cx)
				require.NoError(t, err)
				assert.Equal(t, want, got, "restored quantizer must produce identical distances")
			}
		})
	}
}

// The RQ compressor factories must dispatch bits=4 to the 4-bit quantizer on
// both the fresh-creation and the restore path, for single and multi vectors,
// and reject unsupported bit widths.
func TestRQ4CompressorFactoryDispatch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dim := 128
	dist := distancer.NewCosineDistanceProvider()

	newCompressor := func(store *lsmkv.Store, bits int) (compressionhelpers.VectorCompressor, error) {
		return compressionhelpers.NewRQCompressor(dist, 1e6, logger, store, memwatch.NewDummyMonitor(),
			lsmkv.MakeNoopBucketOptions, bits, dim, "", nil)
	}

	t.Run("bits=4 creates a 4-bit compressor", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		defer store.Shutdown(context.Background())
		compressor, err := newCompressor(store, 4)
		require.NoError(t, err)
		defer compressor.Drop()

		stats := compressor.Stats()
		assert.Equal(t, "rq", stats.CompressionType())
		rq4Stats, ok := stats.(compressionhelpers.RQ4Stats)
		require.True(t, ok, "expected RQ4Stats, got %T", stats)
		assert.Equal(t, uint32(4), rq4Stats.Bits)

		// Persist and reload through the restore factory.
		commitLogger := &rq4CapturingCommitLogger{}
		compressor.PersistCompression(commitLogger)
		require.NotNil(t, commitLogger.rqData)
		data := commitLogger.rqData

		restoreStore := testinghelpers.NewDummyStore(t)
		defer restoreStore.Shutdown(context.Background())
		restoredCompressor, err := compressionhelpers.RestoreRQCompressor(dist, 1e6, logger,
			int(data.InputDim), int(data.Bits), int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
			data.Rotation.Swaps, data.Rotation.Signs, nil, restoreStore, memwatch.NewDummyMonitor(),
			lsmkv.MakeNoopBucketOptions, "", nil)
		require.NoError(t, err)
		defer restoredCompressor.Drop()
		restoredStats, ok := restoredCompressor.Stats().(compressionhelpers.RQ4Stats)
		require.True(t, ok, "expected RQ4Stats after restore, got %T", restoredCompressor.Stats())
		assert.Equal(t, uint32(4), restoredStats.Bits)
	})

	t.Run("bits=4 creates a 4-bit multi-vector compressor", func(t *testing.T) {
		store := testinghelpers.NewDummyStore(t)
		defer store.Shutdown(context.Background())
		compressor, err := compressionhelpers.NewRQMultiCompressor(dist, 1e6, logger, store,
			memwatch.NewDummyMonitor(), lsmkv.MakeNoopBucketOptions, 4, dim, "", nil)
		require.NoError(t, err)
		defer compressor.Drop()
		stats, ok := compressor.Stats().(compressionhelpers.RQ4Stats)
		require.True(t, ok, "expected RQ4Stats, got %T", compressor.Stats())
		assert.Equal(t, uint32(4), stats.Bits)

		commitLogger := &rq4CapturingCommitLogger{}
		compressor.PersistCompression(commitLogger)
		require.NotNil(t, commitLogger.rqData)
		data := commitLogger.rqData

		restoreStore := testinghelpers.NewDummyStore(t)
		defer restoreStore.Shutdown(context.Background())
		restoredCompressor, err := compressionhelpers.RestoreRQMultiCompressor(dist, 1e6, logger,
			int(data.InputDim), int(data.Bits), int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
			data.Rotation.Swaps, data.Rotation.Signs, nil, restoreStore, memwatch.NewDummyMonitor(),
			lsmkv.MakeNoopBucketOptions, "", nil)
		require.NoError(t, err)
		defer restoredCompressor.Drop()
		restoredStats, ok := restoredCompressor.Stats().(compressionhelpers.RQ4Stats)
		require.True(t, ok, "expected RQ4Stats after restore, got %T", restoredCompressor.Stats())
		assert.Equal(t, uint32(4), restoredStats.Bits)
	})

	t.Run("unsupported bit widths are rejected", func(t *testing.T) {
		for _, bits := range []int{0, 2, 3, 5, 16} {
			store := testinghelpers.NewDummyStore(t)
			_, err := newCompressor(store, bits)
			assert.Error(t, err, "bits=%d", bits)
			store.Shutdown(context.Background())
		}
	})
}
