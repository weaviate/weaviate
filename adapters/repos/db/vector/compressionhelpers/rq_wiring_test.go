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

// rqCapturingCommitLogger captures the RQData written by PersistCompression
// so tests can feed it back through the restore path.
type rqCapturingCommitLogger struct {
	rqData *compression.RQData
}

func (l *rqCapturingCommitLogger) AddPQCompression(compression.PQData) error   { return nil }
func (l *rqCapturingCommitLogger) AddSQCompression(compression.SQData) error   { return nil }
func (l *rqCapturingCommitLogger) AddBRQCompression(compression.BRQData) error { return nil }

func (l *rqCapturingCommitLogger) AddRQCompression(data compression.RQData) error {
	l.rqData = &data
	return nil
}

// rqQuantizer adapts the 4-bit and 8-bit rotational quantizers to a common
// shape so the persist/restore round-trip test can run over both.
type rqQuantizer struct {
	encode   func([]float32) []byte
	distance func(q []float32, code []byte) (float32, error)
	persist  func(compressionhelpers.CommitLogger)
}

// A quantizer restored from the persisted commit-log data must be
// byte-identical to the original: same codes and same distance estimates.
// This is what guarantees that an index reloaded after a restart searches the
// codes it wrote before the restart. Covers both RQ bit widths.
func TestRQPersistRestoreRoundTrip(t *testing.T) {
	wrap8 := func(rq *compressionhelpers.RotationalQuantizer) rqQuantizer {
		return rqQuantizer{
			encode: rq.Encode,
			distance: func(q []float32, code []byte) (float32, error) {
				return rq.NewDistancer(q).Distance(code)
			},
			persist: rq.PersistCompression,
		}
	}
	wrap4 := func(rq *compressionhelpers.FourBitRotationalQuantizer) rqQuantizer {
		return rqQuantizer{
			encode: rq.Encode,
			distance: func(q []float32, code []byte) (float32, error) {
				return rq.NewDistancer(q).Distance(code)
			},
			persist: rq.PersistCompression,
		}
	}

	cases := []struct {
		name    string
		bits    uint32
		build   func(d int, seed uint64, m distancer.Provider) rqQuantizer
		restore func(data *compression.RQData, m distancer.Provider) (rqQuantizer, error)
	}{
		{
			name: "rq8",
			bits: 8,
			build: func(d int, seed uint64, m distancer.Provider) rqQuantizer {
				return wrap8(compressionhelpers.NewRotationalQuantizer(d, seed, 8, m))
			},
			restore: func(data *compression.RQData, m distancer.Provider) (rqQuantizer, error) {
				rq, err := compressionhelpers.RestoreRotationalQuantizer(
					int(data.InputDim), int(data.Bits), int(data.Rotation.OutputDim),
					int(data.Rotation.Rounds), data.Rotation.Swaps, data.Rotation.Signs, m)
				if err != nil {
					return rqQuantizer{}, err
				}
				return wrap8(rq), nil
			},
		},
		{
			name: "rq4",
			bits: 4,
			build: func(d int, seed uint64, m distancer.Provider) rqQuantizer {
				return wrap4(compressionhelpers.NewFourBitRotationalQuantizer(d, seed, m))
			},
			restore: func(data *compression.RQData, m distancer.Provider) (rqQuantizer, error) {
				rq, err := compressionhelpers.RestoreFourBitRotationalQuantizer(
					int(data.InputDim), int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
					data.Rotation.Swaps, data.Rotation.Signs, data.Mean, m)
				if err != nil {
					return rqQuantizer{}, err
				}
				return wrap4(rq), nil
			},
		},
	}

	rng := newRNG(20260722)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, d := range []int{2, 64, 100, 768, 1536} {
				t.Run(fmt.Sprintf("d%d", d), func(t *testing.T) {
					for _, m := range allMetrics() {
						rq := tc.build(d, rng.Uint64(), m)

						logger := &rqCapturingCommitLogger{}
						rq.persist(logger)
						require.NotNil(t, logger.rqData)
						data := logger.rqData
						assert.Equal(t, tc.bits, data.Bits)
						assert.Equal(t, uint32(d), data.InputDim)

						restored, err := tc.restore(data, m)
						require.NoError(t, err)

						q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
						cx := rq.encode(x)
						assert.Equal(t, cx, restored.encode(x), "restored quantizer must produce identical codes")

						want, err := rq.distance(q, cx)
						require.NoError(t, err)
						got, err := restored.distance(q, cx)
						require.NoError(t, err)
						assert.Equal(t, want, got, "restored quantizer must produce identical distances")
					}
				})
			}
		})
	}
}

// The RQ compressor factories must dispatch bits=8 and bits=4 to the matching
// quantizer on both the fresh-creation and the restore path, for single and
// multi vectors, and reject unsupported bit widths.
func TestRQCompressorFactoryDispatch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dim := 128
	dist := distancer.NewCosineDistanceProvider()

	newCompressor := func(store *lsmkv.Store, bits int) (compressionhelpers.VectorCompressor, error) {
		return compressionhelpers.NewRQCompressor(dist, 1e6, logger, store, memwatch.NewDummyMonitor(),
			lsmkv.MakeNoopBucketOptions, bits, dim, "", nil)
	}

	// statsBits extracts the bit width from the stats type the given
	// compressor reports, asserting the type matches the expected width.
	statsBits := func(t *testing.T, c compressionhelpers.VectorCompressor, bits int) uint32 {
		t.Helper()
		stats := c.Stats()
		assert.Equal(t, "rq", stats.CompressionType())
		switch bits {
		case 8:
			s, ok := stats.(compressionhelpers.RQStats)
			require.True(t, ok, "expected RQStats, got %T", stats)
			return s.Bits
		case 4:
			s, ok := stats.(compressionhelpers.RQ4Stats)
			require.True(t, ok, "expected RQ4Stats, got %T", stats)
			return s.Bits
		default:
			t.Fatalf("unexpected bits %d", bits)
			return 0
		}
	}

	for _, bits := range []int{8, 4} {
		t.Run(fmt.Sprintf("bits=%d single-vector compressor", bits), func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			defer store.Shutdown(context.Background())
			compressor, err := newCompressor(store, bits)
			require.NoError(t, err)
			defer compressor.Drop()
			assert.Equal(t, uint32(bits), statsBits(t, compressor, bits))

			// Persist and reload through the restore factory.
			commitLogger := &rqCapturingCommitLogger{}
			compressor.PersistCompression(commitLogger)
			require.NotNil(t, commitLogger.rqData)
			data := commitLogger.rqData

			restoreStore := testinghelpers.NewDummyStore(t)
			defer restoreStore.Shutdown(context.Background())
			restoredCompressor, err := compressionhelpers.RestoreRQCompressor(dist, 1e6, logger,
				int(data.InputDim), int(data.Bits), int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
				data.Rotation.Swaps, data.Rotation.Signs, nil, data.Mean, restoreStore,
				memwatch.NewDummyMonitor(), lsmkv.MakeNoopBucketOptions, "", nil)
			require.NoError(t, err)
			defer restoredCompressor.Drop()
			assert.Equal(t, uint32(bits), statsBits(t, restoredCompressor, bits))
		})

		t.Run(fmt.Sprintf("bits=%d multi-vector compressor", bits), func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			defer store.Shutdown(context.Background())
			compressor, err := compressionhelpers.NewRQMultiCompressor(dist, 1e6, logger, store,
				memwatch.NewDummyMonitor(), lsmkv.MakeNoopBucketOptions, bits, dim, "", nil)
			require.NoError(t, err)
			defer compressor.Drop()
			assert.Equal(t, uint32(bits), statsBits(t, compressor, bits))

			commitLogger := &rqCapturingCommitLogger{}
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
			assert.Equal(t, uint32(bits), statsBits(t, restoredCompressor, bits))
		})
	}

	t.Run("unsupported bit widths are rejected", func(t *testing.T) {
		for _, bits := range []int{0, 2, 3, 5, 16} {
			store := testinghelpers.NewDummyStore(t)
			_, err := newCompressor(store, bits)
			assert.Error(t, err, "bits=%d", bits)
			store.Shutdown(context.Background())
		}
	})
}
