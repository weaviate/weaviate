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

package compressionhelpers

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// fakeCommitLogger records whether a PQ codebook was actually handed to the
// commit log, so a test can assert that an incomplete codebook is never written.
type fakeCommitLogger struct {
	pqCalled bool
}

func (f *fakeCommitLogger) AddPQCompression(compression.PQData) error   { f.pqCalled = true; return nil }
func (f *fakeCommitLogger) AddSQCompression(compression.SQData) error   { return nil }
func (f *fakeCommitLogger) AddRQCompression(compression.RQData) error   { return nil }
func (f *fakeCommitLogger) AddBRQCompression(compression.BRQData) error { return nil }

// pqCodebook builds m KMeans segment encoders with ks centroids of length
// subDim. When nilSeg/nilK are in range, that centroid is left nil to model an
// empty kmeans cluster / partially-fit codebook.
func pqCodebook(m, ks, subDim, nilSeg, nilK int) []compression.PQSegmentEncoder {
	encoders := make([]compression.PQSegmentEncoder, m)
	for i := range encoders {
		centers := make([][]float32, ks)
		for k := range centers {
			if i == nilSeg && k == nilK {
				continue // leave nil
			}
			centers[k] = make([]float32, subDim)
		}
		encoders[i] = NewKMeansEncoderWithCenters(ks, subDim, i, centers)
	}
	return encoders
}

// Test_ProductQuantizer_PersistCompression_RejectsIncompleteCodebook pins the
// write-side guard: the compress worker must never persist a codebook with a
// nil/short centroid, because restoring it would segfault the AVX distancer on
// the first Encode/search. PersistCompression returns an error and writes
// nothing, so compress() leaves the index uncompressed and retries.
func Test_ProductQuantizer_PersistCompression_RejectsIncompleteCodebook(t *testing.T) {
	const (
		dims   = 8
		m      = 4
		subDim = dims / m
		ks     = 16
	)

	cfg := ent.PQConfig{
		Enabled:   true,
		Segments:  m,
		Centroids: ks,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
	}

	t.Run("incomplete codebook is refused", func(t *testing.T) {
		pq, err := NewProductQuantizer(cfg, distancer.NewL2SquaredProvider(), dims, logrus.New())
		require.NoError(t, err)
		pq.kms = pqCodebook(m, ks, subDim, m-1, 5) // centroid 5 of last segment is nil

		logger := &fakeCommitLogger{}
		err = pq.PersistCompression(logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "incomplete PQ codebook")
		assert.False(t, logger.pqCalled, "an incomplete codebook must never reach the commit log")
	})

	t.Run("complete codebook is persisted", func(t *testing.T) {
		pq, err := NewProductQuantizer(cfg, distancer.NewL2SquaredProvider(), dims, logrus.New())
		require.NoError(t, err)
		pq.kms = pqCodebook(m, ks, subDim, -1, -1) // all centroids set

		logger := &fakeCommitLogger{}
		err = pq.PersistCompression(logger)
		require.NoError(t, err)
		assert.True(t, logger.pqCalled, "a complete codebook must be persisted")
	})
}
