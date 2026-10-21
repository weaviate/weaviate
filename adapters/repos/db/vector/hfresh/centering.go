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

package hfresh

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

const defaultCenteringTrainAt = 10_000

const defaultRQMaxDims = 512

// rqDims is how many leading dimensions the 1-bit codes encode: the vector
// width capped at HFRESH_RQ1_MAX_DIMS. Routing, splits and rescoring still
// use the full vector.
func (h *HFresh) rqDims(dims uint32) uint32 {
	if h.rqMaxDims > 0 && dims > h.rqMaxDims {
		return h.rqMaxDims
	}
	return dims
}

// rqDistanceProvider ranks capped cosine codes by l2 over the prefix: a
// prefix of a unit vector isn't unit-norm, so its dot product carries the
// prefix norm as noise, while l2 still tracks the full-vector distance.
func (h *HFresh) rqDistanceProvider(codeDims uint32) distancer.Provider {
	if h.rqMaxDims > 0 && codeDims == h.rqMaxDims && h.config.DistanceProvider.Type() == "cosine-dot" {
		return distancer.NewL2SquaredProvider()
	}
	return h.config.DistanceProvider
}

func rqMaxDimsFromEnv() uint32 {
	v := os.Getenv("HFRESH_RQ1_MAX_DIMS")
	if v == "" {
		return defaultRQMaxDims
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return defaultRQMaxDims
	}
	return uint32(max(n, 0))
}

func centeringTrainAtFromEnv() int64 {
	v := os.Getenv("HFRESH_RQ1_TRAIN_AT")
	if v == "" {
		return defaultCenteringTrainAt
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return defaultCenteringTrainAt
	}
	return n
}

// trainCentering computes the dataset mean from the stored full-precision
// vectors and re-encodes every posting with it. Experimental: no crash
// safety, a crash mid-way leaves postings encoded with mixed means.
func (h *HFresh) trainCentering(ctx context.Context) error {
	h.quantizerLock.Lock()
	defer h.quantizerLock.Unlock()

	start := time.Now()
	dims, _ := h.loadQuantizer()
	if dims == 0 {
		return errors.New("index not initialized")
	}

	raw := make(map[uint64][]float32)
	postings := make(map[uint64]Posting)
	for postingID := range h.PostingMap.Iter() {
		p, err := h.PostingStore.Get(ctx, postingID)
		if err != nil {
			return errors.Wrapf(err, "get posting %d", postingID)
		}
		p, err = p.GarbageCollect(h.VersionMap)
		if err != nil {
			return errors.Wrapf(err, "garbage collect posting %d", postingID)
		}
		if len(p) == 0 {
			continue
		}
		for _, v := range p {
			if _, ok := raw[v.ID()]; ok {
				continue
			}
			var vec []float32
			if h.muvera.Load() {
				vec, err = h.muveraEncoder.GetMuveraVectorForID(v.ID(), helpers.MuveraBucketName(h.id))
			} else {
				vec, err = h.config.VectorForIDThunk(ctx, v.ID())
			}
			if err != nil {
				return errors.Wrapf(err, "get vector %d", v.ID())
			}
			raw[v.ID()] = h.normalizeVec(vec)
		}
		postings[postingID] = p
	}
	if len(raw) == 0 {
		return errors.New("no vectors to train on")
	}

	qdims := h.rqDims(dims)
	sum := make([]float64, qdims)
	for _, vec := range raw {
		for i, x := range vec[:qdims] {
			sum[i] += float64(x)
		}
	}
	mean := make([]float32, qdims)
	for i := range mean {
		mean[i] = float32(sum[i] / float64(len(raw)))
	}

	quantizer, err := compressionhelpers.NewCenteredBinaryRotationalQuantizer(int(qdims), 42, mean, h.rqDistanceProvider(qdims))
	if err != nil {
		return errors.Wrap(err, "create centered quantizer")
	}

	for postingID, p := range postings {
		encoded := make(Posting, len(p))
		for i, v := range p {
			encoded[i] = NewVector(v.ID(), v.Version(), quantizer.CompressedBytes(quantizer.Encode(raw[v.ID()])))
		}
		if err := h.PostingStore.Put(ctx, postingID, encoded); err != nil {
			return errors.Wrapf(err, "put posting %d", postingID)
		}
		if err := h.setPostingVectorIDs(ctx, postingID, encoded); err != nil {
			return errors.Wrapf(err, "set vector ids for posting %d", postingID)
		}
	}

	h.quantizer = quantizer
	h.distancer = NewDistancer(quantizer, h.config.DistanceProvider)
	if err := h.persistQuantizationData(); err != nil {
		return errors.Wrap(err, "persist RQ data")
	}

	var meanNorm2 float64
	for _, m := range mean {
		meanNorm2 += float64(m) * float64(m)
	}
	h.logger.WithFields(map[string]any{
		"action":    "hfresh_train_centering",
		"vectors":   len(raw),
		"postings":  len(postings),
		"meanNorm2": meanNorm2,
		"took":      time.Since(start),
	}).Info("trained rq1 centering")
	return nil
}
