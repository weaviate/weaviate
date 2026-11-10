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

package postingedge

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// slotLayout orders the vectors by posting. A slot is the position of a
// vector in that order, so every posting owns a contiguous range of slots.
// Slots are how the index addresses vectors internally: an edge stores the
// slot of its endpoint, and the sidecar is indexed by slot.
type slotLayout struct {
	start  []uint32 // posting p owns the slots [start[p], start[p+1])
	order  []uint32 // slot -> position of the vector in the build input
	slotOf []uint32 // position in the build input -> slot
}

// newSlotLayout lays the vectors out by their primary posting, keeping the
// input order within a posting.
func newSlotLayout(primary []uint32, postings int) slotLayout {
	start := make([]uint32, postings+1)
	for _, p := range primary {
		start[p+1]++
	}
	for p := 0; p < postings; p++ {
		start[p+1] += start[p]
	}

	order := make([]uint32, len(primary))
	slotOf := make([]uint32, len(primary))
	next := make([]uint32, postings)
	copy(next, start[:postings])
	for i, p := range primary {
		slot := next[p]
		next[p]++
		order[slot] = uint32(i)
		slotOf[i] = slot
	}
	return slotLayout{start: start, order: order, slotOf: slotOf}
}

// postingStore holds everything a search reads when it loads a posting: the
// doc ids and codes of its members and the edges leaving them. It is the part
// a disk-resident version keeps on disk, one read per posting.
type postingStore struct {
	codeWords int
	start     []uint32 // posting p owns the slots [start[p], start[p+1])
	docIDs    []uint64 // by slot
	codes     []uint64 // by slot, codeWords each; the same array the sidecar serves
	edgeStart []uint32 // the edges of slot s are edges[edgeStart[s]:edgeStart[s+1]]
	edges     []uint32 // endpoint slots, nearest first per source
}

// posting is one loaded posting.
type posting struct {
	docIDs []uint64
	codes  []uint64 // len(docIDs) * codeWords
	edges  []uint32 // endpoint slots of every edge leaving the posting
}

func (s *postingStore) postings() int {
	return len(s.start) - 1
}

func (s *postingStore) vectors() int {
	return len(s.docIDs)
}

func (s *postingStore) load(p uint32) posting {
	from, to := s.start[p], s.start[p+1]
	return posting{
		docIDs: s.docIDs[from:to],
		codes:  s.codes[int(from)*s.codeWords : int(to)*s.codeWords],
		edges:  s.edges[s.edgeStart[from]:s.edgeStart[to]],
	}
}

// edgesOf returns the endpoint slots of the edges leaving one slot.
func (s *postingStore) edgesOf(slot uint32) []uint32 {
	return s.edges[s.edgeStart[slot]:s.edgeStart[slot+1]]
}

// sidecar is the memory-resident part of the index: the code of every vector
// and the posting it lives in, both by slot. It is what lets a search score
// the endpoint of an edge, and know which posting to load for it, without
// touching the endpoint's posting.
//
// In this version the sidecar and the posting store share one code array. A
// disk-resident version stores the codes twice: inline in the postings on
// disk and here in memory.
type sidecar struct {
	codeWords int
	codes     []uint64 // by slot, codeWords each
	postingOf []uint32 // by slot
}

func (s *sidecar) code(slot uint32) []uint64 {
	from := int(slot) * s.codeWords
	return s.codes[from : from+s.codeWords : from+s.codeWords]
}

// wordsPerCacheLine is the stride touch reads at: one word per 64 byte
// cache line, which also covers the wider lines of other CPUs.
const wordsPerCacheLine = 8

// touch reads one word of every cache line of the codes of the given slots
// and returns a digest of what it read, which callers only keep so that the
// reads are not optimized away.
//
// The endpoints a search scores are scattered over the whole sidecar, which
// is far larger than the CPU caches. Scoring them one after the other takes
// one cache miss at a time, each queued behind the bit counting of the code
// before it. These cheap, independent reads let the misses overlap instead:
// on a 1M x 1536d sidecar they cut the cost of scoring an endpoint to a third.
func (s *sidecar) touch(slots []uint32) uint64 {
	var digest uint64
	last := s.codeWords - 1
	for _, slot := range slots {
		code := s.codes[int(slot)*s.codeWords : (int(slot)+1)*s.codeWords]
		for w := 0; w < last; w += wordsPerCacheLine {
			digest ^= code[w]
		}
		digest ^= code[last]
	}
	return digest
}

// trainQuantizer fits the centered 1-bit quantizer: the mean is taken over an
// evenly spread sample of at most limit vectors, like the deferred training of
// centered RQ in HNSW.
func trainQuantizer(vectors [][]float32, dims, limit int, cfg *Config,
) (*compressionhelpers.CenteredBinaryRotationalQuantizer, error) {
	sample := vectors
	if len(vectors) > limit {
		sample = make([][]float32, limit)
		for i := range sample {
			sample[i] = vectors[int(int64(i)*int64(len(vectors))/int64(limit))]
		}
	}
	mean := compressionhelpers.MeanVector(sample, dims)
	quantizer, err := compressionhelpers.NewCenteredBinaryRotationalQuantizer(
		dims, compressionhelpers.DefaultFastRotationSeed, mean, cfg.DistanceProvider)
	if err != nil {
		return nil, errors.Wrap(err, "create centered rq1 quantizer")
	}
	return quantizer, nil
}

// encode writes the code of every vector into one array ordered by slot.
func encode(ctx context.Context, logger logrus.FieldLogger, vectors [][]float32, layout slotLayout,
	quantizer *compressionhelpers.CenteredBinaryRotationalQuantizer,
) ([]uint64, int, error) {
	n := len(vectors)
	codeWords := len(quantizer.Encode(vectors[layout.order[0]]))
	codes := make([]uint64, n*codeWords)

	err := parallelChunks(ctx, logger, n, 1024, func(_, start, end int) error {
		for slot := start; slot < end; slot++ {
			code := quantizer.Encode(vectors[layout.order[slot]])
			if len(code) != codeWords {
				return errors.Errorf("code of slot %d has %d words, expected %d", slot, len(code), codeWords)
			}
			copy(codes[slot*codeWords:], code)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return codes, codeWords, nil
}

// postingOfSlots expands the posting ranges into a posting id per slot.
func postingOfSlots(start []uint32) []uint32 {
	out := make([]uint32, start[len(start)-1])
	for p := 0; p+1 < len(start); p++ {
		for slot := start[p]; slot < start[p+1]; slot++ {
			out[slot] = uint32(p)
		}
	}
	return out
}

// maxVectors bounds the number of vectors of one index: slots and edge
// offsets are 32 bits wide.
const maxVectors = math.MaxUint32 - 1
