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
	"runtime"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
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
	first  uint32 // slot of the first member, the others follow
	docIDs []uint64
	codes  []uint64 // len(docIDs) * codeWords
	// edges holds the endpoint slots of every edge leaving the posting,
	// member after member. edgeStart has one offset per member and a last
	// one: the edges of member i are edges[edgeStart[i]-edgeStart[0] :
	// edgeStart[i+1]-edgeStart[0]].
	edges     []uint32
	edgeStart []uint32
}

// edgesOf returns the endpoint slots of the edges leaving member i.
func (p *posting) edgesOf(i int) []uint32 {
	base := p.edgeStart[0]
	return p.edges[p.edgeStart[i]-base : p.edgeStart[i+1]-base]
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
		first:  from,
		docIDs: s.docIDs[from:to],
		codes:  s.codes[int(from)*s.codeWords : int(to)*s.codeWords],
		edges:  s.edges[s.edgeStart[from]:s.edgeStart[to]],
		// the offsets of the members and the one that closes the last
		edgeStart: s.edgeStart[from : to+1],
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

// scoreAhead is how many codes ahead of the one being scored the sidecar asks
// the CPU to fetch. It follows the batch distancer of the compressors
// (compressionhelpers, DistancesToNodes): hints issued a few codes ahead,
// inside the loop, rather than for the whole batch up front, which would ask
// for more lines than a core keeps in flight. 8 measured best for 25-word
// codes, with 4 and 16 within 15% of it.
const scoreAhead = 8

// codeEstimator estimates the distance from a query to a stored code.
type codeEstimator interface {
	Distance(code []uint64) (float32, error)
}

// score estimates the distance from the query to the vector of every given
// slot, into dists. The values are exactly those of scoring the slots one by
// one; what the batch buys is time.
//
// The endpoints a search scores are scattered over the whole sidecar, which
// is far larger than the CPU caches. Scored one by one, each code is a cache
// miss that waits behind the bit counting of the code before it. Fetching
// ahead lets the misses overlap: BenchmarkSidecarScore measures 49 ns per
// 1536d code against 147 ns one by one, close to the 42 ns of a code that is
// already cached.
func (s *sidecar) score(estimator codeEstimator, slots []uint32, dists []float32) error {
	for i := range min(scoreAhead, len(slots)) {
		s.prefetch(slots[i])
	}
	for i, slot := range slots {
		if next := i + scoreAhead; next < len(slots) {
			s.prefetch(slots[next])
		}
		dist, err := estimator.Distance(s.code(slot))
		if err != nil {
			return errors.Wrapf(err, "estimate distance to slot %d", slot)
		}
		dists[i] = dist
	}
	runtime.KeepAlive(s.codes)
	return nil
}

// prefetch hints the CPU to load the code of a slot.
func (s *sidecar) prefetch(slot uint32) {
	asm.PrefetchN(uintptr(unsafe.Pointer(&s.codes[int(slot)*s.codeWords])), s.codeWords*8)
}

// codec turns vectors into codes, and a query into an estimator of its
// distance to a code. One codec serves the whole index: the posting scan, the
// ranking of edge endpoints and the edge builder all see the same codes.
type codec interface {
	Encode(vector []float32) []uint64
	NewEstimator(query []float32) codeEstimator
	Stats() compressionhelpers.CompressionStats
}

// fullCodec is the centered 1-bit quantizer as it is: one sign bit per
// rotated dimension.
type fullCodec struct {
	*compressionhelpers.CenteredBinaryRotationalQuantizer
}

func (c fullCodec) NewEstimator(query []float32) codeEstimator {
	return c.NewDistancer(query)
}

// cappedCodec keeps only the first sign bits of every code. The estimates
// stay unbiased and get noisier; the codes get smaller.
type cappedCodec struct {
	capped *compressionhelpers.CappedCenteredBinaryRQ
	stats  compressionhelpers.CompressionStats
}

func (c cappedCodec) Encode(vector []float32) []uint64 {
	return c.capped.Encode(vector)
}

func (c cappedCodec) NewEstimator(query []float32) codeEstimator {
	return c.capped.NewDistancer(query)
}

func (c cappedCodec) Stats() compressionhelpers.CompressionStats {
	return c.stats
}

// trainQuantizer fits the centered 1-bit quantizer: the mean is taken over an
// evenly spread sample of at most limit vectors, like the deferred training of
// centered RQ in HNSW. codeBits caps the sign bits kept per code, 0 keeps
// them all.
func trainQuantizer(vectors [][]float32, dims, limit, codeBits int, cfg *Config) (codec, error) {
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
	if codeBits == 0 {
		return fullCodec{quantizer}, nil
	}
	capped, err := quantizer.Capped(codeBits)
	if err != nil {
		return nil, errors.Wrap(err, "cap the codes")
	}
	return cappedCodec{capped: capped, stats: quantizer.Stats()}, nil
}

// encode writes the code of every vector into one array ordered by slot.
func encode(ctx context.Context, logger logrus.FieldLogger, vectors [][]float32, layout slotLayout,
	quantizer codec,
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
