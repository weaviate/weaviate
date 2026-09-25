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

package packed

// The in-register fast scan of the int8-table estimator: sixteen document
// tokens in the byte lanes of a vector register, scored one query token at a
// time, or four at a time on arm64, which has a tiled kernel.
//
// TBL and PSHUFB look up sixteen bytes held in a vector register, so sixteen
// document tokens are scored per instruction. Three things follow from putting
// document tokens in the lanes:
//
//   - The stored layout is interleaved (LayoutTokenBlock16): one 16-byte load
//     is one code byte for sixteen consecutive tokens. See the Layout comment
//     in format.go.
//   - The Step rescale is vectorized: sixteen tokens' Steps are one 64-byte
//     load and one multiply per four lanes.
//   - The max-reduce is vectorized: the running maximum is one FMAX or MAXPS
//     per four lanes, and the horizontal reduce happens once per query token.
//
// Parity with RQ1LUTInt8Scorer is bit equality: the integer group sum is exact
// in any order, every kernel computes (S * scale) * Step in that order, and the
// maximum over lanes and blocks is exact. The trailing Tokens%16 tokens,
// token-major on disk, are transposed into a scratch block once per document
// and scored by the same kernel, with only the live lanes folded into the
// maximum.
//
// The kernel walks the code section once per query token, so the tiled kernel
// scores four query tokens per pass, sharing the unpacking of each code byte;
// each query token's arithmetic is the one-token kernel's, so the result is
// bit-identical. The tile width is bounded by the register file, since each
// tiled query token needs its own accumulators and sixteen lane maxima; the
// SSE path fits none and does not tile (see rq1_lut_fs_amd64.go).
//
// The vector kernels accumulate the int8 lookups in int16 lanes, which is
// exact while nibbles * 127 fits int16 and wraps above, so wider inputs fall
// back to the portable implementation, which accumulates in int32 and runs at
// scalar speed. The scorer applies that gate at construction.
//
// Hand-written arm64 and amd64 kernels with a pure Go fallback and an
// init-time dispatch on x/sys/cpu follow distancer/asm and compressionhelpers;
// the impl variable below follows compressionhelpers/distance.go.

import (
	"fmt"
	"math"
)

// rq1ScanMaxNibbles is the most 4-bit groups a code may have for the vector
// kernels. They add one int8 table entry per group into an int16 lane, and
// every entry can be 127 in magnitude, so 258 groups sum to at most 32766,
// which fits, and 259 could reach 32893, which wraps. A 1024-dimensional code
// has 256 groups.
const rq1ScanMaxNibbles = math.MaxInt16 / rq1Int8Levels

// rq1ScanBlocksImpl scores whole 16-token blocks of one document against one
// query token's int8 tables and updates the running maximum of each lane in
// place. It starts as the portable implementation; init replaces it with a
// vector kernel where the build has one (the same shape as
// compressionhelpers/distance.go).
//
// Arguments:
//   - tbl: the query token's table, nibbles*16 int8, indexed [n*16 + c].
//   - codes: the code section in LayoutTokenBlock16 order, truncated to
//     whole blocks (blocks*16*groups bytes).
//   - steps: the widened Steps, at least blocks*16 long.
//   - best: 16 running lane maxima, seeded by the caller, updated in place.
//   - groups: code bytes per token.
//   - blocks: number of whole blocks to score.
//   - scale: the query token's table scale.
var rq1ScanBlocksImpl func(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32) = rq1ScanBlocksGo

// rq1ScanBlocksVariant is a vector kernel and its name, for the kernel tests.
type rq1ScanBlocksVariant struct {
	name string
	fn   func(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32)
}

// rq1ScanBlocksVariants holds every vector kernel compiled into this build, in
// the order init installs them, whichever one dispatch chose. The kernel tests
// run each one against the portable implementation, so no kernel ships
// untested, and assert that the installed kernel is on the list. The portable
// implementation is not listed: it is what every entry is checked against.
var rq1ScanBlocksVariants []rq1ScanBlocksVariant

// rq1ScanBlocksGo is the portable block kernel and the parity reference for
// every vector kernel: the same loop with the lanes written out as an array.
// Arguments are rq1ScanBlocksImpl's.
//
// It accumulates in int32, so it is correct at any dimensionality. It is what
// runs above rq1ScanMaxNibbles, on a build without vector kernels, and on an
// amd64 machine without SSSE3 and SSE4.1. It is kept as the plain loop because
// it is the reference the kernels are held to bit for bit; the inner lane loop
// carries no bounds checks, and the slice checks left are per code byte and
// per block.
func rq1ScanBlocksGo(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32) {
	_ = best[blockTokens-1]
	stride := blockTokens * groups
	for b := range blocks {
		block := codes[b*stride : (b+1)*stride]
		var acc [blockTokens]int32
		for g := range groups {
			// the low and high nibble tables of code byte g
			lo := tbl[32*g : 32*g+16 : 32*g+16]
			hi := tbl[32*g+16 : 32*g+32 : 32*g+32]
			// code byte g of the block's sixteen tokens
			row := block[g*blockTokens : (g+1)*blockTokens]
			for j, c := range row {
				acc[j] += int32(lo[c&15]) + int32(hi[c>>4])
			}
		}
		// scale first, then Step, then fold into the lane maxima
		st := steps[b*blockTokens : (b+1)*blockTokens]
		for j, v := range acc {
			if e := (float32(v) * scale) * st[j]; e > best[j] {
				best[j] = e
			}
		}
	}
}

// rq1ScanTileImpl is the tiled kernel: rq1ScanTileWidth query tokens scored
// over the same blocks in one pass, so the shared unpacking of each code byte
// is done once per tile. It is nil where the build has no tiled vector kernel.
// Each query token's arithmetic is the block kernel's, so the result is
// bit-identical to running rq1ScanBlocksImpl once per query token.
//
// Arguments:
//   - tbl: the tile's tables, one per query token, tblStride int8 apart.
//   - tblStride: distance between consecutive query tokens' tables.
//   - codes, steps, groups, blocks: as in rq1ScanBlocksImpl.
//   - best: the tile's lane maxima, 16 per query token, consecutive.
//   - scales: one table scale per query token of the tile.
var (
	rq1ScanTileImpl  func(tbl []int8, tblStride int, codes []byte, steps, best, scales []float32, groups, blocks int)
	rq1ScanTileWidth = 1
)

var _ Scorer = (*RQ1LUTFastScanScorer)(nil)

// RQ1LUTFastScanScorer scores LayoutTokenBlock16 RQ1 blobs with the int8-table
// estimator, sixteen document tokens per shuffle. It is bit-exact with
// RQ1LUTInt8Scorer over the same tokens; the blobs differ only in the order of
// the code bytes.
//
// It is not safe for concurrent use, since Distance reuses the steps, lane and
// tail buffers.
type RQ1LUTFastScanScorer struct {
	p       *RQ1Params
	nq      int
	groups  int
	nibbles int

	// scan is the block kernel this scorer runs: the vector one where the
	// shape allows it, the portable one otherwise, bound at construction.
	scan func(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32)

	// scanTile is the tiled kernel and tile how many query tokens it takes at
	// once. tile is 1 where there is no tiled kernel or this scorer must not
	// use it, and then scanTile is never called.
	scanTile func(tbl []int8, tblStride int, codes []byte, steps, best, scales []float32, groups, blocks int)
	tile     int

	// tables is one table per query token, indexed [q*tableLen + n*16 + c],
	// the same layout and values RQ1LUTInt8Scorer builds.
	tables   []int8
	tableLen int

	// scale is the value of one table unit, per query token.
	scale []float32

	// corr is <q, mu> per query token, as in every RQ1 scorer here.
	corr []float32

	// steps holds the document's Step per token, widened once per document.
	// lanes holds the running maximum per lane for one query token, tileLanes
	// the same for each query token of a tile, and tileBest the tile's maxima
	// once the lanes are folded.
	steps     []float32
	lanes     []float32
	tileLanes []float32
	tileBest  []float32

	// tailCodes and tailSteps are the trailing tokens transposed into one
	// block, rebuilt once per document and scanned by the same kernel. Lanes
	// with no token keep whatever the last document left there: scanInto
	// folds only the live lanes, so clearing them would be unobservable work.
	tailCodes []byte
	tailSteps []float32
}

// NewRQ1LUTFastScanScorer prepares query for scoring LayoutTokenBlock16 RQ1
// blobs encoded under p. Each query token is rotated once (the same rotation
// the codes were encoded under) and expanded into its quantized 4-bit tables.
// The query itself is not retained.
//
// Arguments:
//   - query: one slice per query token, each exactly p.dims long.
//   - p: the parameters the blobs were encoded under.
func NewRQ1LUTFastScanScorer(query [][]float32, p *RQ1Params) (*RQ1LUTFastScanScorer, error) {
	if err := validateTokens(query, p.dims); err != nil {
		return nil, fmt.Errorf("packed: query: %w", err)
	}

	nibbles := rq1Nibbles(p)
	s := &RQ1LUTFastScanScorer{
		p:        p,
		nq:       len(query),
		groups:   rq1Groups(p),
		nibbles:  nibbles,
		scan:     rq1ScanBlocksImpl,
		tableLen: nibbles * 16,
		scale:    make([]float32, len(query)),
		corr:     make([]float32, len(query)),
		lanes:    make([]float32, blockTokens),
		scanTile: rq1ScanTileImpl,
		tile:     rq1ScanTileWidth,
	}
	s.tailCodes = make([]byte, blockTokens*s.groups)
	s.tailSteps = make([]float32, blockTokens)
	if nibbles > rq1ScanMaxNibbles {
		// the int16 accumulators would wrap; both vector kernels use them,
		// so the tile is dropped along with the block kernel
		s.scan = rq1ScanBlocksGo
		s.tile = 1
	}
	if s.tile > 1 {
		s.tileLanes = make([]float32, s.tile*blockTokens)
		s.tileBest = make([]float32, s.tile)
	}
	s.tables = make([]int8, len(query)*s.tableLen)

	for q, token := range query {
		rx := p.rot.Rotate(token)
		s.scale[q] = rq1Int8Table(rx, nibbles, s.tables[q*s.tableLen:(q+1)*s.tableLen])
		for j, m := range p.mean {
			s.corr[q] += token[j] * m
		}
	}
	return s, nil
}

// Distance implements Scorer: the negated MaxSim estimate over int8 tables,
// with the <q, mu> correction applied per query token after its maximum.
func (s *RQ1LUTFastScanScorer) Distance(b Blob) (float32, error) {
	if err := checkRQ1LUTBlob(b, s.p, s.groups, LayoutTokenBlock16); err != nil {
		return 0, err
	}
	s.steps = widenRQ1Steps(b, s.steps)

	// split the code section into whole blocks and the token-major tail
	blocks := int(b.Tokens) / blockTokens
	tail := int(b.Tokens) - blocks*blockTokens
	codes := b.Codes()
	blocked := codes[:blocks*blockTokens*s.groups]
	s.buildTail(codes[blocks*blockTokens*s.groups:], blocks, tail)

	// whole tiles of query tokens first, then the leftover ones one at a time
	var sum float32
	q := 0
	for ; s.tile > 1 && q+s.tile <= s.nq; q += s.tile {
		sum = s.scoreTile(q, blocked, blocks, tail, sum)
	}
	for ; q < s.nq; q++ {
		tbl := s.tables[q*s.tableLen : (q+1)*s.tableLen]
		scale := s.scale[q]
		// Seeded like every scorer here: an empty document saturates to the
		// largest finite distance per query token and sorts last.
		best := float32(-math.MaxFloat32)

		if blocks > 0 {
			best = s.scanInto(tbl, blocked, s.steps, blocks, scale, blockTokens, best)
		}
		if tail > 0 {
			best = s.scanInto(tbl, s.tailCodes, s.tailSteps, 1, scale, tail, best)
		}
		sum += -best - s.corr[q]
	}
	return sum, nil
}

// scoreTile scores s.tile query tokens starting at q, the same way the
// one-at-a-time loop in Distance does, and returns the updated running sum.
//
// Arguments:
//   - q: index of the tile's first query token.
//   - blocked: the code section truncated to whole blocks.
//   - blocks: number of whole blocks.
//   - tail: number of trailing tokens in the tail block.
//   - sum: the running sum over query tokens so far.
//
// It adds into the running sum in query-token order and returns no subtotal:
// summing the tile apart would reassociate the sum, and parity with
// RQ1LUTInt8Scorer is bit equality.
func (s *RQ1LUTFastScanScorer) scoreTile(q int, blocked []byte, blocks, tail int, sum float32) float32 {
	tbl := s.tables[q*s.tableLen:]
	scales := s.scale[q : q+s.tile]
	best := s.tileBest
	for t := range best {
		best[t] = -math.MaxFloat32
	}

	if blocks > 0 {
		s.scanTileInto(tbl, blocked, s.steps, blocks, scales, blockTokens, best)
	}
	if tail > 0 {
		s.scanTileInto(tbl, s.tailCodes, s.tailSteps, 1, scales, tail, best)
	}

	for t, v := range best {
		sum += -v - s.corr[q+t]
	}
	return sum
}

// scanTileInto runs the tiled kernel over blocks whole blocks and folds, for
// each query token of the tile, its first lanes lane maxima into its entry of
// best.
//
// Arguments:
//   - tbl: the tile's tables, starting at the first query token's.
//   - codes, steps, blocks: the blocks to score, as in rq1ScanBlocksImpl.
//   - scales: one table scale per query token of the tile.
//   - lanes: how many lanes hold a token (16, or fewer for the tail block).
//   - best: one running maximum per query token, updated in place.
func (s *RQ1LUTFastScanScorer) scanTileInto(tbl []int8, codes []byte, steps []float32,
	blocks int, scales []float32, lanes int, best []float32,
) {
	for j := range s.tileLanes {
		s.tileLanes[j] = -math.MaxFloat32
	}
	s.scanTile(tbl, s.tableLen, codes, steps, s.tileLanes, scales, s.groups, blocks)
	for t := range best {
		for _, v := range s.tileLanes[t*blockTokens : t*blockTokens+lanes] {
			if v > best[t] {
				best[t] = v
			}
		}
	}
}

// buildTail transposes the trailing tokens into the scratch block tailCodes
// and copies their Steps into tailSteps. It runs once per document, outside
// the query loop. Lanes with no token are left as they were (see the field
// comment).
//
// Arguments:
//   - codes: the token-major tail of the code section, tail*groups bytes.
//   - blocks: number of whole blocks before the tail, to locate its Steps.
//   - tail: number of trailing tokens, below blockTokens.
func (s *RQ1LUTFastScanScorer) buildTail(codes []byte, blocks, tail int) {
	if tail == 0 {
		return
	}
	// token j's byte g goes to lane j of row g, the LayoutTokenBlock16 order
	for j := range tail {
		code := codes[j*s.groups : (j+1)*s.groups]
		for g, c := range code {
			s.tailCodes[g*blockTokens+j] = c
		}
	}
	copy(s.tailSteps, s.steps[blocks*blockTokens:])
}

// scanInto runs the block kernel over blocks whole blocks and returns the
// largest of best and the first lanes lane maxima.
//
// Arguments:
//   - tbl: one query token's table.
//   - codes, steps, blocks: the blocks to score, as in rq1ScanBlocksImpl.
//   - scale: the query token's table scale.
//   - lanes: how many lanes hold a token. Below blockTokens only for the tail
//     block, whose other lanes score stale scratch and are not read.
//   - best: the query token's running maximum so far.
func (s *RQ1LUTFastScanScorer) scanInto(tbl []int8, codes []byte, steps []float32,
	blocks int, scale float32, lanes int, best float32,
) float32 {
	for j := range s.lanes {
		s.lanes[j] = -math.MaxFloat32
	}
	s.scan(tbl, codes, steps, s.lanes, s.groups, blocks, scale)
	for _, v := range s.lanes[:lanes] {
		if v > best {
			best = v
		}
	}
	return best
}
