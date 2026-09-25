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

// RQ1 is 1-bit rotational quantization: a random rotation, one sign bit per
// rotated coordinate, and a per-token scale Step such that
// Step * <sign(x~), q~> is an unbiased estimate of <x, q>.
//
// Encoding and scoring delegate to compressionhelpers'
// BinaryRotationalQuantizer (Encode per document token, one BinaryRQDistancer
// per query token), so the estimator, its 5-bit query quantization and its
// Hamming kernels are the shipped ones. This file holds the multi-vector
// container and loop: the blob's sections (sign bits in the code section, Step
// narrowed to binary16 in the scalar section), the per-query-token minimum
// over document tokens, the centering correction, and the unpadded rotation
// width noted at EncodingRQ1.
//
// Centering subtracts a mean mu from every document token before rotating,
// which moves the sign quantization onto where the mass is. Under dot product
// this shifts every token's score against a query token q by the same <q, mu>,
// so the maximum is taken over shifted scores and corrected afterwards, and
// document ranking is preserved exactly (asserted in the tests). mu is derived
// from data, so centered parameters carry a non-zero quantizer reference and
// uncentered ones carry zero.

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/tphakala/simd/f16"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// rq1RotationRounds matches compressionhelpers' rotationRounds: 3 rounds are
// enough when only the signs of the rotated entries matter.
const rq1RotationRounds = 3

// rq1RoundingKey is the PCG stream key BinaryRotationalQuantizer uses to derive
// the query-quantization rounding offsets from the seed. The derivation is
// mirrored here because the from-seed constructor pads dimensions below 256
// up to 256 bits (see EncodingRQ1), and the unpadded constructor
// RestoreBinaryRotationalQuantizer takes the rounding from outside. The parity
// tests at d >= 256 hold this mirror to the house derivation.
const rq1RoundingKey = 0x4f8ebf70e130707f

// RQ1Params holds the parameters of an RQ1 encoding: the quantizer (derived
// from dims and a seed), an optional mean for centering, and the (id, version)
// reference written into the header. A blob is only scorable under the
// parameters it was encoded with; the header reference is checked against
// them. Persisting the parameters is the caller's job.
type RQ1Params struct {
	dims int
	mean []float32

	quantizerID      uint16
	quantizerVersion uint16

	// brq is the house quantizer, restored over an unpadded rotation of
	// exactly ceil(dims/64)*64 bits and the dot product provider. Under that
	// provider the house distance is -(queryStep * Step * dot), and the
	// stored SquaredNorm is unused.
	brq *compressionhelpers.BinaryRotationalQuantizer

	// rot is the same rotation brq applies (brq is restored from this
	// object's Swaps and Signs, so Rotate is bit-identical between the two).
	// The exact-query scorers use it to rotate query tokens without
	// quantizing them.
	rot *compression.FastRotation

	// rounding is the per-coordinate offset brq quantizes query coordinates
	// with. No scorer reads it; the tests rebuild the house 5-bit query levels
	// from it.
	rounding []float32

	// codeWords is the length of one house code in uint64 words: one field
	// word (Step, SquaredNorm) plus the sign-bit words.
	codeWords int
}

// NewRQ1Params derives the RQ1 parameter set for dims-dimensional tokens.
//
// Arguments:
//   - dims: dimensionality of each token vector, in [1, MaxUint16].
//   - seed: seeds the rotation and the query rounding offsets.
//   - mean: mean vector of the dataset. nil for uncentered RQ1. Retained; must
//     not be modified while in use.
//   - id, version: written into every blob's header and checked at scoring
//     time. Uncentered parameters must use (0, 0), the header's "no trained
//     parameters". Centered ones must use a non-zero id, because the mean is
//     trained state the caller persists and the reference names.
func NewRQ1Params(dims int, seed uint64, mean []float32, id, version uint16) (*RQ1Params, error) {
	if dims <= 0 || dims > math.MaxUint16 {
		return nil, fmt.Errorf("packed: dimensions %d out of range [1, %d]", dims, math.MaxUint16)
	}
	if mean == nil {
		if id != 0 || version != 0 {
			return nil, fmt.Errorf("packed: uncentered RQ1 has no trained parameters to reference, got id %d version %d", id, version)
		}
	} else {
		if len(mean) != dims {
			return nil, fmt.Errorf("packed: mean has %d dimensions, want %d", len(mean), dims)
		}
		if id == 0 {
			return nil, fmt.Errorf("packed: centered RQ1 needs a non-zero quantizer id: the mean is trained state the header must reference")
		}
	}

	rotation := compression.NewFastRotation(dims, rq1RotationRounds, seed)
	rounding := make([]float32, rotation.OutputDim)
	rng := rand.New(rand.NewPCG(seed, rq1RoundingKey))
	for i := range rounding {
		rounding[i] = rng.Float32()
	}
	// Restore instead of NewBinaryRotationalQuantizer because Restore takes
	// the rotation from outside, which is the only way to get an unpadded one
	// in (see EncodingRQ1). Restore still clamps its own inputDim field up to
	// minCodeBits, so that field says 256 while the rotation is 128 wide. That
	// is harmless here: inputDim is read only by PersistCompression, and the
	// unclamped originalDim only by Data and Decode, none of which this package
	// calls. Encode, NewDistancer and BinaryRQDistancer.Distance take their
	// width from the rotation itself. The parity tests and golden fixtures
	// would catch this contract breaking upstream.
	brq, err := compressionhelpers.RestoreBinaryRotationalQuantizer(
		dims, int(rotation.OutputDim), rq1RotationRounds,
		rotation.Swaps, rotation.Signs, rounding,
		distancer.NewDotProductProvider())
	if err != nil {
		return nil, fmt.Errorf("packed: restoring the quantizer: %w", err)
	}

	return &RQ1Params{
		dims:             dims,
		mean:             mean,
		quantizerID:      id,
		quantizerVersion: version,
		brq:              brq,
		rot:              rotation,
		rounding:         rounding,
		codeWords:        1 + int(rotation.OutputDim)/64,
	}, nil
}

// EncodeRQ1 packs tokens as token-major RQ1 under p and returns the blob. Per
// token, the code section holds the sign bits of
// BinaryRotationalQuantizer.Encode as ceil(dims/64) little-endian words, and
// the scalar section holds its Step as a binary16 (0 for a zero token, whose
// estimate must be exactly 0).
//
// Arguments:
//   - tokens: one slice per token, each exactly p.dims long.
//   - p: the parameters to encode under.
func EncodeRQ1(tokens [][]float32, p *RQ1Params) ([]byte, error) {
	return encodeRQ1(tokens, p, LayoutTokenMajor)
}

// EncodeRQ1Block16 writes the same codes and scalars as EncodeRQ1, with the
// code section permuted into LayoutTokenBlock16, the interleave the fast scan
// reads. Same estimator, same bytes per token, same header except for the
// layout byte. Arguments are EncodeRQ1's.
func EncodeRQ1Block16(tokens [][]float32, p *RQ1Params) ([]byte, error) {
	return encodeRQ1(tokens, p, LayoutTokenBlock16)
}

// encodeRQ1 is the body of both encoders. The codes are produced token-major
// either way, since that is the order the quantizer hands them over; layout
// decides only whether they are permuted before the blob is assembled.
//
// Arguments:
//   - tokens, p: as in EncodeRQ1.
//   - layout: LayoutTokenMajor or LayoutTokenBlock16.
func encodeRQ1(tokens [][]float32, p *RQ1Params, layout Layout) ([]byte, error) {
	codes, scalars, err := rq1Codes(tokens, p)
	if err != nil {
		return nil, err
	}
	if layout == LayoutTokenBlock16 {
		perToken, err := codeLen(EncodingRQ1, uint16(p.dims))
		if err != nil {
			return nil, err
		}
		codes = interleaveBlock16(codes, len(tokens), perToken)
	}

	return build(Header{
		Encoding:         EncodingRQ1,
		Layout:           layout,
		ScalarKind:       ScalarFloat16,
		Dims:             uint16(p.dims),
		Tokens:           uint32(len(tokens)),
		QuantizerID:      p.quantizerID,
		QuantizerVersion: p.quantizerVersion,
	}, scalars, codes)
}

// rq1Codes packs tokens into token-major RQ1 codes and their per-token Steps,
// without assembling a blob. It returns the code section and one Step per
// token. Kept separate from encodeRQ1 so an encoding built on RQ1 codes (a
// residual against a centroid, say) can write an identical code section by
// calling it.
//
// Arguments:
//   - tokens: one slice per token, each exactly p.dims long.
//   - p: the parameters to encode under. When p.mean is set it is subtracted
//     from every token first.
func rq1Codes(tokens [][]float32, p *RQ1Params) ([]byte, []float32, error) {
	if err := validateTokens(tokens, p.dims); err != nil {
		return nil, nil, err
	}

	perToken, err := codeLen(EncodingRQ1, uint16(p.dims))
	if err != nil {
		return nil, nil, err
	}
	codes := make([]byte, 0, len(tokens)*perToken)
	scalars := make([]float32, len(tokens))
	var centered []float32
	if p.mean != nil {
		centered = make([]float32, p.dims)
	}

	for i, t := range tokens {
		if p.mean != nil {
			for j := range centered {
				centered[j] = t[j] - p.mean[j]
			}
			t = centered
		}
		code := compressionhelpers.RQOneBitCode(p.brq.Encode(t))
		for _, w := range code.Bits() {
			codes = binary.LittleEndian.AppendUint64(codes, w)
		}
		scalars[i] = code.Step()
		// The scalar section is binary16. A Step above its range would be
		// stored as +Inf, and every scorer would then maximize Inf*dot over
		// the document's tokens: the token wins every maximum, or contributes
		// a NaN where its dot is exactly 0. Either way the score is wrong and
		// nothing reports it, so the token is refused here. The check is the
		// same narrowing build performs. Underflow needs no guard: a Step that
		// rounds to zero estimates zero, as a zero token already does.
		if math.IsInf(float64(f16.ToFloat32(f16.FromFloat32(scalars[i]))), 0) {
			return nil, nil, fmt.Errorf("packed: token %d has Step %g, beyond the binary16 range the blob stores", i, scalars[i])
		}
	}
	return codes, scalars, nil
}

var _ Scorer = (*RQ1Scorer)(nil)

// RQ1Scorer scores token-major RQ1 blobs against a fixed query with the house
// estimator: one BinaryRQDistancer per query token, prepared once and reused
// across candidates. This scorer adds the MaxSim loop, the centering
// correction, and the unpacking of blob sections back into house codes.
//
// It is not safe for concurrent use: the reconstructed codes live in scratch
// the scorer owns.
type RQ1Scorer struct {
	p     *RQ1Params
	dists []*compressionhelpers.BinaryRQDistancer
	// corr is <q, mu> per query token, added after its maximum (where it
	// cannot change which document token wins) so the score estimates <q, t>
	// and not <q, t - mu>. All zero when uncentered.
	corr []float32
	// scratch holds the document's reconstructed house codes, p.codeWords
	// words per token, reused across candidates.
	scratch []uint64
}

// NewRQ1Scorer prepares query for scoring RQ1 blobs encoded under p. The query
// is quantized once here and reused across candidates.
//
// Arguments:
//   - query: one slice per query token, each exactly p.dims long. Retained by
//     the underlying distancers; must not be modified while in use.
//   - p: the parameters the blobs were encoded under.
func NewRQ1Scorer(query [][]float32, p *RQ1Params) (*RQ1Scorer, error) {
	if err := validateTokens(query, p.dims); err != nil {
		return nil, fmt.Errorf("packed: query: %w", err)
	}

	dists := make([]*compressionhelpers.BinaryRQDistancer, len(query))
	corr := make([]float32, len(query))
	for i, q := range query {
		dists[i] = p.brq.NewDistancer(q)
		for j, m := range p.mean {
			corr[i] += q[j] * m
		}
	}
	return &RQ1Scorer{p: p, dists: dists, corr: corr}, nil
}

// checkRQ1Blob rejects a blob that is not an RQ1 blob written under p in the
// given layout. The quantizer reference check is the one the header exists
// for: an RQ1 blob encoded under another seed or mean has the same shape and
// would score silently wrong. The layout check matters because RQ1 is written
// in two layouts of the same length, and a scorer walking the other one would
// also score silently wrong.
//
// Arguments:
//   - b: the parsed blob.
//   - p: the parameters the scorer holds.
//   - layout: the layout the scorer reads.
func checkRQ1Blob(b Blob, p *RQ1Params, layout Layout) error {
	if b.Encoding != EncodingRQ1 {
		return fmt.Errorf("packed: blob uses encoding %d, not RQ1", uint8(b.Encoding))
	}
	if b.Layout != layout {
		return fmt.Errorf("packed: scorer reads RQ1 layout %d, blob is layout %d", uint8(layout), uint8(b.Layout))
	}
	if b.ScalarKind != ScalarFloat16 {
		return fmt.Errorf("packed: RQ1 blobs carry a binary16 Step per token, got scalar kind %d", uint8(b.ScalarKind))
	}
	if int(b.Dims) != p.dims {
		return fmt.Errorf("packed: blob has %d dimensions, scorer was prepared for %d", b.Dims, p.dims)
	}
	if b.QuantizerID != p.quantizerID || b.QuantizerVersion != p.quantizerVersion {
		return fmt.Errorf("packed: blob references quantizer (%d, %d), scorer holds (%d, %d)",
			b.QuantizerID, b.QuantizerVersion, p.quantizerID, p.quantizerVersion)
	}
	return nil
}

// Distance implements Scorer: the negated MaxSim estimate, with the <q, mu>
// correction applied per query token after its maximum.
func (s *RQ1Scorer) Distance(b Blob) (float32, error) {
	if err := checkRQ1Blob(b, s.p, LayoutTokenMajor); err != nil {
		return 0, err
	}

	// Rebuild every token's house code once, before the query loop. The
	// field word gets the binary16 Step widened to float32 in its low half;
	// the high half (SquaredNorm) stays zero because the dot product distance
	// never reads it. The sign words go through the unaligned accessors, as
	// format.go explains.
	//
	// Scalar(i) per token is what its own doc says not to do in a kernel.
	// This is the reference scorer, where one obvious loop is worth more than
	// one saved pass; the fast scorers use widenRQ1Steps.
	tokens := int(b.Tokens)
	need := tokens * s.p.codeWords
	if cap(s.scratch) < need {
		s.scratch = make([]uint64, need)
	}
	s.scratch = s.scratch[:need]
	for i := range tokens {
		w := s.scratch[i*s.p.codeWords : (i+1)*s.p.codeWords]
		w[0] = uint64(math.Float32bits(b.Scalar(i)))
		code := b.Code(i)
		for k := 1; k < len(w); k++ {
			w[k] = binary.LittleEndian.Uint64(code[8*(k-1):])
		}
	}

	var sum float32
	for qi, dist := range s.dists {
		// Seeded with the largest finite distance, so a document with no
		// tokens sorts last. The correction is far below MaxFloat32's
		// precision there and cannot unsaturate it.
		best := float32(math.MaxFloat32)
		for i := range tokens {
			d, err := dist.Distance(s.scratch[i*s.p.codeWords : (i+1)*s.p.codeWords])
			if err != nil {
				return 0, err
			}
			if d < best {
				best = d
			}
		}
		sum += best - s.corr[qi]
	}
	return sum, nil
}
