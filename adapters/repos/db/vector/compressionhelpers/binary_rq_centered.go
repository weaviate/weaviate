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
	"math"
	"sync"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// CenteredBinaryRotationalQuantizer is the 1-bit rotational quantizer with
// dataset-mean centering: codes are the sign bits of R(x - mean). Centering
// is what makes 1-bit prefix and shared-store schemes viable on anisotropic
// embedding models. The centered code has its own header layout rather than
// extending the uncentered one:
//
//	word 0, bits  0..31: step  = ‖x-μ‖²/‖x-μ‖₁ of the rotated centered
//	                     vector, float32 (exact, it scales every estimate)
//	word 0, bits 32..47: squaredNorm = ‖x-μ‖², bfloat16
//	word 0, bits 48..63: ⟨μ, x-μ⟩, bfloat16 (the data-side term of the
//	                     centering correction). The CENTERED dot is stored,
//	                     not ⟨μ, x⟩: centering makes x-μ small, so bf16's
//	                     2⁻⁸ relative error lands on a small magnitude.
//	                     Storing the absolute ⟨μ, x⟩ would attach that
//	                     relative error to the large shared-mean component
//	                     and erase the very differences between neighbors
//	                     the header exists to preserve (x≈μ with a large
//	                     ‖μ‖ collapses to bf16 ties).
//	words 1..: sign bits, 64 dimensions per word
//
// The layout is dispatched by construction — this type writes and reads
// only the centered layout, BinaryRotationalQuantizer only the uncentered
// one — never sniffed from bytes, mirroring the centered 4-bit quantizer
// (FourBitRotationalQuantizer with a mean, rq4.go). Total header stays
// 8 bytes, so centered and uncentered codes of one width have identical
// sizes and stride math.
//
// Config validation (bits 1 or 4 for centering), the deferred-training
// activation arm in compress(), the AddRQCentered persistence record and the
// restore path all live alongside the rq4 centered wiring; see
// NewCenteredRQ1Compressor and RestoreCenteredBinaryRotationalQuantizer.
type CenteredBinaryRotationalQuantizer struct {
	inputDim    uint32 // padded to minCodeBits like the uncentered quantizer
	originalDim uint32
	rotation    *compression.FastRotation
	distancer   distancer.Provider
	rounding    []float32
	mean        []float32
	meanNorm2   float32 // ⟨μ,μ⟩, constant term of the centering correction (query and code-to-code)

	// Pool of centering/rotation scratch buffers; Encode runs on every
	// insert and on the bulk re-encode at activation, concurrently.
	scratch sync.Pool

	l2     float32 // indicator: 1 for l2-squared
	cos    float32 // indicator: 1 for cosine-dot
	dotFam float32 // indicator: 1 for dot-based distancers, which need the
	// centering correction; l2 is translation-invariant and needs none
}

// crq1Scratch holds the two throwaway buffers of the encode path, so the
// only allocation left is the code that escapes to the caller.
type crq1Scratch struct {
	cx []float32 // centered input (x - mean)
	rx []float32 // rotated input, output buffer for RotateInto
}

// NewCenteredBinaryRotationalQuantizer builds the centered 1-bit quantizer.
// mean must be the dataset mean over exactly inputDim dimensions; a nil
// mean is a contract error (use BinaryRotationalQuantizer for uncentered).
func NewCenteredBinaryRotationalQuantizer(inputDim int, seed uint64, mean []float32, distancer distancer.Provider) (*CenteredBinaryRotationalQuantizer, error) {
	if len(mean) == 0 {
		return nil, errors.New("centering requires a non-empty mean vector")
	}
	if len(mean) != inputDim {
		return nil, errors.Errorf("centering mean has %d dims, input has %d", len(mean), inputDim)
	}

	originalDim := inputDim
	if inputDim < minCodeBits {
		inputDim = minCodeBits
	}
	rotation := NewFastRotation(inputDim, rotationRounds, seed)

	cos, l2, err := distancerIndicatorsAndError(distancer)
	if err != nil {
		return nil, err
	}

	// Same randomized rounding for the multi-bit query codes as the
	// uncentered quantizer, same derivation from the seed.
	rounding := rqQueryRounding(seed, int(rotation.OutputDim))

	rq := &CenteredBinaryRotationalQuantizer{
		inputDim:    uint32(inputDim),
		originalDim: uint32(originalDim),
		rotation:    rotation,
		distancer:   distancer,
		rounding:    rounding,
		mean:        mean,
		meanNorm2:   dotProduct(mean, mean),
		cos:         cos,
		l2:          l2,
		dotFam:      1 - l2,
	}
	rq.scratch.New = func() any {
		return &crq1Scratch{
			cx: make([]float32, len(mean)),
			rx: make([]float32, rotation.OutputDim),
		}
	}
	return rq, nil
}

// RestoreCenteredBinaryRotationalQuantizer rebuilds a centered 1-bit
// quantizer from a persisted AddRQCentered record. The record carries the
// rotation and the mean but not the query-encoder rounding: the rounding is
// regenerated from DefaultFastRotationSeed — the only seed production code
// ever constructs with — through the same PCG stream as the constructor, so
// the restored offsets are identical to the ones the index was built with.
// The offsets only perturb transient query codes (data codes carry no
// rounding), so this regeneration has no stored state to disagree with.
func RestoreCenteredBinaryRotationalQuantizer(inputDim int, outputDim int, rounds int, swaps [][]compression.Swap, signs [][]float32, mean []float32, distancer distancer.Provider) (*CenteredBinaryRotationalQuantizer, error) {
	if len(mean) != inputDim {
		return nil, errors.Errorf("centering mean has %d dims, input has %d", len(mean), inputDim)
	}

	cos, l2, err := distancerIndicatorsAndError(distancer)
	if err != nil {
		return nil, err
	}

	originalDim := inputDim
	if inputDim < minCodeBits {
		inputDim = minCodeBits
	}

	rq := &CenteredBinaryRotationalQuantizer{
		inputDim:    uint32(inputDim),
		originalDim: uint32(originalDim),
		rotation:    RestoreFastRotation(outputDim, rounds, swaps, signs),
		distancer:   distancer,
		rounding:    rqQueryRounding(DefaultFastRotationSeed, outputDim),
		mean:        mean,
		meanNorm2:   dotProduct(mean, mean),
		cos:         cos,
		l2:          l2,
		dotFam:      1 - l2,
	}
	rq.scratch.New = func() any {
		return &crq1Scratch{
			cx: make([]float32, len(mean)),
			rx: make([]float32, rq.rotation.OutputDim),
		}
	}
	return rq, nil
}

// crqHeader is the decoded per-code metadata, independent of the packed
// representation.
type crqHeader struct {
	step  float32
	norm2 float32 // ‖x-μ‖²
	dmu   float32 // ⟨μ, x-μ⟩, see the layout comment for why it is centered
}

func (rq *CenteredBinaryRotationalQuantizer) putHeader(c []uint64, step, norm2, dmu float32) {
	c[0] = uint64(math.Float32bits(step)) |
		uint64(float32ToBFloat16(norm2))<<32 |
		uint64(float32ToBFloat16(dmu))<<48
}

func (rq *CenteredBinaryRotationalQuantizer) header(c []uint64) crqHeader {
	return crqHeader{
		step:  math.Float32frombits(uint32(c[0])),
		norm2: bfloat16ToFloat32(uint16(c[0] >> 32)),
		dmu:   bfloat16ToFloat32(uint16(c[0] >> 48)),
	}
}

func (rq *CenteredBinaryRotationalQuantizer) Encode(x []float32) []uint64 {
	scratch := rq.scratch.Get().(*crq1Scratch)
	defer rq.scratch.Put(scratch)
	// The package-level centerInto (rq4.go) runs both the subtraction and
	// the dot product on SIMD kernels and zero-pads short inputs before
	// centering, so omitted coordinates center to -mean[i] exactly like an
	// explicit zero would. Its ⟨x-μ, μ⟩ return is exactly what the header
	// stores.
	cx, dmu := centerInto(scratch.cx, x, rq.mean)
	rx := rq.rotation.RotateInto(cx, scratch.rx)
	d := len(rx)
	code := make([]uint64, oneBitFieldWords+d/64)
	codeBits := code[oneBitFieldWords:]
	blocks := d / 64
	var l2NormSquared float32
	var l1Norm float32
	i := 0
	for b := 0; b < blocks; b++ {
		var word uint64
		for bit := uint64(1); bit != 0; bit <<= 1 {
			if rx[i] > 0 {
				word |= bit
				l1Norm += rx[i]
			} else {
				l1Norm += -rx[i]
			}
			l2NormSquared += rx[i] * rx[i]
			i++
		}
		codeBits[b] = word
	}
	step := float32(0)
	if l1Norm > 0 {
		step = l2NormSquared / l1Norm
	}
	// The header is written even for the degenerate x == μ case: the
	// centering correction still applies to the distance.
	rq.putHeader(code, step, l2NormSquared, dmu)
	return code
}

// Restore returns the rotated-space approximation of the CENTERED vector
// (±‖x-μ‖/√D per the sign bits).
func (rq *CenteredBinaryRotationalQuantizer) Restore(b []uint64) []float32 {
	h := rq.header(b)
	dim := 64 * (len(b) - oneBitFieldWords)
	avgNorm := float32(math.Sqrt(float64(h.norm2))) / float32(math.Sqrt(float64(dim)))
	x := make([]float32, dim)
	codeBits := b[oneBitFieldWords:]
	for i := 0; i < dim; i++ {
		if (codeBits[i/64] & (1 << (uint(i) % 64))) != 0 {
			x[i] = avgNorm
		} else {
			x[i] = -avgNorm
		}
	}
	return x
}

func (rq *CenteredBinaryRotationalQuantizer) Decode(compressed []uint64) []float32 {
	restored := rq.Restore(compressed)
	unrotated := rq.rotation.UnRotateInPlace(restored)
	out := unrotated[:rq.originalDim]
	for i := range out {
		if i < len(rq.mean) {
			out[i] += rq.mean[i]
		}
	}
	return out
}

// encodeRotatedQuery builds the 5-bit multi-bit code of an already centered
// and rotated query, same format and randomized rounding as the uncentered
// quantizer. rx is
// scratch owned by the caller; everything retained is freshly allocated.
func (rq *CenteredBinaryRotationalQuantizer) encodeRotatedQuery(rx []float32) RQMultiBitCode {
	abs := maxAbs(rx)
	if abs == 0 {
		return RQMultiBitCode{}
	}
	step := abs / 31
	blocks := len(rx) >> 6
	bits0 := make([]uint64, blocks)
	bits1 := make([]uint64, blocks)
	bits2 := make([]uint64, blocks)
	bits3 := make([]uint64, blocks)
	bits4 := make([]uint64, blocks)
	var squaredNorm float32
	i := 0
	for b := 0; b < blocks; b++ {
		var b0, b1, b2, b3, b4 uint64
		for bit := uint64(1); bit != 0; bit <<= 1 {
			squaredNorm += rx[i] * rx[i]
			c := uint64(((rx[i] + abs) / (2 * step)) + rq.rounding[i])
			if c&1 != 0 {
				b0 |= bit
			}
			if c&2 != 0 {
				b1 |= bit
			}
			if c&4 != 0 {
				b2 |= bit
			}
			if c&8 != 0 {
				b3 |= bit
			}
			if c&16 != 0 {
				b4 |= bit
			}
			i++
		}
		bits0[b] = b0
		bits1[b] = b1
		bits2[b] = b2
		bits3[b] = b3
		bits4[b] = b4
	}
	return RQMultiBitCode{
		Dimension:   len(rx),
		SquaredNorm: squaredNorm,
		Step:        step,
		bits0:       bits0,
		bits1:       bits1,
		bits2:       bits2,
		bits3:       bits3,
		bits4:       bits4,
	}
}

// CenteredBinaryRQDistancer computes distances between a centered multi-bit
// query code and centered 1-bit data codes.
type CenteredBinaryRQDistancer struct {
	query     []float32
	distancer distancer.Provider
	rq        *CenteredBinaryRotationalQuantizer
	cq        RQMultiBitCode
	corrQ     float32 // ⟨μ,q⟩, query-side constant of the correction
}

func (rq *CenteredBinaryRotationalQuantizer) NewDistancer(q []float32) *CenteredBinaryRQDistancer {
	// Centering is unconditional: an empty query is the zero vector, whose
	// centered form is -μ.
	scratch := rq.scratch.Get().(*crq1Scratch)
	cqv, qmd := centerInto(scratch.cx, q, rq.mean)
	cq := rq.encodeRotatedQuery(rq.rotation.RotateInto(cqv, scratch.rx))
	rq.scratch.Put(scratch)
	return &CenteredBinaryRQDistancer{
		query:     q,
		distancer: rq.distancer,
		rq:        rq,
		cq:        cq,
		corrQ:     qmd + rq.meanNorm2,
	}
}

// Distance estimates the distance between the query and a centered 1-bit
// code. The multi-bit/1-bit dot product estimates ⟨x-μ, q-μ⟩; for
// dot-based distancers the additive correction ⟨μ,x-μ⟩ + ⟨μ,q⟩ restores an
// estimate of ⟨x, q⟩. For l2 the correction is skipped entirely, not
// multiplied by zero: distances are translation-invariant so none is
// needed, and 0·(±Inf) from an overflowing mean product would poison the
// finite centered estimate with a NaN.
func (d *CenteredBinaryRQDistancer) Distance(x []uint64) (float32, error) {
	h := d.rq.header(x)
	codeBits := x[oneBitFieldWords:]
	var corr float32
	if d.rq.dotFam != 0 {
		corr = h.dmu + d.corrQ
	}

	dot := float32(31*d.cq.Dimension - 2*d.cq.weightedHamming(codeBits))
	dotEstimate := d.cq.Step * h.step * dot
	return d.rq.l2*(h.norm2+d.cq.SquaredNorm) + d.rq.cos - (1.0+d.rq.l2)*dotEstimate - corr, nil
}

func (d *CenteredBinaryRQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.query) > 0 {
		return d.distancer.SingleDist(d.query, x)
	}
	return d.Distance(d.rq.Encode(x))
}

// centeredBinaryRQCompressedDistancer computes distances from a stored
// centered code (graph reconnection after deletes), always code-to-code.
type centeredBinaryRQCompressedDistancer struct {
	rq *CenteredBinaryRotationalQuantizer
	cq []uint64
}

func (d *centeredBinaryRQCompressedDistancer) Distance(x []uint64) (float32, error) {
	return d.rq.DistanceBetweenCompressedVectors(d.cq, x)
}

func (d *centeredBinaryRQCompressedDistancer) DistanceToFloat(x []float32) (float32, error) {
	return d.rq.DistanceBetweenCompressedVectors(d.cq, d.rq.Encode(x))
}

func (rq *CenteredBinaryRotationalQuantizer) NewQuantizerDistancer(vec []float32) quantizerDistancer[uint64] {
	return rq.NewDistancer(vec)
}

func (rq *CenteredBinaryRotationalQuantizer) NewCompressedQuantizerDistancer(c []uint64) quantizerDistancer[uint64] {
	return &centeredBinaryRQCompressedDistancer{rq: rq, cq: c}
}

func (rq *CenteredBinaryRotationalQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[uint64]) {
}

func (rq *CenteredBinaryRotationalQuantizer) CompressedBytes(compressed []uint64) []byte {
	slice := make([]byte, len(compressed)*8)
	byteops.CopySliceToBytes(slice, compressed)
	return slice
}

func (rq *CenteredBinaryRotationalQuantizer) FromCompressedBytes(compressed []byte) []uint64 {
	l := len(compressed) / 8
	if len(compressed)%8 != 0 {
		l++
	}
	slice := make([]uint64, l)
	byteops.CopyBytesToSlice(slice, compressed)
	return slice
}

func (rq *CenteredBinaryRotationalQuantizer) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]uint64) []uint64 {
	l := len(compressed) / 8
	if len(compressed)%8 != 0 {
		l++
	}
	if len(*buffer) < l {
		*buffer = make([]uint64, 1000*l)
	}
	// take from end so we can address the start of the buffer
	slice := (*buffer)[len(*buffer)-l:]
	*buffer = (*buffer)[:len(*buffer)-l]
	byteops.CopyBytesToSlice(slice, compressed)
	return slice
}

// PersistCompression writes the centered RQ record (rotation + mean). The
// query-encoder rounding is deliberately absent: the AddRQ/AddRQCentered
// commit has no field for it, and none is needed — restore regenerates the
// identical offsets from DefaultFastRotationSeed (see
// RestoreCenteredBinaryRotationalQuantizer).
func (rq *CenteredBinaryRotationalQuantizer) PersistCompression(logger CommitLogger) {
	logger.AddRQCompression(compression.RQData{
		InputDim: rq.originalDim,
		Bits:     1,
		Rotation: *rq.rotation,
		Mean:     rq.mean,
	})
}

func (rq *CenteredBinaryRotationalQuantizer) Data() compression.RQData {
	return compression.RQData{
		InputDim: rq.originalDim,
		Bits:     1,
		Rotation: *rq.rotation,
		Rounding: rq.rounding,
		Mean:     rq.mean,
	}
}

func (rq *CenteredBinaryRotationalQuantizer) Stats() CompressionStats {
	return BinaryRQStats{
		dataBits:  1,
		queryBits: 5,
	}
}

// DistanceBetweenCompressedVectors estimates the distance between two
// centered 1-bit codes via the Hamming angle estimate on the centered
// vectors, plus the centering correction ⟨μ,x-μ⟩ + ⟨μ,y-μ⟩ + ⟨μ,μ⟩ for
// dot-based distancers (⟨x,y⟩ = ⟨x-μ,y-μ⟩ + ⟨μ,x-μ⟩ + ⟨μ,y-μ⟩ + ⟨μ,μ⟩).
// For l2 the correction is skipped entirely, not multiplied by zero — see
// Distance. The dimension comes from the code itself, never from the
// quantizer's rotation width.
func (rq *CenteredBinaryRotationalQuantizer) DistanceBetweenCompressedVectors(x, y []uint64) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.Errorf("code lengths don't match: %d vs %d", len(x), len(y))
	}
	hx, hy := rq.header(x), rq.header(y)
	dim := 64 * (len(x) - oneBitFieldWords)
	fractionDiff := float64(HammingDist(x[oneBitFieldWords:], y[oneBitFieldWords:])) / float64(dim)
	cosEstimate := math.Cos(math.Pi * fractionDiff)
	dotEstimate := float32(math.Sqrt(float64(hx.norm2)) * math.Sqrt(float64(hy.norm2)) * cosEstimate)
	var corr float32
	if rq.dotFam != 0 {
		corr = hx.dmu + hy.dmu + rq.meanNorm2
	}
	return rq.l2*(hx.norm2+hy.norm2) + rq.cos - (1.0+rq.l2)*dotEstimate - corr, nil
}
