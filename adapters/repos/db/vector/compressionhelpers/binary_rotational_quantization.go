//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type BinaryRotationalQuantizer struct {
	inputDim  uint32
	rotation  *FastRotation
	distancer distancer.Provider
	rounding  []float32
	l2        float32
	cos       float32
}

func NewBinaryRotationalQuantizer(inputDim int, seed uint64, distancer distancer.Provider) *BinaryRotationalQuantizer {
	// Pad the input if it is low-dimensional.
	const minCodeBits = 256
	if inputDim < minCodeBits {
		inputDim = minCodeBits
	}
	// For the rotated point to look fully random we need 4 or 5 rotational
	// rounds, but since we only care about the sign of the entries and not the
	// complete distribution, it seems like 3 rounds suffice.
	rotationRounds := 3
	rotation := NewFastRotation(inputDim, rotationRounds, seed)

	cos, l2, err := distancerIndicatorsAndError(distancer)
	if err != nil {
		return nil
	}
	// Randomized rounding for the query quantization to make the estimator
	// unbiased. It may produce better recall to not use randomized rounding
	// since adding the random noise increases the quantization error. With
	// 8-bit RQ we are not using randomized rounding.
	rounding := make([]float32, rotation.OutputDim)
	rng := rand.New(rand.NewPCG(seed, 0x4f8ebf70e130707f))
	for i := range rounding {
		rounding[i] = rng.Float32()
	}

	rq := &BinaryRotationalQuantizer{
		inputDim:  uint32(inputDim),
		rotation:  rotation,
		distancer: distancer,
		rounding:  rounding,
		l2:        l2,
		cos:       cos,
	}
	return rq
}

func RestoreBinaryRotationalQuantizer(inputDim int, outputDim int, rounds int, swaps [][]Swap, signs [][]float32, rounding []float32, distancer distancer.Provider) (*BinaryRotationalQuantizer, error) {
	cos, l2, err := distancerIndicatorsAndError(distancer)
	if err != nil {
		return nil, err
	}
	rq := &BinaryRotationalQuantizer{
		inputDim:  uint32(inputDim),
		rotation:  RestoreFastRotation(outputDim, rounds, swaps, signs),
		distancer: distancer,
		rounding:  rounding,
		cos:       cos,
		l2:        l2,
	}
	return rq, nil
}

func putFloat32Upper(v uint64, x float32) uint64 {
	const upper32 uint64 = ((1 << 32) - 1) << 32
	return (v &^ upper32) | uint64(math.Float32bits(x))<<32
}

func getFloat32Upper(v uint64) float32 {
	return math.Float32frombits(uint32(v >> 32))
}

func putFloat32Lower(v uint64, x float32) uint64 {
	const lower32 uint64 = (1 << 32) - 1
	return (v &^ lower32) | uint64(math.Float32bits(x))
}

func getFloat32Lower(v uint64) float32 {
	return math.Float32frombits(uint32(v))
}

// RaBitQ 1-bit code. Instead of normalizing explicitly prior to rotating and
// quantizing we can just bake the normalization factor and adjustment of the
// estimator into "step". Suppose that x is a randomly rotated vector. Then we
// quantize the ith entry of x by taking its sign:
// quantized x_i = step * sign(x_i) = (<x,x>/(sum_i |x_i|)) sign(x_i).
// See the first RaBitQ paper for details https://arxiv.org/abs/2405.12497.
type RQOneBitCode []uint64

// <x,x>/(sum_i |x_i|)
func (c RQOneBitCode) Step() float32 {
	return getFloat32Lower(c[0])
}

func (c RQOneBitCode) setStep(x float32) {
	c[0] = putFloat32Lower(c[0], x)
}

// Euclidean norm squared.
func (c RQOneBitCode) SquaredNorm() float32 {
	return getFloat32Upper(c[0])
}

func (c RQOneBitCode) setSquaredNorm(x float32) {
	c[0] = putFloat32Upper(c[0], x)
}

const oneBitFieldWords = 1

func (c RQOneBitCode) Bits() []uint64 {
	return c[oneBitFieldWords:]
}

func (c RQOneBitCode) Dimension() int {
	return 64 * (len(c) - oneBitFieldWords)
}

func NewRQOneBitCode(d int) RQOneBitCode {
	return make([]uint64, oneBitFieldWords+d/64)
}

func (c RQOneBitCode) String() string {
	return fmt.Sprintf("RQOneBitCode{Step: %.4f, SquaredNorm: %.4f, Bits[0]: %064b",
		c.Step(), c.SquaredNorm(), c.Bits()[0])
}

func (rq *BinaryRotationalQuantizer) Encode(x []float32) []uint64 {
	rx := rq.rotation.Rotate(x)
	d := len(rx)
	code := NewRQOneBitCode(d)
	blocks := d / 64
	var l2NormSquared float32
	var l1Norm float32
	i := 0
	for b := range blocks {
		var bits uint64
		for bit := uint64(1); bit != 0; bit <<= 1 {
			if rx[i] > 0 {
				bits |= bit
				l1Norm += rx[i]
			} else {
				l1Norm += -rx[i]
			}
			l2NormSquared += rx[i] * rx[i]
			i++
		}
		code.Bits()[b] = bits
	}
	if l1Norm == 0 {
		return code
	}
	code.setSquaredNorm(l2NormSquared)
	code.setStep(l2NormSquared / l1Norm)
	return code
}

// Restore -> NewCompressedQuantizerDistancer -> NewDistancerFromID -> reassignNeighbor in when deleting
// distancer for PQ,SQ etc. use the compressed vector, in this case we can't use it because we have different encoding for the query and the data.
func (rq *BinaryRotationalQuantizer) Restore(b []uint64) []float32 {
	code := RQOneBitCode(b)
	step := code.Step()
	dim := code.Dimension()
	x := make([]float32, dim)
	bits := code.Bits()
	for i := range dim {
		block := i / 64
		bit := uint(i) % 64
		if (bits[block] & (1 << bit)) != 0 {
			x[i] = step
		} else {
			x[i] = -step
		}
	}
	return x
}

// The binary encoding of q. We quantize q using k bits using the format:
//
// q_i = step * (s_i,k-1 * 2^(k-1) + s_i,k-2 * 2^(k-2) + ... + s_i,0)
//
// where s_i,j is the j-th bit of the k-bit integer quantization of q_i. We
// interpret s_i,j as a sign indicator so that {0,1} corresponds to {-1, +1}.
// For k = 3 this means that we can generate numbers in the set {-7, -5, -3, -1,
// 1, 3, 5, 7}. We use the step variable to scale this interval so that it
// includes the largest absolute value.
type RQMultiBitCode struct {
	Dimension   int
	SquaredNorm float32
	Step        float32
	bits0       []uint64
	bits1       []uint64
	bits2       []uint64
	bits3       []uint64
	bits4       []uint64
}

func (c RQMultiBitCode) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("bits0[0]: %064b, ", c.bits0[0]))
	return fmt.Sprintf("RQMultiBitCode{Step: %.4f, SquaredNorm: %.4f, bits: %s",
		c.Step, c.SquaredNorm, sb.String())
}

func maxAbs(rx []float32) float32 {
	var max float32
	for _, v := range rx {
		if v < 0 {
			v = -v
		}
		if v > max {
			max = v
		}
	}
	return max
}

// TODO: Handle corner cases as we do for 8-bit RQ.
func (rq *BinaryRotationalQuantizer) encodeQuery(x []float32) RQMultiBitCode {
	rx := rq.rotation.Rotate(x)
	abs := maxAbs(rx)
	if abs == 0 {
		// The input vector is the zero vector.
		return RQMultiBitCode{}
	}
	step := abs / 31
	// Encode each rotated entry to an unsigned integer and extract the bits.
	blocks := len(rx) >> 6
	bits0 := make([]uint64, blocks)
	bits1 := make([]uint64, blocks)
	bits2 := make([]uint64, blocks)
	bits3 := make([]uint64, blocks)
	bits4 := make([]uint64, blocks)
	var squaredNorm float32
	i := 0
	for b := range blocks {
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

type BinaryRQDistancer struct {
	query     []float32
	distancer distancer.Provider
	rq        *BinaryRotationalQuantizer
	cos       float32
	l2        float32
	cq        RQMultiBitCode
}

func (d *BinaryRQDistancer) QueryCode() RQMultiBitCode {
	return d.cq
}

func (rq *BinaryRotationalQuantizer) NewDistancer(q []float32) *BinaryRQDistancer {
	var cos float32
	if rq.distancer.Type() == "cosine-dot" {
		cos = 1.0
	}
	var l2 float32
	if rq.distancer.Type() == "l2-squared" {
		l2 = 1.0
	}
	return &BinaryRQDistancer{
		query:     q,
		distancer: rq.distancer,
		rq:        rq,
		cos:       cos,
		l2:        l2,
		cq:        rq.encodeQuery(q),
	}
}

func HammingDist(x, y []uint64) int {
	var count int
	for i := range x {
		count += bits.OnesCount64(x[i] ^ y[i])
	}
	return count
}

// Exported in case we need to use it later.
func HammingDistSIMD(x, y []uint64) float32 {
	return hammingBitwiseImpl(x, y)
}

// Notes:
// SIMD only seems to outperform bits.OnesCount64 once the dimensionality is
// greater than ~512, at least on an M4 (ARM neon). However adding the
// if-statement to determine whether to use SIMD also comes at a cost.
// For binary quantization we always use SIMD, so maybe that is the way to go.
func (d *BinaryRQDistancer) Distance(x []uint64) (float32, error) {
	cx := RQOneBitCode(x)
	bits := cx.Bits()
	const hammingDistSIMDThreshold = 512
	if d.cq.Dimension < hammingDistSIMDThreshold {
		dot := 31 * d.cq.Dimension
		dot -= HammingDist(d.cq.bits0, bits) << 1
		dot -= HammingDist(d.cq.bits1, bits) << 2
		dot -= HammingDist(d.cq.bits2, bits) << 3
		dot -= HammingDist(d.cq.bits3, bits) << 4
		dot -= HammingDist(d.cq.bits4, bits) << 5
		dotEstimate := d.cq.Step * cx.Step() * float32(dot)
		return d.l2*(cx.SquaredNorm()+d.cq.SquaredNorm) + d.cos - (1.0+d.l2)*dotEstimate, nil
	}
	dot := float32(31 * d.cq.Dimension)
	dot -= 2 * HammingDistSIMD(d.cq.bits0, bits)
	dot -= 4 * HammingDistSIMD(d.cq.bits1, bits)
	dot -= 8 * HammingDistSIMD(d.cq.bits2, bits)
	dot -= 16 * HammingDistSIMD(d.cq.bits3, bits)
	dot -= 32 * HammingDistSIMD(d.cq.bits4, bits)
	dotEstimate := d.cq.Step * cx.Step() * dot
	return d.l2*(cx.SquaredNorm()+d.cq.SquaredNorm) + d.cos - (1.0+d.l2)*dotEstimate, nil
}

func (d *BinaryRQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.query) > 0 {
		return d.distancer.SingleDist(d.query, x)
	}
	cx := d.rq.Encode(x)
	return d.Distance(cx)
}

// DistanceBetweenCompressedVectors is used in:
// 1. Distance in compression_distance_bag.go -> selectNeighborsHeuristic in compression_distance_bag.go
// 2. DistanceBetweenCompressedVectorsFromIDs -> distBetweenNodes -> connectNeighborAtLevel
func (brq *BinaryRotationalQuantizer) DistanceBetweenCompressedVectors(x, y []uint64) (float32, error) {
	cx, cy := RQOneBitCode(x), RQOneBitCode(y)
	signDot := cx.Dimension() - 2*HammingDist(cx.Bits(), cy.Bits())
	dotEstimate := cx.Step() * cy.Step() * float32(signDot)
	return brq.l2*(cx.SquaredNorm()+cy.SquaredNorm()) + brq.cos - (1.0+brq.l2)*dotEstimate, nil
}

func (brq *BinaryRotationalQuantizer) CompressedBytes(compressed []uint64) []byte {
	slice := make([]byte, len(compressed)*8)
	for i := range compressed {
		binary.LittleEndian.PutUint64(slice[i*8:], compressed[i])
	}
	return slice
}

func (brq *BinaryRotationalQuantizer) FromCompressedBytes(compressed []byte) []uint64 {
	l := len(compressed) / 8
	if len(compressed)%8 != 0 {
		l++
	}
	slice := make([]uint64, l)

	for i := range slice {
		slice[i] = binary.LittleEndian.Uint64(compressed[i*8:])
	}
	return slice
}

func (brq *BinaryRotationalQuantizer) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]uint64) []uint64 {
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

	for i := range slice {
		slice[i] = binary.LittleEndian.Uint64(compressed[i*8:])
	}
	return slice
}

func (brq *BinaryRotationalQuantizer) NewCompressedQuantizerDistancer(c []uint64) quantizerDistancer[uint64] {
	restored := brq.Restore(c)
	return brq.NewDistancer(restored)
}

func (brq *BinaryRotationalQuantizer) NewQuantizerDistancer(vec []float32) quantizerDistancer[uint64] {
	return brq.NewDistancer(vec)
}

func (brq *BinaryRotationalQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[uint64]) {
}

type BRQData struct {
	InputDim  uint32
	QueryBits uint32
	Rotation  FastRotation
	Rounding  []float32
}

func (brq *BinaryRotationalQuantizer) PersistCompression(logger CommitLogger) {
	logger.AddBRQCompression(BRQData{
		InputDim:  brq.inputDim,
		QueryBits: 5,
		Rotation:  *brq.rotation,
		Rounding:  brq.rounding,
	})
}

type BinaryRQStats struct {
	dataBits  uint32
	queryBits uint32
}

func (brq BinaryRQStats) CompressionType() string {
	return "rq"
}

func (brq BinaryRQStats) CompressionRatio(dimensionality int) float64 {
	// RQ compression: original size = inputDim * 4 bytes (float32)
	// compressed size = 8 bytes (metadata) + outputDim * 1 bit (compressed data)
	// where outputDim is typically the same as inputDim after rotation
	originalSize := dimensionality * 4
	compressedSize := 8 + (dimensionality / 8) // 8 bytes metadata + 1 bit per dimension
	return float64(originalSize) / float64(compressedSize)
}

func (brq *BinaryRotationalQuantizer) Stats() CompressionStats {
	return BinaryRQStats{
		dataBits:  1,
		queryBits: uint32(5),
	}
}
