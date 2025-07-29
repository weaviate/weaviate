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
	"slices"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type BinaryRotationalQuantizer struct {
	inputDim  uint32
	rotation  *FastRotation
	distancer distancer.Provider
	queryBits int
	rounding  []float32
	l2        float32
	cos       float32
}

const (
	DefaultRQQueryBits = 4
)

func NewBinaryRotationalQuantizer(inputDim int, queryBits int, seed uint64, distancer distancer.Provider) *BinaryRotationalQuantizer {
	rotationRounds := 5 // 4 might be sufficient, but 3 is probably not enough.
	rotation := NewFastRotation(inputDim, rotationRounds, seed)
	cos, l2, err := distancerIndicatorsAndError(distancer)
	if err != nil {
		return nil
	}
	// Randomized rounding for the query quantization to make the estimator unbiased.
	// It may produce better recall to not use randomized rounding since adding the random noise increases the quantization error.
	// With 8-bit RQ we are not using randomized rounding.
	rounding := make([]float32, rotation.OutputDim)
	rng := rand.New(rand.NewPCG(seed, 0x4f8ebf70e130707f))
	for i := range rounding {
		rounding[i] = 0.5 - rng.Float32()
	}

	rq := &BinaryRotationalQuantizer{
		inputDim:  uint32(inputDim),
		rotation:  rotation,
		distancer: distancer,
		queryBits: queryBits,
		rounding:  rounding,
		l2:        l2,
		cos:       cos,
	}
	return rq
}

func RestoreBinaryRotationalQuantizer(inputDim int, queryBits int, outputDim int, rounds int, swaps [][]Swap, signs [][]float32, rounding []float32, distancer distancer.Provider) (*BinaryRotationalQuantizer, error) {
	cos, l2, err := distancerIndicatorsAndError(distancer)
	if err != nil {
		return nil, err
	}
	rq := &BinaryRotationalQuantizer{
		inputDim:  uint32(inputDim),
		rotation:  RestoreFastRotation(outputDim, rounds, swaps, signs),
		queryBits: queryBits,
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
// The sign is turned from s in {-1, +1} to b in {0, 1} by using s = 1 - 2b.
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

func (c RQOneBitCode) OnesCount() float32 {
	return getFloat32Lower(c[1])
}

func (c RQOneBitCode) setOnesCount(x float32) {
	c[1] = putFloat32Lower(c[1], x)
}

// Maybe we can get away with a smaller overhead. Using 128 bits seems like too
// much. We could of course pack OnesCount into a 16-bit integer, and maybe Step
// and SquaredNorm could be stored as multiples of the same float. I think they
// also drop some hints about what they store in the paper.
const oneBitFieldWords = 2

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
			if rx[i] < 0 {
				bits |= bit
				l1Norm += -rx[i]
			} else {
				l1Norm += rx[i]
			}
			l2NormSquared += rx[i] * rx[i]
			i++
		}
		code.Bits()[b] = bits
	}
	code.setSquaredNorm(l2NormSquared)
	code.setStep(l2NormSquared / l1Norm) // Is this robust to non-unit vectors? Double-check the paper..
	code.setOnesCount(onesCount(code.Bits()))
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
	for i := 0; i < dim; i++ {
		block := i / 64
		bit := uint(i) % 64
		if (bits[block] & (1 << bit)) != 0 {
			x[i] = -step
		} else {
			x[i] = step
		}
	}
	return x
}

// TODO: replace this with SIMD-optimized instructions. A version of this can be
// implemented using the HammingBitwise SIMD functions we already have, but it
// may require changing some aspects of the encoding to use signs instead of bits.
func binaryDot(x, y []uint64) float32 {
	var count int
	for i := range x {
		count += bits.OnesCount64(x[i] & y[i])
	}
	return float32(count)
}

func onesCount(x []uint64) float32 {
	return binaryDot(x, x)
}

// The binary encoding of q.
// We quantize q using k bits using the format:
// q_i = lower + step*v_i
//
//	= lower + step * (b_i,k-1 * 2^(k-1) + b_i,k-2 * 2^(k-2) + ...)
//
// where b_i,j is the j-th bit of the integer quantization v_i of q_i.
// bits stores the queryBits * dimension bits of the integer quantization
// described above. The bits are laid out so that bits[:dimension] contains
// the lowest order bit of each entry, bits[dimension:2*dimension] contains
// the next-lowest order and so on...
type RQMultiBitCode struct {
	QueryBits    int
	Dimension    int
	SquaredNorm  float32
	Lower        float32
	Step         float32
	CodeSum      float32
	blocksPerBit int
	bits         []uint64
}

func (c *RQMultiBitCode) Bits(k int) []uint64 {
	return c.bits[k*c.blocksPerBit : (k+1)*c.blocksPerBit]
}

func (c RQMultiBitCode) String() string {
	var sb strings.Builder
	for i := range c.QueryBits {
		sb.WriteString(fmt.Sprintf("Bits(%d)[0]: %064b, ", i, c.Bits(i)[0]))
	}
	return fmt.Sprintf("RQMultiBitCode{Lower: %.4f, Step: %.4f, SquaredNorm: %.4f, CodeSum: %.4f, Bits(): %s",
		c.Lower, c.Step, c.SquaredNorm, c.CodeSum, sb.String())
}

func extractBit(x uint64, k int) uint64 {
	return (x & (1 << k)) >> k
}

// TODO: Handle corner cases as we do for 8-bit RQ.
func (rq *BinaryRotationalQuantizer) encodeQuery(x []float32) RQMultiBitCode {
	rx := rq.rotation.Rotate(x)
	var maxCode uint8 = (1 << rq.queryBits) - 1
	lower := slices.Min(rx)
	step := (slices.Max(rx) - lower) / float32(maxCode)

	if step <= 0 {
		return RQMultiBitCode{}
	}

	blocksPerBit := len(rx) >> 6
	queryBits := rq.queryBits
	bits := make([]uint64, blocksPerBit*queryBits)

	// Encode each rotated entry to an unsigned integer and extract the bits.
	// This can likely be optimized a lot by processing in blocks, similarly to how to encode the data points and BQ.
	var squaredNorm float32
	var codeSum float32
	for i, v := range rx {
		c := uint64((v-lower)/step + rq.rounding[i] + 0.5)
		block := i >> 6
		bitPos := uint64(i) & ((1 << 6) - 1)
		for j := range queryBits {
			bit := extractBit(c, j)
			blockPos := j*blocksPerBit + block
			bits[blockPos] |= bit << bitPos
		}
		codeSum += lower + step*float32(c)
		squaredNorm += rx[i] * rx[i]
	}
	return RQMultiBitCode{
		QueryBits:    queryBits,
		Dimension:    len(rx),
		SquaredNorm:  squaredNorm,
		Lower:        lower,
		Step:         step,
		CodeSum:      codeSum,
		blocksPerBit: blocksPerBit,
		bits:         bits,
	}
}

func estimateDotProduct(cq RQMultiBitCode, cx RQOneBitCode) float32 {
	dots := cq.Lower * cx.OnesCount()
	for i := range cq.QueryBits {
		dots += cq.Step * float32(int(1)<<i) * binaryDot(cq.Bits(i), cx.Bits())
	}
	return cx.Step() * (cq.CodeSum - 2*dots)
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

func (d *BinaryRQDistancer) Distance(x []uint64) (float32, error) {
	cx := RQOneBitCode(x)
	dotEstimate := estimateDotProduct(d.cq, cx)
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
	dots := cy.OnesCount() * cx.OnesCount()
	dots += cy.Step() * float32(int(1)<<0) * binaryDot(cy.Bits(), cx.Bits())
	dotEstimate := cy.Step() * (cx.SquaredNorm() - 2*dots)
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
		QueryBits: uint32(brq.queryBits),
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
	// compressed size = 12 bytes (metadata) + outputDim * 1 bit (compressed data)
	// where outputDim is typically the same as inputDim after rotation
	originalSize := dimensionality * 4
	compressedSize := 12 + (dimensionality / 8) // 12 bytes metadata + 1 bit per dimension
	return float64(originalSize) / float64(compressedSize)
}

func (brq *BinaryRotationalQuantizer) Stats() CompressionStats {
	return BinaryRQStats{
		dataBits:  1,
		queryBits: uint32(brq.queryBits),
	}
}
