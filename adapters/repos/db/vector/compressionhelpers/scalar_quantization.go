//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers

import (
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

const (
	codes  = 255.0
	codes2 = codes * codes
)

type ScalarQuantizer struct {
	a          float32
	b          float32
	a2         float32
	ab         float32
	ib2        float32
	distancer  distancer.Provider
	dimensions int
}

type SQData struct {
	A          float32
	B          float32
	Dimensions uint16
}

func (sq *ScalarQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.Errorf("vector lengths don't match: %d vs %d",
			len(x), len(y))
	}
	switch sq.distancer.Type() {
	case "l2-squared":
		return sq.a2 * float32(l2SquaredByteImpl(x[:len(x)-8], y[:len(y)-8])), nil
	case "dot":
		return -(sq.a2*float32(dotByteImpl(x[:len(x)-8], y[:len(y)-8])) + sq.ab*float32(sq.norm(x)+sq.norm(y)) + sq.ib2), nil
	case "cosine-dot":
		return 1 - (sq.a2*float32(dotByteImpl(x[:len(x)-8], y[:len(y)-8])) + sq.ab*float32(sq.norm(x)+sq.norm(y)) + sq.ib2), nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", sq.distancer)
}

func (pq *ScalarQuantizer) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]byte) []byte {
	if len(*buffer) < len(compressed) {
		*buffer = make([]byte, len(compressed)*1000)
	}

	// take from end so we can address the start of the buffer
	out := (*buffer)[len(*buffer)-len(compressed):]
	copy(out, compressed)
	*buffer = (*buffer)[:len(*buffer)-len(compressed)]

	return out
}

func NewScalarQuantizer(data [][]float32, distance distancer.Provider) *ScalarQuantizer {
	if len(data) == 0 {
		return nil
	}

	sq := &ScalarQuantizer{
		distancer:  distance,
		dimensions: len(data[0]),
	}
	sq.b = data[0][0]
	for i := 0; i < len(data); i++ {
		vec := data[i]
		for _, x := range vec {
			if x < sq.b {
				sq.a += sq.b - x
				sq.b = x
			} else if x-sq.b > sq.a {
				sq.a = x - sq.b
			}
		}
	}
	sq.a2 = sq.a * sq.a / codes2
	sq.ab = sq.a * sq.b / codes
	sq.ib2 = sq.b * sq.b * float32(sq.dimensions)
	return sq
}

func RestoreScalarQuantizer(a, b float32, dimensions uint16, distance distancer.Provider) (*ScalarQuantizer, error) {
	if a == 0 {
		return nil, errors.New("invalid range value while restoring SQ settings")
	}

	sq := &ScalarQuantizer{
		distancer:  distance,
		a:          a,
		b:          b,
		a2:         a * a / codes2,
		ab:         a * b / codes,
		ib2:        b * b * float32(dimensions),
		dimensions: int(dimensions),
	}
	return sq, nil
}

func codeFor(x, a, b, codes float32) byte {
	if x < b {
		return 0
	} else if x-b > a {
		return byte(codes)
	} else {
		return byte(math.Floor(float64((x - b) * codes / a)))
	}
}

func (sq *ScalarQuantizer) Encode(vec []float32) []byte {
	var sum uint32 = 0
	var sum2 uint32 = 0
	code := make([]byte, len(vec)+8)
	for i := 0; i < len(vec); i++ {
		code[i] = codeFor(vec[i], sq.a, sq.b, codes)
		sum += uint32(code[i])
		sum2 += uint32(code[i]) * uint32(code[i])
	}
	binary.BigEndian.PutUint32(code[len(vec):], sum)
	binary.BigEndian.PutUint32(code[len(vec)+4:], sum2)
	return code
}

type SQDistancer struct {
	x          []float32
	sq         *ScalarQuantizer
	compressed []byte
}

func (sq *ScalarQuantizer) NewDistancer(a []float32) *SQDistancer {
	sum := float32(0)
	sum2 := float32(0)
	for _, x := range a {
		sum += x
		sum2 += (x * x)
	}
	return &SQDistancer{
		x:          a,
		sq:         sq,
		compressed: sq.Encode(a),
	}
}

func (d *SQDistancer) Distance(x []byte) (float32, error) {
	return d.sq.DistanceBetweenCompressedVectors(d.compressed, x)
}

func (d *SQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.x) > 0 {
		return d.sq.distancer.SingleDist(d.x, x)
	}
	xComp := d.sq.Encode(x)
	return d.sq.DistanceBetweenCompressedVectors(d.compressed, xComp)
}

func (sq *ScalarQuantizer) NewQuantizerDistancer(a []float32) quantizerDistancer[byte] {
	return sq.NewDistancer(a)
}

func (sq *ScalarQuantizer) NewCompressedQuantizerDistancer(a []byte) quantizerDistancer[byte] {
	return &SQDistancer{
		x:          nil,
		sq:         sq,
		compressed: a,
	}
}

func (sq *ScalarQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {}

func (sq *ScalarQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *ScalarQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *ScalarQuantizer) PersistCompression(logger CommitLogger) {
	logger.AddSQCompression(SQData{
		A:          sq.a,
		B:          sq.b,
		Dimensions: uint16(sq.dimensions),
	})
}

func (sq *ScalarQuantizer) norm(code []byte) uint32 {
	return binary.BigEndian.Uint32(code[len(code)-8:])
}

type SQStats struct {
	A float32 `json:"a"`
	B float32 `json:"b"`
}

func (s SQStats) CompressionType() string {
	return "sq"
}

func (sq *ScalarQuantizer) Stats() CompressionStats {
	return SQStats{
		A: sq.a,
		B: sq.b,
	}
}
