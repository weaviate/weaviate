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

var l2SquaredByteImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		diff := uint32(a[i]) - uint32(b[i])
		sum += diff * diff
	}

	return sum
}

var dotByteImpl func(a, b []uint8) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}

	return sum
}

var dotFloatByteImpl func(a []float32, b []uint8) float32 = func(a []float32, b []uint8) float32 {
	var sum float32

	for i := range a {
		sum += a[i] * float32(b[i])
	}

	return sum
}

type ScalarQuantizer struct {
	a         float32
	b         float32
	a2        float32
	ab        float32
	ib2       float32
	distancer distancer.Provider
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

func (sq *ScalarQuantizer) dot(x []float32, encoded []byte, normX float32) (float32, error) {
	return -(sq.a/codes*float32(dotFloatByteImpl(x, encoded[:len(encoded)-8])) + sq.b*normX), nil
}

func (sq *ScalarQuantizer) l2(x []float32, encoded []byte, normX, normX2 float32) (float32, error) {
	return normX2 + float32(sq.norm2(encoded))*sq.a2 + sq.ib2 - 2*sq.a/codes*dotFloatByteImpl(x, encoded[:len(encoded)-8]) - 2*sq.b*normX + 2*sq.a*sq.b/codes*float32(sq.norm(encoded)), nil
}

func (sq *ScalarQuantizer) DistanceBetweenCompressedAndUncompressedVectors2(x []float32, encoded []byte, normX, normX2 float32) (float32, error) {
	switch sq.distancer.Type() {
	case "l2-squared":
		return sq.l2(x, encoded, normX, normX2)
	case "cosine-dot":
		d, err := sq.dot(x, encoded, normX)
		return 1 + d, err
	case "dot":
		return sq.dot(x, encoded, normX)
	}
	return 0, errors.Errorf("Distance not supported yet %s", sq.distancer)
}

func NewScalarQuantizer(data [][]float32, distance distancer.Provider) *ScalarQuantizer {
	if len(data) == 0 {
		return nil
	}

	sq := &ScalarQuantizer{
		distancer: distance,
	}
	sq.b = data[0][0]
	for i := 1; i < len(data); i++ {
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
	sq.ib2 = sq.b * sq.b * float32(len(data[0]))
	return sq
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
	normX2     float32
	sq         *ScalarQuantizer
	compressed []byte
	normX      float32
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
		normX:      sum,
		normX2:     sum2,
	}
}

func (d *SQDistancer) Distance(x []byte) (float32, bool, error) {
	if len(d.x) > 0 {
		dist, err := d.sq.DistanceBetweenCompressedAndUncompressedVectors2(d.x, x, d.normX, d.normX2)
		return dist, err == nil, err
	}
	return d.sq.a2 * float32(l2SquaredByteImpl(d.compressed, x)), true, nil
}

func (d *SQDistancer) DistanceToFloat(x []float32) (float32, bool, error) {
	if len(d.x) > 0 {
		return d.sq.distancer.SingleDist(d.x, x)
	}
	xComp := d.sq.Encode(x)
	dist, err := d.sq.DistanceBetweenCompressedVectors(d.compressed, xComp)
	return dist, err == nil, err
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

func (sq *ScalarQuantizer) ExposeFields() PQData {
	return PQData{}
}

func (sq *ScalarQuantizer) norm(code []byte) uint32 {
	return binary.BigEndian.Uint32(code[len(code)-8:])
}

func (sq *ScalarQuantizer) norm2(code []byte) uint32 {
	return binary.BigEndian.Uint32(code[len(code)-4:])
}
