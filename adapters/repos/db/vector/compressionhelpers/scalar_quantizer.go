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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

var l2SquaredByteImpl func(a, b []byte) uint16 = func(a, b []byte) uint16 {
	var sum uint16

	for i := range a {
		diff := uint16(a[i]) - uint16(b[i])
		sum += diff * diff
	}

	return sum
}

var dotByteImpl func(a, b []byte) uint16 = func(a, b []byte) uint16 {
	var sum uint16

	for i := range a {
		sum += uint16(a[i]) * uint16(b[i])
	}

	return sum
}

type scalarQuantizer struct {
	a         float32
	b         float32
	a2        float32
	ab        float32
	ib2       float32
	distancer distancer.Provider
}

func (sq *scalarQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.Errorf("vector lengths don't match: %d vs %d",
			len(x), len(y))
	}
	switch sq.distancer.Type() {
	case "l2-squared":
		return sq.a2 * float32(l2SquaredByteImpl(x[:len(x)-2], y[:len(y)-2])), nil
	case "dot":
		return -(sq.a2*float32(dotByteImpl(x[:len(x)-2], y[:len(y)-2])) + sq.ab*float32(sq.norm(x)+sq.norm(y)) + sq.ib2), nil
	case "cosine-dot":
		return 1 - (sq.a2*float32(dotByteImpl(x[:len(x)-2], y[:len(y)-2])) + sq.ab*float32(sq.norm(x)+sq.norm(y)) + sq.ib2), nil
	}
	return 0, errors.Errorf("Distance not supported yet %s", sq.distancer)
}

func (sq *scalarQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, encoded []byte) (float32, error) {
	return sq.DistanceBetweenCompressedVectors(sq.Encode(x), encoded)
}

func NewScalarQuantizer(data [][]float32, distance distancer.Provider) *scalarQuantizer {
	if len(data) == 0 {
		return nil
	}

	sq := &scalarQuantizer{
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
	sq.a2 = sq.a * sq.a / 2500.0
	sq.ab = sq.a * sq.b / 50.0
	sq.ib2 = sq.b * sq.b * float32(len(data[0]))
	return sq
}

func (sq *scalarQuantizer) Encode(vec []float32) []byte {
	var sum uint16 = 0
	code := make([]byte, len(vec)+2)
	for i := 0; i < len(vec); i++ {
		if vec[i] < sq.b {
			code[i] = 0
		} else if vec[i]-sq.b > sq.a {
			code[i] = 50
		} else {
			code[i] = byte((vec[i] - sq.b) * 50 / sq.a)
		}
		sum += uint16(code[i])
	}
	binary.BigEndian.PutUint16(code[len(vec):], sum)
	return code
}

func (sq *scalarQuantizer) norm(code []byte) uint16 {
	return binary.BigEndian.Uint16(code[len(code)-2:])
}

type SQDistancer struct {
	x          []float32
	sq         *scalarQuantizer
	compressed []byte
}

func (sq *scalarQuantizer) NewDistancer(a []float32) *SQDistancer {
	return &SQDistancer{
		x:          a,
		sq:         sq,
		compressed: sq.Encode(a),
	}
}

func (d *SQDistancer) Distance(x []byte) (float32, bool, error) {
	dist, err := d.sq.DistanceBetweenCompressedVectors(d.compressed, x)
	return dist, err == nil, err
}

func (d *SQDistancer) DistanceToFloat(x []float32) (float32, bool, error) {
	if len(d.x) > 0 {
		return d.sq.distancer.SingleDist(d.x, x)
	}
	xComp := d.sq.Encode(x)
	dist, err := d.sq.DistanceBetweenCompressedVectors(d.compressed, xComp)
	return dist, err == nil, err
}

func (sq *scalarQuantizer) NewQuantizerDistancer(a []float32) quantizerDistancer[byte] {
	return sq.NewDistancer(a)
}

func (sq *scalarQuantizer) NewCompressedQuantizerDistancer(a []byte) quantizerDistancer[byte] {
	return &SQDistancer{
		x:          nil,
		sq:         sq,
		compressed: a,
	}
}

func (sq *scalarQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {}

func (sq *scalarQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *scalarQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

func (sq *scalarQuantizer) ExposeFields() PQData {
	return PQData{}
}
