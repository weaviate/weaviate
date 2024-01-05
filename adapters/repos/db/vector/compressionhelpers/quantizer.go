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

import "encoding/binary"

type quantizerDistancer[T byte | uint64] interface {
	Distance(x []T) (float32, bool, error)
	DistanceToFloat(x []float32) (float32, bool, error)
}

type quantizer[T byte | uint64] interface {
	DistanceBetweenCompressedVectors(x, y []T) (float32, error)
	DistanceBetweenCompressedAndUncompressedVectors(x []float32, encoded []T) (float32, error)
	Encode(vec []float32) []T
	NewQuantizerDistancer(a []float32) quantizerDistancer[T]
	NewCompressedQuantizerDistancer(a []T) quantizerDistancer[T]
	ReturnQuantizerDistancer(distancer quantizerDistancer[T])
	CompressedBytes(compressed []T) []byte
	FromCompressedBytes(compressed []byte) []T
	ExposeFields() PQData
}

func (bq *BinaryQuantizer) ExposeFields() PQData {
	return PQData{}
}

func (bq *BinaryQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, y []uint64) (float32, error) {
	encoded := bq.Encode(x)
	return bq.DistanceBetweenCompressedVectors(encoded, y)
}

func (pq *ProductQuantizer) NewQuantizerDistancer(vec []float32) quantizerDistancer[byte] {
	return pq.NewDistancer(vec)
}

func (pq *ProductQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {
	concreteDistancer := distancer.(*PQDistancer)
	if concreteDistancer == nil {
		return
	}
	pq.ReturnDistancer(concreteDistancer)
}

func (bq *BinaryQuantizer) CompressedBytes(compressed []uint64) []byte {
	slice := make([]byte, len(compressed)*8)
	for i := range compressed {
		binary.LittleEndian.PutUint64(slice[i*8:], compressed[i])
	}
	return slice
}

func (bq *BinaryQuantizer) FromCompressedBytes(compressed []byte) []uint64 {
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

func (pq *ProductQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (pq *ProductQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

type BQDistancer struct {
	x          []float32
	bq         *BinaryQuantizer
	compressed []uint64
}

func (bq *BinaryQuantizer) NewDistancer(a []float32) *BQDistancer {
	return &BQDistancer{
		x:          a,
		bq:         bq,
		compressed: bq.Encode(a),
	}
}

func (bq *BinaryQuantizer) NewCompressedQuantizerDistancer(a []uint64) quantizerDistancer[uint64] {
	return &BQDistancer{
		x:          nil,
		bq:         bq,
		compressed: a,
	}
}

func (d *BQDistancer) Distance(x []uint64) (float32, bool, error) {
	dist, err := d.bq.DistanceBetweenCompressedVectors(d.compressed, x)
	return dist, err == nil, err
}

func (d *BQDistancer) DistanceToFloat(x []float32) (float32, bool, error) {
	if len(d.x) > 0 {
		return d.bq.distancer.SingleDist(d.x, x)
	}
	xComp := d.bq.Encode(x)
	dist, err := d.bq.DistanceBetweenCompressedVectors(d.compressed, xComp)
	return dist, err == nil, err
}

func (bq *BinaryQuantizer) NewQuantizerDistancer(vec []float32) quantizerDistancer[uint64] {
	return bq.NewDistancer(vec)
}

func (bq *BinaryQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[uint64]) {}
