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
	Distance(x []T) (float32, error)
	DistanceToFloat(x []float32) (float32, error)
}

type quantizer[T byte | uint64] interface {
	DistanceBetweenCompressedVectors(x, y []T) (float32, error)
	Encode(vec []float32) []T
	NewQuantizerDistancer(a []float32) quantizerDistancer[T]
	NewCompressedQuantizerDistancer(a []T) quantizerDistancer[T]
	ReturnQuantizerDistancer(distancer quantizerDistancer[T])
	CompressedBytes(compressed []T) []byte
	FromCompressedBytes(compressed []byte) []T
	PersistCompression(logger CommitLogger)
	Stats() CompressionStats

	// FromCompressedBytesWithSubsliceBuffer is like FromCompressedBytes, but
	// instead of allocating a new slice you can pass in a buffer to use. It will
	// slice something off of that buffer. If the buffer is too small, it will
	// allocate a new buffer.
	FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]T) []T
}

func (bq *BinaryQuantizer) PersistCompression(logger CommitLogger) {
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

// FromCompressedBytesWithSubsliceBuffer is like FromCompressedBytes, but
// instead of allocating a new slice you can pass in a buffer to use. It will
// slice something off of that buffer. If the buffer is too small, it will
// allocate a new buffer.
func (bq *BinaryQuantizer) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]uint64) []uint64 {
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

func (pq *ProductQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (pq *ProductQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

func (pq *ProductQuantizer) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]byte) []byte {
	if len(*buffer) < len(compressed) {
		*buffer = make([]byte, len(compressed)*1000)
	}

	// take from end so we can address the start of the buffer
	out := (*buffer)[len(*buffer)-len(compressed):]
	copy(out, compressed)
	*buffer = (*buffer)[:len(*buffer)-len(compressed)]

	return out
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

func (d *BQDistancer) Distance(x []uint64) (float32, error) {
	return d.bq.DistanceBetweenCompressedVectors(d.compressed, x)
}

func (d *BQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.x) > 0 {
		return d.bq.distancer.SingleDist(d.x, x)
	}
	xComp := d.bq.Encode(x)
	return d.bq.DistanceBetweenCompressedVectors(d.compressed, xComp)
}

func (bq *BinaryQuantizer) NewQuantizerDistancer(vec []float32) quantizerDistancer[uint64] {
	return bq.NewDistancer(vec)
}

func (bq *BinaryQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[uint64]) {}
