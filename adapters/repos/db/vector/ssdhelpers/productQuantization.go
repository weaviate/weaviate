//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package ssdhelpers

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type Encoder byte

const (
	UseTileEncoder   Encoder = 0
	UseKMeansEncoder Encoder = 1
)

type DistanceLookUpTable struct {
	calculated  [][]atomic.Bool
	distances   [][]float32
	center      []float32
	segmentSize int
}

func NewDistanceLookUpTable(segments int, centroids int, center []float32) *DistanceLookUpTable {
	distances := make([][]float32, segments)
	calculated := make([][]atomic.Bool, segments)
	for m := 0; m < segments; m++ {
		distances[m] = make([]float32, centroids)
		calculated[m] = make([]atomic.Bool, centroids)
	}

	return &DistanceLookUpTable{
		distances:   distances,
		calculated:  calculated,
		center:      center,
		segmentSize: len(center) / segments,
	}
}

func (lut *DistanceLookUpTable) LookUp(
	encoded []byte,
	pq *ProductQuantizer,
) float32 {
	var sum float32

	k := 0
	for i := range pq.kms {
		c := pq.ExtractCode(encoded, i)
		if lut.calculated[i][c].Load() {
			sum += lut.distances[i][c]
			k += lut.segmentSize
		} else {
			centroid := pq.kms[i].Centroid(c)
			dist := float32(0)
			for _, centroidX := range centroid {
				dist += pq.distance.Step(lut.center[k], centroidX)
				k++
			}
			lut.distances[i][c] = dist
			lut.calculated[i][c].Store(true)
			sum += dist
		}
	}
	return pq.distance.Wrap(sum)
}

type ProductQuantizer struct {
	ks                  int // centroids
	bits                int // bits amount
	bytes               int // bytes amount
	m                   int // segments
	ds                  int // dimensions per segment
	distance            distancer.Provider
	dimensions          int
	kms                 []PQEncoder
	encoderType         Encoder
	encoderDistribution EncoderDistribution
	extractCode         func(encoded []byte) uint64
	putCode             func(code uint64, buffer []byte)
	codingMask          uint64
	sharpCodes          bool
	useBitsEncoding     bool
	ExtractCode         func(encoded []byte, index int) uint64
	PutCode             func(code uint64, encoded []byte, index int)
}

type PQData struct {
	Ks                  uint16
	M                   uint16
	Dimensions          uint16
	EncoderType         Encoder
	EncoderDistribution byte
	Encoders            []PQEncoder
	UseBitsEncoding     bool
}

type PQEncoder interface {
	Encode(x []float32) uint64
	Centroid(b uint64) []float32
	Add(x []float32)
	Fit(data [][]float32) error
	ExposeDataForRestore() []byte
}

// ToDo: Add a settings struct. Already necessary!!
func NewProductQuantizer(segments int, centroids int, useBitsEncoding bool, distance distancer.Provider, dimensions int, encoderType Encoder, encoderDistribution EncoderDistribution) (*ProductQuantizer, error) {
	if segments <= 0 {
		return nil, errors.New("Segments cannot be 0 nor negative")
	}
	if dimensions%segments != 0 {
		return nil, errors.New("Segments should be an integer divisor of dimensions")
	}
	pq := &ProductQuantizer{
		ks:                  centroids,
		bits:                int(math.Log2(float64(centroids))),
		bytes:               int(math.Log2(float64(centroids-1)))/8 + 1,
		m:                   segments,
		ds:                  int(dimensions / segments),
		distance:            distance,
		dimensions:          dimensions,
		encoderType:         encoderType,
		encoderDistribution: encoderDistribution,
		useBitsEncoding:     useBitsEncoding,
	}
	pq.sharpCodes = pq.bits%8 == 0
	if pq.bits > 32 {
		return nil, errors.New("Centroids amount not supported, please use a lower value")
	}
	switch pq.bytes {
	case 1:
		if pq.bits == 8 || !pq.useBitsEncoding {
			pq.extractCode = extractCode8
			pq.putCode = putCode8
			pq.ExtractCode = pq.extractSharpCode
			pq.PutCode = pq.putSharpCode
		} else {
			pq.extractCode = extractCode16
			pq.putCode = putCode16
			pq.ExtractCode = pq.extractBitsCode
			pq.PutCode = pq.putBitsCode
		}
	case 2:
		if pq.bits == 16 || !pq.useBitsEncoding {
			pq.extractCode = extractCode16
			pq.putCode = putCode16
			pq.ExtractCode = pq.extractSharpCode
			pq.PutCode = pq.putSharpCode
		} else {
			pq.extractCode = extractCode24
			pq.putCode = putCode24
			pq.ExtractCode = pq.extractBitsCode
			pq.PutCode = pq.putBitsCode
		}
	case 3:
		if pq.bits == 32 || !pq.useBitsEncoding {
			pq.extractCode = extractCode24
			pq.putCode = putCode24
			pq.ExtractCode = pq.extractSharpCode
			pq.PutCode = pq.putSharpCode
		} else {
			pq.extractCode = extractCode32
			pq.putCode = putCode32
			pq.ExtractCode = pq.extractBitsCode
			pq.PutCode = pq.putBitsCode
		}
	}
	pq.codingMask = uint64(math.Pow(2, float64(pq.bits))) - 1
	return pq, nil
}

func NewProductQuantizerWithEncoders(segments int, centroids int, useBitsEncoding bool, distance distancer.Provider, dimensions int, encoderType Encoder, encoders []PQEncoder) (*ProductQuantizer, error) {
	pq, err := NewProductQuantizer(segments, centroids, useBitsEncoding, distance, dimensions, encoderType, LogNormalEncoderDistribution)
	if err != nil {
		return nil, err
	}

	pq.kms = encoders
	return pq, nil
}

func extractCode8(encoded []byte) uint64 {
	return uint64(encoded[0])
}

func extractCode16(encoded []byte) uint64 {
	return uint64(binary.BigEndian.Uint16(encoded))
}

func extractCode24(encoded []byte) uint64 {
	return uint64(binary.BigEndian.Uint32(encoded)) >> 8
}

func extractCode32(encoded []byte) uint64 {
	return uint64(binary.BigEndian.Uint32(encoded))
}

func putCode8(code uint64, buffer []byte) {
	buffer[0] = byte(code)
}

func putCode16(code uint64, buffer []byte) {
	binary.BigEndian.PutUint16(buffer, uint16(code))
}

func putCode24(code uint64, buffer []byte) {
	binary.BigEndian.PutUint32(buffer, uint32(code<<8))
}

func putCode32(code uint64, buffer []byte) {
	binary.BigEndian.PutUint32(buffer, uint32(code))
}

func (pq *ProductQuantizer) extractBitsCode(encoded []byte, index int) uint64 {
	correctedIndex := index * pq.bits / 8
	code := pq.extractCode(encoded[correctedIndex:])
	if pq.sharpCodes {
		return code
	}
	rest := (index + 1) * pq.bits % 8
	restFromStart := index * pq.bits % 8
	if restFromStart < rest {
		code >>= 16 - rest
	} else {
		code >>= 8 - rest
	}
	return code & pq.codingMask
}

func (pq *ProductQuantizer) extractSharpCode(encoded []byte, index int) uint64 {
	return pq.extractCode(encoded[index*pq.bytes:])
}

func (pq *ProductQuantizer) putBitsCode(code uint64, encoded []byte, index int) {
	correctedIndex := index * pq.bits / 8
	if pq.sharpCodes {
		pq.putCode(code, encoded[correctedIndex:])
		return
	}

	rest := (index + 1) * pq.bits % 8
	restFromStart := index * pq.bits % 8
	if restFromStart < rest {
		code <<= 16 - rest
	} else {
		code <<= 8 - rest
	}
	m1 := uint64(encoded[correctedIndex])
	m2 := uint64(pq.bytes * 8)
	mask := m1 << m2
	code |= mask
	pq.putCode(code, encoded[correctedIndex:])
}

func (pq *ProductQuantizer) putSharpCode(code uint64, encoded []byte, index int) {
	pq.putCode(code, encoded[index*pq.bytes:])
}

func (pq *ProductQuantizer) ExposeFields() PQData {
	return PQData{
		Dimensions:          uint16(pq.dimensions),
		EncoderType:         pq.encoderType,
		Ks:                  uint16(pq.ks),
		M:                   uint16(pq.m),
		EncoderDistribution: byte(pq.encoderDistribution),
		Encoders:            pq.kms,
		UseBitsEncoding:     pq.useBitsEncoding,
	}
}

func (pq *ProductQuantizer) DistanceBetweenCompressedVectors(x, y []byte) float32 {
	dist := float32(0)

	for i := 0; i < pq.m; i++ {
		cX := pq.kms[i].Centroid(pq.ExtractCode(x, i))
		cY := pq.kms[i].Centroid(pq.ExtractCode(y, i))
		for j := range cX {
			dist += pq.distance.Step(cX[j], cY[j])
		}
	}

	return pq.distance.Wrap(dist)
}

func (pq *ProductQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, encoded []byte) float32 {
	dist := float32(0)
	k := 0
	for i := 0; i < pq.m; i++ {
		cY := pq.kms[i].Centroid(pq.ExtractCode(encoded, i))
		for j := range cY {
			dist += pq.distance.Step(x[k], cY[j])
			k++
		}
	}
	return pq.distance.Wrap(dist)
}

type PQDistancer struct {
	x   []float32
	pq  *ProductQuantizer
	lut *DistanceLookUpTable
}

func (pq *ProductQuantizer) NewDistancer(a []float32) *PQDistancer {
	lut := pq.CenterAt(a)
	return &PQDistancer{
		x:   a,
		pq:  pq,
		lut: lut,
	}
}

func (d *PQDistancer) Distance(x []byte) (float32, bool, error) {
	return d.pq.Distance(x, d.lut), true, nil
}

func (pq *ProductQuantizer) Fit(data [][]float32) {
	switch pq.encoderType {
	case UseTileEncoder:
		pq.kms = make([]PQEncoder, pq.m)
		Concurrently(uint64(pq.m), func(_ uint64, i uint64, _ *sync.Mutex) {
			pq.kms[i] = NewTileEncoder(int(math.Log2(float64(pq.ks))), int(i), pq.encoderDistribution)
			for j := 0; j < len(data); j++ {
				pq.kms[i].Add(data[j])
			}
			pq.kms[i].Fit(data)
		})
	case UseKMeansEncoder:
		pq.kms = make([]PQEncoder, pq.m)
		Concurrently(uint64(pq.m), func(_ uint64, i uint64, _ *sync.Mutex) {
			pq.kms[i] = NewKMeansWithFilter(
				int(pq.ks),
				pq.ds,
				FilterSegment(int(i), pq.ds),
			)
			err := pq.kms[i].Fit(data)
			if err != nil {
				panic(err)
			}
		})
	}
	/*for i := 0; i < 1; i++ {
		fmt.Println("********")
		centers := make([]float64, 0)
		for c := 0; c < pq.ks; c++ {
			centers = append(centers, float64(pq.kms[i].Centroid(byte(c))[0]))
		}
		hist := histogram.Hist(60, centers)
		histogram.Fprint(os.Stdout, hist, histogram.Linear(5))
	}*/
}

func (pq *ProductQuantizer) Encode(vec []float32) []byte {
	codes := make([]byte, pq.m*pq.bytes)
	for i := 0; i < pq.m; i++ {
		pq.PutCode(pq.kms[i].Encode(vec), codes, i)
	}
	return codes
}

func (pq *ProductQuantizer) Decode(code []byte) []float32 {
	vec := make([]float32, 0, len(code)/pq.bytes)
	for i := 0; i < pq.m; i++ {
		vec = append(vec, pq.kms[i].Centroid(pq.ExtractCode(code, i))...)
	}
	return vec
}

func (pq *ProductQuantizer) CenterAt(vec []float32) *DistanceLookUpTable {
	return NewDistanceLookUpTable(int(pq.m), int(pq.ks), vec)
}

func (pq *ProductQuantizer) Distance(encoded []byte, lut *DistanceLookUpTable) float32 {
	return lut.LookUp(encoded, pq)
}
