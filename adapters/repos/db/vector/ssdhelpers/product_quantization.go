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
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type Encoder byte

const (
	UseTileEncoder   Encoder = 0
	UseKMeansEncoder Encoder = 1
)

type DistanceLookUpTable struct {
	calculated []bool
	distances  []float32
	center     [][]float32
	segments   int
	centroids  int
	flatCenter []float32
}

func NewDistanceLookUpTable(segments int, centroids int, center []float32) *DistanceLookUpTable {
	distances := make([]float32, segments*centroids)
	calculated := make([]bool, segments*centroids)
	parsedCenter := make([][]float32, segments)
	ds := len(center) / segments
	for c := 0; c < segments; c++ {
		parsedCenter[c] = center[c*ds : (c+1)*ds]
	}

	dlt := &DistanceLookUpTable{
		distances:  distances,
		calculated: calculated,
		center:     parsedCenter,
		segments:   segments,
		centroids:  centroids,
		flatCenter: center,
	}
	return dlt
}

func (lut *DistanceLookUpTable) Reset(segments int, centroids int, center []float32) {
	elems := segments * centroids
	lut.segments = segments
	lut.centroids = centroids
	if len(lut.distances) != elems ||
		len(lut.calculated) != elems ||
		len(lut.center) != segments {
		lut.distances = make([]float32, segments*centroids)
		lut.calculated = make([]bool, segments*centroids)
		lut.center = make([][]float32, segments)
	} else {
		for i := range lut.calculated {
			lut.calculated[i] = false
		}
	}

	ds := len(center) / segments
	for c := 0; c < segments; c++ {
		lut.center[c] = center[c*ds : (c+1)*ds]
	}
	lut.flatCenter = center
}

func (lut *DistanceLookUpTable) LookUp(
	encoded []byte,
	pq *ProductQuantizer,
) float32 {
	var sum float32

	for i := range pq.kms {
		c := ExtractCode8(encoded, i)
		if lut.distCalculated(i, c) {
			sum += lut.codeDist(i, c)
		} else {
			centroid := pq.kms[i].Centroid(c)
			dist := pq.distance.Step(lut.center[i], centroid)
			lut.setCodeDist(i, c, dist)
			lut.setDistCalculated(i, c)
			sum += dist
		}
	}
	return pq.distance.Wrap(sum)
}

// meant for better readability, rely on the fact that the compiler will inline this
func (lut *DistanceLookUpTable) posForSegmentAndCode(segment int, code byte) int {
	return segment*lut.centroids + int(code)
}

// meant for better readability, rely on the fact that the compiler will inline this
func (lut *DistanceLookUpTable) distCalculated(segment int, code byte) bool {
	return lut.calculated[lut.posForSegmentAndCode(segment, code)]
}

// meant for better readability, rely on the fact that the compiler will inline this
func (lut *DistanceLookUpTable) setDistCalculated(segment int, code byte) {
	lut.calculated[lut.posForSegmentAndCode(segment, code)] = true
}

// meant for better readability, rely on the fact that the compiler will inline this
func (lut *DistanceLookUpTable) codeDist(segment int, code byte) float32 {
	return lut.distances[lut.posForSegmentAndCode(segment, code)]
}

// meant for better readability, rely on the fact that the compiler will inline this
func (lut *DistanceLookUpTable) setCodeDist(segment int, code byte, dist float32) {
	lut.distances[lut.posForSegmentAndCode(segment, code)] = dist
}

type DLUTPool struct {
	pool sync.Pool
}

func NewDLUTPool() *DLUTPool {
	return &DLUTPool{
		pool: sync.Pool{
			New: func() any {
				return &DistanceLookUpTable{}
			},
		},
	}
}

func (p *DLUTPool) Get(segments, centroids int, centers []float32) *DistanceLookUpTable {
	dlt := p.pool.Get().(*DistanceLookUpTable)
	dlt.Reset(segments, centroids, centers)
	return dlt
}

func (p *DLUTPool) Return(dlt *DistanceLookUpTable) {
	p.pool.Put(dlt)
}

type ProductQuantizer struct {
	ks                  int // centroids
	m                   int // segments
	ds                  int // dimensions per segment
	distance            distancer.Provider
	dimensions          int
	kms                 []PQEncoder
	encoderType         Encoder
	encoderDistribution EncoderDistribution
	dlutPool            *DLUTPool
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
	Encode(x []float32) byte
	Centroid(b byte) []float32
	Add(x []float32)
	Fit(data [][]float32) error
	ExposeDataForRestore() []byte
}

// ToDo: Add a settings struct. Already necessary!!
func NewProductQuantizer(segments int, centroids int, useBitsEncoding bool, distance distancer.Provider, dimensions int, encoderType Encoder, encoderDistribution EncoderDistribution) (*ProductQuantizer, error) {
	if segments <= 0 {
		return nil, errors.New("Segments cannot be 0 nor negative")
	}
	if centroids > 256 {
		return nil, fmt.Errorf("Centroids should not be higher than 256. Attempting to use %d", centroids)
	}
	if dimensions%segments != 0 {
		return nil, errors.New("Segments should be an integer divisor of dimensions")
	}
	pq := &ProductQuantizer{
		ks:                  centroids,
		m:                   segments,
		ds:                  int(dimensions / segments),
		distance:            distance,
		dimensions:          dimensions,
		encoderType:         encoderType,
		encoderDistribution: encoderDistribution,
		dlutPool:            NewDLUTPool(),
	}

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

// Only made public for testing purposes... Not sure we need it outside
func ExtractCode8(encoded []byte, index int) byte {
	return encoded[index]
}

// Only made public for testing purposes... Not sure we need it outside
func PutCode8(code byte, buffer []byte, index int) {
	buffer[index] = code
}

func (pq *ProductQuantizer) ExposeFields() PQData {
	return PQData{
		Dimensions:          uint16(pq.dimensions),
		EncoderType:         pq.encoderType,
		Ks:                  uint16(pq.ks),
		M:                   uint16(pq.m),
		EncoderDistribution: byte(pq.encoderDistribution),
		Encoders:            pq.kms,
	}
}

func (pq *ProductQuantizer) DistanceBetweenCompressedVectors(x, y []byte) float32 {
	dist := float32(0)

	for i := 0; i < pq.m; i++ {
		cX := pq.kms[i].Centroid(ExtractCode8(x, i))
		cY := pq.kms[i].Centroid(ExtractCode8(y, i))
		dist += pq.distance.Step(cX, cY)
	}

	return pq.distance.Wrap(dist)
}

func (pq *ProductQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, encoded []byte) float32 {
	dist := float32(0)
	for i := 0; i < pq.m; i++ {
		cY := pq.kms[i].Centroid(ExtractCode8(encoded, i))
		dist += pq.distance.Step(x[i*pq.ds:(i+1)*pq.ds], cY)
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

func (pq *ProductQuantizer) ReturnDistancer(d *PQDistancer) {
	pq.dlutPool.Return(d.lut)
}

func (d *PQDistancer) Distance(x []byte) (float32, bool, error) {
	return d.pq.Distance(x, d.lut), true, nil
}

func (d *PQDistancer) DistanceToFloat(x []float32) (float32, bool, error) {
	return d.pq.distance.SingleDist(x, d.lut.flatCenter)
}

func (pq *ProductQuantizer) Fit(data [][]float32) {
	switch pq.encoderType {
	case UseTileEncoder:
		pq.kms = make([]PQEncoder, pq.m)
		Concurrently(uint64(pq.m), func(i uint64) {
			pq.kms[i] = NewTileEncoder(int(math.Log2(float64(pq.ks))), int(i), pq.encoderDistribution)
			for j := 0; j < len(data); j++ {
				pq.kms[i].Add(data[j])
			}
			pq.kms[i].Fit(data)
		})
	case UseKMeansEncoder:
		pq.kms = make([]PQEncoder, pq.m)
		Concurrently(uint64(pq.m), func(i uint64) {
			pq.kms[i] = NewKMeans(
				int(pq.ks),
				pq.ds,
				int(i),
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
	codes := make([]byte, pq.m)
	for i := 0; i < pq.m; i++ {
		PutCode8(pq.kms[i].Encode(vec), codes, i)
	}
	return codes
}

func (pq *ProductQuantizer) Decode(code []byte) []float32 {
	vec := make([]float32, 0, len(code))
	for i := 0; i < pq.m; i++ {
		vec = append(vec, pq.kms[i].Centroid(ExtractCode8(code, i))...)
	}
	return vec
}

func (pq *ProductQuantizer) CenterAt(vec []float32) *DistanceLookUpTable {
	return pq.dlutPool.Get(int(pq.m), int(pq.ks), vec)
}

func (pq *ProductQuantizer) Distance(encoded []byte, lut *DistanceLookUpTable) float32 {
	return lut.LookUp(encoded, pq)
}
