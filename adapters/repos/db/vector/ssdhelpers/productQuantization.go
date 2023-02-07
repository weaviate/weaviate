//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package ssdhelpers

import (
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
	encoders []PQEncoder,
	distanceProvider distancer.Provider,
) float32 {
	var sum float32

	k := 0
	for i, c := range encoded {
		if lut.calculated[i][c].Load() {
			sum += lut.distances[i][c]
			k += lut.segmentSize
		} else {
			centroid := encoders[i].Centroid(c)
			dist := float32(0)
			for _, centroidX := range centroid {
				dist += distanceProvider.Step(lut.center[k], centroidX)
				k++
			}
			lut.distances[i][c] = dist
			lut.calculated[i][c].Store(true)
			sum += dist
		}
	}
	return distanceProvider.Wrap(sum)
}

type ProductQuantizer struct {
	ks                  int //centroids
	m                   int //segments
	ds                  int //dimensions per segment
	distance            distancer.Provider
	dimensions          int
	kms                 []PQEncoder
	encoderType         Encoder
	encoderDistribution EncoderDistribution
}

type PQData struct {
	Ks                  uint16
	M                   uint16
	Dimensions          uint16
	EncoderType         Encoder
	EncoderDistribution byte
	Encoders            []PQEncoder
}

type PQEncoder interface {
	Encode(x []float32) byte
	Centroid(b byte) []float32
	Add(x []float32)
	Fit(data [][]float32) error
	ExposeDataForRestore() []byte
}

// ToDo: Add a settings struct. Already necessary!!
func NewProductQuantizer(segments int, centroids int, distance distancer.Provider, dimensions int, encoderType Encoder, encoderDistribution EncoderDistribution) *ProductQuantizer {
	if dimensions%segments != 0 {
		panic("dimension must be a multiple of m")
	}
	pq := &ProductQuantizer{
		ks:                  centroids,
		m:                   segments,
		ds:                  int(dimensions / segments),
		distance:            distance,
		dimensions:          dimensions,
		encoderType:         encoderType,
		encoderDistribution: encoderDistribution,
	}
	return pq
}

func NewProductQuantizerWithEncoders(segments int, centroids int, distance distancer.Provider, dimensions int, encoderType Encoder, encoders []PQEncoder) *ProductQuantizer {
	if dimensions%segments != 0 {
		panic("dimension must be a multiple of m")
	}
	pq := &ProductQuantizer{
		ks:          centroids,
		m:           segments,
		ds:          int(dimensions / segments),
		distance:    distance,
		dimensions:  dimensions,
		encoderType: encoderType,
		kms:         encoders,
	}
	return pq
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
	for i := range x {
		cX := pq.kms[i].Centroid(x[i])
		cY := pq.kms[i].Centroid(y[i])
		for j := range cX {
			dist += pq.distance.Step(cX[j], cY[j])
		}
	}
	return pq.distance.Wrap(dist)
}

func (pq *ProductQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, encoded []byte) float32 {
	dist := float32(0)
	k := 0
	for i := range encoded {
		cY := pq.kms[i].Centroid(encoded[i])
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
				pq.distance,
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
	codes := make([]byte, pq.m)
	for i := 0; i < pq.m; i++ {
		codes[i] = byte(pq.kms[i].Encode(vec))
	}
	return codes
}

func (pq *ProductQuantizer) Decode(code []byte) []float32 {
	vec := make([]float32, 0, len(code))
	for i, b := range code {
		vec = append(vec, pq.kms[i].Centroid(b)...)
	}
	return vec
}

func (pq *ProductQuantizer) CenterAt(vec []float32) *DistanceLookUpTable {
	return NewDistanceLookUpTable(int(pq.m), int(pq.ks), vec)
}

func (pq *ProductQuantizer) Distance(encoded []byte, lut *DistanceLookUpTable) float32 {
	return lut.LookUp(encoded, pq.kms, pq.distance)
}
