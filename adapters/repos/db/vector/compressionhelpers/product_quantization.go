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
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type Encoder byte

const (
	UseTileEncoder   Encoder = 0
	UseKMeansEncoder Encoder = 1
)

type ProductQuantizer struct {
	ks                  int // centroids
	m                   int // segments
	ds                  int // dimensions per segment
	distance            distancer.Provider
	dimensions          int
	kms                 []PQEncoder
	encoderType         Encoder
	encoderDistribution EncoderDistribution
	trainingLimit       int
	globalDistances     []float32
	logger              logrus.FieldLogger
}

type PQData struct {
	Ks                  uint16
	M                   uint16
	Dimensions          uint16
	EncoderType         Encoder
	EncoderDistribution byte
	Encoders            []PQEncoder
	UseBitsEncoding     bool
	TrainingLimit       int
}

type PQEncoder interface {
	Encode(x []float32) byte
	Centroid(b byte) []float32
	Add(x []float32)
	Fit(data [][]float32) error
	ExposeDataForRestore() []byte
}

func NewProductQuantizer(cfg ent.PQConfig, distance distancer.Provider, dimensions int, logger logrus.FieldLogger) (*ProductQuantizer, error) {
	if cfg.Segments <= 0 {
		return nil, errors.New("segments cannot be 0 nor negative")
	}
	if cfg.Centroids > 256 {
		return nil, fmt.Errorf("centroids should not be higher than 256. Attempting to use %d", cfg.Centroids)
	}
	if dimensions%cfg.Segments != 0 {
		return nil, errors.New("segments should be an integer divisor of dimensions")
	}
	encoderType, err := parseEncoder(cfg.Encoder.Type)
	if err != nil {
		return nil, errors.New("invalid encoder type")
	}

	encoderDistribution, err := parseEncoderDistribution(cfg.Encoder.Distribution)
	if err != nil {
		return nil, errors.New("invalid encoder distribution")
	}
	pq := &ProductQuantizer{
		ks:                  cfg.Centroids,
		m:                   cfg.Segments,
		ds:                  int(dimensions / cfg.Segments),
		distance:            distance,
		trainingLimit:       cfg.TrainingLimit,
		dimensions:          dimensions,
		encoderType:         encoderType,
		encoderDistribution: encoderDistribution,
		logger:              logger,
	}

	return pq, nil
}

func NewProductQuantizerWithEncoders(cfg ent.PQConfig, distance distancer.Provider, dimensions int, encoders []PQEncoder, logger logrus.FieldLogger) (*ProductQuantizer, error) {
	cfg.Segments = len(encoders)
	pq, err := NewProductQuantizer(cfg, distance, dimensions, logger)
	if err != nil {
		return nil, err
	}

	pq.kms = encoders
	pq.buildGlobalDistances()
	return pq, nil
}

func (pq *ProductQuantizer) buildGlobalDistances() {
	// This hosts the partial distances between the centroids. This way we do not need
	// to recalculate all the time when calculating full distances between compressed vecs
	pq.globalDistances = make([]float32, pq.m*pq.ks*pq.ks)
	for segment := 0; segment < pq.m; segment++ {
		for i := 0; i < pq.ks; i++ {
			cX := pq.kms[segment].Centroid(byte(i))
			for j := 0; j <= i; j++ {
				cY := pq.kms[segment].Centroid(byte(j))
				pq.globalDistances[segment*pq.ks*pq.ks+i*pq.ks+j] = pq.distance.Step(cX, cY)
				// Just copy from already calculated cell since step should be symmetric.
				pq.globalDistances[segment*pq.ks*pq.ks+j*pq.ks+i] = pq.globalDistances[segment*pq.ks*pq.ks+i*pq.ks+j]
			}
		}
	}
}

// Only made public for testing purposes... Not sure we need it outside
func ExtractCode8(encoded []byte, index int) byte {
	return encoded[index]
}

func parseEncoder(encoder string) (Encoder, error) {
	switch encoder {
	case ent.PQEncoderTypeTile:
		return UseTileEncoder, nil
	case ent.PQEncoderTypeKMeans:
		return UseKMeansEncoder, nil
	default:
		return 0, fmt.Errorf("invalid encoder type: %s", encoder)
	}
}

func parseEncoderDistribution(distribution string) (EncoderDistribution, error) {
	switch distribution {
	case ent.PQEncoderDistributionLogNormal:
		return LogNormalEncoderDistribution, nil
	case ent.PQEncoderDistributionNormal:
		return NormalEncoderDistribution, nil
	default:
		return 0, fmt.Errorf("invalid encoder distribution: %s", distribution)
	}
}

// Only made public for testing purposes... Not sure we need it outside
func PutCode8(code byte, buffer []byte, index int) {
	buffer[index] = code
}

func (pq *ProductQuantizer) PersistCompression(logger CommitLogger) {
	logger.AddPQCompression(PQData{
		Dimensions:          uint16(pq.dimensions),
		EncoderType:         pq.encoderType,
		Ks:                  uint16(pq.ks),
		M:                   uint16(pq.m),
		EncoderDistribution: byte(pq.encoderDistribution),
		Encoders:            pq.kms,
		TrainingLimit:       pq.trainingLimit,
	})
}

func (pq *ProductQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	if len(x) != pq.m || len(y) != pq.m {
		return 0, fmt.Errorf("inconsistent compressed vectors lengths")
	}

	dist := float32(0)

	for i := 0; i < pq.m; i++ {
		cX := ExtractCode8(x, i)
		cY := ExtractCode8(y, i)
		dist += pq.globalDistances[i*pq.ks*pq.ks+int(cX)*pq.ks+int(cY)]
	}

	return pq.distance.Wrap(dist), nil
}

type PQDistancer struct {
	x          []float32
	pq         *ProductQuantizer
	compressed []byte
}

func (pq *ProductQuantizer) NewDistancer(a []float32) *PQDistancer {
	return &PQDistancer{
		x:          a,
		pq:         pq,
		compressed: nil,
	}
}

func (pq *ProductQuantizer) NewCompressedQuantizerDistancer(a []byte) quantizerDistancer[byte] {
	return &PQDistancer{
		x:          nil,
		pq:         pq,
		compressed: a,
	}
}

func (d *PQDistancer) Distance(x []byte) (float32, bool, error) {
	if d.compressed != nil {
		dist, err := d.pq.DistanceBetweenCompressedVectors(d.compressed, x)
		return dist, err == nil, err
	}
	if len(x) != d.pq.m {
		return 0, false, fmt.Errorf("inconsistent compressed vector length")
	}
	return d.pq.Distance(x, d.x), true, nil
}

func (d *PQDistancer) DistanceToFloat(x []float32) (float32, bool, error) {
	if d.x != nil {
		return d.pq.distance.SingleDist(x, d.x)
	}
	xComp := d.pq.Encode(x)
	dist, err := d.pq.DistanceBetweenCompressedVectors(d.compressed, xComp)
	return dist, err == nil, err
}

func (pq *ProductQuantizer) Fit(data [][]float32) error {
	if pq.trainingLimit > 0 && len(data) > pq.trainingLimit {
		data = data[:pq.trainingLimit]
	}
	switch pq.encoderType {
	case UseTileEncoder:
		pq.kms = make([]PQEncoder, pq.m)
		err := ConcurrentlyWithError(pq.logger, uint64(pq.m), func(i uint64) error {
			pq.kms[i] = NewTileEncoder(int(math.Log2(float64(pq.ks))), int(i), pq.encoderDistribution)
			for j := 0; j < len(data); j++ {
				pq.kms[i].Add(data[j])
			}
			return pq.kms[i].Fit(data)
		})
		if err != nil {
			return err
		}
	case UseKMeansEncoder:
		mutex := sync.Mutex{}
		var errorResult error = nil
		pq.kms = make([]PQEncoder, pq.m)
		Concurrently(pq.logger, uint64(pq.m), func(i uint64) {
			mutex.Lock()
			if errorResult != nil {
				mutex.Unlock()
				return
			}
			mutex.Unlock()
			pq.kms[i] = NewKMeans(
				pq.ks,
				pq.ds,
				int(i),
			)
			err := pq.kms[i].Fit(data)
			mutex.Lock()
			if errorResult == nil && err != nil {
				errorResult = err
			}
			mutex.Unlock()
		})
		if errorResult != nil {
			return errorResult
		}
	}
	pq.buildGlobalDistances()
	return nil
}

func (pq *ProductQuantizer) Encode(vec []float32) []byte {
	codes := make([]byte, pq.m)
	for i := 0; i < pq.m; i++ {
		PutCode8(pq.kms[i].Encode(vec), codes, i)
	}
	return codes
}

func (pq *ProductQuantizer) Decode(code []byte) []float32 {
	vec := make([]float32, 0, pq.m)
	for i := 0; i < pq.m; i++ {
		vec = append(vec, pq.kms[i].Centroid(ExtractCode8(code, i))...)
	}
	return vec
}
