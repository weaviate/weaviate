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
	"fmt"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/kmeans"
)

type KMeansEncoder struct {
	k        int                // Number of centers/centroids.
	d        int                // Dimensions of the data.
	s        int                // Segment where it operates.
	centers  [][]float32        // k-means centroids used for encoding data.
	distance distancer.Provider // Distance measure used to encode to nearest center.
}

func NewKMeansEncoder(k int, dimensions int, segment int) *KMeansEncoder {
	encoder := &KMeansEncoder{
		k:        k,
		d:        dimensions,
		s:        segment,
		distance: distancer.NewL2SquaredProvider(),
	}
	return encoder
}

func NewKMeansEncoderWithCenters(k int, dimensions int, segment int, centers [][]float32) *KMeansEncoder {
	encoder := NewKMeansEncoder(k, dimensions, segment)
	encoder.centers = centers
	return encoder
}

// Assumes that data contains only non-nil vectors.
func (m *KMeansEncoder) Fit(data [][]float32) error {
	km := kmeans.New(m.k, m.d, m.s)
	km.DeltaThreshold = 0.01
	km.IterationThreshold = 10
	// Experiments on ANN datasets reveal that random initialization is ~20%
	// faster than k-means++ initialization when used for PQ and gives the same
	// or slightly better recall.
	km.Initialization = kmeans.RandomInitialization
	// Experiments show that GraphPruning is always faster for short segments,
	// typically giving a 2x speedup for d = 8. On some datasets such as SIFT
	// and GIST it is faster also up to 32 dimensions, and it will never be much
	// slower than brute force assignment since the additional overhead is
	// ~k^2 distance computations and k << n. We therefore always enable it.
	km.Assignment = kmeans.GraphPruning
	err := km.Fit(data)
	m.centers = km.Centers
	return err
}

func (m *KMeansEncoder) Encode(point []float32) byte {
	var minDist float32 = math.MaxFloat32
	idx := 0
	segment := point[m.s*m.d : (m.s+1)*m.d]
	for i := range m.centers {
		if dist, _ := m.distance.SingleDist(segment, m.centers[i]); dist < minDist {
			minDist = dist
			idx = i
		}
	}
	return byte(idx)
}

func (m *KMeansEncoder) Centroid(i byte) []float32 {
	return m.centers[i]
}

func (m *KMeansEncoder) Add(x []float32) {
	// Only here to satisfy the PQEncoder interface.
}

func (m *KMeansEncoder) ExposeDataForRestore() []byte {
	ds := len(m.centers[0])
	len := 4 * m.k * ds
	buffer := make([]byte, len)
	for i := 0; i < len/4; i++ {
		binary.LittleEndian.PutUint32(buffer[i*4:(i+1)*4], math.Float32bits(m.centers[i/ds][i%ds]))
	}
	return buffer
}

// String prints some minimal information about the encoder. This can be
// used for viability checks to see if the encoder was initialized
// correctly – for example after a restart.
func (m *KMeansEncoder) String() string {
	maxElem := 5
	var firstCenters []float32
	i := 0
	for _, center := range m.centers {
		for _, centerVal := range center {
			if i == maxElem {
				break
			}

			firstCenters = append(firstCenters, centerVal)
			i++
		}
		if i == maxElem {
			break
		}
	}
	return fmt.Sprintf("KMeans Encoder: K=%d, dim=%d, segment=%d first_center_truncated=%v",
		m.k, m.d, m.s, firstCenters)
}
