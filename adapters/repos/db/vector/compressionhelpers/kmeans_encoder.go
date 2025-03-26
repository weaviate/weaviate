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
	"errors"
	"fmt"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type KMeansEncoder struct {
	k        int         // Number of centers/centroids
	d        int         // Dimensions of the data
	s        int         // Segment where it operates
	centers  [][]float32 // k-means centroids used for encoding data
	distance distancer.Provider
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
	if len(data) < m.k {
		return errors.New("not enough data to fit k-means")
	}
	kmeans := NewKMeans(m.k, m.d, m.s)
	// Ensure that we use a different seed for every segment.
	kmeans.Seed ^= uint64(m.s)
	kmeans.Fit(data)
	m.centers = kmeans.Centers()
	return nil
}

func (m *KMeansEncoder) Encode(point []float32) byte {
	var minDist float32 = math.MaxFloat32
	idx := 0
	for i := range m.centers {
		if dist, _ := m.distance.SingleDist(point[m.s*m.d:(m.s+1)*m.d], m.centers[i]); dist < minDist {
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
