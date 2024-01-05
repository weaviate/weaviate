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
	"math/rand"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type FilterFunc func([]float32) []float32

type KMeans struct {
	K                  int     // How many centroids
	DeltaThreshold     float32 // Used to stop fitting if there are not too much changes in the centroids anymore
	IterationThreshold int     // Used to stop fitting after a certain amount of iterations
	Distance           distancer.Provider
	centers            [][]float32 // The current centroids
	dimensions         int         // Dimensions of the data
	segment            int         // Segment where it operates

	data KMeansPartitionData // Non-persistent data used only during the fitting process
}

// String prints some minimal information about the encoder. This can be
// used for viability checks to see if the encoder was initialized
// correctly – for example after a restart.
func (k *KMeans) String() string {
	maxElem := 5
	var firstCenters []float32
	i := 0
	for _, center := range k.centers {
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
	return fmt.Sprintf("KMeans Encoder: K=%d, dim=%d, segment=%d first_center_truncated=%v", k.K, k.dimensions, k.segment, firstCenters)
}

type KMeansPartitionData struct {
	changes int        // How many vectors has jumped to a new cluster
	points  []uint64   // Cluster assigned to each point
	cc      [][]uint64 // Partition of the data into the clusters
}

func NewKMeans(k int, dimensions int, segment int) *KMeans {
	kMeans := &KMeans{
		K:                  k,
		DeltaThreshold:     0.01,
		IterationThreshold: 10,
		Distance:           distancer.NewL2SquaredProvider(),
		dimensions:         dimensions,
		segment:            segment,
	}
	return kMeans
}

func NewKMeansWithCenters(k int, dimensions int, segment int, centers [][]float32) *KMeans {
	kmeans := NewKMeans(k, dimensions, segment)
	kmeans.centers = centers
	return kmeans
}

func (m *KMeans) ExposeDataForRestore() []byte {
	ds := len(m.centers[0])
	len := 4 * m.K * ds
	buffer := make([]byte, len)
	for i := 0; i < len/4; i++ {
		binary.LittleEndian.PutUint32(buffer[i*4:(i+1)*4], math.Float32bits(m.centers[i/ds][i%ds]))
	}
	return buffer
}

func (m *KMeans) Add(x []float32) {
	// nothing to do here
}

func (m *KMeans) Centers() [][]float32 {
	return m.centers
}

func (m *KMeans) Encode(point []float32) byte {
	return byte(m.Nearest(point))
}

func (m *KMeans) Nearest(point []float32) uint64 {
	return m.NNearest(point, 1)[0]
}

func (m *KMeans) nNearest(point []float32, n int) ([]uint64, []float32) {
	mins := make([]uint64, n)
	minD := make([]float32, n)
	for i := range mins {
		mins[i] = 0
		minD[i] = math.MaxFloat32
	}
	filteredPoint := point[m.segment*m.dimensions : (m.segment+1)*m.dimensions]
	for i, c := range m.centers {
		distance, _, _ := m.Distance.SingleDist(filteredPoint, c)
		j := 0
		for (j < n) && minD[j] < distance {
			j++
		}
		if j < n {
			for l := n - 1; l >= j+1; l-- {
				mins[l] = mins[l-1]
				minD[l] = minD[l-1]
			}
			minD[j] = distance
			mins[j] = uint64(i)
		}
	}
	return mins, minD
}

func (m *KMeans) NNearest(point []float32, n int) []uint64 {
	nearest, _ := m.nNearest(point, n)
	return nearest
}

func (m *KMeans) initCenters(data [][]float32) {
	if len(m.centers) == m.K {
		return
	}
	m.centers = make([][]float32, 0, m.K)
	for i := 0; i < m.K; i++ {
		var vec []float32
		for vec == nil {
			vec = data[rand.Intn(len(data))]
		}
		vecCopy := make([]float32, m.dimensions)
		copy(vecCopy, vec[m.segment*m.dimensions:(m.segment+1)*m.dimensions])
		m.centers = append(m.centers, vecCopy)
	}
}

func (m *KMeans) recluster(data [][]float32) {
	for p := 0; p < len(data); p++ {
		point := data[p]
		if point == nil {
			continue
		}
		cis, _ := m.nNearest(point, 1)
		ci := cis[0]
		m.data.cc[ci] = append(m.data.cc[ci], uint64(p))
		if m.data.points[p] != ci {
			m.data.points[p] = ci
			m.data.changes++
		}
	}
}

func (m *KMeans) resortOnEmptySets(data [][]float32) {
	k64 := uint64(m.K)
	dataSize := len(data)
	for ci := uint64(0); ci < k64; ci++ {
		if len(m.data.cc[ci]) == 0 {
			var ri int
			for {
				ri = rand.Intn(dataSize)
				if data[ri] == nil {
					continue
				}
				if len(m.data.cc[m.data.points[ri]]) > 1 {
					break
				}
			}
			m.data.cc[ci] = append(m.data.cc[ci], uint64(ri))
			m.data.points[ri] = ci
			m.data.changes = dataSize
		}
	}
}

func (m *KMeans) recalcCenters(data [][]float32) {
	for index := 0; index < m.K; index++ {
		for j := range m.centers[index] {
			m.centers[index][j] = 0
		}
		size := len(m.data.cc[index])
		for _, ci := range m.data.cc[index] {
			vec := data[ci]
			v := vec[m.segment*m.dimensions : (m.segment+1)*m.dimensions]
			for j := 0; j < m.dimensions; j++ {
				m.centers[index][j] += v[j]
			}
		}
		for j := 0; j < m.dimensions; j++ {
			m.centers[index][j] /= float32(size)
		}
	}
}

func (m *KMeans) stopCondition(iterations int, dataSize int) bool {
	return iterations >= m.IterationThreshold ||
		m.data.changes < int(float32(dataSize)*m.DeltaThreshold)
}

func (m *KMeans) Fit(data [][]float32) error { // init centers using min/max per dimension
	dataSize := len(data)
	if dataSize < m.K {
		return errors.New("not enough data to fit kmeans")
	}
	m.initCenters(data)
	m.data.points = make([]uint64, dataSize)
	m.data.changes = 1

	for i := 0; m.data.changes > 0; i++ {
		m.data.changes = 0
		m.data.cc = make([][]uint64, m.K)
		for j := range m.data.cc {
			m.data.cc[j] = make([]uint64, 0)
		}

		m.recluster(data)
		m.resortOnEmptySets(data)
		if m.data.changes > 0 {
			m.recalcCenters(data)
		}

		if m.stopCondition(i, dataSize) {
			break
		}

	}

	m.clearData()
	return nil
}

func (m *KMeans) clearData() {
	m.data.points = nil
	m.data.cc = nil
}

func (m *KMeans) Center(point []float32) []float32 {
	return m.centers[m.Nearest(point)]
}

func (m *KMeans) Centroid(i byte) []float32 {
	return m.centers[i]
}
