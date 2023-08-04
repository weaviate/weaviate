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
	centers            []float32 // The current centroids as a flat array. Use index*dimensions to get offset. E.g. to get pos 7 with dim=4 read from 28 to 32
	dimensions         int       // Dimensions of the data
	segment            int       // Segment where it operates

	data KMeansPartitionData // Non persistent data used only during the fitting process
}

// String prints some minimal information about the encoder. This can be
// used for viability checks to see if the encoder was initialized
// correctly – for example after a restart.
func (k *KMeans) String() string {
	// maxElem := 5
	// var firstCenters []float32
	// i := 0
	// for _, center := range k.centers {
	// 	for _, centerVal := range center {
	// 		if i == maxElem {
	// 			break
	// 		}

	// 		firstCenters = append(firstCenters, centerVal)
	// 		i++
	// 	}
	// 	if i == maxElem {
	// 		break
	// 	}
	// }
	// return fmt.Sprintf("KMeans Encoder: K=%d, dim=%d, segment=%d first_center_truncated=%v", k.K, k.dimensions, k.segment, firstCenters)
	return "todo"
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
	fmt.Printf("created k means with %d dims\n", dimensions)
	return kMeans
}

func NewKMeansWithCenters(k int, dimensions int, segment int, centers [][]float32) *KMeans {
	kmeans := NewKMeans(k, dimensions, segment)
	kmeans.centers = make([]float32, len(centers)*dimensions)
	for i, center := range centers {
		copy(kmeans.centers[i*dimensions:(i+1)*dimensions], center)
	}
	return kmeans
}

func (m *KMeans) ExposeDataForRestore() []byte {
	buffer := make([]byte, len(m.centers)*4) // 4 bytes per float32
	for i := range m.centers {
		binary.LittleEndian.PutUint32(buffer[i*4:(i+1)*4], math.Float32bits(m.centers[i]))
	}
	return buffer
}

func (m *KMeans) Add(x []float32) {
	// nothing to do here
}

func (m *KMeans) Centers() [][]float32 {
	out := make([][]float32, len(m.centers)/m.dimensions)
	i := 0
	for i < len(out) {
		out[i/m.dimensions] = m.centers[i : i+m.dimensions]
	}

	return out
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

	centerIndex := 0  // the logical index, e.g. "the third center"
	centerOffset := 0 // the physical offset, e.g. "byte 6"
	for centerOffset < len(m.centers) {
		distance, _, _ := m.Distance.SingleDist(filteredPoint, m.centers[centerOffset:centerOffset+m.dimensions])
		centerOffset += m.dimensions
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
			mins[j] = uint64(centerIndex)
		}

		centerIndex++
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
	m.centers = make([]float32, m.K*m.dimensions)
	for i := 0; i < m.K; i++ {
		var vec []float32
		for vec == nil {
			vec = data[rand.Intn(len(data))]
		}
		copy(m.centers[i*m.dimensions:(i+1)*m.dimensions], vec[m.segment*m.dimensions:(m.segment+1)*m.dimensions])
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
		for j := 0; j < m.dimensions; j++ {
			m.centers[index*m.dimensions+j] = 0
		}
		size := len(m.data.cc[index])
		for _, ci := range m.data.cc[index] {
			vec := data[ci]
			v := vec[m.segment*m.dimensions : (m.segment+1)*m.dimensions]
			for j := 0; j < m.dimensions; j++ {
				m.centers[index*m.dimensions+j] += v[j]
			}
		}
		for j := 0; j < m.dimensions; j++ {
			m.centers[index*m.dimensions+j] /= float32(size)
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
		return errors.New("Too few data to fit KMeans")
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
	logicalPos := int(m.Nearest(point))
	return m.centers[logicalPos*m.dimensions : (logicalPos+1)*m.dimensions]
}

func (m *KMeans) Centroid(i byte) []float32 {
	ii := int(i)
	// fmt.Printf("There are %d centers.\n", len(m.centers))
	// for i, c := range m.centers {
	// 	fmt.Printf("Center %d has length %d\n", i, len(c))

	// 	if i > 5 {
	// 		break
	// 	}
	// }
	return m.centers[ii*m.dimensions : (ii+1)*m.dimensions]
}
