package ssdhelpers

import (
	"math"
	"sync"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type Encoder byte

const (
	UseTileEncoder   Encoder = 0
	UseKMeansEncoder         = 1
)

type DistanceLookUpTable struct {
	distances [][]float32
	center    []float32
}

func NewDistanceLookUpTable(segments int, centroids int, center []float32) *DistanceLookUpTable {
	distances := make([][]float32, segments)
	for m := 0; m < segments; m++ {
		distances[m] = make([]float32, centroids)
	}

	return &DistanceLookUpTable{
		distances: distances,
		center:    center,
	}
}

func (lut *DistanceLookUpTable) LookUp(
	encoded []byte,
	distance func(segment int, b byte, center []float32) float32,
	aggregate func(current float32, x float32) float32,
) float32 {
	d := lut.distances[0][encoded[0]]
	if d == 0 {
		d = distance(0, encoded[0], lut.center)
		lut.distances[0][encoded[0]] = d
	}
	dist := d

	for i := 1; i < len(encoded); i++ {
		b := encoded[i]
		d = lut.distances[i][b]
		if d == 0 {
			d = distance(i, b, lut.center)
			lut.distances[i][b] = d
		}
		dist = aggregate(dist, d)
	}
	return dist
}

type DistanceProvider struct {
	Provider  distancer.Provider
	Aggregate func(d1, d2 float32) float32
}

func NewDistanceProvider(provider distancer.Provider) *DistanceProvider {
	dp := &DistanceProvider{
		Provider: provider,
	}
	if provider.Type() == "cosine-dot" {
		dp.Aggregate = dp.dotAggregate
	} else {
		dp.Aggregate = dp.addAggregate
	}
	return dp
}

func (dp *DistanceProvider) Distance(x, y []float32) float32 {
	dist, _, _ := dp.Provider.SingleDist(x, y)
	return dist
}

func (dp *DistanceProvider) addAggregate(d1, d2 float32) float32 {
	return d1 + d2
}

func (dp *DistanceProvider) dotAggregate(d1, d2 float32) float32 {
	return 1 - (2 - d1 - d2)
}

type ProductQuantizer struct {
	ks          int
	m           int
	ds          int
	distance    *DistanceProvider
	dimensions  int
	kms         []PQEncoder
	encoderType Encoder
}

type PQData struct {
	Ks          uint16
	M           uint16
	Dimensions  uint16
	EncoderType Encoder
	Encoders    []PQEncoder
}

type PQEncoder interface {
	Encode(x []float32) byte
	Centroid(b byte) []float32
	Add(x []float32)
	Fit(data [][]float32) error
	ExposeDataForRestore() []byte
}

func NewProductQuantizer(segments int, centroids int, distance *DistanceProvider, dimensions int, encoderType Encoder) *ProductQuantizer {
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
	}
	return pq
}

func NewProductQuantizerWithEncoders(segments int, centroids int, distance *DistanceProvider, dimensions int, encoderType Encoder, encoders []PQEncoder) *ProductQuantizer {
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
		Dimensions:  uint16(pq.dimensions),
		EncoderType: pq.encoderType,
		Ks:          uint16(pq.ks),
		M:           uint16(pq.m),
		Encoders:    pq.kms,
	}
}

func (pq *ProductQuantizer) DistanceBetweenCompressedVectors(x, y []byte) float32 {
	dist := pq.distance.Distance(pq.kms[0].Centroid(x[0]), pq.kms[0].Centroid(y[0]))
	for i := 1; i < len(x); i++ {
		dist = pq.distance.Aggregate(dist, pq.distance.Distance(pq.kms[i].Centroid(x[i]), pq.kms[i].Centroid(y[i])))
	}
	return dist
}

func (pq *ProductQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, encoded []byte) float32 {
	dist := pq.distance.Distance(extractSegment(0, x, pq.ds), pq.kms[0].Centroid(encoded[0]))
	for i := 1; i < len(encoded); i++ {
		b := encoded[i]
		dist = pq.distance.Aggregate(dist, pq.distance.Distance(extractSegment(i, x, pq.ds), pq.kms[i].Centroid(b)))
	}
	return dist
}

type PQDistancer struct {
	x         []float32
	distancer DistanceProvider
	pq        *ProductQuantizer
	lut       *DistanceLookUpTable
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
			pq.kms[i] = NewTileEncoder(int(math.Log2(float64(pq.ks))), int(i))
			for j := 0; j < len(data); j++ {
				pq.kms[i].Add(data[j])
			}
		})
	case UseKMeansEncoder:
		pq.kms = make([]PQEncoder, pq.m)
		Concurrently(uint64(pq.m), func(_ uint64, i uint64, _ *sync.Mutex) {
			pq.kms[i] = NewKMeansWithFilter(
				int(pq.ks),
				pq.distance.Provider,
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

func (pq *ProductQuantizer) DistanceBetweenNodes(x, y []byte) float32 {
	dist := pq.distance.Distance(pq.kms[0].Centroid(x[0]), pq.kms[0].Centroid(y[0]))
	for i := 1; i < len(x); i++ {
		dist = pq.distance.Aggregate(dist, pq.distance.Distance(pq.kms[i].Centroid(x[i]), pq.kms[i].Centroid(y[i])))
	}
	return dist
}

func (pq *ProductQuantizer) DistanceBetweenNodeAndVector(x []float32, encoded []byte) float32 {
	dist := pq.distance.Distance(extractSegment(0, x, pq.ds), pq.kms[0].Centroid(encoded[0]))
	for i := 1; i < len(encoded); i++ {
		b := encoded[i]
		dist = pq.distance.Aggregate(dist, pq.distance.Distance(extractSegment(i, x, pq.ds), pq.kms[i].Centroid(b)))
	}
	return dist
}

func (pq *ProductQuantizer) distanceForSegment(segment int, b byte, center []float32) float32 {
	return pq.distance.Distance(extractSegment(segment, center, pq.ds), pq.kms[segment].Centroid(b))
}

func (pq *ProductQuantizer) Distance(encoded []byte, lut *DistanceLookUpTable) float32 {
	return lut.LookUp(encoded, pq.distanceForSegment, pq.distance.Aggregate)
}

type PQDistanceProvider struct {
	pq         *ProductQuantizer
	distancer  DistanceProvider
	dimensions int
	typeTxt    string
}
