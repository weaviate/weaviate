package ssdhelpers

import (
	"encoding/gob"
	"fmt"
	"math"
	"os"

	"github.com/pkg/errors"
	"gonum.org/v1/gonum/stat/distuv"
)

type TileEncoderData struct {
	Bins    float64
	Mean    float64
	StdDev  float64
	Size    float64
	S1      float64
	S2      float64
	Segment int
}

type TileEncoder struct {
	bins    float64
	mean    float64
	stdDev  float64
	size    float64
	s1      float64
	s2      float64
	segment int
}

func NewTileEncoder(bits int, segment int) *TileEncoder {
	return &TileEncoder{
		bins:    math.Pow(2, float64(bits)),
		mean:    0,
		stdDev:  0,
		size:    0,
		s1:      0,
		s2:      0,
		segment: segment,
	}
}

const TileDataFileName = "tile.gob"

func (m *TileEncoder) ToDisk(path string, id int) {
	if m == nil {
		return
	}
	fData, err := os.Create(fmt.Sprintf("%s/%d.%s", path, id, TileDataFileName))
	if err != nil {
		panic(errors.Wrap(err, "Could not create tiles encoder file"))
	}
	defer fData.Close()

	dEnc := gob.NewEncoder(fData)
	err = dEnc.Encode(TileEncoderData{
		Bins:    m.bins,
		Mean:    m.mean,
		StdDev:  m.stdDev,
		Size:    m.size,
		S1:      m.s1,
		S2:      m.s2,
		Segment: m.segment,
	})
	if err != nil {
		panic(errors.Wrap(err, "Could not encode tile"))
	}
}

func TileEncoderFromDisk(path string, id int) *TileEncoder {
	fData, err := os.Open(fmt.Sprintf("%s/%d.%s", path, id, TileDataFileName))
	if err != nil {
		return nil
	}
	defer fData.Close()

	data := TileEncoderData{}
	dDec := gob.NewDecoder(fData)
	err = dDec.Decode(&data)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode data"))
	}
	tile := NewTileEncoder(1, id)
	tile.bins = data.Bins
	tile.mean = data.Mean
	tile.s1 = data.S1
	tile.s2 = data.S2
	tile.size = data.Size
	tile.stdDev = data.StdDev
	tile.segment = data.Segment
	return tile
}

func (te *TileEncoder) Fit(data [][]float32) error {
	//No need
	return nil
}

func (te *TileEncoder) Add(x []float32) {
	x64 := float64(x[te.segment])
	if x64 != 0 {
		x64 = math.Log(x64)
	}
	te.s1 += x64
	te.s2 += x64 * x64
	te.size++
	te.mean = te.s1 / te.size
	sum := te.s2 + te.size*te.mean*te.mean
	prod := 2 * te.mean * te.s1
	te.stdDev = math.Sqrt((sum - prod) / te.size)
}

func (te *TileEncoder) Encode(x []float32) byte {
	dist := distuv.LogNormal{
		Mu:    te.mean,
		Sigma: te.stdDev,
	}
	cdf := dist.CDF(float64(x[te.segment]))
	intPart, _ := math.Modf(cdf * float64(te.bins))
	return byte(intPart)
}

func (te *TileEncoder) Centroid(b byte) []float32 {
	dist := distuv.LogNormal{
		Mu:    te.mean,
		Sigma: te.stdDev,
	}
	res := make([]float32, 0, 1)
	if b == 0 {
		res = append(res, float32(dist.Quantile(1/te.bins)))
	} else if b == byte(te.bins) {
		res = append(res, float32(dist.Quantile((te.bins-1)/te.bins)))
	} else {
		b64 := float64(b)
		mean := (b64/te.bins + (b64+1)/te.bins) / 2
		res = append(res, float32(dist.Quantile(mean)))
	}
	return res
}
