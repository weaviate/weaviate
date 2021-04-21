package distancer

import (
	"fmt"
	"math"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

type DotProduct struct {
	a []float32
}

func (d *DotProduct) Distance(b []float32) (float32, bool, error) {
	dist := 1 - asm.Dot(d.a, b)
	return dist, true, nil
}

type DotProductProvider struct{}

func NewDotProductProvider() DotProductProvider {
	return DotProductProvider{}
}

func DotProductGo(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}

	return 1 - sum
}

func (d DotProductProvider) SingleDist(a, b []float32) (float32, bool, error) {
	if len(a) != len(b) {
		panic("len different")
	}

	prod := 1 - asm.Dot(a, b)
	if math.IsNaN(float64(prod)) {
		fmt.Println(a)
		fmt.Println(b)

		fmt.Printf("go-dot product is %f\n", DotProductGo(a, b))
		panic("NaN")

	}

	return prod, true, nil
}

func (d DotProductProvider) New(a []float32) Distancer {
	return &DotProduct{a: a}
}
