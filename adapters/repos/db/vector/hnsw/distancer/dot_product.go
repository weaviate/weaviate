package distancer

import (
	"fmt"
	"math"
	"unsafe"
)

type DotProduct struct {
	a []float32
}

func (d *DotProduct) Distance(b []float32) (float32, bool, error) {
	l := uint(len(d.a))
	dist := 1 - DotProductAVX(uintptr(unsafe.Pointer(&d.a[0])),
		uintptr(unsafe.Pointer(&b[0])), uintptr(unsafe.Pointer(&l)))
	// fmt.Printf("distance: %f\n", dist)
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

	if len(a)%32 != 0 {
		panic("unsupported length")
	}

	l := uint(len(a))
	prod := 1 - DotProductAVX(uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])), uintptr(unsafe.Pointer(&l)))
	if math.IsNaN(float64(prod)) {
		fmt.Println(a)
		fmt.Println(b)

		fmt.Printf("go-dot product is %f\n", DotProductGo(a, b))
		panic("NaN")

	}

	return 1 - prod, true, nil
}

func (d DotProductProvider) New(a []float32) Distancer {
	return &DotProduct{a: a}
}
