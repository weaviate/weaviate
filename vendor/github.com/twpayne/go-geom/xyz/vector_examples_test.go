package xyz_test

import (
	"fmt"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xyz"
)

func ExampleVectorDot() {

	v1 := geom.Coord{0.44022007739138613, 0.833525569726002, 0.49302724302422873}
	v2 := geom.Coord{0.05162589254031058, 0.977176382882891, 0.8789402270478548}
	v3 := geom.Coord{0.7534455876162328, 0.6173555367190986, 0.27126435983727104}
	v4 := geom.Coord{0.8452219342333697, 0.5825792503932398, 0.4764854482064663}
	dot := xyz.VectorDot(v1, v2, v3, v4)

	fmt.Println(dot)
	// Output: 0.038538086185549936
}

func ExampleVectorLength() {

	v1 := geom.Coord{0.050809870984833916, 0.31035561291492797, 0.001499306503938036}
	length := xyz.VectorLength(v1)

	fmt.Println(length)
	// Output: 0.3144908542029304
}

func ExampleVectorNormalize() {

	v1 := geom.Coord{0.9807055460429551, 0.8643056316322373, 0.08720913878428183}
	normalized := xyz.VectorNormalize(v1)

	fmt.Println(normalized)
	// Output: [0.7485619198694545 0.6597151260937918 0.06656579094706616]
}
