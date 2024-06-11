package compressionhelpers

var l2SquaredByteImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		diff := uint32(a[i]) - uint32(b[i])
		sum += diff * diff
	}

	return sum
}

var dotByteImpl func(a, b []uint8) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}

	return sum
}

var dotFloatByteImpl func(a []float32, b []uint8) float32 = func(a []float32, b []uint8) float32 {
	var sum float32

	for i := range a {
		sum += a[i] * float32(b[i])
	}

	return sum
}
