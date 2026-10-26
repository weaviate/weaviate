//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build arm64

package compressionhelpers

//go:noescape
func rq4QuantCorrAsm(xs *float32, ci *int32, n int, invStep, offset float32) (sumXC float32, sumC int32, sumC2 int32)

//go:noescape
func rq4MinMaxSumAsm(xs *float32, n int) (minV, maxV, sum float32)

// rq4MinMaxSumNEON runs the fused min/max/sum kernel on the largest
// multiple-of-16 prefix and folds in the tail with Go. See rq4QuantCorrNEON
// for why the tail only runs in tests.
func rq4MinMaxSumNEON(xs []float32) (float32, float32, float32) {
	n := len(xs) &^ 15
	if n == 0 {
		return rq4MinMaxSumGo(xs)
	}
	minV, maxV, sum := rq4MinMaxSumAsm(&xs[0], n)
	if n < len(xs) {
		tailMin, tailMax, tailSum := rq4MinMaxSumGo(xs[n:])
		minV = min(minV, tailMin)
		maxV = max(maxV, tailMax)
		sum += tailSum
	}
	return minV, maxV, sum
}

// rq4QuantCorrNEON runs the fused quantize+correlate kernel on the largest
// multiple-of-16 prefix and finishes the tail in Go. In production n is
// always a multiple of 16 (rotation output dims are multiples of 64 and the
// clip-search sample is 512), so the tail only runs in tests.
func rq4QuantCorrNEON(ci []int32, xs []float32, invStep, offset float32) (float32, int32, int32) {
	n := len(xs) &^ 15
	var sumXC float32
	var sumC, sumC2 int32
	if n > 0 {
		sumXC, sumC, sumC2 = rq4QuantCorrAsm(&xs[0], &ci[0], n, invStep, offset)
	}
	if n < len(xs) {
		tailXC, tailC, tailC2 := rq4QuantCorrGo(ci[n:], xs[n:], invStep, offset)
		sumXC += tailXC
		sumC += tailC
		sumC2 += tailC2
	}
	return sumXC, sumC, sumC2
}
