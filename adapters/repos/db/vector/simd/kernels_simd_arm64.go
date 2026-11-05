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

//go:build goexperiment.simd

package simd

import (
	"simd/archsimd"
	"unsafe"
)

// NEON (128-bit) kernels. Advanced SIMD is part of the arm64 baseline, so
// these are selected unconditionally when the simd experiment is compiled in.
//
// The kernels address lanes through raw pointers instead of re-slicing:
// archsimd's slice loads re-derive length and capacity on every call, and a
// variable pointer offset costs an integer add per access, while a constant
// offset folds into the FMOVQ. Each loop keeps a block cursor that it advances
// only when another block follows, so the cursor always points inside the
// allocation as the unsafe.Pointer rules and checkptr require, and addresses
// the block's lanes with constant offsets. Every kernel receives equal-length, non-empty
// slices from the wrappers, and the loop bounds keep every access inside them.

func init() {
	addImpl = addNEON
	subImpl = subNEON
	scaleImpl = scaleNEON
	addScalarImpl = addScalarNEON
	sumImpl = sumNEON
	dotImpl = dotNEON
	minImpl = minNEON
	maxImpl = maxNEON
	maxAbsImpl = maxAbsNEON
	int32ToFloat32ScaleImpl = int32ToFloat32ScaleNEON
	float32ToInt32ScaleClampImpl = float32ToInt32ScaleClampNEON
	dotInt8Impl = dotInt8NEON
}

func ldF32x4(p unsafe.Pointer, off uintptr) archsimd.Float32x4 {
	return archsimd.LoadFloat32x4Array((*[4]float32)(unsafe.Add(p, off)))
}

func stF32x4(v archsimd.Float32x4, p unsafe.Pointer, off uintptr) {
	v.StoreArray((*[4]float32)(unsafe.Add(p, off)))
}

func ldI32x4(p unsafe.Pointer, off uintptr) archsimd.Int32x4 {
	return archsimd.LoadInt32x4Array((*[4]int32)(unsafe.Add(p, off)))
}

func ldI8x16(p unsafe.Pointer, off uintptr) archsimd.Int8x16 {
	return archsimd.LoadInt8x16Array((*[16]int8)(unsafe.Add(p, off)))
}

// hsum4 reduces a 4-lane vector with two pairwise adds (FADDP).
func hsum4(v archsimd.Float32x4) float32 {
	v = v.ConcatAddPairs(v)
	v = v.ConcatAddPairs(v)
	return v.GetElem(0)
}

func addNEON(dst, a, b []float32) {
	n := len(dst)
	ba, bb, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+16 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x4(pa, 0).Add(ldF32x4(pb, 0))
			r1 := ldF32x4(pa, 16).Add(ldF32x4(pb, 16))
			r2 := ldF32x4(pa, 32).Add(ldF32x4(pb, 32))
			r3 := ldF32x4(pa, 48).Add(ldF32x4(pb, 48))
			stF32x4(r0, pd, 0)
			stF32x4(r1, pd, 16)
			stF32x4(r2, pd, 32)
			stF32x4(r3, pd, 48)
			i += 16
			if i+16 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 64), unsafe.Add(pb, 64), unsafe.Add(pd, 64)
		}
	}
	if i+4 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x4(ldF32x4(pa, 0).Add(ldF32x4(pb, 0)), pd, 0)
			i += 4
			if i+4 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 16), unsafe.Add(pb, 16), unsafe.Add(pd, 16)
		}
	}
	for ; i < n; i++ {
		dst[i] = a[i] + b[i]
	}
}

func subNEON(dst, a, b []float32) {
	n := len(dst)
	ba, bb, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+16 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x4(pa, 0).Sub(ldF32x4(pb, 0))
			r1 := ldF32x4(pa, 16).Sub(ldF32x4(pb, 16))
			r2 := ldF32x4(pa, 32).Sub(ldF32x4(pb, 32))
			r3 := ldF32x4(pa, 48).Sub(ldF32x4(pb, 48))
			stF32x4(r0, pd, 0)
			stF32x4(r1, pd, 16)
			stF32x4(r2, pd, 32)
			stF32x4(r3, pd, 48)
			i += 16
			if i+16 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 64), unsafe.Add(pb, 64), unsafe.Add(pd, 64)
		}
	}
	if i+4 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x4(ldF32x4(pa, 0).Sub(ldF32x4(pb, 0)), pd, 0)
			i += 4
			if i+4 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 16), unsafe.Add(pb, 16), unsafe.Add(pd, 16)
		}
	}
	for ; i < n; i++ {
		dst[i] = a[i] - b[i]
	}
}

func scaleNEON(dst, a []float32, s float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x4(s)
	ba, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+16 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x4(pa, 0).Mul(sv)
			r1 := ldF32x4(pa, 16).Mul(sv)
			r2 := ldF32x4(pa, 32).Mul(sv)
			r3 := ldF32x4(pa, 48).Mul(sv)
			stF32x4(r0, pd, 0)
			stF32x4(r1, pd, 16)
			stF32x4(r2, pd, 32)
			stF32x4(r3, pd, 48)
			i += 16
			if i+16 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 64), unsafe.Add(pd, 64)
		}
	}
	if i+4 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x4(ldF32x4(pa, 0).Mul(sv), pd, 0)
			i += 4
			if i+4 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 16), unsafe.Add(pd, 16)
		}
	}
	for ; i < n; i++ {
		dst[i] = a[i] * s
	}
}

func addScalarNEON(dst, a []float32, s float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x4(s)
	ba, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+16 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x4(pa, 0).Add(sv)
			r1 := ldF32x4(pa, 16).Add(sv)
			r2 := ldF32x4(pa, 32).Add(sv)
			r3 := ldF32x4(pa, 48).Add(sv)
			stF32x4(r0, pd, 0)
			stF32x4(r1, pd, 16)
			stF32x4(r2, pd, 32)
			stF32x4(r3, pd, 48)
			i += 16
			if i+16 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 64), unsafe.Add(pd, 64)
		}
	}
	if i+4 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x4(ldF32x4(pa, 0).Add(sv), pd, 0)
			i += 4
			if i+4 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 16), unsafe.Add(pd, 16)
		}
	}
	for ; i < n; i++ {
		dst[i] = a[i] + s
	}
}

// sumNEON and dotNEON keep eight accumulators: the M-series cores have four
// FP pipes with multi-cycle FADD/FMLA latency, and eight chains measured
// faster than four at d1536.
func sumNEON(a []float32) float32 {
	n := len(a)
	var acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7 archsimd.Float32x4
	ba := unsafe.Pointer(&a[0])
	i := 0
	if i+32 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			acc0 = acc0.Add(ldF32x4(pa, 0))
			acc1 = acc1.Add(ldF32x4(pa, 16))
			acc2 = acc2.Add(ldF32x4(pa, 32))
			acc3 = acc3.Add(ldF32x4(pa, 48))
			acc4 = acc4.Add(ldF32x4(pa, 64))
			acc5 = acc5.Add(ldF32x4(pa, 80))
			acc6 = acc6.Add(ldF32x4(pa, 96))
			acc7 = acc7.Add(ldF32x4(pa, 112))
			i += 32
			if i+32 > n {
				break
			}
			pa = unsafe.Add(pa, 128)
		}
	}
	acc0 = acc0.Add(acc1).Add(acc2.Add(acc3)).Add(acc4.Add(acc5).Add(acc6.Add(acc7)))
	if i+4 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			acc0 = acc0.Add(ldF32x4(pa, 0))
			i += 4
			if i+4 > n {
				break
			}
			pa = unsafe.Add(pa, 16)
		}
	}
	s := hsum4(acc0)
	for ; i < n; i++ {
		s += a[i]
	}
	return s
}

func dotNEON(a, b []float32) float32 {
	n := len(a)
	var acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7 archsimd.Float32x4
	ba, bb := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])
	i := 0
	if i+32 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4)
		for {
			acc0 = ldF32x4(pa, 0).MulAdd(ldF32x4(pb, 0), acc0)
			acc1 = ldF32x4(pa, 16).MulAdd(ldF32x4(pb, 16), acc1)
			acc2 = ldF32x4(pa, 32).MulAdd(ldF32x4(pb, 32), acc2)
			acc3 = ldF32x4(pa, 48).MulAdd(ldF32x4(pb, 48), acc3)
			acc4 = ldF32x4(pa, 64).MulAdd(ldF32x4(pb, 64), acc4)
			acc5 = ldF32x4(pa, 80).MulAdd(ldF32x4(pb, 80), acc5)
			acc6 = ldF32x4(pa, 96).MulAdd(ldF32x4(pb, 96), acc6)
			acc7 = ldF32x4(pa, 112).MulAdd(ldF32x4(pb, 112), acc7)
			i += 32
			if i+32 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 128), unsafe.Add(pb, 128)
		}
	}
	acc0 = acc0.Add(acc1).Add(acc2.Add(acc3)).Add(acc4.Add(acc5).Add(acc6.Add(acc7)))
	if i+4 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4)
		for {
			acc0 = ldF32x4(pa, 0).MulAdd(ldF32x4(pb, 0), acc0)
			i += 4
			if i+4 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 16), unsafe.Add(pb, 16)
		}
	}
	s := hsum4(acc0)
	for ; i < n; i++ {
		s += a[i] * b[i]
	}
	return s
}

// The min/max kernels fold the tail with an overlapping load of the last four
// lanes, which is exact because min and max are idempotent.

func minNEON(a []float32) float32 {
	n := len(a)
	if n < 4 {
		return minGo(a)
	}
	ba := unsafe.Pointer(&a[0])
	m0 := ldF32x4(ba, 0)
	m1 := m0
	i := 4
	if i+8 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			m0 = m0.Min(ldF32x4(pa, 0))
			m1 = m1.Min(ldF32x4(pa, 16))
			i += 8
			if i+8 > n {
				break
			}
			pa = unsafe.Add(pa, 32)
		}
	}
	if i+4 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			m0 = m0.Min(ldF32x4(pa, 0))
			i += 4
			if i+4 > n {
				break
			}
			pa = unsafe.Add(pa, 16)
		}
	}
	if i < n {
		m1 = m1.Min(ldF32x4(ba, uintptr(n-4)*4))
	}
	return m0.Min(m1).ReduceMin()
}

func maxNEON(a []float32) float32 {
	n := len(a)
	if n < 4 {
		return maxGo(a)
	}
	ba := unsafe.Pointer(&a[0])
	m0 := ldF32x4(ba, 0)
	m1 := m0
	i := 4
	if i+8 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x4(pa, 0))
			m1 = m1.Max(ldF32x4(pa, 16))
			i += 8
			if i+8 > n {
				break
			}
			pa = unsafe.Add(pa, 32)
		}
	}
	if i+4 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x4(pa, 0))
			i += 4
			if i+4 > n {
				break
			}
			pa = unsafe.Add(pa, 16)
		}
	}
	if i < n {
		m1 = m1.Max(ldF32x4(ba, uintptr(n-4)*4))
	}
	return m0.Max(m1).ReduceMax()
}

func maxAbsNEON(a []float32) float32 {
	n := len(a)
	if n < 4 {
		return maxAbsGo(a)
	}
	ba := unsafe.Pointer(&a[0])
	m0 := ldF32x4(ba, 0).Abs()
	m1 := m0
	i := 4
	if i+8 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x4(pa, 0).Abs())
			m1 = m1.Max(ldF32x4(pa, 16).Abs())
			i += 8
			if i+8 > n {
				break
			}
			pa = unsafe.Add(pa, 32)
		}
	}
	if i+4 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x4(pa, 0).Abs())
			i += 4
			if i+4 > n {
				break
			}
			pa = unsafe.Add(pa, 16)
		}
	}
	if i < n {
		m1 = m1.Max(ldF32x4(ba, uintptr(n-4)*4).Abs())
	}
	return m0.Max(m1).ReduceMax()
}

func int32ToFloat32ScaleNEON(dst []float32, src []int32, scale float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x4(scale)
	bs, bd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+16 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldI32x4(ps, 0).ConvertToFloat32().Mul(sv)
			r1 := ldI32x4(ps, 16).ConvertToFloat32().Mul(sv)
			r2 := ldI32x4(ps, 32).ConvertToFloat32().Mul(sv)
			r3 := ldI32x4(ps, 48).ConvertToFloat32().Mul(sv)
			stF32x4(r0, pd, 0)
			stF32x4(r1, pd, 16)
			stF32x4(r2, pd, 32)
			stF32x4(r3, pd, 48)
			i += 16
			if i+16 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 64), unsafe.Add(pd, 64)
		}
	}
	if i+4 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x4(ldI32x4(ps, 0).ConvertToFloat32().Mul(sv), pd, 0)
			i += 4
			if i+4 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 16), unsafe.Add(pd, 16)
		}
	}
	for ; i < n; i++ {
		dst[i] = float32(src[i]) * scale
	}
}

func float32ToInt32ScaleClampNEON(dst []int32, src []float32, scale, offset, minV, maxV float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x4(scale)
	ov := archsimd.BroadcastFloat32x4(offset)
	lo := archsimd.BroadcastFloat32x4(minV)
	hi := archsimd.BroadcastFloat32x4(maxV)
	bs, bd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	i := 0
	// Each lane is a separate Mul and Add (two roundings, matching the Go
	// reference), the clamp, and a truncating FCVTZS. NEON FMAX/FMIN
	// propagate a NaN operand and FCVTZS converts NaN to 0, so no explicit
	// NaN mask is needed. The chain is written out per lane rather than
	// through a helper: the compiler left three inline-mark NOPs per lane
	// behind the helper form, a 12% slowdown at d1536.
	if i+16 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			v0 := archsimd.LoadFloat32x4Array((*[4]float32)(ps)).Mul(sv).Add(ov).Max(lo).Min(hi).ConvertToInt32()
			v1 := archsimd.LoadFloat32x4Array((*[4]float32)(unsafe.Add(ps, 16))).Mul(sv).Add(ov).Max(lo).Min(hi).ConvertToInt32()
			v2 := archsimd.LoadFloat32x4Array((*[4]float32)(unsafe.Add(ps, 32))).Mul(sv).Add(ov).Max(lo).Min(hi).ConvertToInt32()
			v3 := archsimd.LoadFloat32x4Array((*[4]float32)(unsafe.Add(ps, 48))).Mul(sv).Add(ov).Max(lo).Min(hi).ConvertToInt32()
			v0.StoreArray((*[4]int32)(pd))
			v1.StoreArray((*[4]int32)(unsafe.Add(pd, 16)))
			v2.StoreArray((*[4]int32)(unsafe.Add(pd, 32)))
			v3.StoreArray((*[4]int32)(unsafe.Add(pd, 48)))
			i += 16
			if i+16 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 64), unsafe.Add(pd, 64)
		}
	}
	if i+4 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			archsimd.LoadFloat32x4Array((*[4]float32)(ps)).Mul(sv).Add(ov).Max(lo).Min(hi).ConvertToInt32().StoreArray((*[4]int32)(pd))
			i += 4
			if i+4 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 16), unsafe.Add(pd, 16)
		}
	}
	if i < n {
		float32ToInt32ScaleClampGo(dst[i:], src[i:], scale, offset, minV, maxV)
	}
}

// dotInt8NEON widens int8 products to int16 (SMULL/SMULL2) and then to int32
// before accumulating. archsimd exposes no SDOT, so this is the portable
// NEON formulation.
func dotInt8NEON(a, b []int8) int32 {
	n := len(a)
	var acc0, acc1, acc2, acc3 archsimd.Int32x4
	ba, bb := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])
	i := 0
	if i+16 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)), unsafe.Add(bb, uintptr(i))
		for {
			va := ldI8x16(pa, 0)
			vb := ldI8x16(pb, 0)
			lo := va.MulWidenLo(vb)
			hi := va.HiToLo().MulWidenLo(vb.HiToLo())
			acc0 = acc0.Add(lo.ExtendLo4ToInt32())
			acc1 = acc1.Add(lo.HiToLo().ExtendLo4ToInt32())
			acc2 = acc2.Add(hi.ExtendLo4ToInt32())
			acc3 = acc3.Add(hi.HiToLo().ExtendLo4ToInt32())
			i += 16
			if i+16 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 16), unsafe.Add(pb, 16)
		}
	}
	s := acc0.Add(acc1).Add(acc2.Add(acc3)).ReduceSum()
	for ; i < n; i++ {
		s += int32(a[i]) * int32(b[i])
	}
	return s
}
