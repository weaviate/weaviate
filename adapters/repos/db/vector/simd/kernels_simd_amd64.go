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

	"golang.org/x/sys/cpu"
)

// AVX2+FMA (256-bit) and AVX-512 (512-bit) kernels. The 512-bit tier requires
// the archsimd AVX512 virtual feature (F+CD+BW+DQ+VL) and, like the asm
// float distance kernels in hnsw/distancer, AMX-BF16 as a proxy for Sapphire
// Rapids and newer cores, which run 512-bit code without the frequency
// penalty of the Skylake/Cascade Lake generations.
//
// The kernels address lanes through raw pointers instead of re-slicing:
// archsimd's slice loads re-derive length and capacity on every call, and a
// variable pointer offset costs an integer op per access, while a constant
// offset folds into the instruction (the amd64 backend does not fold
// base+index+constant through an unsafe.Pointer, so a base-derived pointer
// per block costs two LEAQs per load and halves the AVX2 throughput). Each
// loop keeps a block cursor that it advances only when another block follows,
// so the cursor always points inside the allocation as the unsafe.Pointer rules
// and checkptr require, and addresses the block's lanes with constant offsets. Every kernel receives equal-length, non-empty
// slices from the wrappers, and the loop bounds keep every access inside them.
//
// Every kernel ends with archsimd.ClearAVXUpperBits (VZEROUPPER) before the
// first scalar instruction. The compiler does not insert it, and Go scalar
// float code uses legacy SSE encodings: running those with dirty YMM/ZMM upper
// halves costs an AVX/SSE state transition on every boundary, which showed up
// as a ~150ns fixed cost per call on Sapphire Rapids.

func init() {
	if !archsimd.X86.AVX2() || !archsimd.X86.FMA() {
		return
	}
	addImpl = addAVX2
	subImpl = subAVX2
	scaleImpl = scaleAVX2
	addScalarImpl = addScalarAVX2
	sumImpl = sumAVX2
	dotImpl = dotAVX2
	minImpl = minAVX2
	maxImpl = maxAVX2
	maxAbsImpl = maxAbsAVX2
	int32ToFloat32ScaleImpl = int32ToFloat32ScaleAVX2
	float32ToInt32ScaleClampImpl = float32ToInt32ScaleClampAVX2
	dotInt8Impl = dotInt8AVX2
	if useAVX512() {
		addImpl = addAVX512
		subImpl = subAVX512
		scaleImpl = scaleAVX512
		addScalarImpl = addScalarAVX512
		sumImpl = sumAVX512
		dotImpl = dotAVX512
		minImpl = minAVX512
		maxImpl = maxAVX512
		maxAbsImpl = maxAbsAVX512
		int32ToFloat32ScaleImpl = int32ToFloat32ScaleAVX512
		float32ToInt32ScaleClampImpl = float32ToInt32ScaleClampAVX512
		dotInt8Impl = dotInt8AVX512
	}
}

// useAVX512 reports whether the 512-bit tier is selected on this CPU.
func useAVX512() bool {
	return archsimd.X86.AVX512() && cpu.X86.HasAMXBF16
}

// ---- pointer loads and stores ----

func ldF32x8(p unsafe.Pointer, off uintptr) archsimd.Float32x8 {
	return archsimd.LoadFloat32x8Array((*[8]float32)(unsafe.Add(p, off)))
}

func stF32x8(v archsimd.Float32x8, p unsafe.Pointer, off uintptr) {
	v.StoreArray((*[8]float32)(unsafe.Add(p, off)))
}

func ldF32x16(p unsafe.Pointer, off uintptr) archsimd.Float32x16 {
	return archsimd.LoadFloat32x16Array((*[16]float32)(unsafe.Add(p, off)))
}

func stF32x16(v archsimd.Float32x16, p unsafe.Pointer, off uintptr) {
	v.StoreArray((*[16]float32)(unsafe.Add(p, off)))
}

func ldI32x8(p unsafe.Pointer, off uintptr) archsimd.Int32x8 {
	return archsimd.LoadInt32x8Array((*[8]int32)(unsafe.Add(p, off)))
}

func ldI32x16(p unsafe.Pointer, off uintptr) archsimd.Int32x16 {
	return archsimd.LoadInt32x16Array((*[16]int32)(unsafe.Add(p, off)))
}

func ldI8x16(p unsafe.Pointer, off uintptr) archsimd.Int8x16 {
	return archsimd.LoadInt8x16Array((*[16]int8)(unsafe.Add(p, off)))
}

func ldI8x32(p unsafe.Pointer, off uintptr) archsimd.Int8x32 {
	return archsimd.LoadInt8x32Array((*[32]int8)(unsafe.Add(p, off)))
}

// ---- horizontal reductions (each clears the AVX upper state) ----

func hsum4(v archsimd.Float32x4) float32 {
	v = v.ConcatAddPairs(v)
	v = v.ConcatAddPairs(v)
	archsimd.ClearAVXUpperBits()
	return v.GetElem(0)
}

func hsum8(v archsimd.Float32x8) float32 {
	return hsum4(v.GetLo().Add(v.GetHi()))
}

func hsum16(v archsimd.Float32x16) float32 {
	return hsum8(v.GetLo().Add(v.GetHi()))
}

func hmin8(v archsimd.Float32x8) float32 {
	var t [4]float32
	v.GetLo().Min(v.GetHi()).StoreArray(&t)
	archsimd.ClearAVXUpperBits()
	return min(min(t[0], t[1]), min(t[2], t[3]))
}

func hmax8(v archsimd.Float32x8) float32 {
	var t [4]float32
	v.GetLo().Max(v.GetHi()).StoreArray(&t)
	archsimd.ClearAVXUpperBits()
	return max(max(t[0], t[1]), max(t[2], t[3]))
}

func hsumInt32x8(v archsimd.Int32x8) int32 {
	s := v.GetLo().Add(v.GetHi())
	s = s.ConcatAddPairs(s)
	s = s.ConcatAddPairs(s)
	archsimd.ClearAVXUpperBits()
	return s.GetElem(0)
}

// ---- AVX2 ----

func addAVX2(dst, a, b []float32) {
	n := len(dst)
	ba, bb, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x8(pa, 0).Add(ldF32x8(pb, 0))
			r1 := ldF32x8(pa, 32).Add(ldF32x8(pb, 32))
			r2 := ldF32x8(pa, 64).Add(ldF32x8(pb, 64))
			r3 := ldF32x8(pa, 96).Add(ldF32x8(pb, 96))
			stF32x8(r0, pd, 0)
			stF32x8(r1, pd, 32)
			stF32x8(r2, pd, 64)
			stF32x8(r3, pd, 96)
			i += 32
			if i+32 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 128), unsafe.Add(pb, 128), unsafe.Add(pd, 128)
		}
	}
	if i+8 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x8(ldF32x8(pa, 0).Add(ldF32x8(pb, 0)), pd, 0)
			i += 8
			if i+8 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 32), unsafe.Add(pb, 32), unsafe.Add(pd, 32)
		}
	}
	archsimd.ClearAVXUpperBits()
	for ; i < n; i++ {
		dst[i] = a[i] + b[i]
	}
}

func subAVX2(dst, a, b []float32) {
	n := len(dst)
	ba, bb, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x8(pa, 0).Sub(ldF32x8(pb, 0))
			r1 := ldF32x8(pa, 32).Sub(ldF32x8(pb, 32))
			r2 := ldF32x8(pa, 64).Sub(ldF32x8(pb, 64))
			r3 := ldF32x8(pa, 96).Sub(ldF32x8(pb, 96))
			stF32x8(r0, pd, 0)
			stF32x8(r1, pd, 32)
			stF32x8(r2, pd, 64)
			stF32x8(r3, pd, 96)
			i += 32
			if i+32 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 128), unsafe.Add(pb, 128), unsafe.Add(pd, 128)
		}
	}
	if i+8 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x8(ldF32x8(pa, 0).Sub(ldF32x8(pb, 0)), pd, 0)
			i += 8
			if i+8 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 32), unsafe.Add(pb, 32), unsafe.Add(pd, 32)
		}
	}
	archsimd.ClearAVXUpperBits()
	for ; i < n; i++ {
		dst[i] = a[i] - b[i]
	}
}

func scaleAVX2(dst, a []float32, s float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x8(s)
	ba, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x8(pa, 0).Mul(sv)
			r1 := ldF32x8(pa, 32).Mul(sv)
			r2 := ldF32x8(pa, 64).Mul(sv)
			r3 := ldF32x8(pa, 96).Mul(sv)
			stF32x8(r0, pd, 0)
			stF32x8(r1, pd, 32)
			stF32x8(r2, pd, 64)
			stF32x8(r3, pd, 96)
			i += 32
			if i+32 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 128), unsafe.Add(pd, 128)
		}
	}
	if i+8 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x8(ldF32x8(pa, 0).Mul(sv), pd, 0)
			i += 8
			if i+8 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 32), unsafe.Add(pd, 32)
		}
	}
	archsimd.ClearAVXUpperBits()
	for ; i < n; i++ {
		dst[i] = a[i] * s
	}
}

func addScalarAVX2(dst, a []float32, s float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x8(s)
	ba, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x8(pa, 0).Add(sv)
			r1 := ldF32x8(pa, 32).Add(sv)
			r2 := ldF32x8(pa, 64).Add(sv)
			r3 := ldF32x8(pa, 96).Add(sv)
			stF32x8(r0, pd, 0)
			stF32x8(r1, pd, 32)
			stF32x8(r2, pd, 64)
			stF32x8(r3, pd, 96)
			i += 32
			if i+32 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 128), unsafe.Add(pd, 128)
		}
	}
	if i+8 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x8(ldF32x8(pa, 0).Add(sv), pd, 0)
			i += 8
			if i+8 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 32), unsafe.Add(pd, 32)
		}
	}
	archsimd.ClearAVXUpperBits()
	for ; i < n; i++ {
		dst[i] = a[i] + s
	}
}

func sumAVX2(a []float32) float32 {
	n := len(a)
	var acc0, acc1, acc2, acc3 archsimd.Float32x8
	ba := unsafe.Pointer(&a[0])
	i := 0
	if i+32 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			acc0 = acc0.Add(ldF32x8(pa, 0))
			acc1 = acc1.Add(ldF32x8(pa, 32))
			acc2 = acc2.Add(ldF32x8(pa, 64))
			acc3 = acc3.Add(ldF32x8(pa, 96))
			i += 32
			if i+32 > n {
				break
			}
			pa = unsafe.Add(pa, 128)
		}
	}
	acc0 = acc0.Add(acc1).Add(acc2.Add(acc3))
	for ; i+8 <= n; i += 8 {
		acc0 = acc0.Add(ldF32x8(ba, uintptr(i)*4))
	}
	s := hsum8(acc0)
	for ; i < n; i++ {
		s += a[i]
	}
	return s
}

func dotAVX2(a, b []float32) float32 {
	n := len(a)
	var acc0, acc1, acc2, acc3 archsimd.Float32x8
	ba, bb := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])
	i := 0
	if i+32 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4)
		for {
			acc0 = ldF32x8(pa, 0).MulAdd(ldF32x8(pb, 0), acc0)
			acc1 = ldF32x8(pa, 32).MulAdd(ldF32x8(pb, 32), acc1)
			acc2 = ldF32x8(pa, 64).MulAdd(ldF32x8(pb, 64), acc2)
			acc3 = ldF32x8(pa, 96).MulAdd(ldF32x8(pb, 96), acc3)
			i += 32
			if i+32 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 128), unsafe.Add(pb, 128)
		}
	}
	acc0 = acc0.Add(acc1).Add(acc2.Add(acc3))
	for ; i+8 <= n; i += 8 {
		acc0 = ldF32x8(ba, uintptr(i)*4).MulAdd(ldF32x8(bb, uintptr(i)*4), acc0)
	}
	s := hsum8(acc0)
	for ; i < n; i++ {
		s += a[i] * b[i]
	}
	return s
}

// The min/max kernels keep two accumulator chains and fold the tail with an
// overlapping load of the last lanes, which is exact because min and max are
// idempotent.

func minAVX2(a []float32) float32 {
	n := len(a)
	if n < 8 {
		return minGo(a)
	}
	base := unsafe.Pointer(&a[0])
	m0 := ldF32x8(base, 0)
	m1 := m0
	i := 8
	if i+16 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Min(ldF32x8(pa, 0))
			m1 = m1.Min(ldF32x8(pa, 32))
			i += 16
			if i+16 > n {
				break
			}
			pa = unsafe.Add(pa, 64)
		}
	}
	if i+8 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Min(ldF32x8(pa, 0))
			i += 8
			if i+8 > n {
				break
			}
			pa = unsafe.Add(pa, 32)
		}
	}
	if i < n {
		m1 = m1.Min(ldF32x8(base, uintptr(n-8)*4))
	}
	return hmin8(m0.Min(m1))
}

func maxAVX2(a []float32) float32 {
	n := len(a)
	if n < 8 {
		return maxGo(a)
	}
	base := unsafe.Pointer(&a[0])
	m0 := ldF32x8(base, 0)
	m1 := m0
	i := 8
	if i+16 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x8(pa, 0))
			m1 = m1.Max(ldF32x8(pa, 32))
			i += 16
			if i+16 > n {
				break
			}
			pa = unsafe.Add(pa, 64)
		}
	}
	if i+8 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x8(pa, 0))
			i += 8
			if i+8 > n {
				break
			}
			pa = unsafe.Add(pa, 32)
		}
	}
	if i < n {
		m1 = m1.Max(ldF32x8(base, uintptr(n-8)*4))
	}
	return hmax8(m0.Max(m1))
}

func maxAbsAVX2(a []float32) float32 {
	n := len(a)
	if n < 8 {
		return maxAbsGo(a)
	}
	base := unsafe.Pointer(&a[0])
	m0 := ldF32x8(base, 0).Abs()
	m1 := m0
	i := 8
	if i+16 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x8(pa, 0).Abs())
			m1 = m1.Max(ldF32x8(pa, 32).Abs())
			i += 16
			if i+16 > n {
				break
			}
			pa = unsafe.Add(pa, 64)
		}
	}
	if i+8 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x8(pa, 0).Abs())
			i += 8
			if i+8 > n {
				break
			}
			pa = unsafe.Add(pa, 32)
		}
	}
	if i < n {
		m1 = m1.Max(ldF32x8(base, uintptr(n-8)*4).Abs())
	}
	return hmax8(m0.Max(m1))
}

func int32ToFloat32ScaleAVX2(dst []float32, src []int32, scale float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x8(scale)
	bs, bd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+16 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldI32x8(ps, 0).ConvertToFloat32().Mul(sv)
			r1 := ldI32x8(ps, 32).ConvertToFloat32().Mul(sv)
			stF32x8(r0, pd, 0)
			stF32x8(r1, pd, 32)
			i += 16
			if i+16 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 64), unsafe.Add(pd, 64)
		}
	}
	if i+8 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x8(ldI32x8(ps, 0).ConvertToFloat32().Mul(sv), pd, 0)
			i += 8
			if i+8 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 32), unsafe.Add(pd, 32)
		}
	}
	archsimd.ClearAVXUpperBits()
	for ; i < n; i++ {
		dst[i] = float32(src[i]) * scale
	}
}

func float32ToInt32ScaleClampAVX2(dst []int32, src []float32, scale, offset, minV, maxV float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x8(scale)
	ov := archsimd.BroadcastFloat32x8(offset)
	lo := archsimd.BroadcastFloat32x8(minV)
	hi := archsimd.BroadcastFloat32x8(maxV)
	bs, bd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	i := 0
	// Each lane is a separate Mul and Add (two roundings, matching the Go
	// reference), then the clamp. VMAXPS and VMINPS return the second operand
	// for a NaN first operand, so a NaN lane leaves the clamp as minV; the
	// mask zeroes it after the truncating VCVTTPS2DQ. The chain is written
	// out per lane rather than through a helper, which left inline-mark NOPs
	// in the loop.
	if i+16 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			v0 := archsimd.LoadFloat32x8Array((*[8]float32)(ps)).Mul(sv).Add(ov)
			v1 := archsimd.LoadFloat32x8Array((*[8]float32)(unsafe.Add(ps, 32))).Mul(sv).Add(ov)
			v0.Max(lo).Min(hi).ConvertToInt32().Masked(v0.Equal(v0)).StoreArray((*[8]int32)(pd))
			v1.Max(lo).Min(hi).ConvertToInt32().Masked(v1.Equal(v1)).StoreArray((*[8]int32)(unsafe.Add(pd, 32)))
			i += 16
			if i+16 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 64), unsafe.Add(pd, 64)
		}
	}
	if i+8 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			v0 := archsimd.LoadFloat32x8Array((*[8]float32)(ps)).Mul(sv).Add(ov)
			v0.Max(lo).Min(hi).ConvertToInt32().Masked(v0.Equal(v0)).StoreArray((*[8]int32)(pd))
			i += 8
			if i+8 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 32), unsafe.Add(pd, 32)
		}
	}
	archsimd.ClearAVXUpperBits()
	if i < n {
		float32ToInt32ScaleClampGo(dst[i:], src[i:], scale, offset, minV, maxV)
	}
}

// dotInt8AVX2 sign-extends 16 bytes at a time (VPMOVSXBW) and reduces pairs
// of int16 products into int32 lanes (VPMADDWD), which cannot overflow.
func dotInt8AVX2(a, b []int8) int32 {
	n := len(a)
	var acc0, acc1 archsimd.Int32x8
	ba, bb := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])
	i := 0
	if i+32 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)), unsafe.Add(bb, uintptr(i))
		for {
			a0 := ldI8x16(pa, 0).ExtendToInt16()
			b0 := ldI8x16(pb, 0).ExtendToInt16()
			a1 := ldI8x16(pa, 16).ExtendToInt16()
			b1 := ldI8x16(pb, 16).ExtendToInt16()
			acc0 = acc0.Add(a0.DotProductPairs(b0))
			acc1 = acc1.Add(a1.DotProductPairs(b1))
			i += 32
			if i+32 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 32), unsafe.Add(pb, 32)
		}
	}
	acc0 = acc0.Add(acc1)
	if i+16 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)), unsafe.Add(bb, uintptr(i))
		for {
			a0 := ldI8x16(pa, 0).ExtendToInt16()
			b0 := ldI8x16(pb, 0).ExtendToInt16()
			acc0 = acc0.Add(a0.DotProductPairs(b0))
			i += 16
			if i+16 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 16), unsafe.Add(pb, 16)
		}
	}
	s := hsumInt32x8(acc0)
	for ; i < n; i++ {
		s += int32(a[i]) * int32(b[i])
	}
	return s
}

// ---- AVX-512 ----

func addAVX512(dst, a, b []float32) {
	n := len(dst)
	ba, bb, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x16(pa, 0).Add(ldF32x16(pb, 0))
			r1 := ldF32x16(pa, 64).Add(ldF32x16(pb, 64))
			stF32x16(r0, pd, 0)
			stF32x16(r1, pd, 64)
			i += 32
			if i+32 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 128), unsafe.Add(pb, 128), unsafe.Add(pd, 128)
		}
	}
	if i+16 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x16(ldF32x16(pa, 0).Add(ldF32x16(pb, 0)), pd, 0)
			i += 16
			if i+16 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 64), unsafe.Add(pb, 64), unsafe.Add(pd, 64)
		}
	}
	archsimd.ClearAVXUpperBits()
	if i < n {
		addAVX2(dst[i:], a[i:], b[i:])
	}
}

func subAVX512(dst, a, b []float32) {
	n := len(dst)
	ba, bb, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x16(pa, 0).Sub(ldF32x16(pb, 0))
			r1 := ldF32x16(pa, 64).Sub(ldF32x16(pb, 64))
			stF32x16(r0, pd, 0)
			stF32x16(r1, pd, 64)
			i += 32
			if i+32 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 128), unsafe.Add(pb, 128), unsafe.Add(pd, 128)
		}
	}
	if i+16 <= n {
		pa, pb, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x16(ldF32x16(pa, 0).Sub(ldF32x16(pb, 0)), pd, 0)
			i += 16
			if i+16 > n {
				break
			}
			pa, pb, pd = unsafe.Add(pa, 64), unsafe.Add(pb, 64), unsafe.Add(pd, 64)
		}
	}
	archsimd.ClearAVXUpperBits()
	if i < n {
		subAVX2(dst[i:], a[i:], b[i:])
	}
}

func scaleAVX512(dst, a []float32, s float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x16(s)
	ba, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x16(pa, 0).Mul(sv)
			r1 := ldF32x16(pa, 64).Mul(sv)
			stF32x16(r0, pd, 0)
			stF32x16(r1, pd, 64)
			i += 32
			if i+32 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 128), unsafe.Add(pd, 128)
		}
	}
	if i+16 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x16(ldF32x16(pa, 0).Mul(sv), pd, 0)
			i += 16
			if i+16 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 64), unsafe.Add(pd, 64)
		}
	}
	archsimd.ClearAVXUpperBits()
	if i < n {
		scaleAVX2(dst[i:], a[i:], s)
	}
}

func addScalarAVX512(dst, a []float32, s float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x16(s)
	ba, bd := unsafe.Pointer(&a[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldF32x16(pa, 0).Add(sv)
			r1 := ldF32x16(pa, 64).Add(sv)
			stF32x16(r0, pd, 0)
			stF32x16(r1, pd, 64)
			i += 32
			if i+32 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 128), unsafe.Add(pd, 128)
		}
	}
	if i+16 <= n {
		pa, pd := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x16(ldF32x16(pa, 0).Add(sv), pd, 0)
			i += 16
			if i+16 > n {
				break
			}
			pa, pd = unsafe.Add(pa, 64), unsafe.Add(pd, 64)
		}
	}
	archsimd.ClearAVXUpperBits()
	if i < n {
		addScalarAVX2(dst[i:], a[i:], s)
	}
}

func sumAVX512(a []float32) float32 {
	n := len(a)
	var acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7 archsimd.Float32x16
	ba := unsafe.Pointer(&a[0])
	i := 0
	if i+128 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			acc0 = acc0.Add(ldF32x16(pa, 0))
			acc1 = acc1.Add(ldF32x16(pa, 64))
			acc2 = acc2.Add(ldF32x16(pa, 128))
			acc3 = acc3.Add(ldF32x16(pa, 192))
			acc4 = acc4.Add(ldF32x16(pa, 256))
			acc5 = acc5.Add(ldF32x16(pa, 320))
			acc6 = acc6.Add(ldF32x16(pa, 384))
			acc7 = acc7.Add(ldF32x16(pa, 448))
			i += 128
			if i+128 > n {
				break
			}
			pa = unsafe.Add(pa, 512)
		}
	}
	acc0 = acc0.Add(acc1).Add(acc2.Add(acc3)).Add(acc4.Add(acc5).Add(acc6.Add(acc7)))
	if i+16 <= n {
		pa := unsafe.Add(ba, uintptr(i)*4)
		for {
			acc0 = acc0.Add(ldF32x16(pa, 0))
			i += 16
			if i+16 > n {
				break
			}
			pa = unsafe.Add(pa, 64)
		}
	}
	s := hsum16(acc0)
	if i < n {
		s += sumAVX2(a[i:])
	}
	return s
}

func dotAVX512(a, b []float32) float32 {
	n := len(a)
	var acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7 archsimd.Float32x16
	ba, bb := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])
	i := 0
	if i+128 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4)
		for {
			acc0 = ldF32x16(pa, 0).MulAdd(ldF32x16(pb, 0), acc0)
			acc1 = ldF32x16(pa, 64).MulAdd(ldF32x16(pb, 64), acc1)
			acc2 = ldF32x16(pa, 128).MulAdd(ldF32x16(pb, 128), acc2)
			acc3 = ldF32x16(pa, 192).MulAdd(ldF32x16(pb, 192), acc3)
			acc4 = ldF32x16(pa, 256).MulAdd(ldF32x16(pb, 256), acc4)
			acc5 = ldF32x16(pa, 320).MulAdd(ldF32x16(pb, 320), acc5)
			acc6 = ldF32x16(pa, 384).MulAdd(ldF32x16(pb, 384), acc6)
			acc7 = ldF32x16(pa, 448).MulAdd(ldF32x16(pb, 448), acc7)
			i += 128
			if i+128 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 512), unsafe.Add(pb, 512)
		}
	}
	acc0 = acc0.Add(acc1).Add(acc2.Add(acc3)).Add(acc4.Add(acc5).Add(acc6.Add(acc7)))
	if i+16 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)*4), unsafe.Add(bb, uintptr(i)*4)
		for {
			acc0 = ldF32x16(pa, 0).MulAdd(ldF32x16(pb, 0), acc0)
			i += 16
			if i+16 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 64), unsafe.Add(pb, 64)
		}
	}
	s := hsum16(acc0)
	if i < n {
		s += dotAVX2(a[i:], b[i:])
	}
	return s
}

func minAVX512(a []float32) float32 {
	n := len(a)
	if n < 16 {
		return minAVX2(a)
	}
	base := unsafe.Pointer(&a[0])
	m0 := ldF32x16(base, 0)
	m1 := m0
	i := 16
	if i+32 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Min(ldF32x16(pa, 0))
			m1 = m1.Min(ldF32x16(pa, 64))
			i += 32
			if i+32 > n {
				break
			}
			pa = unsafe.Add(pa, 128)
		}
	}
	if i+16 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Min(ldF32x16(pa, 0))
			i += 16
			if i+16 > n {
				break
			}
			pa = unsafe.Add(pa, 64)
		}
	}
	if i < n {
		m1 = m1.Min(ldF32x16(base, uintptr(n-16)*4))
	}
	m := m0.Min(m1)
	return hmin8(m.GetLo().Min(m.GetHi()))
}

func maxAVX512(a []float32) float32 {
	n := len(a)
	if n < 16 {
		return maxAVX2(a)
	}
	base := unsafe.Pointer(&a[0])
	m0 := ldF32x16(base, 0)
	m1 := m0
	i := 16
	if i+32 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x16(pa, 0))
			m1 = m1.Max(ldF32x16(pa, 64))
			i += 32
			if i+32 > n {
				break
			}
			pa = unsafe.Add(pa, 128)
		}
	}
	if i+16 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x16(pa, 0))
			i += 16
			if i+16 > n {
				break
			}
			pa = unsafe.Add(pa, 64)
		}
	}
	if i < n {
		m1 = m1.Max(ldF32x16(base, uintptr(n-16)*4))
	}
	m := m0.Max(m1)
	return hmax8(m.GetLo().Max(m.GetHi()))
}

func maxAbsAVX512(a []float32) float32 {
	n := len(a)
	if n < 16 {
		return maxAbsAVX2(a)
	}
	base := unsafe.Pointer(&a[0])
	m0 := ldF32x16(base, 0).Abs()
	m1 := m0
	i := 16
	if i+32 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x16(pa, 0).Abs())
			m1 = m1.Max(ldF32x16(pa, 64).Abs())
			i += 32
			if i+32 > n {
				break
			}
			pa = unsafe.Add(pa, 128)
		}
	}
	if i+16 <= n {
		pa := unsafe.Add(base, uintptr(i)*4)
		for {
			m0 = m0.Max(ldF32x16(pa, 0).Abs())
			i += 16
			if i+16 > n {
				break
			}
			pa = unsafe.Add(pa, 64)
		}
	}
	if i < n {
		m1 = m1.Max(ldF32x16(base, uintptr(n-16)*4).Abs())
	}
	m := m0.Max(m1)
	return hmax8(m.GetLo().Max(m.GetHi()))
}

func int32ToFloat32ScaleAVX512(dst []float32, src []int32, scale float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x16(scale)
	bs, bd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			r0 := ldI32x16(ps, 0).ConvertToFloat32().Mul(sv)
			r1 := ldI32x16(ps, 64).ConvertToFloat32().Mul(sv)
			stF32x16(r0, pd, 0)
			stF32x16(r1, pd, 64)
			i += 32
			if i+32 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 128), unsafe.Add(pd, 128)
		}
	}
	if i+16 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			stF32x16(ldI32x16(ps, 0).ConvertToFloat32().Mul(sv), pd, 0)
			i += 16
			if i+16 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 64), unsafe.Add(pd, 64)
		}
	}
	archsimd.ClearAVXUpperBits()
	if i < n {
		int32ToFloat32ScaleAVX2(dst[i:], src[i:], scale)
	}
}

func float32ToInt32ScaleClampAVX512(dst []int32, src []float32, scale, offset, minV, maxV float32) {
	n := len(dst)
	sv := archsimd.BroadcastFloat32x16(scale)
	ov := archsimd.BroadcastFloat32x16(offset)
	lo := archsimd.BroadcastFloat32x16(minV)
	hi := archsimd.BroadcastFloat32x16(maxV)
	bs, bd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	i := 0
	if i+32 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			v0 := archsimd.LoadFloat32x16Array((*[16]float32)(ps)).Mul(sv).Add(ov)
			v1 := archsimd.LoadFloat32x16Array((*[16]float32)(unsafe.Add(ps, 64))).Mul(sv).Add(ov)
			v0.Max(lo).Min(hi).ConvertToInt32().Masked(v0.Equal(v0)).StoreArray((*[16]int32)(pd))
			v1.Max(lo).Min(hi).ConvertToInt32().Masked(v1.Equal(v1)).StoreArray((*[16]int32)(unsafe.Add(pd, 64)))
			i += 32
			if i+32 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 128), unsafe.Add(pd, 128)
		}
	}
	if i+16 <= n {
		ps, pd := unsafe.Add(bs, uintptr(i)*4), unsafe.Add(bd, uintptr(i)*4)
		for {
			v0 := archsimd.LoadFloat32x16Array((*[16]float32)(ps)).Mul(sv).Add(ov)
			v0.Max(lo).Min(hi).ConvertToInt32().Masked(v0.Equal(v0)).StoreArray((*[16]int32)(pd))
			i += 16
			if i+16 > n {
				break
			}
			ps, pd = unsafe.Add(ps, 64), unsafe.Add(pd, 64)
		}
	}
	archsimd.ClearAVXUpperBits()
	if i < n {
		float32ToInt32ScaleClampAVX2(dst[i:], src[i:], scale, offset, minV, maxV)
	}
}

func dotInt8AVX512(a, b []int8) int32 {
	n := len(a)
	var acc0, acc1 archsimd.Int32x16
	ba, bb := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])
	i := 0
	if i+64 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)), unsafe.Add(bb, uintptr(i))
		for {
			a0 := ldI8x32(pa, 0).ExtendToInt16()
			b0 := ldI8x32(pb, 0).ExtendToInt16()
			a1 := ldI8x32(pa, 32).ExtendToInt16()
			b1 := ldI8x32(pb, 32).ExtendToInt16()
			acc0 = acc0.Add(a0.DotProductPairs(b0))
			acc1 = acc1.Add(a1.DotProductPairs(b1))
			i += 64
			if i+64 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 64), unsafe.Add(pb, 64)
		}
	}
	acc0 = acc0.Add(acc1)
	if i+32 <= n {
		pa, pb := unsafe.Add(ba, uintptr(i)), unsafe.Add(bb, uintptr(i))
		for {
			a0 := ldI8x32(pa, 0).ExtendToInt16()
			b0 := ldI8x32(pb, 0).ExtendToInt16()
			acc0 = acc0.Add(a0.DotProductPairs(b0))
			i += 32
			if i+32 > n {
				break
			}
			pa, pb = unsafe.Add(pa, 32), unsafe.Add(pb, 32)
		}
	}
	s := hsumInt32x8(acc0.GetLo().Add(acc0.GetHi()))
	if i < n {
		s += dotInt8AVX2(a[i:], b[i:])
	}
	return s
}
