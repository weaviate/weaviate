# SIMD kernels and `GOEXPERIMENT=simd`

`adapters/repos/db/vector/simd` holds the vectorized float32 and int8 slice
kernels used by the quantizers (`compressionhelpers`) and the distancer
normalization helpers: elementwise add/sub/scale, reductions (sum, dot, min,
max, max-abs) and the quantize/dequantize conversions. It replaced the
third-party `github.com/tphakala/simd` module.

## Two build modes, one API

Every function has a portable Go implementation in `generic.go`. The exported
functions in `f32.go` and `i8.go` dispatch through package-level function
variables (`dispatch.go`) that default to those Go kernels.

When the toolchain is invoked with `GOEXPERIMENT=simd`, the build tag
`goexperiment.simd` is set and `kernels_simd_amd64.go` / `kernels_simd_arm64.go`
are compiled in. They are written against the standard library's
`simd/archsimd` intrinsics (Go 1.26+, experimental) and their `init` swaps the
dispatch variables to the best tier the running CPU supports:

| arch  | tier       | requirement                               |
|-------|------------|-------------------------------------------|
| amd64 | AVX-512    | `archsimd.X86.AVX512()` (F+CD+BW+DQ+VL) and AMX-BF16, the same Sapphire-Rapids-or-newer proxy the asm float distancers use to avoid the older generations' 512-bit frequency penalty |
| amd64 | AVX2       | `archsimd.X86.AVX2()` and `FMA()`         |
| arm64 | NEON       | always (Advanced SIMD is baseline)        |
| any   | Go         | fallback, and the only tier without the experiment |

The experiment is therefore a pure performance switch. The Makefile,
Dockerfile, release workflow, `test/run.sh`, the dev-server script and the CI
`setup-go` action all set it; a plain `go build` or `go test` without it still
compiles and behaves identically, only slower on these paths.

## Contracts the kernels must keep

- **Length**: every function processes `min(len(...))` over its slice
  arguments. The wrappers clamp before dispatch, so kernels always see equal,
  non-empty slices.
- **Aliasing**: `dst` may be exactly one of the inputs (in-place). Shifted
  overlap is unsupported.
- **Bit-exactness**: elementwise ops, `Int32ToFloat32Scale` and
  `Float32ToInt32ScaleClamp` must be bit-identical across tiers. The clamp in
  particular is defined as two roundings (`float32(x*scale) + offset`, never a
  fused multiply-add), `max(minV)` then `min(maxV)`, truncation toward zero,
  NaN to 0. The 4-bit encoder's codes depend on this.
- **Reductions**: `Sum`, `SumOfSquares` and `DotProduct` accumulate in lanes
  and may fuse multiply-add, so tiers differ at reassociation level. Callers
  must not rely on exact bits.
- **Min/Max/MaxAbs**: exact for NaN-free inputs; NaN handling unspecified.

## Testing

`simd_test.go` runs every test against every tier available in the current
build and CPU (`kernelSets()`), comparing to the Go reference: exactly for
elementwise, conversion, min/max and int8 ops, and on integer-valued inputs
for the float reductions, with a tolerance check on real-valued inputs.
`TestSIMDActive` fails if a `GOEXPERIMENT=simd` build silently fell back to
the Go kernels on a capable CPU, so CI cannot lose the SIMD path unnoticed.

Run both modes locally when touching the package:

```bash
GOEXPERIMENT=simd go test -race ./adapters/repos/db/vector/simd/
go test ./adapters/repos/db/vector/simd/
GOEXPERIMENT=simd go test -run xxx -bench BenchmarkKernels ./adapters/repos/db/vector/simd/
```

## Writing kernels that are actually fast

Two codegen facts cost most of the speed in the first draft of this package:

- **Step pointers, not slices.** `archsimd.LoadFloat32x4(a[i:])` re-derives
  length and capacity and runs the `(*[4]float32)` conversion check on every
  call, roughly ten scalar instructions around one vector load; a re-slicing
  Add ran slower than scalar Go. The kernels therefore take `unsafe.Pointer(&a[0])`
  once, derive each iteration's block pointer from it (`base + i*elem`, which
  always lies inside the allocation as the unsafe rules and `-race`'s checkptr
  require; advancing a cursor past the last block does not), and address the
  block's lanes through the `*Array` loads and stores with constant offsets.
  A variable offset costs one integer add per access, a constant offset folds
  into the load instruction (Add at d1536 went from 154 to 106 ns on M1).
- **Write the lane body out, not through helpers.** A helper per lane left
  three inline-mark `NOP`s per vector in the clamp kernel, a 12% slowdown at
  d1536. Tiny load/store helpers are fine; the arithmetic chain is not. The wrappers
  guarantee equal, non-empty slices, and the loop bounds keep every access in
  range.
- **Clear the AVX upper state before scalar code (amd64).** The compiler does
  not emit `VZEROUPPER`, and Go scalar float code uses legacy SSE encodings.
  Running those with dirty YMM/ZMM upper halves costs a state transition per
  boundary on Ice Lake and newer cores, measured as a fixed ~150ns per call.
  Every amd64 kernel calls `archsimd.ClearAVXUpperBits()` after its last
  vector instruction and before any scalar tail or return.

Wide unrolling pays: eight NEON accumulators for the reductions and 16
elements per iteration for elementwise ops matched or beat the hand-written
assembly they replaced.

## Adding a kernel

1. Add the Go reference to `generic.go`, the dispatch variable to
   `dispatch.go`, and the exported wrapper (with the length clamp) to `f32.go`
   or `i8.go`.
2. Add the archsimd kernels to both `kernels_simd_*.go` files and register
   them in their `init`.
3. Extend `kernelSet` and the `kernelSets()` lists in the `*_test.go` files so
   the new kernel runs through the shared comparison tests on every tier.

The hand-written assembly distance kernels in
`adapters/repos/db/vector/hnsw/distancer/asm` predate this package and are
candidates to migrate here as archsimd matures.
