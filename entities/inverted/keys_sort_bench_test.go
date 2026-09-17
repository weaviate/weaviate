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

package inverted

import (
	"fmt"
	"math/rand"
	"testing"
)

// benchArms runs one shape through every arm that can order it, so the numbers
// the dispatch is built on can be re-derived rather than taken on trust.
//
// Each arm is handed a fresh copy of the same slab. The copy is inside the timed
// region because an arm that sorts an already-sorted slab measures nothing, and
// subtracting it would compare arms on different inputs.
func benchArms(b *testing.B, src []byte, w int, arms map[string]func(slab []byte, n int)) {
	n := len(src) / w
	buf := make([]byte, len(src))
	for name, arm := range arms {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				copy(buf, src)
				arm(buf, n)
			}
		})
	}
}

// BenchmarkFixedArmsSmall is the measurement radixCutoff rests on for batches
// below it: the packed branches against both the branch the cutoff hands over
// to and the sort.Interface fallback, which is still live for wider keys.
//
// All three run at the same sizes so the boundary can be read here rather than
// across two benchmarks. The packed branches' stack array is sized to
// radixCutoff, so this cannot run past it.
func BenchmarkFixedArmsSmall(b *testing.B) {
	for _, w := range []int{8, 16} {
		for _, n := range []int{4, 8, 16, 32, 63} {
			src := randomFixedSlab(b, n, w)
			b.Run(fmt.Sprintf("w=%d/n=%d", w, n), func(b *testing.B) {
				benchArms(b, src, w, map[string]func([]byte, int){
					"packed": func(slab []byte, n int) {
						if w <= 8 {
							packedSmall(slab, w, 0, n)
						} else {
							widePackedSmall(slab, w, 0, n)
						}
					},
					"radix": func(slab []byte, n int) {
						var sc sortScratch
						if w <= 8 {
							packedRadix(slab, w, 0, n, &sc)
						} else {
							widePackedRadix(slab, w, 0, n, &sc)
						}
					},
					"interface": func(slab []byte, n int) { sortSlabComparison(slab, w, n, make([]byte, w)) },
				})
			})
		}
	}
}

// BenchmarkFixedArmsLarge is the other half: the radix arms against the
// comparison sort, across the sizes radixCutoff sits between. The comparison
// arm is unbounded, so this can run well past the constant.
func BenchmarkFixedArmsLarge(b *testing.B) {
	for _, w := range []int{8, 16} {
		for _, n := range []int{32, 64, 128, 256, 4096, 65536} {
			src := randomFixedSlab(b, n, w)
			b.Run(fmt.Sprintf("w=%d/n=%d", w, n), func(b *testing.B) {
				benchArms(b, src, w, map[string]func([]byte, int){
					"radix": func(slab []byte, n int) {
						var sc sortScratch
						if w <= 8 {
							packedRadix(slab, w, 0, n, &sc)
						} else {
							widePackedRadix(slab, w, 0, n, &sc)
						}
					},
					"interface": func(slab []byte, n int) { sortSlabComparison(slab, w, n, make([]byte, w)) },
				})
			})
		}
	}
}

// BenchmarkSortVariableWidth measures the arm that rebuilds slab and offsets,
// across the cutoff its own dispatch uses.
func BenchmarkSortVariableWidth(b *testing.B) {
	rng := rand.New(rand.NewSource(9))
	for _, n := range []int{8, 63, 64, 4096, 65536} {
		keys := shapeKeys(n, func(i int) string {
			return fmt.Sprintf("item_%d", rng.Intn(1<<40))
		})
		srcSlab, srcOffs := buildVar(keys)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			slab := make([]byte, len(srcSlab))
			offs := make([]uint32, len(srcOffs))
			for i := 0; i < b.N; i++ {
				copy(slab, srcSlab)
				copy(offs, srcOffs)
				var sc sortScratch
				sortVariableWidth(slab, offs, n, &sc)
			}
		})
	}
}

// BenchmarkBuild is the end-to-end cost a query pays, per family shape: fill a
// builder and take the finished list. Allocation counts here are what the
// batched path adds to a filter.
func BenchmarkBuild(b *testing.B) {
	rng := rand.New(rand.NewSource(5))
	for _, n := range []int{8, 64, 10_000, 100_000} {
		for _, w := range []int{8, 16} {
			src := randomFixedSlab(b, n, w)
			b.Run(fmt.Sprintf("fixed/w=%d/n=%d", w, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					kb := NewFixedKeyBuilder(n, w)
					for j := 0; j < n; j++ {
						copy(kb.AppendBuf(), src[j*w:])
					}
					built, err := kb.Build()
					if err != nil {
						b.Fatal(err)
					}
					if built.Len() == 0 && n > 0 {
						b.Fatal("empty")
					}
				}
			})
		}

		for _, shape := range []struct {
			name string
			gen  func(i int) string
		}{
			{"uniform", func(i int) string { return fmt.Sprintf("%012d", rng.Intn(1_000_000_000_000)) }},
			{"variable", func(i int) string { return fmt.Sprintf("item_%d", rng.Intn(1<<40)) }},
		} {
			keys := shapeKeys(n, shape.gen)
			total := 0
			for _, k := range keys {
				total += len(k)
			}
			b.Run(fmt.Sprintf("text-%s/n=%d", shape.name, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					kb := NewVarKeyBuilder(n, total)
					for _, k := range keys {
						kb.AppendString(k)
					}
					built, err := kb.Build()
					if err != nil {
						b.Fatal(err)
					}
					if built.Len() == 0 && n > 0 {
						b.Fatal("empty")
					}
				}
			})
		}
	}
}

// permuteScratch is the permutation widePackedRadix used before permuteFixed:
// gather into a second slab, then copy it back. Kept as the baseline the
// in-place cycle is measured against, since the trade it makes — one array
// instead of two, against a dependent load chain instead of independent reads —
// is only readable as a comparison.
func permuteScratch(slab []byte, w int, idx []uint32, out []byte) {
	for i, e := range idx {
		copy(out[i*w:], slab[int(e)*w:int(e)*w+w])
	}
	copy(slab, out)
}

// BenchmarkPermute measures the two ways of applying the sorted order to the
// slab, which is what the paragraph on permuteFixed rests on.
//
// Both arms restore the slab and the permutation inside the timed region, since
// each consumes both — permuteFixed leaves idx as the identity. That charges
// roughly 15% of the gather arm and 5% of the cycle's at the largest size, so
// the gap between them is wider than the raw figures show.
func BenchmarkPermute(b *testing.B) {
	const w = 16
	rng := rand.New(rand.NewSource(3))
	for _, n := range []int{64, 256, 2048, 4096, 65536} {
		src := randomFixedSlab(b, n, w)
		perm := make([]uint32, n)
		for i, v := range rng.Perm(n) {
			perm[i] = uint32(v)
		}
		b.Run(fmt.Sprintf("n=%d/cycle", n), func(b *testing.B) {
			b.ReportAllocs()
			slab, idx, tmp := make([]byte, len(src)), make([]uint32, n), make([]byte, w)
			for i := 0; i < b.N; i++ {
				copy(slab, src)
				copy(idx, perm)
				permuteFixed(slab, w, idx, tmp)
			}
		})
		b.Run(fmt.Sprintf("n=%d/scratch", n), func(b *testing.B) {
			b.ReportAllocs()
			slab, idx, out := make([]byte, len(src)), make([]uint32, n), make([]byte, len(src))
			for i := 0; i < b.N; i++ {
				copy(slab, src)
				copy(idx, perm)
				permuteScratch(slab, w, idx, out)
			}
		})
	}
}

// BenchmarkIterate measures walking the finished keys the way the fold does,
// against the same walk driven by At. It is the number All's godoc quotes for
// branching on the layout once rather than per key.
func BenchmarkIterate(b *testing.B) {
	const w = 12
	for _, n := range []int{64, 100_000} {
		slab := randomFixedSlab(b, n, w)
		offs := make([]uint32, n+1)
		for i := range offs {
			offs[i] = uint32(i * w)
		}
		for name, keys := range map[string]SortedKeys{
			"offsets": {slab: slab, offs: offs},
			"width":   {slab: slab, w: w},
		} {
			b.Run(fmt.Sprintf("n=%d/%s/All", n, name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var total int
					for _, k := range keys.All() {
						total += len(k)
					}
					if total != n*w {
						b.Fatal("bad")
					}
				}
			})
			b.Run(fmt.Sprintf("n=%d/%s/At", n, name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var total int
					for j, end := 0, keys.Len(); j < end; j++ {
						total += len(keys.At(j))
					}
					if total != n*w {
						b.Fatal("bad")
					}
				}
			})
		}
	}
}

// BenchmarkVariableArms measures the variable-width branches against each other
// at a shared size, which sortVariableWidth's own benchmark cannot do — it runs
// whichever branch the dispatch picks.
func BenchmarkVariableArms(b *testing.B) {
	rng := rand.New(rand.NewSource(9))
	for _, n := range []int{2, 63, 64, 96, 128, 160, 256, 1024, 10_000} {
		keys := shapeKeys(n, func(i int) string {
			return fmt.Sprintf("item_%d", rng.Intn(1<<40))
		})
		srcSlab, srcOffs := buildVar(keys)
		for name, arm := range map[string]func([]byte, []uint32, *sortScratch){
			"comparison": func(slab []byte, offs []uint32, sc *sortScratch) {
				idx := sc.ensureIdx(n)
				for j := range idx {
					idx[j] = uint32(j)
				}
				sortRunByBytes(slab, offs, idx, 0)
			},
			"radix": func(slab []byte, offs []uint32, sc *sortScratch) {
				sc.ensureIndexed(n)
				idx := sc.idx
				for j := range idx {
					idx[j] = uint32(j)
				}
				lcp := commonPrefixVariable(slab, offs, n)
				for j := 0; j < n; j++ {
					sc.keys[j] = packSuffix(slab[offs[j]:offs[j+1]], lcp)
				}
				radixU64Keyed(sc.keys, sc.keysAlt, idx, sc.idxAlt)
				repairCollisions(slab, offs, n, lcp, sc)
			},
		} {
			// Neither arm writes to the slab or the offsets — both order a
			// permutation and read through it — so unlike benchArms there is
			// nothing to reset between iterations.
			b.Run(fmt.Sprintf("n=%d/%s", n, name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var sc sortScratch
					arm(srcSlab, srcOffs, &sc)
				}
			})
		}
	}
}
