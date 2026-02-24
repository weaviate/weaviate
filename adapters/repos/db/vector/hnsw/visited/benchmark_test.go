package visited

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"
)

const universeN = 1_000_000

const (
	sparseCollisionRate = 4096
	fastCapFactor       = 2
)

// VisitedSet is the common interface we benchmark.
type VisitedSet interface {
	Visit(uint64)
	Visited(uint64) bool
	VisitIfNotVisited(uint64) bool
	Reset()
}

// ---------------- Array impl for comparison ----------------

type ArraySet struct {
	data   []uint8
	marker uint8
}

func NewArraySet(size int) *ArraySet {
	return &ArraySet{
		data:   make([]uint8, size),
		marker: 1,
	}
}

func (a *ArraySet) Visit(id uint64) {
	a.data[id] = a.marker
}

func (a *ArraySet) Visited(id uint64) bool {
	return a.data[id] == a.marker
}

func (a *ArraySet) VisitIfNotVisited(id uint64) bool {
	b := a.Visited(id)
	if !b {
		a.Visit(id)
	}
	return b
}

// Reset is O(1) most of the time. On marker overflow, we clear the array.
func (a *ArraySet) Reset() {
	a.marker++
	if a.marker == 0 {
		clear(a.data)
		a.marker = 1
	}
}

// ---------------- Sparse ID generation ----------------
//
// Bijection over Z_n: id = (base*mul + add) mod n
// For n=1,000,000 (=2^6 * 5^6), choose mul not divisible by 2 or 5.

const permMul uint64 = 1_000_003
const permAdd uint64 = 97_121

func permuteID(base uint64) uint64 {
	return (base*permMul + permAdd) % universeN
}

// Query offset: vary base indices per query without RNG in hot loop.
// Keep stride not divisible by 2 or 5 (so it walks the space well for n=1e6).
const queryStride uint64 = 1_000_019

// buildQueryBases returns base indices (NOT permuted yet) for:
// - K unique IDs: baseUnique[i] = i
// - repeats count derived from repeatRate, repeats reference indices into [0..K)
func buildQueryBases(K int, repeatRate float64) (baseUnique []uint64, repeatIdx []int, opsPerQuery int) {
	if K <= 0 {
		panic("K must be > 0")
	}
	if K >= universeN {
		panic("K must be < universeN")
	}
	if repeatRate < 0 || repeatRate >= 1 {
		panic("repeatRate must be in [0,1)")
	}

	repeats := int((repeatRate / (1.0 - repeatRate)) * float64(K))
	if repeats < 0 {
		repeats = 0
	}

	baseUnique = make([]uint64, K)
	for i := 0; i < K; i++ {
		baseUnique[i] = uint64(i)
	}

	// repeats: deterministic, stride walk across unique indices
	repeatIdx = make([]int, repeats)
	stride := 131
	idx := 0
	for i := 0; i < repeats; i++ {
		repeatIdx[i] = idx
		idx += stride
		if idx >= K {
			idx %= K
		}
	}

	return baseUnique, repeatIdx, K + repeats
}

// Deterministic shuffle indices for a pattern of length L, to avoid phase effects.
// Precompute once per sub-benchmark.
func makeShuffleIdx(L int) []int {
	idx := make([]int, L)
	for i := 0; i < L; i++ {
		idx[i] = i
	}

	x := uint64(0x9e3779b97f4a7c15)
	for i := L - 1; i > 0; i-- {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 2685821657736338717
		j := int(x % uint64(i+1))
		idx[i], idx[j] = idx[j], idx[i]
	}

	return idx
}

// ---------------- Memory measurement helpers ----------------

func heapAllocBuildDelta(build func()) uint64 {
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)

	build()

	runtime.GC()
	runtime.ReadMemStats(&m1)

	if m1.HeapAlloc >= m0.HeapAlloc {
		return m1.HeapAlloc - m0.HeapAlloc
	}
	return 0
}

func bytesArraySet(a *ArraySet) uint64 {
	return uint64(cap(a.data)) * uint64(unsafe.Sizeof(a.data[0]))
}

func bytesFastSet(f *FastSet) uint64 {
	var bytes uint64
	bytes += uint64(cap(f.keys)) * uint64(unsafe.Sizeof(f.keys[0]))
	bytes += uint64(cap(f.markers)) * uint64(unsafe.Sizeof(f.markers[0]))
	return bytes
}

func bytesSparseSet(s *SparseSet) (bytes uint64, activeSegs uint64) {
	bytes += uint64(cap(s.collidingBitSet)) * uint64(unsafe.Sizeof(s.collidingBitSet[0]))

	segs := s.segmentedBitSets.segments
	if cap(segs) > 0 {
		bytes += uint64(cap(segs)) * uint64(unsafe.Sizeof(segs[0]))
	}

	for i := range segs {
		if segs[i].words != nil {
			activeSegs++
			bytes += uint64(cap(segs[i].words)) * uint64(unsafe.Sizeof(segs[i].words[0]))
		}
	}
	return bytes, activeSegs
}

// ---------------- Query-like table benchmark ----------------

func BenchmarkQueryLikeVisitedThenVisit_Table(b *testing.B) {
	visitedSizes := []int{1_000, 10_000, 100_000}
	repeatRates := []float64{0.05, 0.50, 0.95}

	type implCase struct {
		name string
		new  func(K int) (VisitedSet, any)
	}

	impls := []implCase{
		{
			name: "Array",
			new: func(_ int) (VisitedSet, any) {
				s := NewArraySet(universeN)
				return s, s
			},
		},
		{
			name: "Sparse",
			new: func(_ int) (VisitedSet, any) {
				s := NewSparseSet(universeN, sparseCollisionRate)
				return s, s
			},
		},
		{
			name: "Fast",
			new: func(K int) (VisitedSet, any) {
				fs := NewFastSet(K * fastCapFactor)
				return &fs, &fs
			},
		},
	}

	for _, K := range visitedSizes {
		K := K
		for _, rr := range repeatRates {
			rr := rr

			baseUnique, repeatIdx, opsPerQuery := buildQueryBases(K, rr)
			shuf := makeShuffleIdx(opsPerQuery)

			for _, impl := range impls {
				impl := impl

				b.Run(fmt.Sprintf("%s/N%d/K%d/repeat%.0f", impl.name, universeN, K, rr*100), func(b *testing.B) {
					var (
						set      VisitedSet
						concrete any

						heapDelta uint64

						structBytes    uint64
						segmentsActive uint64
						segmentsCap    int
						cbWordsCap     int
						wordsPerSeg    int
						fastCap        int
					)

					heapDelta = heapAllocBuildDelta(func() {
						set, concrete = impl.new(K)
					})

					// Warmup query to materialize segments/capacity realistically before measuring memory.
					runOneQuery := func(q uint64) {
						set.Reset()

						// We map the "operation index" to either a unique base or a repeat base.
						// Then permute with a per-query offset and a bijection to make it sparse.
						qOff := q * queryStride

						for oi := 0; oi < opsPerQuery; oi++ {
							op := shuf[oi]

							var base uint64
							if op < K {
								base = baseUnique[op] + qOff
							} else {
								base = baseUnique[repeatIdx[op-K]] + qOff
							}

							id := permuteID(base)

							set.VisitIfNotVisited(id)
						}
					}

					runOneQuery(1)

					// Measure structural bytes AFTER warmup so segments_active is meaningful.
					switch c := concrete.(type) {
					case *ArraySet:
						structBytes = bytesArraySet(c)
					case *FastSet:
						structBytes = bytesFastSet(c)
						fastCap = c.capacity
					case *SparseSet:
						structBytes, segmentsActive = bytesSparseSet(c)
						segmentsCap = cap(c.segmentedBitSets.segments)
						cbWordsCap = cap(c.collidingBitSet)
						wordsPerSeg = c.segmentedBitSets.wordsPerSeg
					}

					b.ReportAllocs()
					b.ReportMetric(float64(opsPerQuery), "ops/query")

					// Benchmark: each b.N iteration is one query (includes Reset).
					b.ResetTimer()
					for q := 0; q < b.N; q++ {
						runOneQuery(uint64(q + 2))
					}
					b.StopTimer()

					b.ReportMetric(float64(heapDelta), "B_heap_build")
					if structBytes > 0 {
						b.ReportMetric(float64(structBytes), "B_struct")
					}
					if fastCap > 0 {
						b.ReportMetric(float64(fastCap), "fast_cap")
					}
					if impl.name == "Sparse" {
						b.ReportMetric(float64(segmentsActive), "segments_active")
						b.ReportMetric(float64(segmentsCap), "segments_cap")
						b.ReportMetric(float64(cbWordsCap), "cb_words_cap")
						b.ReportMetric(float64(wordsPerSeg), "words_per_seg")
					}

					_ = set
					_ = concrete
				})
			}
		}
	}
}

// ---------------- Benchmark: vary collisionRate ----------------

func BenchmarkQueryLikeVisitedThenVisit_CollisionRate_Table(b *testing.B) {
	visitedSizes := []int{1_000, 10_000, 100_000}
	repeatRates := []float64{0.05, 0.50, 0.95}

	// Must be powers of 2 for the current SparseSet implementation.
	collisionRates := []int{256, 512, 1024, 2048, 4096, 8192, 16384}

	type implCase struct {
		name string
		new  func(K int, collisionRate int) (VisitedSet, any)
	}

	impls := []implCase{
		{
			name: "ArrayMarker",
			new: func(_ int, _ int) (VisitedSet, any) {
				s := NewArraySet(universeN)
				return s, s
			},
		},
		{
			name: "Fast",
			new: func(K int, _ int) (VisitedSet, any) {
				fs := NewFastSet(K * fastCapFactor)
				return &fs, &fs
			},
		},
		{
			name: "Sparse",
			new: func(_ int, collisionRate int) (VisitedSet, any) {
				s := NewSparseSet(universeN, collisionRate)
				return s, s
			},
		},
	}

	for _, K := range visitedSizes {
		K := K
		for _, rr := range repeatRates {
			rr := rr

			baseUnique, repeatIdx, opsPerQuery := buildQueryBases(K, rr)
			shuf := makeShuffleIdx(opsPerQuery)

			for _, cr := range collisionRates {
				cr := cr

				for _, impl := range impls {
					impl := impl

					// Only Sparse varies collisionRate; keep other impls at a single label to avoid explosion.
					name := impl.name
					if impl.name == "Sparse" {
						name = fmt.Sprintf("Sparse/CR%d", cr)
					} else {
						name = impl.name
					}

					b.Run(fmt.Sprintf("%s/N%d/K%d/repeat%.0f", name, universeN, K, rr*100), func(b *testing.B) {
						var (
							set      VisitedSet
							concrete any

							heapDelta uint64

							structBytes    uint64
							segmentsActive uint64
							segmentsCap    int
							cbWordsCap     int
							wordsPerSeg    int
							fastCap        int
						)

						heapDelta = heapAllocBuildDelta(func() {
							set, concrete = impl.new(K, cr)
						})

						runOneQuery := func(q uint64) {
							set.Reset()
							qOff := q * queryStride

							for oi := 0; oi < opsPerQuery; oi++ {
								op := shuf[oi]

								var base uint64
								if op < K {
									base = baseUnique[op] + qOff
								} else {
									base = baseUnique[repeatIdx[op-K]] + qOff
								}

								id := permuteID(base)

								if !set.Visited(id) {
									set.Visit(id)
								}
							}
						}

						// Warmup one query to materialize segments/capacity realistically.
						runOneQuery(1)

						// Measure memory after warmup.
						switch c := concrete.(type) {
						case *ArraySet:
							structBytes = bytesArraySet(c)

						case *FastSet:
							structBytes = bytesFastSet(c)
							fastCap = c.capacity

						case *SparseSet:
							structBytes, segmentsActive = bytesSparseSet(c)
							segmentsCap = cap(c.segmentedBitSets.segments)
							cbWordsCap = cap(c.collidingBitSet)
							wordsPerSeg = c.segmentedBitSets.wordsPerSeg
						}

						b.ReportAllocs()
						b.ReportMetric(float64(opsPerQuery), "ops/query")

						// For Sparse, also report CR so it's visible in output as a metric
						if impl.name == "Sparse" {
							b.ReportMetric(float64(cr), "collision_rate")
						}

						b.ResetTimer()
						for q := 0; q < b.N; q++ {
							runOneQuery(uint64(q + 2))
						}
						b.StopTimer()

						b.ReportMetric(float64(heapDelta), "B_heap_build")
						if structBytes > 0 {
							b.ReportMetric(float64(structBytes), "B_struct")
						}
						if fastCap > 0 {
							b.ReportMetric(float64(fastCap), "fast_cap")
						}
						if impl.name == "Sparse" {
							b.ReportMetric(float64(segmentsActive), "segments_active")
							b.ReportMetric(float64(segmentsCap), "segments_cap")
							b.ReportMetric(float64(cbWordsCap), "cb_words_cap")
							b.ReportMetric(float64(wordsPerSeg), "words_per_seg")
						}

						_ = set
						_ = concrete
					})
				}
			}
		}
	}
}
