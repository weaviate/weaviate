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

package roaringsetrange

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// warmLeafCache drives the two sightings the admission filter needs, so the
// next call to the same key is a hit.
func warmLeafCache(t *testing.T, r *segmentInMemoryReader, value uint64) {
	t.Helper()

	for i := 0; i < 2; i++ {
		bm, release := r.mergeGreaterThanEqual(value, 1)
		require.NotNil(t, bm)
		release()
	}
}

// leafBytesBound must stay sized from plane 0, not the seed: a leaf routinely
// outgrows its seed plane once the cascade ORs higher planes back in.
func TestLeafBytesBoundCoversTheSeededLeaf(t *testing.T) {
	sawLeafLargerThanSeed := 0

	for seed := int64(0); seed < 8; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			segment := newCascadeFixture(t, seed)

			readers, release := segment.Readers(roaringset.NewBitmapBufPoolNoop())
			defer release()
			reader := readers[0].(*segmentInMemoryReader)

			values := append([]uint64{}, cascadeEdgeValues...)
			for i := 0; i < 16; i++ {
				values = append(values, cascadeRandomValue(rng))
			}

			for _, value := range values {
				start := reader.cascadeSeed(value)
				leaf, leafRelease := reader.mergeGreaterThanEqual(value, 1)
				// store admits the clone, not the pooled working buffer
				stored := leaf.Clone().LenInBytes()
				leafRelease()

				require.LessOrEqualf(t, stored, reader.leafBytesBound(),
					"leaf outgrew the admission bound; seed=%d value=%#016x", seed, value)

				if stored > start.seed.LenInBytes() {
					sawLeafLargerThanSeed++
				}
			}
		})
	}

	require.NotZero(t, sawLeafLargerThanSeed,
		"no leaf outgrew its seed plane, so this fixture cannot show why the seed is not a bound")
}

// recordingBufPool captures what the clone helpers ask the pool for.
type recordingBufPool struct {
	roaringset.BitmapBufPool
	minCaps []int
}

func newRecordingBufPool() *recordingBufPool {
	return &recordingBufPool{BitmapBufPool: roaringset.NewBitmapBufPoolNoop()}
}

func (p *recordingBufPool) Get(minCap int) ([]byte, func()) {
	p.minCaps = append(p.minCaps, minCap)
	return p.BitmapBufPool.Get(minCap)
}

// The seeded and cache-hit paths size their buffer from a bound wider than the
// bitmap they clone, so they cannot call CloneToBuf and would silently drop its
// growth headroom on a raw Get. These are the two hot paths this series adds,
// so dropping it there leaves headroom on exactly the cold path.
func TestCloneHelpersRequestTheGrowthHeadroom(t *testing.T) {
	segment := newCascadeFixture(t, 5)
	value := cascadeEncodeInt64(101)

	pool := newRecordingBufPool()
	readers, release := segment.Readers(pool)
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	start := reader.cascadeSeed(value)
	require.True(t, start.narrowed, "fixture did not seed")
	wantSeed := roaringset.CloneBufSize(max(start.seed.LenInBytes(), reader.bitmaps[0].LenInBytes()))

	pool.minCaps = nil
	_, seedRelease := reader.cloneSeed(start.seed)
	seedRelease()
	require.Equal(t, []int{wantSeed}, pool.minCaps)

	warmLeafCache(t, reader, value)
	require.NotZero(t, cachedEntries(segment))
	cached := segment.leafCache.entries[0].bm
	wantCached := roaringset.CloneBufSize(max(cached.LenInBytes(), reader.bitmaps[0].LenInBytes()))

	pool.minCaps = nil
	_, hitRelease := reader.mergeGreaterThanEqual(value, 1)
	hitRelease()
	require.Equal(t, []int{wantCached}, pool.minCaps, "the cache hit did not go through cloneCached")
}

// The clone buffers ask for CloneBufSize, i.e. 1.25x the bound. That headroom
// is spare capacity in a transient buffer returned to the pool, so it must not
// reach the cache's byte budget: sroar's InitCloneToBuf trims data to the used
// length and Clone compacts, so LenInBytes reports content, never capacity. If
// either stopped holding, the budget would under-count by 25% and the cache
// would retain more than its configured cap.
func TestGrowthHeadroomStaysOutOfTheCacheBudget(t *testing.T) {
	segment := newCascadeFixture(t, 5)
	readers, release := segment.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	value := cascadeEncodeInt64(101)
	warmLeafCache(t, reader, value)
	require.NotZero(t, cachedEntries(segment))

	bound := reader.leafBytesBound()
	require.Greater(t, roaringset.CloneBufSize(bound), bound, "the headroom is not applied at all")

	segment.leafCache.lock.Lock()
	defer segment.leafCache.lock.Unlock()
	for _, entry := range segment.leafCache.entries {
		assert.Equal(t, entry.bm.LenInBytes(), entry.bytes, "budget charged something other than the entry")
		assert.LessOrEqual(t, entry.bytes, bound, "an entry outgrew the bound admission used")
	}
	assert.LessOrEqual(t, segment.leafCache.bytes, segment.leafCache.maxBytes)
}

// A cache hit must serve the seeded cascade's bytes, not the unseeded
// plane-0 cascade's: the two agree as sets, not byte-for-byte.
func TestMemoisedLeafIsTheSeededResult(t *testing.T) {
	segment := newCascadeFixture(t, 5)
	readers, release := segment.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	value := cascadeEncodeInt64(101)
	start := reader.cascadeSeed(value)
	require.True(t, start.narrowed, "fixture did not seed, the assertion below would be vacuous")

	fresh, freshRelease := reader.mergeGreaterThanEqualUncached(value, start, 1)
	want := fresh.Clone().ToBuffer()
	freshRelease()

	warmLeafCache(t, reader, value)
	require.NotZero(t, cachedEntries(segment), "nothing was admitted, no hit is reachable")

	hit, hitRelease := reader.mergeGreaterThanEqual(value, 1)
	defer hitRelease()
	assert.Equal(t, want, hit.Clone().ToBuffer())
	assert.Equal(t, canonicalBytes(unseededGreaterThanEqual(reader.bitmaps, value, 1)),
		canonicalBytes(hit))
}

// A leaf cached while seeding was on must still be correct after the kill
// switch flips: both cascades compute the same set.
func TestCachedLeafSurvivesTheKillSwitch(t *testing.T) {
	segment := newCascadeFixture(t, 6)
	readers, release := segment.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	value := cascadeEncodeInt64(7)
	warmLeafCache(t, reader, value)
	require.NotZero(t, cachedEntries(segment))

	withCascadeSeedDisabled(t, true)

	hit, hitRelease := reader.mergeGreaterThanEqual(value, 1)
	defer hitRelease()
	assert.Equal(t, canonicalBytes(unseededGreaterThanEqual(reader.bitmaps, value, 1)),
		canonicalBytes(hit))
}

func TestSeedResolvesBeforeTheCacheProbe(t *testing.T) {
	fset, files := parsePackageSources(t)

	for _, v := range findSeedOrderViolations(fset, files) {
		t.Errorf("%s %s at %s: resolve the cascade seed before probing the leaf cache. "+
			"The plane invariant the seed rests on is only checked while resolving it, and a "+
			"hit never runs the cascade, so a seed resolved later stops guarding every hot "+
			"predicate — in race builds too, where the guard is the only thing that fires.",
			v.function, v.kind, v.position)
	}
}

// Proves the guard fires, rather than being green because it matches nothing.
func TestSeedOrderGuardDetectsALateSeed(t *testing.T) {
	const rogue = `package roaringsetrange

// the seed moved back onto the miss path: a hit never reaches it
func (r *segmentInMemoryReader) mergeGreaterThanEqual(value uint64, conc int) (*sroar.Bitmap, func()) {
	cached, admit := r.cache.probe(r.generation, leafKey{}, r.leafBytesBound())
	if cached != nil {
		return r.cloneCached(cached)
	}
	start := r.cascadeSeed(value)
	return r.mergeGreaterThanEqualUncached(value, start, conc)
}

// resolving it inside the uncached cascade is the same defect one level down
func (r *segmentInMemoryReader) mergeBetweenUncached(a, b uint64, conc int) (*sroar.Bitmap, func()) {
	start := r.cascadeSeed(a)
	return r.cloneSeed(start.seed)
}
`

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "rogue.go", rogue, 0)
	require.NoError(t, err)

	violations := findSeedOrderViolations(fset, []*ast.File{file})
	require.Len(t, violations, 2)

	kinds := map[string]string{}
	for _, v := range violations {
		kinds[v.kind] = v.function
	}
	assert.Equal(t, "mergeGreaterThanEqual", kinds["resolves the seed after probing the cache"])
	assert.Equal(t, "mergeBetweenUncached", kinds["resolves the seed on the cache-miss path"])
}

type seedOrderViolation struct {
	function string
	kind     string
	position string
}

// findSeedOrderViolations reports cascadeSeed calls a cache hit would skip:
// ones in a *Uncached cascade, or made after the probe in a memoised entry point.
func findSeedOrderViolations(fset *token.FileSet, files []*ast.File) []seedOrderViolation {
	var out []seedOrderViolation

	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "cascadeSeed" {
				continue
			}

			seedPos, probePos := token.NoPos, token.NoPos
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := callee(n)
				if !ok {
					return true
				}
				switch sel {
				case "cascadeSeed":
					if !seedPos.IsValid() {
						seedPos = n.Pos()
					}
				case "probe":
					if !probePos.IsValid() {
						probePos = n.Pos()
					}
				}
				return true
			})

			if !seedPos.IsValid() {
				continue
			}
			switch {
			case strings.HasSuffix(fn.Name.Name, "Uncached"):
				out = append(out, seedOrderViolation{
					function: fn.Name.Name,
					kind:     "resolves the seed on the cache-miss path",
					position: fset.Position(seedPos).String(),
				})
			case probePos.IsValid() && probePos < seedPos:
				out = append(out, seedOrderViolation{
					function: fn.Name.Name,
					kind:     "resolves the seed after probing the cache",
					position: fset.Position(seedPos).String(),
				})
			}
		}
	}
	return out
}

func callee(n ast.Node) (string, bool) {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return "", false
	}
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		return sel.Sel.Name, true
	}
	if ident, ok := call.Fun.(*ast.Ident); ok {
		return ident.Name, true
	}
	return "", false
}
