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
	"os"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// DefaultLeafCacheMaxMemory caps the cached leaf bitmaps of a single
// SegmentInMemory. The instance it sits on already holds 65 whole-shard planes
// (~187 MiB for a 24 M-document shard), so this bound adds at most ~9% to a
// structure that has to exist anyway. Sizing is by bytes rather than by entry
// count because a leaf bitmap's size tracks the cardinality it matches, and a
// near-full-shard predicate is both the most useful and the most expensive
// thing to keep.
const DefaultLeafCacheMaxMemory = 16 << 20

// LeafCacheMaxMemoryEnv sets the per-SegmentInMemory budget. 0 disables the
// cache entirely and is the kill switch: it removes every cache read and write,
// leaving the uncached merge path exactly as it shipped.
const LeafCacheMaxMemoryEnv = "QUERY_RANGEABLE_LEAF_CACHE_MAX_MEMORY"

// leafCacheAdmissions is the width of the second-sight admission filter. Keys
// are 24 bytes, so the whole filter is under 1 KiB and is scanned linearly.
const leafCacheAdmissions = 32

var leafCacheMaxMemory = parseLeafCacheMaxMemory(os.Getenv(LeafCacheMaxMemoryEnv))

// parseLeafCacheMaxMemory falls back to the default on anything unparseable
// rather than failing startup: this cache is an optimisation, and a typo in an
// operator's env should not stop a node from serving.
func parseLeafCacheMaxMemory(v string) int {
	if v == "" {
		return DefaultLeafCacheMaxMemory
	}
	bytes, err := humanize.ParseBytes(v)
	if err != nil {
		return DefaultLeafCacheMaxMemory
	}
	return int(bytes)
}

var (
	leafCacheOps = promauto.With(monitoring.GetMetrics().Registerer).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: monitoring.DefaultMetricsNamespace,
			Name:      "lsm_roaringsetrange_leaf_cache_ops_total",
			Help: "Range-filter leaf bitmap cache operations. " +
				"hit/(hit+miss) is the hit rate; store counts admitted bitmaps, " +
				"evict counts bitmaps dropped for the byte budget, " +
				"invalidate counts writes to the in-memory segment that dropped the cache.",
		}, []string{"operation"})

	leafCacheBytes = promauto.With(monitoring.GetMetrics().Registerer).NewGauge(
		prometheus.GaugeOpts{
			Namespace: monitoring.DefaultMetricsNamespace,
			Name:      "lsm_roaringsetrange_leaf_cache_bytes",
			Help:      "Bytes currently held by range-filter leaf bitmap caches across all in-memory segments.",
		})

	leafCacheHits         = leafCacheOps.WithLabelValues("hit")
	leafCacheMisses       = leafCacheOps.WithLabelValues("miss")
	leafCacheStores       = leafCacheOps.WithLabelValues("store")
	leafCacheEvictions    = leafCacheOps.WithLabelValues("evict")
	leafCacheInvalidation = leafCacheOps.WithLabelValues("invalidate")
)

type leafKind uint8

const (
	// leafGreaterThanEqual keys mergeGreaterThanEqual. Every range operator
	// funnels into it, so ">= 14" and "> 13" share one entry.
	leafGreaterThanEqual leafKind = iota
	// leafBetween keys mergeBetween, which backs Equal and NotEqual.
	leafBetween
)

type leafKey struct {
	kind     leafKind
	valueMin uint64
	valueMax uint64
}

type leafEntry struct {
	key   leafKey
	bm    *sroar.Bitmap
	bytes int
}

// leafCache memoises the whole-shard bitmap a range predicate merges out of the
// 65 bit-planes of a SegmentInMemory. The planes only change under the
// segment's write lock, so a generation counter bumped inside those two write
// critical sections and read under the same read lock is a complete
// invalidation token: any reader whose generation matches the cache is looking
// at the exact planes the cached bitmap was built from.
//
// Cached bitmaps are never handed out. Callers get a clone, which is the clone
// the uncached path already pays, so nothing downstream can mutate an entry.
//
// Admission is on second sight. A predicate value seen once is recorded in a
// fixed-width filter and nothing else; only a repeat within the same generation
// is worth a bitmap. A workload with no repeated predicates therefore behaves
// as if the cache were switched off: no bitmap is cloned and no byte is
// retained.
type leafCache struct {
	lock sync.Mutex

	maxBytes int

	// generation is the segment generation every entry below was built from.
	generation uint64

	// entries is ordered oldest-first and evicted from the front.
	entries []leafEntry
	bytes   int

	// The admission filter deliberately survives generation changes: a
	// predicate that was hot before a flush is still hot after it, and
	// re-learning it would cost a miss per flush.
	admissions    [leafCacheAdmissions]leafKey
	admissionUsed [leafCacheAdmissions]bool
	nextAdmission int
}

func newLeafCache(maxBytes int) *leafCache {
	if maxBytes <= 0 {
		return nil
	}
	return &leafCache{maxBytes: maxBytes}
}

// probe returns the bitmap cached for key at this generation, if any, and
// whether a freshly computed result for key should be admitted.
//
// The returned bitmap may be evicted by another goroutine before the caller
// clones it. That is safe: eviction only drops the reference, it never recycles
// the backing buffer, so the caller's pointer keeps the bitmap alive.
func (c *leafCache) probe(generation uint64, key leafKey) (bm *sroar.Bitmap, admit bool) {
	if c == nil {
		return nil, false
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.generation != generation {
		if c.generation > generation {
			// A reader older than the cache cannot happen while readers hold the
			// segment's read lock for their whole lifetime. If it ever does, serve
			// and record nothing rather than risk a stale bitmap.
			return nil, false
		}
		c.dropLocked(generation)
	}

	for i := range c.entries {
		if c.entries[i].key == key {
			leafCacheHits.Inc()
			return c.entries[i].bm, false
		}
	}

	leafCacheMisses.Inc()
	return nil, c.admitLocked(key)
}

// store takes ownership of bm, which must be an independent bitmap the caller
// no longer writes to.
func (c *leafCache) store(generation uint64, key leafKey, bm *sroar.Bitmap) {
	if c == nil || bm == nil {
		return
	}
	size := bm.LenInBytes()
	if size > c.maxBytes {
		// One oversized leaf must not evict everything else to fit.
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.generation != generation {
		return
	}
	for i := range c.entries {
		if c.entries[i].key == key {
			// Another reader computed the same leaf concurrently.
			return
		}
	}

	for c.bytes+size > c.maxBytes && len(c.entries) > 0 {
		c.bytes -= c.entries[0].bytes
		leafCacheBytes.Sub(float64(c.entries[0].bytes))
		last := len(c.entries) - 1
		copy(c.entries, c.entries[1:])
		// clear the vacated slot so the backing array stops referencing the bitmap
		c.entries[last] = leafEntry{}
		c.entries = c.entries[:last]
		leafCacheEvictions.Inc()
	}

	c.entries = append(c.entries, leafEntry{key: key, bm: bm, bytes: size})
	c.bytes += size
	leafCacheBytes.Add(float64(size))
	leafCacheStores.Inc()
}

// admitLocked reports whether key has been asked for before, recording it when
// it has not.
func (c *leafCache) admitLocked(key leafKey) bool {
	for i := range c.admissions {
		if c.admissionUsed[i] && c.admissions[i] == key {
			return true
		}
	}
	c.admissions[c.nextAdmission] = key
	c.admissionUsed[c.nextAdmission] = true
	c.nextAdmission = (c.nextAdmission + 1) % leafCacheAdmissions
	return false
}

func (c *leafCache) dropLocked(generation uint64) {
	if len(c.entries) > 0 {
		leafCacheBytes.Sub(float64(c.bytes))
		leafCacheInvalidation.Inc()
	}
	c.entries = nil
	c.bytes = 0
	c.generation = generation
}
