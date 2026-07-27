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
	"os"
	"sync"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// DefaultLeafCacheMaxMemory caps cached leaf bitmaps by bytes, not entry
// count, since a leaf's size tracks the cardinality it matches and is a small
// fraction of the ~187 MiB a 24M-doc shard's planes already cost.
const DefaultLeafCacheMaxMemory = 16 << 20

// LeafCacheMaxMemoryEnv sets the per-segment cache budget in bytes; 0 is the
// kill switch that disables the cache entirely.
const LeafCacheMaxMemoryEnv = "QUERY_RANGEABLE_LEAF_CACHE_MAX_MEMORY"

// leafCacheAdmissions is the width of the second-sight admission filter. Keys
// are 24 bytes, so the whole filter is under 1 KiB and is scanned linearly.
const leafCacheAdmissions = 32

var (
	leafCacheEnvValue  = os.Getenv(LeafCacheMaxMemoryEnv)
	leafCacheEnvErr    error
	leafCacheMaxMemory int
	leafCacheLogged    atomic.Bool
)

func init() {
	leafCacheMaxMemory, leafCacheEnvErr = parseLeafCacheMaxMemory(leafCacheEnvValue)
}

// parseLeafCacheMaxMemory falls back to the default rather than failing
// startup: the memo is an optimisation, so a typo in its budget should not stop
// a node from serving. It returns the error anyway, or a mistyped budget is
// indistinguishable from an unset one.
func parseLeafCacheMaxMemory(v string) (int, error) {
	if v == "" {
		return DefaultLeafCacheMaxMemory, nil
	}
	bytes, err := humanize.ParseBytes(v)
	if err != nil {
		return DefaultLeafCacheMaxMemory, fmt.Errorf("%s: %q: %w", LeafCacheMaxMemoryEnv, v, err)
	}
	return int(bytes), nil
}

// logLeafCacheConfig reports the memo's budget once per process: loudly when the
// value was not understood and the default is standing in for it, and at info
// when it resolves to the kill switch, which is otherwise invisible because a
// disabled cache never touches a counter.
func logLeafCacheConfig(logger logrus.FieldLogger) {
	if logger == nil || !leafCacheLogged.CompareAndSwap(false, true) {
		return
	}

	entry := logger.WithField("action", "roaringsetrange_leaf_cache")
	switch {
	case leafCacheEnvErr != nil:
		entry.Warnf("%v, falling back to the default of %s",
			leafCacheEnvErr, humanize.IBytes(DefaultLeafCacheMaxMemory))
	case leafCacheMaxMemory <= 0:
		entry.Infof("%s=%q disables the range-filter leaf cache",
			LeafCacheMaxMemoryEnv, leafCacheEnvValue)
	}
}

var (
	leafCacheOps = promauto.With(monitoring.GetMetrics().Registerer).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      leafCacheOpsName,
			Help: "Range-filter leaf bitmap cache operations, summed over every rangeable " +
				"bucket in the process: there is no class, shard or property dimension, so " +
				"absolute values aggregate across all of them. " +
				"hit/(hit+miss) is the hit rate; store counts admitted bitmaps, " +
				"rejected counts repeat predicates declined because the byte budget is full " +
				"(a sustained non-zero rate means the budget is too small for the working set), " +
				"disabled counts lookups that found no cache at all. " +
				"invalidate is not conserved and its magnitude carries no information: use it " +
				"as presence or absence only, never as a rate and never as an alert threshold. " +
				"It counts drop events raised lazily by the next lookup rather than by the " +
				"write, so a run of writes with no lookup between them raises one and writes " +
				"against an already-empty cache raise none; flushes coalesce upstream of that " +
				"too. A non-zero value means invalidation fires, and nothing more. " +
				"disabled rising means the cache is off while queries flow; hit/miss moving " +
				"means it is working. Every child flat is ambiguous: only the in-memory range " +
				"segment reaches these counters, so it reads the same whether " +
				IndexRangeableInMemoryEnv + " is off — the default — or on with no eligible " +
				"traffic. " + leafCacheConfigSeries + " separates the two.",
		}, []string{"operation"})

	// Every child is created here rather than on first increment, so a flat
	// series is a reading and not an absence. Without that, "off" and "never
	// exercised" are the same observation.
	leafCacheHits         = leafCacheOps.WithLabelValues("hit")
	leafCacheMisses       = leafCacheOps.WithLabelValues("miss")
	leafCacheStores       = leafCacheOps.WithLabelValues("store")
	leafCacheRejections   = leafCacheOps.WithLabelValues("rejected")
	leafCacheInvalidation = leafCacheOps.WithLabelValues("invalidate")
	leafCacheDisabled     = leafCacheOps.WithLabelValues("disabled")
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

// leafCache memoises the bitmap merged from a SegmentInMemory's bit-planes.
// Entries are keyed by a generation counter bumped only inside the segment's
// write-lock sections, so a matching generation guarantees the planes are
// unchanged since the entry was built. Bitmaps are only ever cloned out,
// never handed out directly.
//
// A predicate is cached only on second sight, so a workload with no repeated
// predicates retains nothing and clones nothing. A full cache declines new
// entries rather than evicting, so a working set larger than the budget never
// pays a clone-and-discard per query either.
type leafCache struct {
	lock sync.Mutex

	maxBytes int

	// generation is the segment generation every entry below was built from.
	generation uint64

	// entries is append-only; a generation change clears it wholesale.
	entries []leafEntry
	bytes   int

	// The admission filter survives generation changes: a predicate hot
	// before a flush should not need re-learning after it.
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

// probe reports the cached bitmap for key, if any, and whether the caller
// should compute and admit one. maxEntryBytes is the caller's upper bound on
// the bitmap it is about to compute, so admission can be declined before the
// caller pays for a clone that would not fit. A returned bitmap stays valid
// even if a concurrent generation change drops it from the cache before the
// caller clones it, since dropping only releases the reference, never the
// buffer.
func (c *leafCache) probe(generation uint64, key leafKey, maxEntryBytes int) (bm *sroar.Bitmap, admit bool) {
	if c == nil {
		// counted, because a nil cache otherwise leaves hit and miss both at
		// zero, which is exactly what an unexercised cache looks like
		leafCacheDisabled.Inc()
		return nil, false
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.generation != generation {
		if c.generation > generation {
			// Should be unreachable while readers hold the segment's read lock for
			// their whole lifetime; guard anyway rather than risk a stale bitmap.
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
	if !c.admitLocked(key) {
		return nil, false
	}
	if c.bytes+maxEntryBytes > c.maxBytes {
		leafCacheRejections.Inc()
		return nil, false
	}
	return nil, true
}

// store takes ownership of bm, which must be an independent bitmap the caller
// no longer writes to.
func (c *leafCache) store(generation uint64, key leafKey, bm *sroar.Bitmap) {
	if c == nil || bm == nil {
		return
	}
	size := bm.LenInBytes()

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

	if c.bytes+size > c.maxBytes {
		// raced another store into the last of the budget
		leafCacheRejections.Inc()
		return
	}

	c.entries = append(c.entries, leafEntry{key: key, bm: bm, bytes: size})
	c.bytes += size
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
		// Raised by the lookup that notices, not by the write: several generation
		// bumps with no lookup between them raise one. Don't read it as a count.
		leafCacheInvalidation.Inc()
	}
	c.entries = nil
	c.bytes = 0
	c.generation = generation
}
