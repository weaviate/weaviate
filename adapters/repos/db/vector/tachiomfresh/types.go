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

package tachiomfresh

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
)

// This file holds what token types add to the index.
// Every token type (vocabulary id) has its own group of centroids. A token
// is routed, split, merged and reassigned inside the group of its type, so
// every posting holds tokens of one type. Nothing here has a counterpart in
// hfresh, which has no token types.

// typeGroup is the centroids of one token type. mu guards ids: a scan holds
// it for reading, a split or a merge holds it for writing while the group
// changes.
type typeGroup struct {
	mu  sync.RWMutex
	ids []uint64

	// entries is the number of posting entries of the type. The type's
	// posting limits follow this size (postingLimits). It counts what the
	// postings hold: the entries of a deleted document leave the count
	// when a split or a merge collects them, not at the delete, which does
	// not deal with token types. Not persisted: restore recounts it from
	// the postings.
	entries atomic.Int64

	// The spread of the type's vectors (postingLimits), kept as running
	// sums over the tokens inserted since the type reached
	// spreadObserveEntries: the sum of the vectors, the sum of their
	// squared norms and their number. A delete does not subtract. The
	// vector is gone when its entries are collected, so the spread
	// describes the type's history. spreadMu guards the sums and the
	// reference bookkeeping. postingLimits reads varianceBits without the
	// lock.
	spreadMu sync.Mutex
	sum      []float64
	sumSq    float64
	seen     uint64
	dirty    bool // the sums changed since they were persisted
	// varianceBits holds the bits of the variance of the observed tokens
	// (variance). It is refreshed every spreadRefresh observed tokens and
	// on open. varianceKnown is false until the first refresh.
	varianceBits  atomic.Uint64
	varianceKnown atomic.Bool
	// table is the type table the group belongs to. The variance is
	// counted in the table's reference (countVariance): refVar is the
	// value this type last contributed, and counted says whether it has
	// contributed yet.
	table   *typeTable
	refVar  float64
	counted bool
}

// Constants of the spread rule (postingLimits).
const (
	// spreadMinEntries is the size from which a type's posting limits
	// follow its spread. Smaller types keep the configured limits, as in
	// hfresh. It is TACHIOM's small threshold: under it TAC does not
	// score by spread either.
	spreadMinEntries = 256
	// spreadObserveEntries is the size from which a type's tokens are
	// summed, TACHIOM's micro threshold. It is early enough for the spread
	// to be known when the type reaches spreadMinEntries, and late enough
	// that the many rare types hold no sums.
	spreadObserveEntries = 128
	// minRelativeSpread and maxRelativeSpread bound the relative spread of
	// postingLimits. Without the lower bound, a type far tighter than the
	// reference would get an unbounded posting size. The upper bound is
	// 1, the mean: a type more spread out than the mean gets the square
	// root of its size and no more. The reason for the cap at the mean:
	// the most spread-out types are the common ones. They are already
	// well served, and they are the ones that make the build slow when
	// they are split finer. The rare types have the smaller spreads, so
	// the cap keeps the difference between the two bounded.
	minRelativeSpread = 0.05
	maxRelativeSpread = 1
	// spreadRefresh is how many observed tokens pass between two updates
	// of the variance. An update costs a pass over the sum vector.
	spreadRefresh = 64
)

// observe adds a token inserted into the type to the spread's sums. The
// caller passes the token as inserted, before centering (fileDocument in
// insert.go); the centered token would give the same variance (variance).
func (g *typeGroup) observe(vec []float32) {
	if g.entries.Load() < spreadObserveEntries {
		return
	}
	g.spreadMu.Lock()
	defer g.spreadMu.Unlock()
	if g.sum == nil {
		g.sum = make([]float64, len(vec))
	}
	if len(g.sum) != len(vec) {
		return
	}
	for i, v := range vec {
		f := float64(v)
		g.sum[i] += f
		g.sumSq += f * f
	}
	g.seen++
	g.dirty = true
	if g.seen%spreadRefresh == 0 {
		g.refreshVariance()
	}
}

// variance returns the spread of the observed tokens: TACHIOM's measure,
// the sum of the per-dimension variances. This is the mean squared
// distance of the tokens to their mean, mean |x|^2 - |mean|^2. It does
// not change when the tokens are translated. A spread must not depend on
// the mean of the dataset. The same quantity divided by mean |x|^2 would
// depend on it: on an encoder whose raw tokens share a common mean, that
// quotient reads every large type as tight, and the clamp gives them 20
// times the square root. ok is false while nothing was observed. The
// caller holds spreadMu.
func (g *typeGroup) variance() (variance float64, ok bool) {
	if g.seen == 0 {
		return 0, false
	}
	var meanNorm float64
	for _, s := range g.sum {
		meanNorm += s * s
	}
	n := float64(g.seen)
	return max(g.sumSq/n-meanNorm/(n*n), 0), true
}

// refreshVariance publishes the variance for postingLimits and, once the
// type has spreadMinEntries entries, counts it in the table's reference.
// The caller holds spreadMu.
func (g *typeGroup) refreshVariance() {
	v, ok := g.variance()
	if !ok {
		return
	}
	g.varianceBits.Store(math.Float64bits(v))
	g.varianceKnown.Store(true)
	if g.table != nil && g.entries.Load() >= spreadMinEntries {
		g.table.countVariance(g, v)
	}
}

// typeTable records the type of every posting and, per type, the centroids
// that are live. The type of a posting never changes. It is kept after the
// posting is gone, because a queued reassign may still name the posting
// its token was in.
type typeTable struct {
	store  *lsmkv.Store
	bucket string

	// mu guards the growth of types and the groups map, not the groups
	// themselves
	mu     sync.RWMutex
	types  []uint32 // by posting id
	groups map[uint32]*typeGroup

	// The reference of the spread rule: the mean variance over the types
	// that have reached spreadMinEntries entries and have a known
	// variance. Each type counts once, with its latest value
	// (countVariance). Not persisted: it is rebuilt from the restored
	// sums on open (refreshVariances). refMu guards the sum and the
	// count. reference holds the bits of their quotient for
	// postingLimits, 0 while no type counts.
	refMu     sync.Mutex
	refSum    float64
	refCount  int
	reference atomic.Uint64

	// persistMu makes persistSpread and resetGroups exclusive: a
	// persistSpread that took the groups before a reset would otherwise
	// write their sums back after the reset deleted them.
	persistMu sync.Mutex
	// afterSpreadSnapshot, set by tests only, is called by persistSpread
	// once it has taken the groups.
	afterSpreadSnapshot func()
}

// countVariance makes v the share of type g in the reference. The first
// time, v is added; after that, v replaces the type's previous value. The
// caller holds g.spreadMu; refMu is taken inside it, never the other way
// round.
func (tt *typeTable) countVariance(g *typeGroup, v float64) {
	tt.refMu.Lock()
	defer tt.refMu.Unlock()
	if g.counted {
		tt.refSum += v - g.refVar
	} else {
		tt.refSum += v
		tt.refCount++
		g.counted = true
	}
	g.refVar = v
	tt.reference.Store(math.Float64bits(tt.refSum / float64(tt.refCount)))
}

// relativeSpread is the variance of type g over the reference, clamped to
// [minRelativeSpread, maxRelativeSpread]. It is 1 while the type's
// variance or the reference is unknown (see postingLimits).
func (tt *typeTable) relativeSpread(g *typeGroup) float64 {
	if !g.varianceKnown.Load() {
		return 1
	}
	ref := math.Float64frombits(tt.reference.Load())
	if ref <= 0 {
		return 1
	}
	v := math.Float64frombits(g.varianceBits.Load())
	return min(max(v/ref, minRelativeSpread), maxRelativeSpread)
}

// refreshVariances recomputes every type's variance and the reference
// from the sums. It runs once restore has recounted the entries: the sums
// come back before the postings, when no type has its size yet.
func (tt *typeTable) refreshVariances() {
	tt.mu.RLock()
	groups := make([]*typeGroup, 0, len(tt.groups))
	for _, g := range tt.groups {
		groups = append(groups, g)
	}
	tt.mu.RUnlock()
	for _, g := range groups {
		g.spreadMu.Lock()
		g.refreshVariance()
		g.spreadMu.Unlock()
	}
}

// spreadKeyPrefix starts the key of a type's spread sums in the type
// bucket: the prefix and the type, 5 bytes, so it collides neither with a
// posting key nor with another key of the bucket.
const spreadKeyPrefix byte = 's'

func spreadKey(typ uint32) []byte {
	key := make([]byte, 5)
	key[0] = spreadKeyPrefix
	binary.LittleEndian.PutUint32(key[1:], typ)
	return key
}

// persistSpread writes the spread sums of every type whose sums changed
// since the last call. Unlike the entry counts, the sums cannot be
// recounted from the postings on open. Value: observed tokens (u64), sum
// of squared norms (f64), the sum vector (f64 each).
func (tt *typeTable) persistSpread() error {
	tt.persistMu.Lock()
	defer tt.persistMu.Unlock()
	bucket, release, err := tt.acquire()
	if err != nil {
		return err
	}
	defer release()

	tt.mu.RLock()
	groups := make(map[uint32]*typeGroup, len(tt.groups))
	for typ, g := range tt.groups {
		groups[typ] = g
	}
	tt.mu.RUnlock()
	if tt.afterSpreadSnapshot != nil {
		tt.afterSpreadSnapshot()
	}

	for typ, g := range groups {
		g.spreadMu.Lock()
		if !g.dirty {
			g.spreadMu.Unlock()
			continue
		}
		val := make([]byte, 16+8*len(g.sum))
		binary.LittleEndian.PutUint64(val, g.seen)
		binary.LittleEndian.PutUint64(val[8:], math.Float64bits(g.sumSq))
		for i, s := range g.sum {
			binary.LittleEndian.PutUint64(val[16+8*i:], math.Float64bits(s))
		}
		err := bucket.Put(spreadKey(typ), val)
		if err == nil {
			g.dirty = false
		}
		g.spreadMu.Unlock()
		if err != nil {
			return errors.Wrapf(err, "failed to persist the spread of type %d", typ)
		}
	}
	return nil
}

// resetGroups forgets the groups and the spread sums, in memory and on disk,
// for a build that starts over (resetBuild): its tokens are observed again.
// The type recorded per posting id stays.
func (tt *typeTable) resetGroups() error {
	tt.persistMu.Lock()
	defer tt.persistMu.Unlock()
	tt.mu.Lock()
	tt.groups = make(map[uint32]*typeGroup)
	tt.mu.Unlock()
	tt.refMu.Lock()
	tt.refSum, tt.refCount = 0, 0
	tt.reference.Store(0)
	tt.refMu.Unlock()

	bucket, release, err := tt.acquire()
	if err != nil {
		return err
	}
	defer release()
	var keys [][]byte
	c := bucket.CursorReplaceReusable()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if len(k) == 5 && k[0] == spreadKeyPrefix {
			keys = append(keys, append([]byte(nil), k...))
		}
	}
	c.Close()
	for _, k := range keys {
		if err := bucket.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// restoreSpread loads the sums persistSpread wrote for a type.
func (tt *typeTable) restoreSpread(typ uint32, val []byte) {
	if len(val) < 16 || (len(val)-16)%8 != 0 {
		return
	}
	g := tt.ensureGroup(typ)
	g.spreadMu.Lock()
	defer g.spreadMu.Unlock()
	g.seen = binary.LittleEndian.Uint64(val)
	g.sumSq = math.Float64frombits(binary.LittleEndian.Uint64(val[8:]))
	g.sum = make([]float64, (len(val)-16)/8)
	for i := range g.sum {
		g.sum[i] = math.Float64frombits(binary.LittleEndian.Uint64(val[16+8*i:]))
	}
	g.refreshVariance()
}

// newTypeTable opens the bucket that persists the posting types. The table
// has its own bucket. hfresh's shared bucket is not used: its accessor is
// unexported, and nothing under hfresh changes for this index.
func newTypeTable(store *lsmkv.Store, id string, cfg hfresh.StoreConfig) (*typeTable, error) {
	name := helpers.TachiomFreshTypesBucketName(id)
	err := store.CreateOrLoadBucket(context.Background(), name,
		cfg.MakeBucketOptions(lsmkv.StrategyReplace)...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", name)
	}
	return &typeTable{
		store:  store,
		bucket: name,
		groups: make(map[uint32]*typeGroup),
	}, nil
}

// acquire pins the bucket for one operation, as hfresh's bucketRef.acquire.
func (tt *typeTable) acquire() (*lsmkv.Bucket, func(), error) {
	bucket, release := tt.store.AcquireBucketForRead(tt.bucket)
	if bucket == nil {
		release()
		return nil, nil, errors.Wrapf(lsmkv.ErrBucketNotFound, "bucket %s", tt.bucket)
	}
	return bucket, release, nil
}

// postingFormatKey holds the format of the persisted postings in the type
// bucket: 6 bytes, so it collides with no other key.
// postingFormat is the current format, written when the index is
// initialised: one Set key per posting id (see postingStore). An index
// without the key keeps its postings in hfresh's posting bucket, which
// this version does not read.
var postingFormatKey = []byte("format")

// 3: an entry above version 1 carries its version (store.go, posting.go).
const postingFormat byte = 3

func (tt *typeTable) setPostingFormat() error {
	bucket, release, err := tt.acquire()
	if err != nil {
		return err
	}
	defer release()
	return bucket.Put(postingFormatKey, []byte{postingFormat})
}

// checkPostingFormat fails on an index whose postings this version cannot
// read. Opened anyway, the index would have centroids and no postings, and
// every search would find nothing.
func (tt *typeTable) checkPostingFormat() error {
	bucket, release, err := tt.acquire()
	if err != nil {
		return err
	}
	defer release()
	v, err := bucket.Get(postingFormatKey)
	if err != nil {
		return err
	}
	if len(v) != 1 || v[0] != postingFormat {
		return errors.Errorf("the index was saved in another posting format (%v, this version reads %d) and must be rebuilt", v, postingFormat)
	}
	return nil
}

// centerKey holds the centering mean in the type bucket (see staging.go),
// float32s little-endian: 6 bytes, so it collides with no posting key (8),
// spread key (5) or the format key ("format", another 6 bytes). It is
// written once, when the index is built from its staged
// documents (buildFromStaging).
var centerKey = []byte("center")

// phaseKey holds the phase of the index in the type bucket (staging.go):
// phaseStaging or phaseBuilt. It is 5 bytes like a spread key, but a
// spread key starts with spreadKeyPrefix 's' and this one with 'p'. The
// key is absent on an index saved before the phase existed (see the phase
// constants in staging.go).
var phaseKey = []byte("phase")

func (tt *typeTable) setPhase(phase byte) error {
	bucket, release, err := tt.acquire()
	if err != nil {
		return err
	}
	defer release()
	return bucket.Put(phaseKey, []byte{phase})
}

// getPhase returns the persisted phase, phaseUnset when none was written.
func (tt *typeTable) getPhase() (byte, error) {
	bucket, release, err := tt.acquire()
	if err != nil {
		return 0, err
	}
	defer release()
	v, err := bucket.Get(phaseKey)
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return phaseUnset, nil
	}
	if len(v) != 1 || (v[0] != phaseStaging && v[0] != phaseBuilt) {
		return 0, errors.Errorf("the persisted index phase is %v", v)
	}
	return v[0], nil
}

func (tt *typeTable) setCenter(mean []float32) error {
	bucket, release, err := tt.acquire()
	if err != nil {
		return err
	}
	defer release()
	val := make([]byte, 4*len(mean))
	for i, v := range mean {
		binary.LittleEndian.PutUint32(val[4*i:], math.Float32bits(v))
	}
	return bucket.Put(centerKey, val)
}

// getCenter returns the persisted centering mean, nil when none was written.
func (tt *typeTable) getCenter() ([]float32, error) {
	bucket, release, err := tt.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	val, err := bucket.Get(centerKey)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}
	if len(val)%4 != 0 {
		return nil, errors.Errorf("the persisted centering mean has %d bytes", len(val))
	}
	mean := make([]float32, len(val)/4)
	for i := range mean {
		mean[i] = math.Float32frombits(binary.LittleEndian.Uint32(val[4*i:]))
	}
	return mean, nil
}

// register records and persists the type of a new posting. It is called
// before the posting's centroid becomes searchable, so every reader that
// reaches the posting finds its type.
func (tt *typeTable) register(postingID uint64, typ uint32) error {
	bucket, release, err := tt.acquire()
	if err != nil {
		return err
	}
	defer release()

	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], postingID)
	var val [4]byte
	binary.LittleEndian.PutUint32(val[:], typ)
	if err := bucket.Put(key[:], val[:]); err != nil {
		return errors.Wrapf(err, "failed to persist the type of posting %d", postingID)
	}
	tt.set(postingID, typ)
	return nil
}

func (tt *typeTable) set(postingID uint64, typ uint32) {
	tt.mu.Lock()
	if uint64(len(tt.types)) <= postingID {
		tt.types = append(tt.types, make([]uint32, postingID+1-uint64(len(tt.types)))...)
	}
	tt.types[postingID] = typ
	tt.mu.Unlock()
}

// typeOf returns the type of a posting registered earlier.
func (tt *typeTable) typeOf(postingID uint64) uint32 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	if uint64(len(tt.types)) <= postingID {
		return 0
	}
	return tt.types[postingID]
}

func (tt *typeTable) group(typ uint32) *typeGroup {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	return tt.groups[typ]
}

// groupOf returns the group of the type of a posting registered earlier, nil
// when the type has no group yet.
func (tt *typeTable) groupOf(postingID uint64) *typeGroup {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	if uint64(len(tt.types)) <= postingID {
		return nil
	}
	return tt.groups[tt.types[postingID]]
}

// ensureGroup returns the group of a type, creating it when the type is new.
func (tt *typeTable) ensureGroup(typ uint32) *typeGroup {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	g := tt.groups[typ]
	if g == nil {
		g = &typeGroup{table: tt}
		tt.groups[typ] = g
	}
	return g
}

// addEntries adds delta to the entry count of the type of a posting
// registered earlier.
func (tt *typeTable) addEntries(postingID uint64, delta int) {
	if delta == 0 {
		return
	}
	g := tt.groupOf(postingID)
	if g == nil {
		g = tt.ensureGroup(tt.typeOf(postingID))
	}
	g.entries.Add(int64(delta))
}

// join adds a centroid to the group of its type. It is called once the
// centroid is in the centroid index, so a scan never meets an id the index
// does not know.
func (tt *typeTable) join(postingID uint64, typ uint32) {
	g := tt.ensureGroup(typ)

	g.mu.Lock()
	g.ids = append(g.ids, postingID)
	g.mu.Unlock()
}

// leave removes a centroid from the group of its type. It is called before
// the centroid is deleted from the centroid index, for the same reason join
// is called after the insert. Order within a group carries no meaning.
func (tt *typeTable) leave(postingID uint64, typ uint32) {
	g := tt.group(typ)
	if g == nil {
		return
	}
	g.mu.Lock()
	for i, id := range g.ids {
		if id == postingID {
			g.ids[i] = g.ids[len(g.ids)-1]
			g.ids = g.ids[:len(g.ids)-1]
			break
		}
	}
	g.mu.Unlock()
}

// groupSize returns the number of live centroids of a type.
func (tt *typeTable) groupSize(typ uint32) int {
	g := tt.group(typ)
	if g == nil {
		return 0
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.ids)
}

// restore reloads the posting types from the bucket and rebuilds the groups
// from the postings whose centroid exists.
func (tt *typeTable) restore(exists func(postingID uint64) bool) error {
	bucket, release, err := tt.acquire()
	if err != nil {
		return err
	}
	defer release()

	c := bucket.CursorReplaceReusable()
	defer c.Close()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(k) == 5 && k[0] == spreadKeyPrefix {
			tt.restoreSpread(binary.LittleEndian.Uint32(k[1:]), v)
			continue
		}
		if len(k) != 8 || len(v) != 4 {
			continue // the format, center and phase keys
		}
		postingID := binary.LittleEndian.Uint64(k)
		typ := binary.LittleEndian.Uint32(v)
		tt.set(postingID, typ)
		if exists(postingID) {
			tt.join(postingID, typ)
		}
	}
	return nil
}

// nearestCentroids returns the k centroids nearest to vec that a token of
// type typ may be routed to: centroids of its own type, found by a scan of
// the whole group; the centroid HNSW is not searched with a type filter.
// The centroid HNSW is searched by queries only. Distances come from the
// RQ-8 codes the centroid index already holds, the ones an HNSW search
// compares, so no second copy of the centroids is kept.
//
// The scan ranks by the squared L2 distance, computed on the codes
// with nothing decoded. The HNSWIndex doc in hnsw.go says why: the split
// clusters by L2, and the dot product would favour the centroid of
// largest norm. hfresh places a token by the index distance (RNGSelect
// searches its centroid HNSW with it) and has no rule for this. The
// arithmetic is that of Weaviate's own L2 RQ distancer over the same code
// (RQDistancer.Distance with l2 = 1). The results are sorted by that
// distance, ascending.
func (t *TachiomFresh) nearestCentroids(vec []float32, typ uint32, k int) (*hfresh.ResultSet, error) {
	results := hfresh.NewResultSet(k)
	g := t.types.group(typ)
	if g == nil {
		return results, nil
	}

	distancer := t.Centroids.Distancer(vec)
	defer distancer.Close()
	var norm2 float32
	for _, x := range vec {
		norm2 += x * x
	}

	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, id := range g.ids {
		dist, err := distancer.DistanceToNode(id)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to compute distance to centroid %d", id)
		}
		cNorm2, err := t.Centroids.Norm2(id)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read the norm of centroid %d", id)
		}
		results.Insert(id, norm2+cNorm2-2*t.similarity(dist))
	}
	return results, nil
}

// placementDistance is the distance by which a token is placed: the squared
// L2 distance computed on the codes, as nearestCentroids ranks them.
func (t *TachiomFresh) placementDistance(vec []float32, postingID uint64) (float32, error) {
	distancer := t.Centroids.Distancer(vec)
	defer distancer.Close()
	dist, err := distancer.DistanceToNode(postingID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to compute distance to centroid %d", postingID)
	}
	var norm2 float32
	for _, x := range vec {
		norm2 += x * x
	}
	cNorm2, err := t.Centroids.Norm2(postingID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read the norm of centroid %d", postingID)
	}
	return norm2 + cNorm2 - 2*t.similarity(dist), nil
}

// aloneInGroup reports whether posting id is the only one of its type. Such
// a posting has nothing to merge with, however small it is, unless it is
// empty, which a merge deletes. Most token types are rare, and many never
// reach minPostingSize.
func (t *TachiomFresh) aloneInGroup(postingID uint64) bool {
	return t.types.groupSize(t.types.typeOf(postingID)) <= 1
}

// postingLimits returns the sizes below which posting id is merged and
// above which it is split. They follow the size of the posting's type and its spread relative to the other large types:
//
//	max = max(maxPostingSize, floor(sqrt(entries of the type) / s))
//	s   = clamp(variance of the type / mean variance over the large types, 0.05, 1)
//	min = max(max/3, minPostingSizeFloor), hfresh's ratio
//
// The postings of a frequent type grow with the square root of its size,
// and so does its centroid count. Without the square root, the centroid
// count would grow with the size itself. This is the sqrt damping of TAC's
// centroid allocation. The spread divides the size, as TAC's allocation
// multiplies the centroid share by it. A
// type of average spread has sqrt(entries). A tighter type has larger
// postings and fewer centroids. A more spread-out type has the same
// sqrt(entries), because the ratio is capped at maxRelativeSpread, 1 (the
// constant explains why). The reference is the mean of the variances over
// the types of spreadMinEntries entries or more (typeTable.reference).
// The spread is relative to the reference for this reason: TACHIOM's score
// sqrt(n) x spread is a share of a global budget, so a factor common to
// every type cancels there. Without a budget, the absolute spread sets the
// scale of every posting. Measured with sqrt(n) / spread, the limits were
// a multiple of those of the formula without the spread. On an encoder
// whose raw tokens share a common mean they reached the clamp: its large
// types held many times more entries per centroid than the small ones.
//
// A type under spreadMinEntries entries keeps the configured pair, and so
// does a type whose size by the formula is not above maxPostingSize. s is
// 1 while the type's variance or the reference is unknown: an index
// restored without spread sums, a type that has seen fewer than
// spreadRefresh tokens, or no large type yet. hfresh has one pair for the
// whole index.
//
// The limits move with the type, and with the reference when another
// large type refreshes its variance. When a type grows, a posting that
// was big enough can fall under the minimum; it is merged when a search
// visits it. When a type shrinks, a posting over the new maximum is split
// at its next append.
func (t *TachiomFresh) postingLimits(postingID uint64) (minSize, maxSize uint32) {
	g := t.types.groupOf(postingID)
	if g == nil {
		return t.minPostingSize, t.maxPostingSize
	}
	entries := g.entries.Load()
	if entries < spreadMinEntries {
		return t.minPostingSize, t.maxPostingSize
	}
	maxSize = uint32(math.Sqrt(float64(entries)) / t.types.relativeSpread(g))
	if maxSize <= t.maxPostingSize {
		return t.minPostingSize, t.maxPostingSize
	}
	return max(maxSize/3, minPostingSizeFloor), maxSize
}

// countEntries keeps the entry count of a posting's type current. It is
// called wherever the posting table gains or loses entries (append,
// removeToken, putPosting).
func (t *TachiomFresh) countEntries(postingID uint64, delta int) {
	t.types.addEntries(postingID, delta)
}
