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

package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/config"
)

// shardView is one published answer of the serving rule for one shard. A
// caller acquires it once and takes every property index from it. A retire
// deletes a folder only after this view and every older one are released.
type shardView struct {
	shard  *Shard
	store  *lsmkv.Store
	props  map[string]reindexPropView
	served map[string]*lsmkv.Bucket // keyed by the plain name; nil while the index is off
	staged []*stagedBuild

	refs       atomic.Int64
	superseded atomic.Bool
	isDrained  atomic.Bool
	drained    chan struct{}
	prev, next atomic.Pointer[shardView]
}

type stagedBuild struct {
	taskID    string
	gen       uint64
	prop      string
	indexType string
	name      string
	bucket    *lsmkv.Bucket

	target   *models.Property
	strategy MigrationStrategy
	overlay  map[string]inverted.PropertyOverlay
	add      onAddToPropertyValueIndex
	del      onDeleteFromPropertyValueIndex
	scanned  atomic.Bool
}

var _ inverted.IndexView = (*shardView)(nil)

func newShardView(s *Shard, props map[string]reindexPropView) *shardView {
	return &shardView{shard: s, store: s.store, props: props, served: map[string]*lsmkv.Bucket{}, drained: make(chan struct{})}
}

// Tokenization falls back to the swap overlay for a property the view does not govern.
func (v *shardView) Tokenization(propName, schemaTokenization string) string {
	if pv, ok := v.props[propName]; ok {
		return pv.tokenization
	}
	return v.shard.TokenizationFor(propName, schemaTokenization)
}

// RangeableReady follows the view for a property it governs.
func (v *shardView) RangeableReady(prop *models.Property) bool {
	name := helpers.BucketRangeableFromPropNameLSM(prop.Name)
	if b, ok := v.served[name]; ok {
		return b != nil
	}
	if !inverted.HasRangeableIndex(prop) {
		return false
	}
	if ready, set := v.shard.rangeableReadyMark(prop.Name); set {
		return ready
	}
	return !v.shard.rangeableUndecidable.Load() && v.Bucket(name) != nil
}

func (v *shardView) FallbackToSearchable() bool {
	return v.shard.isFallbackToSearchable()
}

func (v *shardView) Bucket(name string) *lsmkv.Bucket {
	if b, ok := v.served[name]; ok {
		return b
	}
	return v.store.Bucket(name)
}

// AcquireBucketForRead needs no pin for an index the view serves, because the
// view's own reference holds off its retire.
func (v *shardView) AcquireBucketForRead(name string) (*lsmkv.Bucket, func()) {
	if b, ok := v.served[name]; ok {
		return b, func() {}
	}
	return v.store.AcquireBucketForRead(name)
}

func (s *Shard) loadView() *shardView {
	if v := s.view.current.Load(); v != nil {
		return v
	}
	return newShardView(s, nil)
}

func (s *Shard) acquireView() *shardView {
	for {
		v := s.view.current.Load()
		if v == nil {
			return newShardView(s, nil)
		}
		v.refs.Add(1)
		if s.view.current.Load() == v {
			return v
		}
		v.release()
	}
}

func (v *shardView) release() {
	v.refs.Add(-1)
	v.tryDrain()
}

// tryDrain marks a replaced view drained once no one holds it and every older
// view is drained, then gives the next view the same chance.
func (v *shardView) tryDrain() {
	for v != nil {
		if !v.superseded.Load() || v.refs.Load() > 0 {
			return
		}
		if p := v.prev.Load(); p != nil && !p.isDrained.Load() {
			return
		}
		if !v.isDrained.CompareAndSwap(false, true) {
			return
		}
		v.prev.Store(nil)
		close(v.drained)
		v = v.next.Load()
	}
}

// publishView replaces the current view in one atomic store; it never waits.
func (s *Shard) publishView(next *shardView) {
	cur := s.view.current.Load()
	next.prev.Store(cur)
	if cur != nil {
		cur.next.Store(next)
	}
	s.view.current.Store(next)
	if cur != nil {
		cur.superseded.Store(true)
		cur.tryDrain()
	}
}

func viewProperty(prop *models.Property, pv reindexPropView, ok bool) *models.Property {
	if !ok {
		return prop
	}
	q := *prop
	filterable, searchable, rangeable := pv.on[indexTypeFilterable], pv.on[indexTypeSearchable], pv.on[indexTypeRangeable]
	q.IndexFilterable, q.IndexSearchable, q.IndexRangeFilters = &filterable, &searchable, &rangeable
	q.Tokenization = pv.tokenization
	if pv.blockmax {
		blockmax := true
		q.SearchableBlockmax = &blockmax
	}
	return &q
}

func (s *Shard) withViewIndexes(prop *models.Property) *models.Property {
	pv, ok := s.loadView().props[prop.Name]
	return viewProperty(prop, pv, ok)
}

func (s *Shard) servedIndexName(propName, indexType string) string {
	return reindexBucketName(propName, indexType, s.loadView().props[propName].gen[indexType])
}

// loadReindexView decides, before any property bucket opens, which folder
// each property index opens.
func (s *Shard) loadReindexView() error {
	class, props, err := s.readReindexView()
	if err != nil {
		return fmt.Errorf("read reindex state: %w", err)
	}
	if class != nil {
		for _, prop := range class.Properties {
			for _, t := range reindexIndexTypes {
				if err := s.refuseMissingServedFolder(prop.Name, t, props[prop.Name]); err != nil {
					return err
				}
			}
		}
	}
	s.view.current.Store(newShardView(s, props))
	return nil
}

func (s *Shard) finishViewLoad() {
	cur := s.loadView()
	next := newShardView(s, cur.props)
	for prop, pv := range cur.props {
		for _, t := range reindexIndexTypes {
			next.served[reindexBucketName(prop, t, 0)] = nil
			if pv.on[t] {
				next.served[reindexBucketName(prop, t, 0)] = s.store.Bucket(reindexBucketName(prop, t, pv.gen[t]))
			}
		}
	}
	s.publishView(next)
}

// refuseMissingServedFolder refuses to serve an empty index over objects
// when a generation folder the view names is gone.
func (s *Shard) refuseMissingServedFolder(prop, indexType string, pv reindexPropView) error {
	if !pv.on[indexType] || pv.gen[indexType] == 0 {
		return nil
	}
	name := reindexBucketName(prop, indexType, pv.gen[indexType])
	if _, err := os.Stat(filepath.Join(s.pathLSM(), name)); !errors.Is(err, os.ErrNotExist) || !s.holdsObjects() {
		return nil
	}
	return fmt.Errorf("property %q %s index: served folder %q is missing", prop, indexType, name)
}

func (s *Shard) holdsObjects() bool {
	entries, err := os.ReadDir(filepath.Join(s.pathLSM(), helpers.ObjectsBucketLSM))
	return err == nil && slices.ContainsFunc(entries, func(e os.DirEntry) bool {
		return filepath.Ext(e.Name()) == ".db" || filepath.Ext(e.Name()) == ".wal"
	})
}

var errViewNotReady = errors.New("a folder the view names is still being retired")

type stagingError struct {
	taskID string
	err    error
}

func (e *stagingError) Error() string { return e.err.Error() }
func (e *stagingError) Unwrap() error { return e.err }

// refreshView makes a loaded shard serve what the cluster state now says. It
// never waits on a reader. A folder still being retired leaves it not ready,
// and the refresh loop tries again.
func (s *Shard) refreshView(ctx context.Context) error {
	s.view.mu.Lock()
	defer s.view.mu.Unlock()
	if s.view.closed {
		return nil
	}
	err := s.refreshViewLocked(ctx)
	s.view.wait.noteRefresh(err)
	return err
}

func (s *Shard) refreshViewLocked(ctx context.Context) error {
	class, props, err := s.readReindexView()
	if err != nil {
		return fmt.Errorf("read reindex state: %w", err)
	}
	if class == nil {
		return nil
	}
	cur := s.loadView()
	next := newShardView(s, props)
	var opened []string
	if err := s.resolveServed(ctx, class, next, &opened); err != nil {
		s.closeUnpublished(opened)
		return err
	}
	ready, err := s.resolveStaged(ctx, class, cur, next, &opened)
	if err != nil {
		s.closeUnpublished(opened)
		return err
	}
	s.publishView(next)
	s.retireBuckets(replacedBuckets(cur, next), cur)
	if !ready {
		return errViewNotReady
	}
	return nil
}

func (s *Shard) resolveServed(ctx context.Context, class *models.Class, next *shardView, opened *[]string) error {
	for _, prop := range class.Properties {
		pv, ok := next.props[prop.Name]
		if !ok {
			continue
		}
		for _, t := range reindexIndexTypes {
			plain := reindexBucketName(prop.Name, t, 0)
			next.served[plain] = nil
			if !pv.on[t] {
				continue
			}
			name := reindexBucketName(prop.Name, t, pv.gen[t])
			if s.retirePending(name) {
				return errViewNotReady
			}
			if b := s.store.Bucket(name); b != nil {
				next.served[plain] = b
				continue
			}
			if err := s.refuseMissingServedFolder(prop.Name, t, pv); err != nil {
				return err
			}
			opts := s.propertyIndexOptions(viewProperty(prop, pv, true), t, s.makeDefaultBucketOptions)
			if err := s.store.CreateOrLoadBucket(ctx, name, opts...); err != nil {
				return fmt.Errorf("open served folder %q: %w", name, err)
			}
			*opened = append(*opened, name)
			next.served[plain] = s.store.Bucket(name)
		}
	}
	return nil
}

// resolveStaged opens the folder of every build the view stages. A folder
// still held by a replaced build or a retire is left out until it is free.
func (s *Shard) resolveStaged(ctx context.Context, class *models.Class, cur, next *shardView, opened *[]string,
) (ready bool, err error) {
	ready = true
	for _, prop := range class.Properties {
		for _, st := range next.props[prop.Name].staged {
			for _, t := range st.target.indexTypes {
				b := &stagedBuild{
					taskID: st.taskID, gen: st.gen, prop: prop.Name, indexType: t,
					name: reindexBucketName(prop.Name, t, st.gen),
				}
				i := slices.IndexFunc(cur.staged, func(o *stagedBuild) bool { return o.name == b.name })
				switch {
				case i >= 0 && cur.staged[i].taskID == b.taskID:
					next.staged = append(next.staged, cur.staged[i])
				case i >= 0 || s.retirePending(b.name):
					ready = false
				default:
					if err := s.openStagedBuild(ctx, prop, next, st, b); err != nil {
						return false, &stagingError{taskID: st.taskID, err: err}
					}
					*opened = append(*opened, b.name)
					next.staged = append(next.staged, b)
				}
			}
		}
	}
	return ready, nil
}

// openStagedBuild opens a build's folder, emptied while its local unit has
// not started, with the options a build keeps until the next load.
func (s *Shard) openStagedBuild(ctx context.Context, prop *models.Property, next *shardView, st reindexStaged,
	b *stagedBuild,
) error {
	if s.store.Bucket(b.name) != nil {
		return fmt.Errorf("build folder %q is open outside the view", b.name)
	}
	if !st.unitStarted {
		if err := os.RemoveAll(filepath.Join(s.pathLSM(), b.name)); err != nil {
			return fmt.Errorf("empty build folder %q: %w", b.name, err)
		}
	}
	target := viewProperty(prop, next.props[prop.Name], true)
	if st.target.tokenization != "" {
		target.Tokenization = st.target.tokenization
	}
	blockmax := st.target.blockmax
	if served := next.served[helpers.BucketSearchableFromPropNameLSM(prop.Name)]; !blockmax && served != nil {
		blockmax = served.Strategy() == lsmkv.StrategyInverted
	}
	target.SearchableBlockmax = &blockmax
	opts := append(s.propertyIndexOptions(target, b.indexType, s.makeDefaultBucketOptions),
		lsmkv.WithKeepLevelCompaction(true), lsmkv.WithKeepTombstones(true), lsmkv.WithKeepSegmentsInMemory(false))
	if err := s.store.CreateOrLoadBucket(ctx, b.name, opts...); err != nil {
		return fmt.Errorf("open build folder %q: %w", b.name, err)
	}
	b.bucket = s.store.Bucket(b.name)
	strategy, err := stagedStrategy(s.index.Config.ClassName.String(), target, st, b)
	if err != nil {
		return err
	}
	b.target, b.strategy, b.overlay = target, strategy, strategy.AnalyzerOverlay([]string{prop.Name})
	namer := func(string) string { return b.name }
	armed := armedMirror{props: map[string]struct{}{prop.Name: {}}, buckets: map[string]*lsmkv.Bucket{prop.Name: b.bucket}}
	b.add, b.del = strategy.MakeAddCallback(namer, armed), strategy.MakeDeleteCallback(namer, armed)
	return nil
}

// stagedStrategy builds one index type of a staged task with target's settings.
func stagedStrategy(className string, target *models.Property, st reindexStaged, b *stagedBuild,
) (MigrationStrategy, error) {
	gen, props := int(b.gen), []string{b.prop}
	switch st.target.migrationType {
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		if b.indexType == indexTypeFilterable {
			return &FilterableRetokenizeStrategy{
				propName: b.prop, targetTokenization: target.Tokenization,
				className: className, generation: gen,
			}, nil
		}
		return &SearchableRetokenizeStrategy{
			propName: b.prop, targetTokenization: target.Tokenization,
			className: className, bucketStrategy: b.bucket.Strategy(), generation: gen,
		}, nil
	case ReindexTypeEnableFilterable:
		return &EnableFilterableStrategy{propNames: props, generation: gen}, nil
	case ReindexTypeEnableSearchable:
		return &EnableSearchableStrategy{propNames: props, tokenization: target.Tokenization, generation: gen}, nil
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return &FilterableToRangeableStrategy{propNames: props, generation: gen}, nil
	case ReindexTypeChangeAlgorithm:
		return &MapToBlockmaxStrategy{generation: gen}, nil
	case ReindexTypeRebuildSearchable:
		return &RebuildSearchableStrategy{propNames: props, generation: gen}, nil
	case ReindexTypeRepairFilterable:
		return &RoaringSetRefreshStrategy{generation: gen}, nil
	default:
		return nil, fmt.Errorf("build %q: unknown migration type %q", b.name, st.target.migrationType)
	}
}

// writeStaged moves a write's previous and new version through every build its
// view stages, each analyzed the way that build's strategy indexes it.
func (s *Shard) writeStaged(v *shardView, object, prev *storobj.Object, docID, prevDocID uint64) error {
	for _, b := range v.staged {
		if prev != nil {
			if err := s.mirrorStaged(b, prev, prevDocID, b.del); err != nil {
				return err
			}
		}
		if object != nil {
			if err := s.mirrorStaged(b, object, docID, b.add); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Shard) mirrorStaged(b *stagedBuild, obj *storobj.Object, docID uint64,
	write func(*Shard, uint64, *inverted.Property) error,
) error {
	props, err := s.analyzeForBuild(b, obj)
	if err != nil {
		return err
	}
	for i := range props {
		if err := write(s, docID, &props[i]); err != nil {
			return fmt.Errorf("write into build %q: %w", b.name, err)
		}
	}
	return nil
}

// analyzeForBuild analyzes obj with the settings the view serves once b switches,
// not the class's, which differ while an earlier run's switched tenants stand.
func (s *Shard) analyzeForBuild(b *stagedBuild, obj *storobj.Object) ([]inverted.Property, error) {
	className := s.index.Config.ClassName.String()
	c := s.index.getSchema.ReadOnlyClass(className)
	if c == nil {
		return nil, fmt.Errorf("analyze for build %q: class %s not in schema", b.name, className)
	}
	schemaMap, _, err := s.analyzeObjectCommon(obj, c)
	if err != nil {
		return nil, fmt.Errorf("analyze for build %q: %w", b.name, err)
	}
	analyzer := inverted.NewAnalyzerWithRawValues(s.isFallbackToSearchable, className).WithSchemaOverlay(b.overlay)
	props, _, err := analyzer.Object(schemaMap, []*models.Property{b.target}, obj.ID())
	if err != nil {
		return nil, fmt.Errorf("analyze for build %q: %w", b.name, err)
	}
	return slices.DeleteFunc(props, func(p inverted.Property) bool { return p.Name != b.prop }), nil // drops the internal _id
}

const reindexBuildSuffix = ".build"

// ReindexBuild fills the task's builds of prop once staged. started runs in
// between, so a unit records its start only over an emptied folder. The scan
// waits until no write that loaded a view without the builds still runs.
func (s *Shard) ReindexBuild(ctx context.Context, taskID string, gen uint64, prop string,
	started func(context.Context) error,
) error {
	builds, err := s.awaitStagedBuilds(ctx, taskID, gen, prop)
	if err != nil {
		return err
	}
	if err := started(ctx); err != nil {
		return err
	}
	v := s.acquireView() // holds off the retire of every build it stages
	defer v.release()
	for _, b := range builds {
		if !slices.Contains(v.staged, b) {
			return fmt.Errorf("build %q is no longer staged", b.name)
		}
	}
	if prev := v.prev.Load(); prev != nil {
		select {
		case <-prev.drained:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, b := range builds {
		if b.scanned.Load() {
			continue
		}
		if err := s.scanIntoBuild(ctx, b); err != nil {
			return fmt.Errorf("build %q: %w", b.name, err)
		}
		// Only after the last prepend, which refuses a bucket that has it, so a relaunch in this process never scans again.
		if b.indexType == indexTypeRangeable && s.index.Config.keepRangeableInMemory(prop) {
			if err := b.bucket.RebuildRangeableSegmentInMemory(ctx); err != nil {
				return fmt.Errorf("build %q: %w", b.name, err)
			}
		}
		b.scanned.Store(true)
	}
	return nil
}

// scanIntoBuild places a scan under the live writes, which win. Every exit deletes the scratch.
func (s *Shard) scanIntoBuild(ctx context.Context, b *stagedBuild) error {
	name := b.name + reindexBuildSuffix
	dir := filepath.Join(s.pathLSM(), name)
	if s.store.Bucket(name) != nil {
		if err := s.store.ShutdownBucket(ctx, name); err != nil {
			return fmt.Errorf("close stale scratch: %w", err)
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("empty scratch: %w", err)
	}
	if err := s.store.CreateOrLoadBucket(ctx, name, s.makeDefaultBucketOptions(b.strategy.TargetStrategy(),
		lsmkv.WithKeepSegmentsInMemory(false))...); err != nil {
		return fmt.Errorf("open scratch: %w", err)
	}
	defer func() {
		if s.store.Bucket(name) != nil {
			if err := s.store.ShutdownBucket(context.Background(), name); err != nil {
				s.index.logger.WithField("action", "reindex_build").WithField("bucket", name).
					Errorf("cannot close the scan scratch: %v", err)
			}
		}
		if err := os.RemoveAll(dir); err != nil {
			s.index.logger.WithField("action", "reindex_build").WithField("dir", dir).
				Errorf("cannot delete the scan scratch: %v", err)
		}
	}()
	scratch := s.store.Bucket(name)
	if err := s.scanObjects(ctx, b, scratch); err != nil {
		return err
	}
	if err := scratch.FlushAndSwitch(); err != nil {
		return fmt.Errorf("flush scratch: %w", err)
	}
	if err := s.store.ShutdownBucket(ctx, name); err != nil {
		return fmt.Errorf("close scratch: %w", err)
	}
	if err := b.bucket.PrependSegmentsFromBucket(ctx, dir); err != nil {
		return fmt.Errorf("place the scan under live writes: %w", err)
	}
	return b.bucket.FlushAndSwitch()
}

// scanObjects has no time filter, since the coordinator stamps update times on
// its own clock and a local cut-off could drop an object written before staging.
func (s *Shard) scanObjects(ctx context.Context, b *stagedBuild, scratch *lsmkv.Bucket) error {
	objects, release := s.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
	if objects == nil {
		return fmt.Errorf("objects bucket: %w", lsmkv.ErrBucketNotFound)
	}
	defer release()
	c := objects.Cursor()
	defer c.Close()
	className := s.index.Config.ClassName.String()
	for k, val := c.First(); k != nil; k, val = c.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		obj, err := storobj.FromBinaryDisk(val, className)
		if err != nil {
			return fmt.Errorf("decode object: %w", err)
		}
		props, err := s.analyzeForBuild(b, obj)
		if err != nil {
			return err
		}
		for _, p := range props {
			if err := b.strategy.WriteToReindexBucket(s, scratch, obj.DocID, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// closeUnpublished closes what a failed refresh opened and keeps the folders,
// since a served one may hold data.
func (s *Shard) closeUnpublished(names []string) {
	for _, name := range names {
		if err := s.store.ShutdownBucket(context.Background(), name); err != nil {
			s.index.logger.WithField("action", "reindex_refresh").WithField("bucket", name).
				Errorf("cannot close a folder a failed refresh opened: %v", err)
		}
	}
}

// replacedBuckets is every bucket cur names and next does not. The plain
// bucket of a property next no longer governs stays, as the store serves it.
func replacedBuckets(cur, next *shardView) []*lsmkv.Bucket {
	kept := map[*lsmkv.Bucket]bool{}
	for _, b := range next.served {
		kept[b] = true
	}
	for _, b := range next.staged {
		kept[b.bucket] = true
	}
	var out []*lsmkv.Bucket
	for plain, b := range cur.served {
		_, governed := next.served[plain]
		if b != nil && !kept[b] && (governed || filepath.Base(b.GetDir()) != plain) {
			out = append(out, b)
		}
	}
	for _, b := range cur.staged {
		if !kept[b.bucket] {
			out = append(out, b.bucket)
		}
	}
	return out
}

func (s *Shard) retirePending(name string) bool {
	s.view.retireMu.Lock()
	defer s.view.retireMu.Unlock()
	_, ok := s.view.retiring[filepath.Join(s.store.GetDir(), name)]
	return ok
}

// retireBuckets deletes buckets a publish replaced, off the refresh loop, once
// no one holds the replaced view or an older one.
func (s *Shard) retireBuckets(buckets []*lsmkv.Bucket, replaced *shardView) {
	if len(buckets) == 0 {
		return
	}
	s.view.retireMu.Lock()
	if s.view.retiring == nil {
		s.view.retiring = map[string]struct{}{}
	}
	for _, b := range buckets {
		s.view.retiring[b.GetDir()] = struct{}{}
	}
	s.view.retireMu.Unlock()
	s.view.retires.Add(1)
	enterrors.GoWrapper(func() {
		defer s.view.retires.Done()
		select {
		case <-replaced.drained:
			for _, b := range buckets {
				s.retireBucket(b)
			}
		case <-s.shutCtx.Done():
		}
		s.view.retireMu.Lock()
		for _, b := range buckets {
			delete(s.view.retiring, b.GetDir())
		}
		s.view.retireMu.Unlock()
		s.notifyViewRefresh()
	}, s.index.logger)
}

// retireBucket logs a folder it cannot delete once. No view names it, so no
// load opens it again.
func (s *Shard) retireBucket(b *lsmkv.Bucket) {
	dir := b.GetDir()
	log := s.index.logger.WithField("action", "reindex_retire").WithField("dir", dir)
	if err := s.store.ShutdownBucket(context.Background(), filepath.Base(dir)); err != nil {
		log.Errorf("cannot shut down a replaced index: %v", err)
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Errorf("cannot delete a replaced index: %v", err)
	}
}

func (s *Shard) closeViews() {
	s.view.mu.Lock()
	s.view.closed = true
	s.view.mu.Unlock()
	s.view.retires.Wait()
}

func (s *Shard) notifyViewRefresh() {
	s.index.db.reindexRefresher().notify(s.index.Config.ClassName.String(), []string{s.name})
}

type shardViewState struct {
	current atomic.Pointer[shardView]

	mu     sync.Mutex // serializes refreshes and shutdown
	closed bool

	retireMu sync.Mutex
	retiring map[string]struct{} // folders a retire has not deleted yet
	retires  sync.WaitGroup

	wait stagingWait
}

type stagingWait struct {
	mu           sync.Mutex
	changed      chan struct{}
	failingTask  string
	failingSince time.Time
	failingErr   error
	waiting      map[string]int
}

// noteRefresh restarts the staging clock unless this refresh failed opening
// the same task's build folder as the one before.
func (w *stagingWait) noteRefresh(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var se *stagingError
	switch {
	case !errors.As(err, &se):
		w.failingTask, w.failingErr = "", nil
	case se.taskID != w.failingTask:
		w.failingTask, w.failingSince, w.failingErr = se.taskID, time.Now(), se.err
	default:
		w.failingErr = se.err
	}
	if w.changed != nil {
		close(w.changed)
		w.changed = nil
	}
}

// check returns the channel the next refresh closes, and the staging error
// once it has failed for this task for limit in a row.
func (w *stagingWait) check(taskID string, limit time.Duration) (<-chan struct{}, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.changed == nil {
		w.changed = make(chan struct{})
	}
	if w.failingTask == taskID && time.Since(w.failingSince) >= limit {
		return w.changed, fmt.Errorf("cannot open the build folder for %s: %w", limit, w.failingErr)
	}
	return w.changed, nil
}

func (w *stagingWait) addWaiter(taskID string, delta int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.waiting == nil {
		w.waiting = map[string]int{}
	}
	if w.waiting[taskID] += delta; w.waiting[taskID] <= 0 {
		delete(w.waiting, taskID)
	}
}

func (w *stagingWait) waitingTasks() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.waiting))
	for id := range w.waiting {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// awaitStagedBuilds returns the task's builds of prop once the refresh loop has
// staged them. It fails only after opening the task's build folder kept failing
// for the configured limit.
func (s *Shard) awaitStagedBuilds(ctx context.Context, taskID string, gen uint64, prop string,
) ([]*stagedBuild, error) {
	limit := s.index.db.config.ReindexStagingFailureTimeout
	if limit <= 0 {
		limit = config.DefaultReindexStagingFailureTimeout
	}
	s.view.wait.addWaiter(taskID, 1)
	defer s.view.wait.addWaiter(taskID, -1)
	s.notifyViewRefresh()
	for {
		changed, stagingErr := s.view.wait.check(taskID, limit)
		var builds []*stagedBuild
		for _, b := range s.loadView().staged {
			if b.taskID == taskID && b.gen == gen && b.prop == prop {
				builds = append(builds, b)
			}
		}
		if len(builds) > 0 {
			return builds, nil
		}
		if stagingErr != nil {
			return nil, stagingErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}
