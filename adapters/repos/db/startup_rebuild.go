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
	"slices"
	"sync"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// startupRebuild makes REINDEX_VECTOR_DIMENSIONS_AT_STARTUP or
// RECOUNT_PROPERTIES_AT_STARTUP rebuild a shard once per process, not on every
// lazy reload.
type startupRebuild struct {
	enabled bool

	mu sync.Mutex
	// whether the rebuild succeeded
	outcomes map[rebuiltShard]bool
	objects  int64
}

// rebuiltShard is not keyed by shardId, which joins names that may contain its "_"
type rebuiltShard struct {
	index, shard string
}

func (r *startupRebuild) isEnabled() bool {
	return r != nil && r.enabled
}

func (r *startupRebuild) pending(index, shard string) bool {
	if !r.isEnabled() {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.outcomes[rebuiltShard{index, shard}]
}

func (r *startupRebuild) record(index, shard string, objects int, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcomes == nil {
		r.outcomes = map[rebuiltShard]bool{}
	}
	r.outcomes[rebuiltShard{index, shard}] = ok
	if ok {
		r.objects += int64(objects)
	}
}

func (r *startupRebuild) done(index, shard string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.outcomes[rebuiltShard{index, shard}]
}

// recorded reports whether the shard was rebuilt or failed.
func (r *startupRebuild) recorded(index, shard string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.outcomes[rebuiltShard{index, shard}]
	return ok
}

// forget makes dropped shards pending again, as a restore or a replica copy can bring
// them back with other data. No shards forgets the whole index.
func (r *startupRebuild) forget(index string, shards ...string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.outcomes {
		if key.index == index && (len(shards) == 0 || slices.Contains(shards, key.shard)) {
			delete(r.outcomes, key)
		}
	}
}

func (r *startupRebuild) summary() (rebuilt, failed int, objects int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ok := range r.outcomes {
		if ok {
			rebuilt++
		} else {
			failed++
		}
	}
	return rebuilt, failed, r.objects
}

// rebuildShardsBeforeLoad prepares the dimensions and recounts the property lengths of
// active and cold shards before they load, without loading them, _NUMCPU shards at a
// time. A failed shard is retried at load.
func (i *Index) rebuildShardsBeforeLoad(ctx context.Context, class *models.Class, shardNames []string) {
	dimensions := i.Config.TrackVectorDimensions &&
		(i.Config.MigrateDimensionsToRoaringSet || i.Config.DimensionsReindex.isEnabled())
	recount := i.Config.PropertyLengthsRecount.isEnabled()
	if len(shardNames) == 0 || (!dimensions && !recount) {
		return
	}

	if recount {
		i.logger.WithField("action", "recount_property_lengths").WithField("shards", len(shardNames)).
			Warn("Recounting property lengths before the shards load. It cannot be cancelled, and " +
				"runs on every startup while environment variable RECOUNT_PROPERTIES_AT_STARTUP is set")
	}
	start := time.Now()
	eg := enterrors.NewErrorGroupWrapper(i.logger)
	eg.SetLimit(_NUMCPU)
	for _, name := range shardNames {
		eg.Go(func() error {
			if _, err := os.Stat(shardPath(i.path(), name)); err != nil {
				if !os.IsNotExist(err) {
					i.logger.WithField("action", "rebuild_shard_before_load").WithField("shard", name).
						Errorf("could not stat shard dir: %v", err)
				}
				return nil
			}
			i.rebuildShardBeforeLoad(ctx, class, name, dimensions, recount)
			return nil
		}, name)
	}
	_ = eg.Wait()

	took := time.Since(start)
	if dimensions {
		i.logger.WithField("action", "prepare_dimensions").WithField("shards", len(shardNames)).
			WithField("took", took).Info("dimensions buckets prepared")
	}
	if recount {
		i.logger.WithField("action", "recount_property_lengths").WithField("shards", len(shardNames)).
			WithField("took", took).Info("property lengths recount before load finished")
	}
}

// rebuildShardBeforeLoad runs the pending rebuilds of a shard on one walk over its
// objects. Each records and logs its own outcome.
func (i *Index) rebuildShardBeforeLoad(ctx context.Context, class *models.Class, name string, dimensions, recount bool) {
	var rebuilds []unloadedShardRebuild
	if dimensions {
		logger := i.logger.WithField("action", "prepare_dimensions").WithField("shard", name)
		if i.prepareShardDimensions(ctx, logger, name) {
			rebuilds = append(rebuilds, &unloadedDimensionsReindex{index: i, name: name, logger: logger})
		}
	}
	if recount && i.Config.PropertyLengthsRecount.pending(i.ID(), name) {
		rebuilds = append(rebuilds, &unloadedPropertyLengthsRecount{
			index:     i,
			name:      name,
			logger:    i.logger.WithField("action", "recount_property_lengths").WithField("shard", name),
			recounter: newPropertyLengthsRecounter(shardPath(i.path(), name), i.logger),
		})
	}
	if len(rebuilds) == 0 {
		return
	}

	start := time.Now()
	objects, errs := i.rebuildUnloadedShard(ctx, class, name, rebuilds)
	for j, rebuild := range rebuilds {
		rebuild.conclude(objects, time.Since(start), errs[j])
	}
}

// unloadedShardRebuild is a rebuild of a shard before it loads.
type unloadedShardRebuild interface {
	// empty takes the place of the rebuild on a shard that never held objects.
	empty() error
	// begin readies the rebuild on the shard opened by openUnloadedShard, and returns
	// what the walk over its objects feeds.
	begin(ctx context.Context, s *Shard) (objectsVisitor, error)
	// end writes out the rebuild, unless walkErr is set.
	end(ctx context.Context, walkErr error) error
	conclude(objects int, took time.Duration, err error)
}

// errNotWalked makes end discard a rebuild whose walk did not finish.
var errNotWalked = errors.New("objects not walked")

// rebuildUnloadedShard opens the shard and walks its objects once for all rebuilds.
// errs holds the error of each rebuild, so one failing does not fail the others.
func (i *Index) rebuildUnloadedShard(ctx context.Context, class *models.Class, name string,
	rebuilds []unloadedShardRebuild,
) (objects int, errs []error) {
	errs = make([]error, len(rebuilds))
	if i.unloadedShardIsEmpty(name) {
		for j, rebuild := range rebuilds {
			errs[j] = rebuild.empty()
		}
		return 0, errs
	}

	s, closeShard, err := i.openUnloadedShard(ctx, class, name)
	if err != nil {
		for j := range errs {
			errs[j] = err
		}
		return 0, errs
	}
	var begun []int
	var visitors []objectsVisitor
	var walkErrs []error
	// also on a panic, so that each rebuild lets go of what it holds
	defer func() {
		for k, j := range begun {
			errs[j] = rebuilds[j].end(ctx, walkErrs[k])
		}
		if err := closeShard(); err != nil {
			for j := range errs {
				if errs[j] == nil {
					errs[j] = err
				}
			}
		}
	}()

	for j, rebuild := range rebuilds {
		visitor, err := rebuild.begin(ctx, s)
		if err != nil {
			errs[j] = err
			continue
		}
		begun = append(begun, j)
		visitors = append(visitors, visitor)
		walkErrs = append(walkErrs, errNotWalked)
	}
	if len(visitors) > 0 {
		objects, walkErrs = s.walkObjects(ctx, visitors...)
	}
	return objects, errs
}

// objectsVisitor is fed the objects of a shard by walkObjects.
type objectsVisitor interface {
	// decodeOptions selects the parts of an object that visit reads.
	decodeOptions() additional.Properties
	visit(object *storobj.Object) error
}

// walkObjects feeds each object of s to every visitor, decoding it once in a way that
// serves them all. A visitor that fails gets no more objects, and its error is at its
// index in errs. The others go on.
func (s *Shard) walkObjects(ctx context.Context, visitors ...objectsVisitor) (objects int, errs []error) {
	errs = make([]error, len(visitors))
	failRemaining := func(err error) {
		for j := range errs {
			if errs[j] == nil {
				errs[j] = err
			}
		}
	}
	// a corrupt segment makes the cursor panic, which the walk can recover as it only reads
	defer func() {
		if p := recover(); p != nil {
			failRemaining(fmt.Errorf("scan objects: panic: %v", p))
		}
	}()
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		failRemaining(fmt.Errorf("objects bucket: %w", lsmkv.ErrBucketNotFound))
		return 0, errs
	}
	className := s.index.Config.ClassName.String()

	decode := additional.Properties{NoProps: true}
	for _, visitor := range visitors {
		options := visitor.decodeOptions()
		decode.NoProps = decode.NoProps && options.NoProps
		decode.Vector = decode.Vector || options.Vector
		for _, targetVector := range options.Vectors {
			if !slices.Contains(decode.Vectors, targetVector) {
				decode.Vectors = append(decode.Vectors, targetVector)
			}
		}
	}

	cursor := bucket.Cursor()
	defer cursor.Close()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if ctx.Err() != nil {
			failRemaining(fmt.Errorf("scan objects: %w", context.Cause(ctx)))
			return objects, errs
		}
		object, decodeErr := s.decodeObject(v, className, decode)
		visited := false
		for j, visitor := range visitors {
			if errs[j] == nil {
				errs[j] = s.visitObject(visitor, objects, v, className, object, decodeErr)
				visited = visited || errs[j] == nil
			}
		}
		if !visited {
			return objects, errs
		}
		objects++
	}
	return objects, errs
}

// visitObject decodes an object that did not decode for all visitors again with only
// what visitor reads. A panic fails only this visitor.
func (s *Shard) visitObject(visitor objectsVisitor, index int, data []byte, className string,
	object *storobj.Object, decodeErr error,
) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("visit object %d: panic: %v", index, p)
		}
	}()
	if decodeErr != nil {
		if object, err = s.decodeObject(data, className, visitor.decodeOptions()); err != nil {
			return fmt.Errorf("unmarshal object %d: %w", index, err)
		}
	}
	return visitor.visit(object)
}

func (s *Shard) decodeObject(data []byte, className string, decode additional.Properties) (object *storobj.Object, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return storobj.FromBinaryOptionalDisk(data, className, decode, nil)
}
