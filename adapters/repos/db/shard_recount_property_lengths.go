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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	propLengthsFile        = "proplengths"
	propLengthsRecountFile = propLengthsFile + ".recount"
)

// recountPropertyLengthsOfShards recounts the property lengths of active and cold
// shards before they load, without loading them. A failed shard is retried at load.
func (i *Index) recountPropertyLengthsOfShards(ctx context.Context, class *models.Class, shardNames []string) {
	recount := i.Config.PropertyLengthsRecount
	if recount == nil || !recount.enabled || len(shardNames) == 0 {
		return
	}

	i.logger.WithField("action", "recount_property_lengths").WithField("shards", len(shardNames)).
		Warn("Recounting property lengths before the shards load. It cannot be cancelled, and " +
			"runs on every startup while environment variable RECOUNT_PROPERTIES_AT_STARTUP is set")
	i.rebuildShardsBeforeLoad(ctx, "recount_property_lengths", "property lengths recount before load finished",
		shardNames, func(logger logrus.FieldLogger, name string) {
			i.recountUnloadedShardPropertyLengths(ctx, logger, class, name)
		})
}

func (i *Index) recountUnloadedShardPropertyLengths(ctx context.Context, logger logrus.FieldLogger,
	class *models.Class, name string,
) {
	recount := i.Config.PropertyLengthsRecount
	if !recount.pending(i.ID(), name) {
		return
	}

	start := time.Now()
	objects, err := i.recountUnloadedShard(ctx, class, name)
	recount.record(i.ID(), name, objects, err == nil)
	if err != nil {
		logger.Errorf("could not recount property lengths, retrying when the shard loads: %v", err)
		return
	}
	logger.WithField("objects", objects).WithField("took", time.Since(start)).Info("property lengths recounted")
}

func (i *Index) recountUnloadedShard(ctx context.Context, class *models.Class, name string) (int, error) {
	r := newPropertyLengthsRecounter(shardPath(i.path(), name), i.logger)
	if i.unloadedShardIsEmpty(name) {
		if r.trackerReadable() {
			return 0, nil
		}
		return 0, r.writeEmpty()
	}
	return i.rebuildUnloadedShard(ctx, class, name, func(s *Shard) (int, error) {
		objects, err := r.count(ctx, s)
		if err != nil {
			return 0, err
		}
		return objects, r.replaceTrackerFile()
	})
}

// recountPropertyLengthsOnLoad recounts a shard the recount before load missed. It
// runs before async replication starts, as a write during the scan would be counted
// twice or not at all. A failed count keeps the previous lengths and fails the load
// only if ctx was cancelled. A failed swap always fails it.
func (s *Shard) recountPropertyLengthsOnLoad(ctx context.Context) error {
	recount := s.index.Config.PropertyLengthsRecount
	if !recount.pending(s.index.ID(), s.name) {
		return nil
	}
	// a shard without objects, such as a tenant created after startup, has nothing to recount
	if s.counter.PreviewNext() == 0 {
		recount.record(s.index.ID(), s.name, 0, true)
		return nil
	}

	logger := s.index.logger.WithField("action", "recount_property_lengths").WithField("shard", s.ID())
	r := newPropertyLengthsRecounter(s.path(), s.index.logger)
	start := time.Now()
	objects, err := r.count(ctx, s)
	if err != nil {
		recount.record(s.index.ID(), s.name, 0, false)
		if ctx.Err() != nil {
			return fmt.Errorf("count of shard %q: %w", s.ID(), err)
		}
		logger.Errorf("recounting property lengths failed, keeping the previous ones: %v", err)
		return nil
	}
	if err := r.swapIn(s); err != nil {
		recount.record(s.index.ID(), s.name, 0, false)
		return fmt.Errorf("swap of shard %q: %w", s.ID(), err)
	}
	recount.record(s.index.ID(), s.name, objects, true)
	logger.WithField("objects", objects).WithField("took", time.Since(start)).Info("property lengths recounted")
	return nil
}

// propertyLengthsRecounter rebuilds the property lengths of a shard from its
// objects bucket, before the shard serves requests.
type propertyLengthsRecounter struct {
	logger      logrus.FieldLogger
	trackerPath string
	recountPath string
}

func newPropertyLengthsRecounter(shardPath string, logger logrus.FieldLogger) *propertyLengthsRecounter {
	return &propertyLengthsRecounter{
		logger:      logger,
		trackerPath: path.Join(shardPath, propLengthsFile),
		recountPath: path.Join(shardPath, propLengthsRecountFile),
	}
}

// count builds the property lengths from the objects bucket of s in a separate
// file, so a failure or a crash halfway leaves the previous ones in place. It
// returns the number of objects counted.
func (r *propertyLengthsRecounter) count(ctx context.Context, s *Shard) (int, error) {
	tracker, err := r.createRecountTracker()
	if err != nil {
		return 0, err
	}

	// the shard does not serve yet, so nothing else uses its tracker meanwhile
	previous := s.propLenTracker
	s.propLenTracker = tracker
	count, err := r.scan(ctx, s)
	s.propLenTracker = previous
	if err == nil {
		err = tracker.Close()
	}
	if err != nil {
		tracker.Drop(false)
		return 0, err
	}
	return count, nil
}

func (r *propertyLengthsRecounter) createRecountTracker() (*inverted.JsonShardMetaData, error) {
	// NewJsonShardMetaData would load a crashed recount's file, and the scan would add to it
	if err := os.Remove(r.recountPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove leftover recount file: %w", err)
	}
	tracker, err := inverted.NewJsonShardMetaData(r.recountPath, r.logger)
	if err != nil {
		return nil, fmt.Errorf("create recount tracker: %w", err)
	}
	return tracker, nil
}

func (r *propertyLengthsRecounter) scan(ctx context.Context, s *Shard) (count int, err error) {
	// a corrupt segment makes the cursor panic, which the scan can recover as it only reads
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("scan objects: panic: %v", p)
		}
	}()
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return 0, fmt.Errorf("objects bucket: %w", lsmkv.ErrBucketNotFound)
	}
	className := s.index.Config.ClassName.String()

	cursor := bucket.Cursor()
	defer cursor.Close()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		if ctx.Err() != nil {
			return 0, fmt.Errorf("scan objects: %w", context.Cause(ctx))
		}
		// the vectors are not needed, and decoding them would take most of the time
		object, err := storobj.FromBinaryOptionalDisk(v, className, additional.Properties{}, nil)
		if err != nil {
			return 0, fmt.Errorf("unmarshal object %d: %w", count, err)
		}
		props, _, _, err := s.AnalyzeObject(object)
		if err != nil {
			return 0, fmt.Errorf("analyze object %s: %w", object.ID(), err)
		}
		if err := s.SetPropertyLengths(props); err != nil {
			return 0, fmt.Errorf("track property lengths of object %s: %w", object.ID(), err)
		}
		count++
	}
	return count, nil
}

// swapIn replaces the tracker of s with the one count built. It closes the
// current tracker first, so on failure the shard has no usable tracker.
func (r *propertyLengthsRecounter) swapIn(s *Shard) error {
	if err := s.propLenTracker.Close(); err != nil {
		return fmt.Errorf("close previous tracker: %w", err)
	}
	if err := r.replaceTrackerFile(); err != nil {
		return err
	}
	return s.initProplenTracker()
}

// replaceTrackerFile renames, so after a crash the next load finds either the
// previous tracker file or the recounted one.
func (r *propertyLengthsRecounter) replaceTrackerFile() error {
	if err := os.Rename(r.recountPath, r.trackerPath); err != nil {
		return fmt.Errorf("replace tracker file: %w", err)
	}
	return nil
}

// trackerReadable reports whether the tracker file is missing or holds the current
// format. A missing one loads as empty.
func (r *propertyLengthsRecounter) trackerReadable() bool {
	data, err := os.ReadFile(r.trackerPath)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	var metaData *inverted.ShardMetaData
	return err == nil && json.Unmarshal(data, &metaData) == nil && metaData != nil
}

// writeEmpty replaces the tracker file of a shard without objects.
func (r *propertyLengthsRecounter) writeEmpty() error {
	tracker, err := r.createRecountTracker()
	if err != nil {
		return err
	}
	if err := tracker.Close(); err != nil {
		return fmt.Errorf("close recount tracker: %w", err)
	}
	return r.replaceTrackerFile()
}
