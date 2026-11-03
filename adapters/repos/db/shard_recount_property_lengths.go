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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	propLengthsFile        = "proplengths"
	propLengthsRecountFile = propLengthsFile + ".recount"
)

// unloadedPropertyLengthsRecount recounts the property lengths of a shard before it loads.
type unloadedPropertyLengthsRecount struct {
	index     *Index
	name      string
	logger    logrus.FieldLogger
	recounter *propertyLengthsRecounter
}

func (r *unloadedPropertyLengthsRecount) empty() error {
	if r.recounter.trackerReadable() {
		return nil
	}
	return r.recounter.writeEmpty()
}

func (r *unloadedPropertyLengthsRecount) begin(_ context.Context, s *Shard) (objectsVisitor, error) {
	return r.recounter, r.recounter.begin(s)
}

func (r *unloadedPropertyLengthsRecount) end(_ context.Context, walkErr error) error {
	if err := r.recounter.end(walkErr); err != nil {
		return err
	}
	return r.recounter.replaceTrackerFile()
}

func (r *unloadedPropertyLengthsRecount) conclude(objects int, took time.Duration, err error) {
	r.index.Config.PropertyLengthsRecount.record(r.index.ID(), r.name, objects, err == nil)
	if err != nil {
		r.logger.Errorf("could not recount property lengths, retrying when the shard loads: %v", err)
		return
	}
	r.logger.WithField("objects", objects).WithField("took", took).Info("property lengths recounted")
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

	// set from begin to end
	shard    *Shard
	previous *inverted.JsonShardMetaData
	tracker  *inverted.JsonShardMetaData
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
	if err := r.begin(s); err != nil {
		return 0, err
	}
	objects, errs := s.walkObjects(ctx, r)
	if err := r.end(errs[0]); err != nil {
		return 0, err
	}
	return objects, nil
}

// begin points the tracker of s at a new recount file, for visit to fill. The shard
// does not serve yet, so nothing else uses its tracker meanwhile.
func (r *propertyLengthsRecounter) begin(s *Shard) error {
	tracker, err := r.createRecountTracker()
	if err != nil {
		return err
	}
	r.shard, r.previous, r.tracker = s, s.propLenTracker, tracker
	s.propLenTracker = tracker
	return nil
}

// end gives the shard its tracker back, and keeps the recount file only if walkErr
// is nil.
func (r *propertyLengthsRecounter) end(walkErr error) error {
	r.shard.propLenTracker = r.previous
	err := walkErr
	if err == nil {
		err = r.tracker.Close()
	}
	if err != nil {
		r.tracker.Drop(false)
		return err
	}
	return nil
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

// decodeOptions leaves out the vectors, as decoding them would take most of the time.
func (r *propertyLengthsRecounter) decodeOptions() additional.Properties {
	return additional.Properties{}
}

func (r *propertyLengthsRecounter) visit(object *storobj.Object) error {
	props, _, _, err := r.shard.AnalyzeObject(object)
	if err != nil {
		return fmt.Errorf("analyze object %s: %w", object.ID(), err)
	}
	if err := r.shard.SetPropertyLengths(props); err != nil {
		return fmt.Errorf("track property lengths of object %s: %w", object.ID(), err)
	}
	return nil
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
