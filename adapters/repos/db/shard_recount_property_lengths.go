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
	"path"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	propLengthsFile        = "proplengths"
	propLengthsRecountFile = propLengthsFile + ".recount"
)

// propertyLengthsRecounter rebuilds the property lengths of a loading shard
// from its objects bucket.
type propertyLengthsRecounter struct {
	shard       *Shard
	logger      logrus.FieldLogger
	trackerPath string
	recountPath string
}

func newPropertyLengthsRecounter(s *Shard) *propertyLengthsRecounter {
	return &propertyLengthsRecounter{
		shard:       s,
		logger:      s.index.logger.WithField("action", "recount_property_lengths").WithField("shard", s.ID()),
		trackerPath: path.Join(s.path(), propLengthsFile),
		recountPath: path.Join(s.path(), propLengthsRecountFile),
	}
}

// recountProperties rebuilds the shard's property lengths and reports whether
// it did. It runs while the shard is loading, before any write can reach it: a
// write landing during the scan would be counted twice or not at all.
//
// An error means the shard must not load. A failed count keeps the previous
// property lengths, and is an error only if ctx was cancelled. A failed swap
// always is, as it leaves no usable tracker.
func (r *propertyLengthsRecounter) recountProperties(ctx context.Context) (bool, error) {
	id := r.shard.ID()
	start := time.Now()
	count, err := r.count(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false, fmt.Errorf("count of shard %q: %w", id, err)
		}
		r.logger.Errorf("recounting property lengths failed, keeping the previous ones: %v", err)
		return false, nil
	}
	if err := r.swapIn(); err != nil {
		return false, fmt.Errorf("swap of shard %q: %w", id, err)
	}
	r.logger.Infof("recounted property lengths of %d objects in %s", count, time.Since(start))
	return true, nil
}

// count builds the property lengths from the objects bucket in a separate
// file, so a failure or a crash halfway leaves the previous ones in place. It
// returns the number of objects counted.
func (r *propertyLengthsRecounter) count(ctx context.Context) (int, error) {
	s := r.shard
	// left behind by a recount that crashed, it would be loaded as a head start
	if err := os.Remove(r.recountPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("remove leftover recount file: %w", err)
	}
	tracker, err := inverted.NewJsonShardMetaData(r.recountPath, s.index.logger)
	if err != nil {
		return 0, fmt.Errorf("create recount tracker: %w", err)
	}

	// the shard is not published yet, so nothing else uses its tracker meanwhile
	previous := s.propLenTracker
	s.propLenTracker = tracker
	count := 0
	err = s.store.Bucket(helpers.ObjectsBucketLSM).IterateObjects(ctx, func(object *storobj.Object) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		props, _, _, err := s.AnalyzeObject(object)
		if err != nil {
			return fmt.Errorf("analyze object %s: %w", object.ID(), err)
		}
		if err := s.SetPropertyLengths(props); err != nil {
			return fmt.Errorf("track property lengths of object %s: %w", object.ID(), err)
		}
		count++
		return nil
	})
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

// swapIn replaces the shard's tracker with the one count built. It closes the
// current tracker first, so on failure the shard has no usable tracker. The
// rename keeps a crash safe: the next load finds either the previous file or
// the recounted one.
func (r *propertyLengthsRecounter) swapIn() error {
	if err := r.shard.propLenTracker.Close(); err != nil {
		return fmt.Errorf("close previous tracker: %w", err)
	}
	if err := os.Rename(r.recountPath, r.trackerPath); err != nil {
		return fmt.Errorf("replace tracker file: %w", err)
	}
	return r.shard.initProplenTracker()
}
