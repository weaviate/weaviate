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

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	propLengthsFile        = "proplengths"
	propLengthsRecountFile = propLengthsFile + ".recount"
)

// propertyLengthsRecounter rebuilds the property lengths of a shard from its
// objects bucket, before the shard serves requests.
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

// count builds the property lengths from the objects bucket in a separate
// file, so a failure or a crash halfway leaves the previous ones in place. It
// returns the number of objects counted.
func (r *propertyLengthsRecounter) count(ctx context.Context) (int, error) {
	s := r.shard
	// NewJsonShardMetaData would load a crashed recount's file, and the scan would add to it
	if err := os.Remove(r.recountPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("remove leftover recount file: %w", err)
	}
	tracker, err := inverted.NewJsonShardMetaData(r.recountPath, s.index.logger)
	if err != nil {
		return 0, fmt.Errorf("create recount tracker: %w", err)
	}

	// the shard does not serve yet, so nothing else uses its tracker meanwhile
	previous := s.propLenTracker
	s.propLenTracker = tracker
	count, err := r.scan(ctx)
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

func (r *propertyLengthsRecounter) scan(ctx context.Context) (count int, err error) {
	s := r.shard
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

// swapIn replaces the shard's tracker with the one count built. It closes the
// current tracker first, so on failure the shard has no usable tracker.
func (r *propertyLengthsRecounter) swapIn() error {
	if err := r.shard.propLenTracker.Close(); err != nil {
		return fmt.Errorf("close previous tracker: %w", err)
	}
	if err := r.replaceTrackerFile(); err != nil {
		return err
	}
	return r.shard.initProplenTracker()
}

// replaceTrackerFile renames, so after a crash the next load finds either the
// previous tracker file or the recounted one.
func (r *propertyLengthsRecounter) replaceTrackerFile() error {
	if err := os.Rename(r.recountPath, r.trackerPath); err != nil {
		return fmt.Errorf("replace tracker file: %w", err)
	}
	return nil
}

// propertyLengthsReadable reports whether the tracker file of a shard is missing or
// holds the current format. A missing one loads as empty.
func propertyLengthsReadable(shardPath string) bool {
	data, err := os.ReadFile(path.Join(shardPath, propLengthsFile))
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	var metaData *inverted.ShardMetaData
	return err == nil && json.Unmarshal(data, &metaData) == nil && metaData != nil
}

// writeEmptyPropertyLengths replaces the tracker file of a shard without objects, as
// that file may not be readable.
func writeEmptyPropertyLengths(shardPath string, logger logrus.FieldLogger) error {
	recountPath := path.Join(shardPath, propLengthsRecountFile)
	if err := os.Remove(recountPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove leftover recount file: %w", err)
	}
	tracker, err := inverted.NewJsonShardMetaData(recountPath, logger)
	if err != nil {
		return fmt.Errorf("create recount tracker: %w", err)
	}
	if err := tracker.Close(); err != nil {
		return fmt.Errorf("close recount tracker: %w", err)
	}
	if err := os.Rename(recountPath, path.Join(shardPath, propLengthsFile)); err != nil {
		return fmt.Errorf("replace tracker file: %w", err)
	}
	return nil
}
