//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/sirupsen/logrus"
)

type SegmentGroup struct {
	segments []*segment

	// Lock() for changing the currently active segments, RLock() for normal
	// operation
	maintenanceLock sync.RWMutex
	dir             string

	strategy string

	stopCompactionCycle chan struct{}

	logger logrus.FieldLogger

	// for backward-compatibility with states where the disk state for maps was
	// not guaranteed to be sorted yet
	mapRequiresSorting bool

	status     storagestate.Status
	statusLock sync.Mutex
	metrics    *Metrics
}

func newSegmentGroup(dir string,
	compactionCycle time.Duration, logger logrus.FieldLogger,
	mapRequiresSorting bool, metrics *Metrics, strategy string) (*SegmentGroup, error) {
	list, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	out := &SegmentGroup{
		segments:            make([]*segment, len(list)),
		dir:                 dir,
		logger:              logger,
		metrics:             metrics,
		stopCompactionCycle: make(chan struct{}),
		mapRequiresSorting:  mapRequiresSorting,
		strategy:            strategy,
	}

	segmentIndex := 0
	for _, fileInfo := range list {
		if filepath.Ext(fileInfo.Name()) != ".db" {
			// skip, this could be commit log, etc.
			continue
		}

		// before we can mount this file, we need to check if a WAL exists for it.
		// If yes, we must assume that the flush never finished, as otherwise the
		// WAL would have been deleted. Thus we must remove it.
		potentialWALFileName := strings.TrimSuffix(fileInfo.Name(), ".db") + ".wal"
		ok, err := fileExists(filepath.Join(dir, potentialWALFileName))
		if err != nil {
			return nil, errors.Wrapf(err, "check for presence of wals for segment %s",
				fileInfo.Name())
		}

		if ok {
			if err := os.Remove(filepath.Join(dir, fileInfo.Name())); err != nil {
				return nil, errors.Wrapf(err, "delete corrupt segment %s", fileInfo.Name())
			}

			logger.WithField("action", "lsm_segment_init").
				WithField("path", filepath.Join(dir, fileInfo.Name())).
				WithField("wal_path", potentialWALFileName).
				Info("Discarded (partially written) LSM segment, because an active WAL for " +
					"the same segment was found. A recovery from the WAL will follow.")

			continue
		}

		if fileInfo.Size() == 0 {
			continue
		}

		segment, err := newSegment(filepath.Join(dir, fileInfo.Name()), logger,
			metrics, out.makeExistsOnLower(segmentIndex))
		if err != nil {
			return nil, errors.Wrapf(err, "init segment %s", fileInfo.Name())
		}

		out.segments[segmentIndex] = segment
		segmentIndex++
	}

	out.segments = out.segments[:segmentIndex]

	out.initCompactionCycle(compactionCycle)
	return out, nil
}

func (ig *SegmentGroup) makeExistsOnLower(nextSegmentIndex int) existsOnLowerSegmentsFn {
	return func(key []byte) (bool, error) {
		if nextSegmentIndex == 0 {
			// this is already the lowest possible segment, we can guarantee that
			// any key in this segment is previously unseen.
			return false, nil
		}

		v, err := ig.getWithUpperSegmentBoundary(key, nextSegmentIndex-1)
		if err != nil {
			return false, errors.Wrapf(err, "check exists on segments lower than %d",
				nextSegmentIndex)
		}

		return v != nil, nil
	}
}

func (ig *SegmentGroup) add(path string) error {
	ig.maintenanceLock.Lock()
	defer ig.maintenanceLock.Unlock()

	newSegmentIndex := len(ig.segments)
	segment, err := newSegment(path, ig.logger, ig.metrics,
		ig.makeExistsOnLower(newSegmentIndex))
	if err != nil {
		return errors.Wrapf(err, "init segment %s", path)
	}

	ig.segments = append(ig.segments, segment)
	return nil
}

func (ig *SegmentGroup) get(key []byte) ([]byte, error) {
	ig.maintenanceLock.RLock()
	defer ig.maintenanceLock.RUnlock()

	return ig.getWithUpperSegmentBoundary(key, len(ig.segments)-1)
}

// not thread-safe on its own, as the assumption is that this is called from a
// lockholder, e.g. within .get()
func (ig *SegmentGroup) getWithUpperSegmentBoundary(key []byte, topMostSegment int) ([]byte, error) {
	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := topMostSegment; i >= 0; i-- {
		v, err := ig.segments[i].get(key)
		if err != nil {
			if err == NotFound {
				continue
			}

			if err == Deleted {
				return nil, nil
			}

			panic(fmt.Sprintf("unsupported error in segmentGroup.get(): %v", err))
		}

		return v, nil
	}

	return nil, nil
}

func (ig *SegmentGroup) getBySecondary(pos int, key []byte) ([]byte, error) {
	ig.maintenanceLock.RLock()
	defer ig.maintenanceLock.RUnlock()

	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := len(ig.segments) - 1; i >= 0; i-- {
		v, err := ig.segments[i].getBySecondary(pos, key)
		if err != nil {
			if err == NotFound {
				continue
			}

			if err == Deleted {
				return nil, nil
			}

			panic(fmt.Sprintf("unsupported error in segmentGroup.get(): %v", err))
		}

		return v, nil
	}

	return nil, nil
}

func (ig *SegmentGroup) getCollection(key []byte) ([]value, error) {
	ig.maintenanceLock.RLock()
	defer ig.maintenanceLock.RUnlock()

	var out []value

	// start with first and do not exit
	for _, segment := range ig.segments {
		v, err := segment.getCollection(key)
		if err != nil {
			if err == NotFound {
				continue
			}

			return nil, err
		}

		if len(out) == 0 {
			out = v
		} else {
			out = append(out, v...)
		}
	}

	return out, nil
}

func (ig *SegmentGroup) getCollectionBySegments(key []byte) ([][]value, error) {
	ig.maintenanceLock.RLock()
	defer ig.maintenanceLock.RUnlock()

	out := make([][]value, len(ig.segments))

	i := 0
	// start with first and do not exit
	for _, segment := range ig.segments {
		v, err := segment.getCollection(key)
		if err != nil {
			if err == NotFound {
				continue
			}

			return nil, err
		}

		out[i] = v
		i++
	}

	return out[:i], nil
}

func (ig *SegmentGroup) count() int {
	ig.maintenanceLock.RLock()
	defer ig.maintenanceLock.RUnlock()

	count := 0
	for _, seg := range ig.segments {
		count += seg.countNetAdditions
	}

	return count
}

func (ig *SegmentGroup) shutdown(ctx context.Context) error {
	ig.maintenanceLock.Lock()
	defer ig.maintenanceLock.Unlock()

	ig.stopCompactionCycle <- struct{}{}

	for i, seg := range ig.segments {
		if err := seg.close(); err != nil {
			return err
		}

		ig.segments[i] = nil
	}

	// make sure the segment list itself is set to nil. In case a memtable will
	// still flush after closing, it might try to read from a disk segment list
	// otherwise and run into nil-pointer problems.
	ig.segments = nil

	return nil
}

func (ig *SegmentGroup) updateStatus(status storagestate.Status) {
	ig.statusLock.Lock()
	defer ig.statusLock.Unlock()

	ig.status = status
}

func (ig *SegmentGroup) isReadyOnly() bool {
	ig.statusLock.Lock()
	defer ig.statusLock.Unlock()

	return ig.status == storagestate.StatusReadOnly
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}

	return false, err
}
