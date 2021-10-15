//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type SegmentGroup struct {
	segments []*segment

	// Lock() for changing the currently active segments, RLock() for normal
	// operation
	maintenanceLock sync.RWMutex
	dir             string

	stopCompactionCycle chan struct{}

	logger logrus.FieldLogger
}

func newSegmentGroup(dir string,
	compactionCycle time.Duration, logger logrus.FieldLogger) (*SegmentGroup, error) {
	list, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	out := &SegmentGroup{
		segments:            make([]*segment, len(list)),
		dir:                 dir,
		logger:              logger,
		stopCompactionCycle: make(chan struct{}),
	}

	segmentIndex := 0
	for _, fileInfo := range list {
		if filepath.Ext(fileInfo.Name()) != ".db" {
			// skip, this could be commit log, etc.
			continue
		}

		segment, err := newSegment(filepath.Join(dir, fileInfo.Name()), logger)
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

func (ig *SegmentGroup) add(path string) error {
	ig.maintenanceLock.Lock()
	defer ig.maintenanceLock.Unlock()

	segment, err := newSegment(path, ig.logger)
	if err != nil {
		return errors.Wrapf(err, "init segment %s", path)
	}

	ig.segments = append(ig.segments, segment)
	return nil
}

func (ig *SegmentGroup) get(key []byte) ([]byte, error) {
	ig.maintenanceLock.RLock()
	defer ig.maintenanceLock.RUnlock()

	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := len(ig.segments) - 1; i >= 0; i-- {
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

func (ig *SegmentGroup) shutdown(ctx context.Context) error {
	ig.stopCompactionCycle <- struct{}{}

	for i, seg := range ig.segments {
		if err := seg.close(); err != nil {
			return err
		}

		ig.segments[i] = nil
	}

	return nil
}
