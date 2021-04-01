package lsmkv

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

type SegmentGroup struct {
	segments []*segment

	// Lock() for changing the currently active segments, RLock() for normal
	// operation
	maintenanceLock sync.RWMutex
}

func newSegmentGroup(dir string) (*SegmentGroup, error) {
	list, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	out := &SegmentGroup{
		segments: make([]*segment, len(list)),
	}

	segmentIndex := 0
	for _, fileInfo := range list {
		fmt.Printf("%v\n", fileInfo.Name())
		if filepath.Ext(fileInfo.Name()) != ".db" {
			// skip, this could be commit log, etc.
			continue
		}

		segment, err := newSegment(filepath.Join(dir, fileInfo.Name()))
		if err != nil {
			return nil, errors.Wrapf(err, "init segment %s", fileInfo.Name())
		}

		out.segments[segmentIndex] = segment
		segmentIndex++
	}

	out.segments = out.segments[:segmentIndex]
	return out, nil
}

func (ig *SegmentGroup) add(path string) error {
	ig.maintenanceLock.Lock()
	defer ig.maintenanceLock.Unlock()

	segment, err := newSegment(path)
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
		}

		out = append(out, v...)
	}

	return out, nil
}
