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

package lsmkv

import (
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// SetRawList returns all Set entries for a given key.
//
// SetRawList is specific to the Set Strategy with HFresh postings
func (b *Bucket) SetRawList(key []byte) ([][]byte, error) {
	view := b.GetConsistentView()
	defer view.ReleaseView()

	return b.setRawListFromConsistentView(view, key)
}

// SetRawListStats describes where one SetRawList read found its data. It
// exists for search-path instrumentation: SegmentsHit is the number of
// separate disk segments that contained the key (each one is at least one
// random read), i.e. the fragmentation multiplier of the read.
type SetRawListStats struct {
	SegmentsHit int  // disk segments that contained the key
	FlushingHit bool // key present in the flushing memtable
	MemtableHit bool // key present in the active memtable
	Bytes       int  // total payload bytes returned
}

// SetRawListWithStats behaves exactly like SetRawList but also reports where
// the data came from.
func (b *Bucket) SetRawListWithStats(key []byte) ([][]byte, SetRawListStats, error) {
	view := b.GetConsistentView()
	defer view.ReleaseView()

	var stats SetRawListStats
	var out [][]byte

	for _, segment := range view.Disk {
		v, err := segment.getCollectionBytes(key)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}
			return nil, stats, err
		}
		if len(v) > 0 {
			stats.SegmentsHit++
			out = append(out, v...)
		}
	}

	if view.Flushing != nil {
		v, err := view.Flushing.getCollectionBytes(key)
		if err != nil && !errors.Is(err, lsmkv.NotFound) {
			return nil, stats, err
		}
		if len(v) > 0 {
			stats.FlushingHit = true
			out = append(out, v...)
		}
	}

	v, err := view.Active.getCollectionBytes(key)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, stats, err
	}
	if len(v) > 0 {
		stats.MemtableHit = true
		out = append(out, v...)
	}

	for _, val := range out {
		stats.Bytes += len(val)
	}

	return out, stats, nil
}

func (b *Bucket) setRawListFromConsistentView(view BucketConsistentView, key []byte) ([][]byte, error) {
	var out [][]byte

	v, err := b.disk.getCollectionBytes(key, view.Disk)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	out = v

	if view.Flushing != nil {
		v, err = view.Flushing.getCollectionBytes(key)
		if err != nil && !errors.Is(err, lsmkv.NotFound) {
			return nil, err
		}
		out = append(out, v...)

	}

	v, err = view.Active.getCollectionBytes(key)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	if len(v) > 0 {
		// skip the expensive append operation if there was no memtable
		out = append(out, v...)
	}

	return out, nil
}
