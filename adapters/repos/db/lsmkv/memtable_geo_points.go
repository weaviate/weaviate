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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/geopoints"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// The geopoints write-ahead log reuses the roaringset node encoding so that no
// new record format is needed: the key is the big-endian doc ID, an addition
// carries the coordinates packed into one value, a removal carries one
// deletion value. commitlogger_parser_geo_points.go decodes it.

func packCoordinates(lat, lon float32) uint64 {
	return uint64(math.Float32bits(lat))<<32 | uint64(math.Float32bits(lon))
}

func unpackCoordinates(packed uint64) (lat, lon float32) {
	return math.Float32frombits(uint32(packed >> 32)), math.Float32frombits(uint32(packed))
}

func (m *Memtable) geoPointsAdd(docID uint64, lat, lon float32) error {
	if err := CheckStrategyGeoPoints(m.strategy); err != nil {
		return err
	}
	if err := geopoints.ValidateCoordinates(lat, lon); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.geoPointsAddCommitLog(docID, []uint64{packCoordinates(lat, lon)}, nil); err != nil {
		return err
	}
	m.geoPoints.Add(docID, lat, lon)
	m.geoPointsAdjustMeta()
	return nil
}

func (m *Memtable) geoPointsRemove(docID uint64) error {
	if err := CheckStrategyGeoPoints(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.geoPointsAddCommitLog(docID, nil, []uint64{0}); err != nil {
		return err
	}
	m.geoPoints.Remove(docID)
	m.geoPointsAdjustMeta()
	return nil
}

func (m *Memtable) geoPointsAdjustMeta() {
	m.size = uint64(m.geoPoints.Size())
	m.metrics.observeSize(m.size)
	m.updateDirtyAt()
}

func (m *Memtable) geoPointsAddCommitLog(docID uint64, additions, deletions []uint64) error {
	m.writesSinceLastSync = true // for the reason roaringSetAddCommitLog gives

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], docID)
	node, err := roaringset.NewSegmentNodeList(key[:], additions, deletions)
	if err != nil {
		return fmt.Errorf("create node for commit log: %w", err)
	}
	if err := m.commitlog.add(node); err != nil {
		return fmt.Errorf("add node to commit log: %w", err)
	}
	return nil
}

// newGeoPointsReader hands out the memtable itself: it locks its own reads
// against the writers the lsmkv memtable lock serializes.
func (m *Memtable) newGeoPointsReader() geopoints.InnerReader {
	return m.geoPoints
}

// flushDataGeoPoints writes the tree as the whole data section and leaves the
// index section empty, like roaringsetrange.
func (m *Memtable) flushDataGeoPoints(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	body := m.geoPoints.Serialize()

	header := &segmentindex.Header{
		IndexStart:       uint64(segmentindex.HeaderSize + len(body)),
		Level:            0, // always level zero on a new one
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyGeoPoints,
	}
	if _, err := f.WriteHeader(header); err != nil {
		return nil, err
	}
	if _, err := f.BodyWriter().Write(body); err != nil {
		return nil, fmt.Errorf("write geopoints body: %w", err)
	}
	return make([]segmentindex.Key, 0), nil
}
