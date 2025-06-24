//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
)

func (m *Memtable) flushWAL() error {
	if err := m.commitlog.close(); err != nil {
		return err
	}

	if m.Size() == 0 {
		// this is an empty memtable, nothing to do
		// however, we still have to cleanup the commit log, otherwise we will
		// attempt to recover from it on the next cycle
		if err := m.commitlog.delete(); err != nil {
			return errors.Wrap(err, "delete commit log file")
		}
		return nil
	}

	// fsync parent directory
	err := diskio.Fsync(filepath.Dir(m.path))
	if err != nil {
		return err
	}

	return nil
}

func (m *Memtable) flush() (rerr error) {
	// close the commit log first, this also forces it to be fsynced. If
	// something fails there, don't proceed with flushing. The commit log will
	// only be deleted at the very end, if the flush was successful
	// (indicated by a successful close of the flush file - which indicates a
	// successful fsync)

	if err := m.commitlog.close(); err != nil {
		return errors.Wrap(err, "close commit log file")
	}

	if m.Size() == 0 {
		// this is an empty memtable, nothing to do
		// however, we still have to cleanup the commit log, otherwise we will
		// attempt to recover from it on the next cycle
		if err := m.commitlog.delete(); err != nil {
			return errors.Wrap(err, "delete commit log file")
		}
		return nil
	}

	tmpSegmentPath := m.path + ".db.tmp"

	f, err := os.OpenFile(tmpSegmentPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer func() {
		if rerr != nil {
			f.Close()
			os.Remove(tmpSegmentPath)
		}
	}()

	observeWrite := m.metrics.writeMemtable
	cb := func(written int64) {
		observeWrite(written)
	}
	meteredF := diskio.NewMeteredWriter(f, cb)

	bufw := bufio.NewWriter(meteredF)
	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(bufw),
		segmentindex.WithChecksumsDisabled(!m.enableChecksumValidation),
	)

	var keys []segmentindex.Key
	skipIndices := false

	switch m.strategy {
	case StrategyReplace:
		if keys, err = m.flushDataReplace(segmentFile); err != nil {
			return err
		}

	case StrategySetCollection:
		if keys, err = m.flushDataSet(segmentFile); err != nil {
			return err
		}

	case StrategyRoaringSet:
		if keys, err = m.flushDataRoaringSet(segmentFile); err != nil {
			return err
		}

	case StrategyRoaringSetRange:
		if keys, err = m.flushDataRoaringSetRange(segmentFile); err != nil {
			return err
		}
		skipIndices = true

	case StrategyMapCollection:
		if keys, err = m.flushDataMap(segmentFile); err != nil {
			return err
		}
	case StrategyInverted:
		if keys, _, err = m.flushDataInverted(bufw, f); err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot flush strategy %s", m.strategy)
	}

	if !skipIndices {
		indexes := &segmentindex.Indexes{
			Keys:                keys,
			SecondaryIndexCount: m.secondaryIndices,
			ScratchSpacePath:    m.path + ".scratch.d",
			ObserveWrite: monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
				"strategy":  m.strategy,
				"operation": "writeIndices",
			}),
		}

		// TODO: Currently no checksum validation support for StrategyInverted.
		//       This condition can be removed once support is added, and for
		//       all strategies we can simply `segmentFile.WriteIndexes(indexes)`
		if m.strategy == StrategyInverted {
			if _, err := indexes.WriteTo(bufw); err != nil {
				return err
			}
		} else {
			if _, err := segmentFile.WriteIndexes(indexes); err != nil {
				return err
			}
		}
	}

	// TODO: Currently no checksum validation support for StrategyInverted.
	//       This condition can be removed once support is added, and for
	//       all strategies we can simply `segmentFile.WriteChecksum()`
	if m.strategy != StrategyInverted {
		if _, err := segmentFile.WriteChecksum(); err != nil {
			return err
		}
	} else {
		if err := bufw.Flush(); err != nil {
			return err
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	err = os.Rename(tmpSegmentPath, strings.TrimSuffix(tmpSegmentPath, ".tmp"))
	if err != nil {
		return err
	}

	// fsync parent directory
	err = diskio.Fsync(filepath.Dir(m.path))
	if err != nil {
		return err
	}

	// only now that the file has been flushed is it safe to delete the commit log
	// TODO: there might be an interest in keeping the commit logs around for
	// longer as they might come in handy for replication
	return m.commitlog.delete()
}

func (m *Memtable) flushDataReplace(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	flat := m.key.flattenInOrder()

	totalDataLength := totalKeyAndValueSize(flat)
	perObjectAdditions := len(flat) * (1 + 8 + 4 + int(m.secondaryIndices)*4) // 1 byte for the tombstone, 8 bytes value length encoding, 4 bytes key length encoding, + 4 bytes key encoding for every secondary index
	headerSize := segmentindex.HeaderSize
	header := &segmentindex.Header{
		IndexStart:       uint64(totalDataLength + perObjectAdditions + headerSize),
		Level:            0, // always level zero on a new one
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: m.secondaryIndices,
		Strategy:         SegmentStrategyFromString(m.strategy),
	}

	n, err := f.WriteHeader(header)
	if err != nil {
		return nil, err
	}
	headerSize = int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		segNode := &segmentReplaceNode{
			offset:              totalWritten,
			tombstone:           node.tombstone,
			value:               node.value,
			primaryKey:          node.key,
			secondaryKeys:       node.secondaryKeys,
			secondaryIndexCount: m.secondaryIndices,
		}

		ki, err := segNode.KeyIndexAndWriteTo(f.BodyWriter())
		if err != nil {
			return nil, errors.Wrapf(err, "write node %d", i)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}

func (m *Memtable) flushDataSet(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	flat := m.keyMulti.flattenInOrder()
	return m.flushDataCollection(f, flat)
}

func (m *Memtable) flushDataMap(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	m.RLock()
	flat := m.keyMap.flattenInOrder()
	m.RUnlock()

	// by encoding each map pair we can force the same structure as for a
	// collection, which means we can reuse the same flushing logic
	asMulti := make([]*binarySearchNodeMulti, len(flat))
	for i, mapNode := range flat {
		asMulti[i] = &binarySearchNodeMulti{
			key:    mapNode.key,
			values: make([]value, len(mapNode.values)),
		}

		for j := range asMulti[i].values {
			enc, err := mapNode.values[j].Bytes()
			if err != nil {
				return nil, err
			}

			asMulti[i].values[j] = value{
				value:     enc,
				tombstone: mapNode.values[j].Tombstone,
			}
		}

	}
	return m.flushDataCollection(f, asMulti)
}

func (m *Memtable) flushDataCollection(f *segmentindex.SegmentFile,
	flat []*binarySearchNodeMulti,
) ([]segmentindex.Key, error) {
	totalDataLength := totalValueSizeCollection(flat)
	header := &segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0, // always level zero on a new one
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: m.secondaryIndices,
		Strategy:         SegmentStrategyFromString(m.strategy),
	}

	n, err := f.WriteHeader(header)
	if err != nil {
		return nil, err
	}
	headerSize := int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		ki, err := (&segmentCollectionNode{
			values:     node.values,
			primaryKey: node.key,
			offset:     totalWritten,
		}).KeyIndexAndWriteTo(f.BodyWriter())
		if err != nil {
			return nil, errors.Wrapf(err, "write node %d", i)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}

func totalKeyAndValueSize(in []*binarySearchNode) int {
	var sum int
	for _, n := range in {
		sum += len(n.value)
		sum += len(n.key)
		for _, sec := range n.secondaryKeys {
			sum += len(sec)
		}
	}

	return sum
}

func totalValueSizeCollection(in []*binarySearchNodeMulti) int {
	var sum int
	for _, n := range in {
		sum += 8 // uint64 to indicate array length
		for _, v := range n.values {
			sum += 1 // bool to indicate value tombstone
			sum += 8 // uint64 to indicate value length
			sum += len(v.value)
		}

		sum += 4 // uint32 to indicate key size
		sum += len(n.key)
	}

	return sum
}
