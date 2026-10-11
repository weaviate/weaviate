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
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

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

// flush writes the memtable out through writeSegmentTo and then releases the
// commit log. A commit-log step that fails before the segment write counts the
// attempt here, so failures_total never outruns flush_total.
func (m *Memtable) flush() (segmentPath string, rerr error) {
	// close the commit log first, this also forces it to be fsynced. If
	// something fails there, don't proceed with flushing. The commit log will
	// only be deleted at the very end, if the flush was successful
	// (indicated by a successful close of the flush file - which indicates a
	// successful fsync)

	if err := m.commitlog.close(); err != nil {
		// counted as an attempt as well, because writeSegmentTo is where the attempt
		// is normally counted and this path never reaches it
		m.metrics.incFlushingCount(m.strategy)
		m.metrics.incFlushingFailureCount(m.strategy)
		return "", errors.Wrap(err, "close commit log file")
	}

	if m.Size() == 0 {
		// this is an empty memtable, nothing to do
		// however, we still have to cleanup the commit log, otherwise we will
		// attempt to recover from it on the next cycle
		if err := m.commitlog.delete(); err != nil {
			m.metrics.incFlushingCount(m.strategy)
			m.metrics.incFlushingFailureCount(m.strategy)
			return "", errors.Wrap(err, "delete commit log file")
		}
		return "", nil
	}

	var err error
	segmentPath, err = m.writeSegmentTo(m.path, "")
	if err != nil {
		return "", err
	}

	// only now that the file has been flushed is it safe to delete the commit log
	// TODO: there might be an interest in keeping the commit logs around for
	// longer as they might come in handy for replication
	if err := m.commitlog.delete(); err != nil {
		// no attempt counted here, unlike the two arms above: writeSegmentTo has
		// already counted this one, and counting it twice is what the pairing avoids
		m.metrics.incFlushingFailureCount(m.strategy)
		return segmentPath, err
	}

	return segmentPath, nil
}

// writeSegmentTo writes the memtable out to path, which must carry no extension: the
// segment's level and strategy may be appended, then .db, then suffixAfterDB. It does not
// close or delete the commit log, and it is where the lsm_memtable_flush_* series
// count a segment write, so a WAL replay is counted without entering flush.
func (m *Memtable) writeSegmentTo(path, suffixAfterDB string) (segmentPath string, rerr error) {
	// checked before the meters move, so a caller's mistake does not read as a
	// flush that failed
	if ext := filepath.Ext(path); ext != "" {
		return "", fmt.Errorf("write a segment to %q: the path already carries the extension %q", path, ext)
	}

	start := time.Now()

	m.metrics.incFlushingCount(m.strategy)
	m.metrics.incFlushingInProgress(m.strategy)

	defer func() {
		m.metrics.decFlushingInProgress(m.strategy)

		if rerr != nil {
			m.metrics.incFlushingFailureCount(m.strategy)
			return
		}

		m.metrics.observeFlushMemtableSize(m.strategy, m.Size())
		m.metrics.observeFlushingDuration(m.strategy, time.Since(start))
	}()

	var tmpSegmentPath string
	if m.writeSegmentInfoIntoFileName {
		// new segments are always level 0
		tmpSegmentPath = path + segmentExtraInfo(0, SegmentStrategyFromString(m.strategy)) + ".db.tmp"
	} else {
		tmpSegmentPath = path + ".db.tmp"
	}

	f, err := os.OpenFile(tmpSegmentPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return "", err
	}
	defer func() {
		if rerr != nil {
			f.Close()
			os.Remove(tmpSegmentPath)
		}
	}()

	meteredF := diskio.NewMeteredWriter(f, m.metrics.observeFlushMemtableBytesWritten)

	bufw := bufio.NewWriter(meteredF)
	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(bufw),
		segmentindex.WithChecksumsDisabled(!m.enableChecksumValidation),
	)

	var keys []segmentindex.Key

	switch m.strategy {
	case StrategyReplace:
		if keys, err = m.flushDataReplace(segmentFile); err != nil {
			return "", err
		}

	case StrategySetCollection:
		if keys, err = m.flushDataSet(segmentFile); err != nil {
			return "", err
		}

	case StrategyRoaringSet:
		if err = m.flushDataRoaringSet(segmentFile, meteredF, bufw); err != nil {
			return "", err
		}

	case StrategyRoaringSetRange:
		if keys, err = m.flushDataRoaringSetRange(segmentFile); err != nil {
			return "", err
		}

	case StrategyMapCollection:
		if keys, err = m.flushDataMap(segmentFile); err != nil {
			return "", err
		}
	case StrategyInverted:
		if keys, _, err = m.flushDataInverted(segmentFile, meteredF, bufw); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("cannot flush strategy %s", m.strategy)
	}

	// The arms above that build their own index are exactly the ones this
	// predicate excludes, and NewBucket reads the same answer to decide whether a
	// secondary index is allowed at all.
	if strategyUsesSharedIndexWriter(m.strategy) {
		indexes := &segmentindex.Indexes{
			Keys:                keys,
			SecondaryIndexCount: m.secondaryIndices,
			AllocChecker:        m.allocChecker,
		}

		if _, err := segmentFile.WriteIndexes(indexes); err != nil {
			return "", err
		}
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return "", err
	}

	if err := f.Sync(); err != nil {
		return "", err
	}

	if err := f.Close(); err != nil {
		return "", err
	}

	segmentPath = strings.TrimSuffix(tmpSegmentPath, ".tmp") + suffixAfterDB
	err = os.Rename(tmpSegmentPath, segmentPath)
	if err != nil {
		return "", err
	}

	// fsync parent directory
	err = diskio.Fsync(filepath.Dir(path))
	if err != nil {
		return "", err
	}

	return segmentPath, nil
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
	flat := m.flattenKeyMap()

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
	totalDataLength, keysToSkip, err := totalValueSizeCollection(flat, m.shouldSkipKeyFunc)
	if err != nil {
		return nil, err
	}
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
	i := 0
	for j, node := range flat {
		if _, ok := keysToSkip[j]; ok {
			continue
		}

		ki, err := (&segmentCollectionNode{
			values:     node.values,
			primaryKey: node.key,
			offset:     totalWritten,
		}).KeyIndexAndWriteTo(f.BodyWriter())
		if err != nil {
			return nil, errors.Wrapf(err, "write node %d", i)
		}

		keys[i] = ki
		i++
		totalWritten = ki.ValueEnd
	}

	return keys[:i], nil
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

func totalValueSizeCollection(in []*binarySearchNodeMulti, shouldSkipKeyFunc func(key []byte, ctx context.Context) (bool, error)) (int, map[int]struct{}, error) {
	var sum int
	keysToSkip := map[int]struct{}{}
	for i, n := range in {
		if shouldSkipKeyFunc != nil {
			if shouldSkipKey, err := shouldSkipKeyFunc(n.key, context.Background()); err != nil {
				return 0, nil, errors.Wrap(err, "should skip key")
			} else if shouldSkipKey {
				keysToSkip[i] = struct{}{}
				continue
			}
		}

		sum += 8 // uint64 to indicate array length
		for _, v := range n.values {
			sum += 1 // bool to indicate value tombstone
			sum += 8 // uint64 to indicate value length
			sum += len(v.value)
		}

		sum += 4 // uint32 to indicate key size
		sum += len(n.key)
	}

	return sum, keysToSkip, nil
}
