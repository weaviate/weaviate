//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"os"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (m *MemtableThreaded) closeRequestChannels() {
	for _, channel := range m.requestsChannels {
		close(channel)
	}
	m.wgWorkers.Wait()
}

func (m *MemtableThreaded) flush() error {
	if m.baseline != nil {
		return m.baseline.flush()
	} else {
		// TODO: Do this for the roaring set for now, but this should be done for all strategies
		if m.strategy == StrategyRoaringSet {
			return m.flushRoaringSet()
		}
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedFlush,
		}, true, "Flush")
		m.closeRequestChannels()
		return output.error
	}
}

func (m *MemtableThreaded) getNodesRoaringSet() []*roaringset.BinarySearchNode {
	if m.baseline != nil {
		return m.baseline.RoaringSet().FlattenInOrder()
	} else {
		output := m.threadedOperation(ThreadedMemtableRequest{
			operation: ThreadedRoaringSetFlattenInOrder,
		}, true, "RoaringSetFlattenInOrder")
		return output.nodes
	}
}

func (m *MemtableThreaded) flushRoaringSet() error {
	flat := m.getNodesRoaringSet()
	totalSize := len(flat)

	f, err := os.Create(m.path + ".db")
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, int(float64(totalSize)*1.3))

	keys, err := writeRoaringSet(flat, w)
	if err != nil {
		return err
	}

	return m.writeIndex(keys, w, f)
}

// TODO: this code is a duplicate of the Memtable flush code; I've separated it for tests, in the future, it should be refactored so it can be called for both Memtable and MemtableThreaded
func (m *MemtableThreaded) writeIndex(keys []segmentindex.Key, w *bufio.Writer, f *os.File) error {
	indices := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: m.secondaryIndices,
		ScratchSpacePath:    m.path + ".scratch.d",
	}

	if _, err := indices.WriteTo(w); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	// only now that the file has been flushed is it safe to delete the commit log
	// TODO: there might be an interest in keeping the commit logs around for
	// longer as they might come in handy for replication
	output := m.threadedOperation(ThreadedMemtableRequest{
		operation: ThreadedCommitlogDelete,
	}, true, "CommitlogDelete")
	return output.error
}
