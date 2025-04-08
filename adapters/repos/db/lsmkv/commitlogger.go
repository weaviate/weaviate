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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/usecases/integrity"
)

type memtableCommitLogger interface {
	writeEntry(commitType CommitType, nodeBytes []byte) error
	put(node segmentReplaceNode) error
	append(node segmentCollectionNode) error
	add(node *roaringset.SegmentNodeList) error
	walPath() string
	size() int64
	flushBuffers() error
	close() error
	delete() error
}

var (
	_ memtableCommitLogger = (*lazyCommitLogger)(nil)
	_ memtableCommitLogger = (*commitLogger)(nil)
)

type lazyCommitLogger struct {
	path         string
	commitLogger *commitLogger
	mux          sync.Mutex
}

func (cl *lazyCommitLogger) mayInitCommitLogger() error {
	cl.mux.Lock()
	defer cl.mux.Unlock()

	if cl.commitLogger != nil {
		return nil
	}

	commitLogger, err := newCommitLogger(cl.path)
	if err != nil {
		return err
	}

	cl.commitLogger = commitLogger
	return nil
}

func walPath(path string) string {
	return path + ".wal"
}

func (cl *lazyCommitLogger) walPath() string {
	return walPath(cl.path)
}

func (cl *lazyCommitLogger) writeEntry(commitType CommitType, nodeBytes []byte) error {
	err := cl.mayInitCommitLogger()
	if err != nil {
		return err
	}

	return cl.commitLogger.writeEntry(commitType, nodeBytes)
}

func (cl *lazyCommitLogger) put(node segmentReplaceNode) error {
	err := cl.mayInitCommitLogger()
	if err != nil {
		return err
	}

	return cl.commitLogger.put(node)
}

func (cl *lazyCommitLogger) append(node segmentCollectionNode) error {
	err := cl.mayInitCommitLogger()
	if err != nil {
		return err
	}

	return cl.commitLogger.append(node)
}

func (cl *lazyCommitLogger) add(node *roaringset.SegmentNodeList) error {
	err := cl.mayInitCommitLogger()
	if err != nil {
		return err
	}

	return cl.commitLogger.add(node)
}

func (cl *lazyCommitLogger) size() int64 {
	cl.mux.Lock()
	defer cl.mux.Unlock()

	if cl.commitLogger == nil {
		return 0
	}

	return cl.commitLogger.size()
}

func (cl *lazyCommitLogger) flushBuffers() error {
	cl.mux.Lock()
	defer cl.mux.Unlock()

	if cl.commitLogger == nil {
		return nil
	}

	return cl.commitLogger.flushBuffers()
}

func (cl *lazyCommitLogger) close() error {
	cl.mux.Lock()
	defer cl.mux.Unlock()

	if cl.commitLogger == nil {
		return nil
	}

	return cl.commitLogger.close()
}

func (cl *lazyCommitLogger) delete() error {
	cl.mux.Lock()
	defer cl.mux.Unlock()

	if cl.commitLogger == nil {
		return nil
	}

	return cl.commitLogger.delete()
}

type commitLogger struct {
	file   *os.File
	writer *bufio.Writer
	n      atomic.Int64
	path   string

	checksumWriter integrity.ChecksumWriter

	bufNode *bytes.Buffer

	// e.g. when recovering from an existing log, we do not want to write into a
	// new log again
	paused bool
}

// commit log entry data format
// ---------------------------
// | version == 0 (1byte)    |
// | record (dynamic length) |
// ---------------------------

// ------------------------------------------------------
// | version == 1 (1byte)                               |
// | type (1byte)                                       |
// | node length (4bytes)                               |
// | node (dynamic length)                              |
// | checksum (crc32 4bytes non-checksum fields so far) |
// ------------------------------------------------------

const CurrentCommitLogVersion uint8 = 1

type CommitType uint8

const (
	CommitTypeReplace CommitType = iota // replace strategy

	// collection strategy - this can handle all cases as updates and deletes are
	// only appends in a collection strategy
	CommitTypeCollection
	CommitTypeRoaringSet
	// new version of roaringset that stores data as a list of uint64 values,
	// instead of a roaring bitmap
	CommitTypeRoaringSetList
)

func (ct CommitType) String() string {
	switch ct {
	case CommitTypeReplace:
		return "replace"
	case CommitTypeCollection:
		return "collection"
	case CommitTypeRoaringSet:
		return "roaringset"
	case CommitTypeRoaringSetList:
		return "roaringsetlist"
	default:
		return "unknown"
	}
}

func (ct CommitType) Is(checkedCommitType CommitType) bool {
	return ct == checkedCommitType
}

func newLazyCommitLogger(path string) (*lazyCommitLogger, error) {
	return &lazyCommitLogger{
		path: path,
	}, nil
}

func newCommitLogger(path string) (*commitLogger, error) {
	out := &commitLogger{
		path: walPath(path),
	}

	f, err := os.OpenFile(out.path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	out.file = f

	out.writer = bufio.NewWriter(f)
	out.checksumWriter = integrity.NewCRC32Writer(out.writer)

	out.bufNode = bytes.NewBuffer(nil)

	return out, nil
}

func (cl *commitLogger) walPath() string {
	return cl.path
}

func (cl *commitLogger) writeEntry(commitType CommitType, nodeBytes []byte) error {
	// TODO: do we need a timestamp? if so, does it need to be a vector clock?

	err := binary.Write(cl.checksumWriter, binary.LittleEndian, commitType)
	if err != nil {
		return err
	}

	err = binary.Write(cl.checksumWriter, binary.LittleEndian, CurrentCommitLogVersion)
	if err != nil {
		return err
	}

	err = binary.Write(cl.checksumWriter, binary.LittleEndian, uint32(len(nodeBytes)))
	if err != nil {
		return err
	}

	// write node
	_, err = cl.checksumWriter.Write(nodeBytes)
	if err != nil {
		return err
	}

	// write record checksum directly on the writer
	checksumSize, err := cl.writer.Write(cl.checksumWriter.Hash())
	if err != nil {
		return err
	}

	cl.n.Add(int64(1 + 1 + 4 + len(nodeBytes) + checksumSize))

	return nil
}

func (cl *commitLogger) put(node segmentReplaceNode) error {
	if cl.paused {
		return nil
	}

	cl.bufNode.Reset()

	ki, err := node.KeyIndexAndWriteTo(cl.bufNode)
	if err != nil {
		return err
	}
	if len(cl.bufNode.Bytes()) != ki.ValueEnd-ki.ValueStart {
		return fmt.Errorf("unexpected error, node size mismatch")
	}

	return cl.writeEntry(CommitTypeReplace, cl.bufNode.Bytes())
}

func (cl *commitLogger) append(node segmentCollectionNode) error {
	if cl.paused {
		return nil
	}

	cl.bufNode.Reset()

	ki, err := node.KeyIndexAndWriteTo(cl.bufNode)
	if err != nil {
		return err
	}
	if len(cl.bufNode.Bytes()) != ki.ValueEnd-ki.ValueStart {
		return fmt.Errorf("unexpected error, node size mismatch")
	}

	return cl.writeEntry(CommitTypeCollection, cl.bufNode.Bytes())
}

func (cl *commitLogger) add(node *roaringset.SegmentNodeList) error {
	if cl.paused {
		return nil
	}

	cl.bufNode.Reset()

	ki, err := node.KeyIndexAndWriteTo(cl.bufNode, 0)
	if err != nil {
		return err
	}
	if len(cl.bufNode.Bytes()) != ki.ValueEnd-ki.ValueStart {
		return fmt.Errorf("unexpected error, node size mismatch")
	}

	return cl.writeEntry(CommitTypeRoaringSetList, cl.bufNode.Bytes())
}

// Size returns the amount of data that has been written since the commit
// logger was initialized. After a flush a new logger is initialized which
// automatically resets the logger.
func (cl *commitLogger) size() int64 {
	return cl.n.Load()
}

func (cl *commitLogger) close() error {
	if !cl.paused {
		if err := cl.writer.Flush(); err != nil {
			return err
		}

		if err := cl.file.Sync(); err != nil {
			return err
		}
	}

	return cl.file.Close()
}

func (cl *commitLogger) pause() {
	cl.paused = true
}

func (cl *commitLogger) unpause() {
	cl.paused = false
}

func (cl *commitLogger) delete() error {
	return os.Remove(cl.path)
}

func (cl *commitLogger) flushBuffers() error {
	err := cl.writer.Flush()
	if err != nil {
		return fmt.Errorf("flushing WAL %q: %w", cl.path, err)
	}

	return nil
}
