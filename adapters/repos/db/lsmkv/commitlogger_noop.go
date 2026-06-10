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

import "github.com/weaviate/weaviate/adapters/repos/db/roaringset"

// noopCommitLogger is a memtableCommitLogger that never touches disk. It backs
// the in-memory memtable of a read-only-follower bucket: write-ahead-log entries
// are replayed into the memtable's in-memory structures, but the commit-log
// methods do nothing, so no .wal file is ever created, appended to, or removed.
// Because a read-only bucket is also immutable, none of the write methods are
// reached after recovery; they exist only to satisfy the interface and to make
// the replay path write-free.
type noopCommitLogger struct {
	path string
}

var _ memtableCommitLogger = (*noopCommitLogger)(nil)

func newNoopCommitLogger(path string) *noopCommitLogger {
	return &noopCommitLogger{path: path}
}

func (cl *noopCommitLogger) writeEntry(commitType CommitType, nodeBytes []byte) error {
	return nil
}

func (cl *noopCommitLogger) put(node segmentReplaceNode) error {
	return nil
}

func (cl *noopCommitLogger) append(node segmentCollectionNode) error {
	return nil
}

func (cl *noopCommitLogger) add(node *roaringset.SegmentNodeList) error {
	return nil
}

func (cl *noopCommitLogger) walPath() string {
	return walPath(cl.path)
}

func (cl *noopCommitLogger) size() int64 {
	return 0
}

func (cl *noopCommitLogger) flushBuffers() error {
	return nil
}

func (cl *noopCommitLogger) close() error {
	return nil
}

func (cl *noopCommitLogger) delete() error {
	return nil
}

func (cl *noopCommitLogger) sync() error {
	return nil
}
