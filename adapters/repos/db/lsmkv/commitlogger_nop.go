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

// nopCommitLogger implements memtableCommitLogger with all methods as no-ops.
// It is used when WAL is disabled for a bucket (e.g. when durability is
// provided by the RAFT log instead).
type nopCommitLogger struct{}

var _ memtableCommitLogger = (*nopCommitLogger)(nil)

func (n *nopCommitLogger) writeEntry(commitType CommitType, nodeBytes []byte) error { return nil }
func (n *nopCommitLogger) put(node segmentReplaceNode) error                        { return nil }
func (n *nopCommitLogger) append(node segmentCollectionNode) error                  { return nil }
func (n *nopCommitLogger) add(node *roaringset.SegmentNodeList) error               { return nil }
func (n *nopCommitLogger) walPath() string                                          { return "" }
func (n *nopCommitLogger) size() int64                                              { return 0 }
func (n *nopCommitLogger) flushBuffers() error                                      { return nil }
func (n *nopCommitLogger) close() error                                             { return nil }
func (n *nopCommitLogger) delete() error                                            { return nil }
func (n *nopCommitLogger) sync() error                                              { return nil }
