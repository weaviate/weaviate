//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func guessCommitLogStrategy(wal io.ReadSeeker) (string, error) {
	return guessCommitLogStrategyAmong(wal, prioritizedAllStrategies)
}

func guessCommitLogStrategyAmong(wal io.ReadSeeker, prioritizedStrategies []string) (string, error) {
	for _, strategy := range prioritizedStrategies {
		check, ok := commitlogStrategyCompatibilityChecks[strategy]
		if !ok {
			return "", fmt.Errorf("unknown strategy %q", strategy)
		}
		if compatible, _ := check(wal); compatible {
			return strategy, nil
		}
	}
	return "", fmt.Errorf("not compatible with strategies %v", prioritizedStrategies)
}

const commitlogEntriesToCheck = 8

var (
	prioritizedAllStrategies             []string
	commitlogStrategyCompatibilityChecks map[string]func(io.ReadSeeker) (bool, error)
)

func init() {
	// Some strategies use the same commit log format, therefore it is not possible
	// to clearly determine bucket's strategy.
	// It applies to RoaringSet + RoaringSetRange:
	// - RoaringSetRange is compatible with RoaringSet
	// - RoaringSet is compatible with RoaringSetRange if keys are numeric
	// and MapCollection + Inverted + SetCollection:
	// - MapCollection is compatible with Inverted and SetCollection
	// - Inverted is compatible with MapCollection and SetCollection
	// - but SetCollection is not compatible with MapCollection neither Inverted
	// For that reason strategies are checked in ordered of higher occurrence probability.
	prioritizedAllStrategies = []string{
		StrategyRoaringSet,
		StrategyRoaringSetRange,
		StrategyMapCollection,
		StrategyInverted,
		StrategyReplace,
		StrategySetCollection,
	}

	commitlogStrategyCompatibilityChecks = map[string]func(io.ReadSeeker) (bool, error){
		StrategyReplace:         isCommitLogCompatibleReplace,
		StrategyMapCollection:   isCommitLogCompatibleMapCollection,
		StrategyInverted:        isCommitLogCompatibleInverted,
		StrategySetCollection:   isCommitLogCompatibleSetCollection,
		StrategyRoaringSet:      isCommitLogCompatibleRoaringSet,
		StrategyRoaringSetRange: isCommitLogCompatibleRoaringSetRange,
	}
}

func isCommitLogCompatibleReplace(wal io.ReadSeeker) (compatible bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			compatible = false
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = fmt.Errorf("recover: %v", r)
			}
		}
	}()

	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	memtable, err := newMemtable("", StrategyReplace, 0, &noopMemtableCommitLogger{}, nil, nil, false, nil, false, nil)
	if err != nil {
		return false, err
	}

	parser := newCommitLoggerParser(StrategyReplace, wal, memtable)
	nodeCache := make(map[string]segmentReplaceNode)
	for range commitlogEntriesToCheck {
		if ok, err := parser.doReplaceOnce(nodeCache); err != nil {
			return false, err
		} else if !ok {
			break
		}
	}
	return true, nil
}

func isCommitLogCompatibleSetCollection(wal io.ReadSeeker) (compatible bool, err error) {
	return isCommitLogCompatibleCollection(wal, StrategySetCollection)
}

func isCommitLogCompatibleMapCollection(wal io.ReadSeeker) (compatible bool, err error) {
	return isCommitLogCompatibleCollection(wal, StrategyMapCollection)
}

func isCommitLogCompatibleInverted(wal io.ReadSeeker) (compatible bool, err error) {
	return isCommitLogCompatibleCollection(wal, StrategyInverted)
}

func isCommitLogCompatibleCollection(wal io.ReadSeeker, collectionStrategy string) (compatible bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			compatible = false
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = fmt.Errorf("recover: %v", r)
			}
		}
	}()

	memtable, err := newMemtable("", collectionStrategy, 0, &noopMemtableCommitLogger{}, nil, nil, false, nil, false, nil)
	if err != nil {
		return false, err
	}

	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	parser := newCommitLoggerParser(collectionStrategy, wal, memtable)
	for range commitlogEntriesToCheck {
		if ok, err := parser.doCollectionOnce(); err != nil {
			return false, err
		} else if !ok {
			break
		}
	}
	return true, nil
}

func isCommitLogCompatibleRoaringSet(wal io.ReadSeeker) (compatible bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			compatible = false
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = fmt.Errorf("recover: %v", r)
			}
		}
	}()

	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	parser := newCommitLoggerParser(StrategyRoaringSet, wal, nil)
	rsParser := &commitlogParserRoaringSet{
		parser:  parser,
		consume: func(key []byte, additions, deletions []uint64) error { return nil },
	}
	for range commitlogEntriesToCheck {
		if ok, err := rsParser.parseOnce(); err != nil {
			return false, err
		} else if !ok {
			break
		}
	}
	return true, nil
}

func isCommitLogCompatibleRoaringSetRange(wal io.ReadSeeker) (compatible bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			compatible = false
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = fmt.Errorf("recover: %v", r)
			}
		}
	}()

	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	parser := newCommitLoggerParser(StrategyRoaringSetRange, wal, nil)
	rsParser := &commitlogParserRoaringSet{
		parser: parser,
		consume: func(key []byte, additions, deletions []uint64) error {
			if len(key) != 8 {
				return fmt.Errorf("commitloggerParser: invalid value length %d, should be 8 bytes", len(key))
			}
			return nil
		},
	}
	for range commitlogEntriesToCheck {
		if ok, err := rsParser.parseOnce(); err != nil {
			return false, err
		} else if !ok {
			break
		}
	}
	return true, nil
}

// -----------------------------------------------------------------------------

type noopMemtableCommitLogger struct{}

func (cl *noopMemtableCommitLogger) writeEntry(commitType CommitType, nodeBytes []byte) error {
	return nil
}
func (cl *noopMemtableCommitLogger) put(node segmentReplaceNode) error          { return nil }
func (cl *noopMemtableCommitLogger) append(node segmentCollectionNode) error    { return nil }
func (cl *noopMemtableCommitLogger) add(node *roaringset.SegmentNodeList) error { return nil }
func (cl *noopMemtableCommitLogger) walPath() string                            { return "" }
func (cl *noopMemtableCommitLogger) size() int64                                { return 0 }
func (cl *noopMemtableCommitLogger) flushBuffers() error                        { return nil }
func (cl *noopMemtableCommitLogger) close() error                               { return nil }
func (cl *noopMemtableCommitLogger) delete() error                              { return nil }
func (cl *noopMemtableCommitLogger) sync() error                                { return nil }
