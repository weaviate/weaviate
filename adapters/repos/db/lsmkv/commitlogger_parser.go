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
	"bytes"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rwhasher"
)

type commitloggerParser struct {
	strategy string

	reader         io.Reader
	checksumReader rwhasher.ReaderHasher

	bufNode *bytes.Buffer

	memtable *Memtable
}

func newCommitLoggerParser(strategy string, reader io.Reader, memtable *Memtable,
) *commitloggerParser {
	return &commitloggerParser{
		strategy:       strategy,
		reader:         reader,
		checksumReader: rwhasher.NewCRC32Reader(reader),
		bufNode:        bytes.NewBuffer(nil),
		memtable:       memtable,
	}
}

func (p *commitloggerParser) Do() error {
	switch p.strategy {
	case StrategyReplace:
		return p.doReplace()
	case StrategyMapCollection, StrategySetCollection:
		return p.doCollection()
	case StrategyRoaringSet:
		return p.doRoaringSet()
	default:
		return errors.Errorf("unknown strategy %s on commit log parse", p.strategy)
	}
}
