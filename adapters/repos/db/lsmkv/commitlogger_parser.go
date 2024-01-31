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
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/diskio"
)

type commitloggerParser struct {
	r            io.Reader
	strategy     string
	memtable     *Memtable
	reader       io.Reader
	metrics      *Metrics
	replaceCache map[string]segmentReplaceNode
}

func newCommitLoggerParser(r io.Reader, activeMemtable *Memtable,
	strategy string, metrics *Metrics,
) *commitloggerParser {
	return &commitloggerParser{
		r:            r,
		memtable:     activeMemtable,
		strategy:     strategy,
		metrics:      metrics,
		replaceCache: map[string]segmentReplaceNode{},
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

// doReplace parsers all entries into a cache for deduplication first and only
// imports unique entries into the actual memtable as a final step.
func (p *commitloggerParser) doReplace() error {
	metered := diskio.NewMeteredReader(p.r, p.metrics.TrackStartupReadWALDiskIO)
	p.reader = bufio.NewReaderSize(metered, 1*1024*1024)

	// errUnexpectedLength indicates that we could not read the commit log to the
	// end, for example because the last element on the log was corrupt.
	var errUnexpectedLength error

	for {
		var commitType CommitType

		err := binary.Read(p.reader, binary.LittleEndian, &commitType)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			errUnexpectedLength = errors.Wrap(err, "read commit type")
			break
		}

		if CommitTypeReplace.Is(commitType) {
			if err := p.parseReplaceNode(); err != nil {
				errUnexpectedLength = errors.Wrap(err, "read replace node")
				break
			}
		} else {
			return errors.Errorf("found a %s commit on a replace bucket", commitType.String())
		}
	}

	for _, node := range p.replaceCache {
		var opts []SecondaryKeyOption
		if p.memtable.secondaryIndices > 0 {
			for i, secKey := range node.secondaryKeys {
				opts = append(opts, WithSecondaryKey(i, secKey))
			}
		}
		if node.tombstone {
			p.memtable.setTombstone(node.primaryKey, opts...)
		} else {
			p.memtable.put(node.primaryKey, node.value, opts...)
		}
	}

	if errUnexpectedLength != nil {
		return errUnexpectedLength
	}

	return nil
}

// parseReplaceNode only parses into the deduplication cache, not into the
// final memtable yet. A second step is required to parse from the cache into
// the actual memtable.
func (p *commitloggerParser) parseReplaceNode() error {
	n, err := ParseReplaceNode(p.reader, p.memtable.secondaryIndices)
	if err != nil {
		return err
	}

	if !n.tombstone {
		p.replaceCache[string(n.primaryKey)] = n
	} else {
		if existing, ok := p.replaceCache[string(n.primaryKey)]; ok {
			existing.tombstone = true
			p.replaceCache[string(n.primaryKey)] = existing
		} else {
			p.replaceCache[string(n.primaryKey)] = n
		}
	}

	return nil
}
