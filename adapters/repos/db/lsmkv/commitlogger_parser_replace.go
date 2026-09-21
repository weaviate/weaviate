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
	"io"

	"github.com/pkg/errors"
)

// replaceCache tracks its own byte size because on a chunked replay the cache,
// not the memtable, is what grows with the WAL.
type replaceCache struct {
	nodes map[string]segmentReplaceNode
	bytes uint64
}

func newReplaceCache() *replaceCache {
	return &replaceCache{nodes: make(map[string]segmentReplaceNode)}
}

func replaceNodeSize(n segmentReplaceNode) uint64 {
	size := len(n.primaryKey) + len(n.value)
	for _, secKey := range n.secondaryKeys {
		size += len(secKey)
	}
	return uint64(size)
}

// doReplace parsers all entries into a cache for deduplication first and only
// imports unique entries into the actual memtable as a final step.
func (p *commitloggerParser) doReplace() error {
	cache := newReplaceCache()

	var errWhileParsing error

	for {
		if ok, err := p.doReplaceOnce(cache); err != nil {
			errWhileParsing = err
			break
		} else if !ok {
			break
		}

		if !p.chunkFull(cache.bytes) {
			continue
		}

		if err := p.drainReplaceCache(cache); err != nil && errWhileParsing == nil {
			errWhileParsing = err
		}
		cache = newReplaceCache()

		if err := p.chunkIfFull(); err != nil {
			return err
		}
	}

	if err := p.drainReplaceCache(cache); err != nil && errWhileParsing == nil {
		errWhileParsing = err
	}

	return errWhileParsing
}

// drainReplaceCache reports the first entry it could not store and keeps going. A
// WAL holding one entry the memtable refuses would otherwise fail every startup
// from here on.

func (p *commitloggerParser) drainReplaceCache(cache *replaceCache) error {
	var firstErr error

	for _, node := range cache.nodes {
		var opts []SecondaryKeyOption
		if p.memtable.secondaryIndices > 0 {
			for i, secKey := range node.secondaryKeys {
				opts = append(opts, WithSecondaryKey(i, secKey))
			}
		}
		if node.tombstone {
			if err := p.memtable.setTombstone(node.primaryKey, opts...); err != nil && firstErr == nil {
				firstErr = errors.Wrapf(err, "recover delete of %q", node.primaryKey)
			}
		} else if err := p.memtable.put(node.primaryKey, node.value, opts...); err != nil && firstErr == nil {
			firstErr = errors.Wrapf(err, "recover write of %q", node.primaryKey)
		}
	}

	return firstErr
}

func (p *commitloggerParser) doReplaceOnce(cache *replaceCache) (ok bool, err error) {
	var commitType CommitType

	err = binary.Read(p.checksumReader, binary.LittleEndian, &commitType)
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "read commit type")
	}
	if !CommitTypeReplace.Is(commitType) {
		return false, errors.Errorf("found a %s commit on a replace bucket", commitType.String())
	}

	var version uint8

	err = binary.Read(p.checksumReader, binary.LittleEndian, &version)
	if err != nil {
		return false, errors.Wrap(err, "read commit version")
	}

	switch version {
	case 0:
		{
			err = p.doReplaceRecordV0(cache)
		}
	case 1:
		{
			err = p.doReplaceRecordV1(cache)
		}
	default:
		{
			return false, fmt.Errorf("unsupported commit version %d", version)
		}
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (p *commitloggerParser) doReplaceRecordV0(cache *replaceCache) error {
	return p.parseReplaceNode(p.reader, cache)
}

func (p *commitloggerParser) doReplaceRecordV1(cache *replaceCache) error {
	reader, err := p.doRecord()
	if err != nil {
		return err
	}

	return p.parseReplaceNode(reader, cache)
}

// parseReplaceNode only parses into the deduplication cache, not into the
// final memtable yet. A second step is required to parse from the cache into
// the actual memtable.
func (p *commitloggerParser) parseReplaceNode(r io.Reader, cache *replaceCache) error {
	n, err := ParseReplaceNode(r, p.memtable.secondaryIndices)
	if err != nil {
		return err
	}

	if existing, ok := cache.nodes[string(n.primaryKey)]; ok {
		cache.bytes -= replaceNodeSize(existing)
		if n.tombstone {
			existing.tombstone = true
			n = existing
		}
	}

	cache.nodes[string(n.primaryKey)] = n
	cache.bytes += replaceNodeSize(n)

	return nil
}
