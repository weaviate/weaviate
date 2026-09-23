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
// not the memtable, is what grows with the WAL. bytes counts record payload like
// Memtable.size, so the live heap behind it is several times larger: the node is
// stored by value and the key is copied into the map.
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

		if !p.chunkIsFull(cache.bytes) {
			continue
		}

		p.storeReplaceCache(cache)
		cache = newReplaceCache()

		if err := p.cutChunk(); err != nil {
			errWhileParsing = err
			break
		}
	}

	p.storeReplaceCache(cache)

	return errWhileParsing
}

// storeReplaceCache stores every entry it can and latches the first refusal into
// p.memtableRejectErr, so one unstorable entry costs that entry rather than the rest of the WAL.
func (p *commitloggerParser) storeReplaceCache(cache *replaceCache) {
	var firstErr error

	for _, node := range cache.nodes {
		var opts []SecondaryKeyOption
		if p.memtable.secondaryIndices > 0 {
			for i, secKey := range node.secondaryKeys {
				opts = append(opts, WithSecondaryKey(i, secKey))
			}
		}
		var err error
		if node.tombstone {
			err = errors.Wrapf(p.memtable.setTombstone(node.primaryKey, opts...),
				"recover delete of %q", node.primaryKey)
		} else {
			err = errors.Wrapf(p.memtable.put(node.primaryKey, node.value, opts...),
				"recover write of %q", node.primaryKey)
		}
		if err != nil {
			p.refusedEntries++
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if p.memtableRejectErr == nil {
		p.memtableRejectErr = firstErr
	}
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
