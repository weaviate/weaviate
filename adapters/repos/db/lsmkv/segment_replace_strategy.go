//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (i *segment) get(key []byte) ([]byte, error) {
	if i.strategy != SegmentStrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if !i.bloomFilter.Test(key) {
		return nil, NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return nil, NotFound
		} else {
			return nil, err
		}
	}

	return i.replaceStratParseData(i.contents[node.Start:node.End])
}

func (i *segment) getBySecondary(pos int, key []byte) ([]byte, error) {
	if i.strategy != SegmentStrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if pos > len(i.secondaryIndices) || i.secondaryIndices[pos] == nil {
		return nil, errors.Errorf("no secondary index at pos %d", pos)
	}

	if !i.secondaryBloomFilters[pos].Test(key) {
		return nil, NotFound
	}

	node, err := i.secondaryIndices[pos].Get(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return nil, NotFound
		} else {
			return nil, err
		}
	}

	return i.replaceStratParseData(i.contents[node.Start:node.End])
}

func (i *segment) replaceStratParseData(in []byte) ([]byte, error) {
	if len(in) == 0 {
		return nil, NotFound
	}

	// check the tombstone byte
	if in[0] == 0x01 {
		return nil, Deleted
	}

	r := bytes.NewReader(in[1:])
	var valueLength uint64
	if err := binary.Read(r, binary.LittleEndian, &valueLength); err != nil {
		return nil, errors.Wrap(err, "read value length encoding")
	}

	data := make([]byte, valueLength)
	if _, err := r.Read(data); err != nil {
		return nil, errors.Wrap(err, "read value")
	}

	return data, nil
}

func (i *segment) replaceStratParseDataWithKey(in []byte) (segmentReplaceNode, error) {
	if len(in) == 0 {
		return segmentReplaceNode{}, NotFound
	}

	r := bytes.NewReader(in)

	out, err := ParseReplaceNode(r, i.secondaryIndexCount)
	if err != nil {
		return out, err
	}

	if out.tombstone {
		return out, Deleted
	}

	return out, nil
}

func (i *segment) replaceStratParseDataWithKeyInto(in []byte,
	node *segmentReplaceNode) error {
	if len(in) == 0 {
		return NotFound
	}

	r := bytes.NewReader(in)

	err := ParseReplaceNodeInto(r, i.secondaryIndexCount, node)
	if err != nil {
		return err
	}

	if node.tombstone {
		return Deleted
	}

	return nil
}
