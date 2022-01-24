//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
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

func (i *segment) getCollection(key []byte) ([]value, error) {
	if i.strategy != SegmentStrategySetCollection &&
		i.strategy != SegmentStrategyMapCollection {
		return nil, errors.Errorf("get only possible for strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
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

	return i.collectionStratParseData(i.contents[node.Start:node.End])
}

func (i *segment) collectionStratParseData(in []byte) ([]value, error) {
	if len(in) == 0 {
		return nil, NotFound
	}

	offset := 0

	valuesLen := binary.LittleEndian.Uint64(in[offset : offset+8])
	offset += 8

	values := make([]value, valuesLen)
	valueIndex := 0
	for valueIndex < int(valuesLen) {
		values[valueIndex].tombstone = in[offset] == 0x01
		offset += 1

		valueLen := binary.LittleEndian.Uint64(in[offset : offset+8])
		offset += 8

		values[valueIndex].value = in[offset : offset+int(valueLen)]
		offset += int(valueLen)

		valueIndex++
	}

	return values, nil
}

func (i *segment) collectionStratParseDataWithKey(in []byte) (segmentCollectionNode, error) {
	r := bytes.NewReader(in)

	if len(in) == 0 {
		return segmentCollectionNode{}, NotFound
	}

	return ParseCollectionNode(r)
}
