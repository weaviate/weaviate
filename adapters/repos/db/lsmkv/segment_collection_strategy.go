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
		return nil, err
	}

	return i.collectionStratParseData(i.contents[node.Start:node.End])
}

func (i *segment) collectionStratParseData(in []byte) ([]value, error) {
	if len(in) == 0 {
		return nil, NotFound
	}

	r := bytes.NewReader(in)

	readSoFar := 0

	var valuesLen uint64
	if err := binary.Read(r, binary.LittleEndian, &valuesLen); err != nil {
		return nil, errors.Wrap(err, "read values len")
	}
	readSoFar += 8

	values := make([]value, valuesLen)
	for i := range values {
		if err := binary.Read(r, binary.LittleEndian, &values[i].tombstone); err != nil {
			return nil, errors.Wrap(err, "read value tombstone")
		}
		readSoFar += 1

		var valueLen uint64
		if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
			return nil, errors.Wrap(err, "read value len")
		}
		readSoFar += 8

		values[i].value = make([]byte, valueLen)
		n, err := r.Read(values[i].value)
		if err != nil {
			return nil, errors.Wrap(err, "read value")
		}
		readSoFar += n
	}

	return values, nil
}

// TODO
// nolint:unused
type segmentCollectionParseResult struct {
	key    []byte
	values []value
	read   int // so that the cursor and calculate its offset for the next round
}

// TODO
// nolint:unused
func (i *segment) collectionStratParseDataWithKey(in []byte) (segmentCollectionParseResult, error) {
	out := segmentCollectionParseResult{}

	if len(in) == 0 {
		return out, NotFound
	}

	r := bytes.NewReader(in)

	var valuesLen uint64
	if err := binary.Read(r, binary.LittleEndian, &valuesLen); err != nil {
		return out, errors.Wrap(err, "read values len")
	}
	out.read += 8

	out.values = make([]value, valuesLen)
	for i := range out.values {
		if err := binary.Read(r, binary.LittleEndian, &out.values[i].tombstone); err != nil {
			return out, errors.Wrap(err, "read value tombstone")
		}
		out.read += 1

		var valueLen uint64
		if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
			return out, errors.Wrap(err, "read value len")
		}
		out.read += 8

		out.values[i].value = make([]byte, valueLen)
		n, err := r.Read(out.values[i].value)
		if err != nil {
			return out, errors.Wrap(err, "read value")
		}
		out.read += n
	}

	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return out, errors.Wrap(err, "read key len")
	}
	out.read += 4

	out.key = make([]byte, keyLen)
	n, err := r.Read(out.key)
	if err != nil {
		return out, errors.Wrap(err, "read key")
	}
	out.read += n

	return out, nil
}
