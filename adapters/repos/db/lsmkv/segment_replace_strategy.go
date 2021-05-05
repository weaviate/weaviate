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

func (i *segment) get(key []byte) ([]byte, error) {
	if i.strategy != SegmentStrategyReplace {
		return nil, errors.Errorf("get only possible for strategy %q", StrategyReplace)
	}

	if !i.bloomFilter.Test(key) {
		return nil, NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		return nil, err
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

type segmentParseResult struct {
	deleted bool
	key     []byte
	value   []byte
	read    int // so that the cursor and calculate its offset for the next round
}

func (i *segment) replaceStratParseDataWithKey(in []byte) (segmentParseResult, error) {
	out := segmentParseResult{}

	if len(in) == 0 {
		return out, NotFound
	}

	// check the tombstone byte
	if in[0] == 0x01 {
		out.deleted = true
	}
	out.read += 1

	r := bytes.NewReader(in[1:])
	var valueLength uint64
	if err := binary.Read(r, binary.LittleEndian, &valueLength); err != nil {
		return out, errors.Wrap(err, "read value length encoding")
	}
	out.read += 8

	out.value = make([]byte, valueLength)
	if n, err := r.Read(out.value); err != nil {
		return out, errors.Wrap(err, "read value")
	} else {
		out.read += n
	}

	var keyLength uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLength); err != nil {
		return out, errors.Wrap(err, "read key length encoding")
	}
	out.read += 4

	out.key = make([]byte, keyLength)
	if n, err := r.Read(out.key); err != nil {
		return out, errors.Wrap(err, "read key")
	} else {
		out.read += n
	}

	if out.deleted {
		return out, Deleted
	}

	return out, nil
}
