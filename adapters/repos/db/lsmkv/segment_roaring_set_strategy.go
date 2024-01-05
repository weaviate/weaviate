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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (s *segment) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	out := roaringset.BitmapLayer{}

	if s.strategy != segmentindex.StrategyRoaringSet {
		return out, fmt.Errorf("need strategy %s", StrategyRoaringSet)
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return out, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		return out, err
	}

	sn, err := s.segmentNodeFromBuffer(nodeOffset{node.Start, node.End})
	if err != nil {
		return out, err
	}

	// make sure that any data is copied before exiting this method, otherwise we
	// risk a SEGFAULT as described in
	// https://github.com/weaviate/weaviate/issues/1837
	out.Additions = sn.AdditionsWithCopy()
	out.Deletions = sn.DeletionsWithCopy()
	return out, nil
}

func (s *segment) segmentNodeFromBuffer(offset nodeOffset) (*roaringset.SegmentNode, error) {
	var contents []byte
	if s.mmapContents {
		contents = s.contents[offset.start:offset.end]
	} else {
		contents = make([]byte, offset.end-offset.start)
		r, err := s.bufferedReaderAt(offset.start)
		if err != nil {
			return nil, err
		}
		_, err = r.Read(contents)
		if err != nil {
			return nil, err
		}
	}

	return roaringset.NewSegmentNodeFromBuffer(contents), nil
}
