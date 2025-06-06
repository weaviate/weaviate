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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// returned bitmaps are cloned and safe to mutate
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

	sn, copied, err := s.segmentNodeFromBuffer(nodeOffset{node.Start, node.End})
	if err != nil {
		return out, err
	}

	if copied {
		out.Additions = sn.Additions()
		out.Deletions = sn.Deletions()
	} else {
		// make sure that any data is copied before exiting this method, otherwise we
		// risk a SEGFAULT as described in
		// https://github.com/weaviate/weaviate/issues/1837
		out.Additions = sn.AdditionsWithCopy()
		out.Deletions = sn.DeletionsWithCopy()
	}
	return out, nil
}

func (s *segment) segmentNodeFromBuffer(offset nodeOffset) (*roaringset.SegmentNode, bool, error) {
	var contents []byte
	copied := false
	if s.readFromMemory {
		contents = s.contents[offset.start:offset.end]
	} else {
		contents = make([]byte, offset.end-offset.start)
		r, err := s.bufferedReaderAt(offset.start, "roaringSetRead")
		if err != nil {
			return nil, false, err
		}
		_, err = r.Read(contents)
		if err != nil {
			return nil, false, err
		}
		copied = true
	}

	return roaringset.NewSegmentNodeFromBuffer(contents), copied, nil
}
