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
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// returned bitmaps are cloned and safe to mutate
func (s *segment) roaringSetGet(key []byte, bitmapBufPool roaringset.BitmapBufPool,
) (l roaringset.BitmapLayer, release func(), err error) {
	out := roaringset.BitmapLayer{}

	if err := segmentindex.CheckExpectedStrategy(s.strategy, segmentindex.StrategyRoaringSet); err != nil {
		return out, noopRelease, err
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return out, noopRelease, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		return out, noopRelease, err
	}

	sn, poolUsed, release, err := s.segmentNodeFromBuffer(nodeOffset{node.Start, node.End}, bitmapBufPool)
	if err != nil {
		return out, noopRelease, err
	}
	defer func() {
		if err != nil {
			release()
		}
	}()

	var releaseAdd, releaseDel func()
	out.Deletions, releaseDel = bitmapBufPool.CloneToBuf(sn.Deletions())
	if poolUsed {
		out.Additions, releaseAdd = sn.Additions(), release
	} else {
		out.Additions, releaseAdd = bitmapBufPool.CloneToBuf(sn.Additions())
	}
	release = func() {
		releaseAdd()
		releaseDel()
	}

	return out, release, nil
}

func (s *segment) roaringSetMergeWith(key []byte, bitmapBufPool roaringset.BitmapBufPool, input roaringset.BitmapLayer,
) (merged roaringset.BitmapLayer, err error) {
	if err := segmentindex.CheckExpectedStrategy(s.strategy, segmentindex.StrategyRoaringSet); err != nil {
		return input, err
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return input, nil
	}

	node, err := s.index.Get(key)
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			return input, nil
		}
		return input, err
	}

	sn, _, release, err := s.segmentNodeFromBuffer(nodeOffset{node.Start, node.End}, bitmapBufPool)
	if err != nil {
		return input, err
	}
	defer release()

	input.Additions.
		AndNotConc(sn.Deletions(), concurrency.SROAR_MERGE).
		OrConc(sn.Additions(), concurrency.SROAR_MERGE)

	return input, nil
}

func (s *segment) segmentNodeFromBuffer(offset nodeOffset, bitmapBufPool roaringset.BitmapBufPool,
) (sn *roaringset.SegmentNode, poolUsed bool, release func(), err error) {
	var contents []byte
	var put func()

	if s.mmapContents {
		contents = s.contents[offset.start:offset.end]
		put = noopRelease
	} else {
		r, err := s.bufferedReaderAt(offset.start)
		if err != nil {
			return nil, false, noopRelease, err
		}

		ln := int(offset.end - offset.start)
		contents, put = bitmapBufPool.Get(ln)
		defer func() {
			if err != nil {
				put()
			}
		}()

		_, err = r.Read(contents)
		if err != nil {
			return nil, false, noopRelease, err
		}
		poolUsed = true
	}

	return roaringset.NewSegmentNodeFromBuffer(contents), poolUsed, put, nil
}

var noopRelease func()
