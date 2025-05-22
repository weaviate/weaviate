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

	sn, release, err := s.segmentNodeFromBuffer(nodeOffset{node.Start, node.End}, bitmapBufPool)
	if err != nil {
		return out, noopRelease, err
	}
	defer release()

	var releaseAdd, releaseDel func()
	out.Additions, releaseAdd = bitmapBufPool.CloneToBuf(sn.Additions())
	out.Deletions, releaseDel = bitmapBufPool.CloneToBuf(sn.Deletions())
	release = func() {
		releaseAdd()
		releaseDel()
	}

	return out, release, nil
}

func (s *segment) roaringSetMergeWith(key []byte, input roaringset.BitmapLayer, bitmapBufPool roaringset.BitmapBufPool,
) error {
	if err := segmentindex.CheckExpectedStrategy(s.strategy, segmentindex.StrategyRoaringSet); err != nil {
		return err
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return nil
	}

	node, err := s.index.Get(key)
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			return nil
		}
		return err
	}

	sn, release, err := s.segmentNodeFromBuffer(nodeOffset{node.Start, node.End}, bitmapBufPool)
	if err != nil {
		return err
	}
	defer release()

	input.Additions.
		AndNotConc(sn.Deletions(), concurrency.SROAR_MERGE).
		OrConc(sn.Additions(), concurrency.SROAR_MERGE)

	return nil
}

func (s *segment) segmentNodeFromBuffer(offset nodeOffset, bitmapBufPool roaringset.BitmapBufPool,
) (sn *roaringset.SegmentNode, release func(), err error) {
	var contents []byte
	if s.readFromMemory {
		contents = s.contents[offset.start:offset.end]
		release = noopRelease
	} else {
		r, err := s.bufferedReaderAt(offset.start, "roaringSetRead")
		if err != nil {
			return nil, noopRelease, err
		}

		ln := int(offset.end - offset.start)
		contents, release = bitmapBufPool.Get(ln)
		contents = contents[:ln] // buffer's len is 0, it has to be adjusted
		defer func() {
			if err != nil {
				release()
			}
		}()

		_, err = r.Read(contents)
		if err != nil {
			return nil, noopRelease, err
		}
	}

	return roaringset.NewSegmentNodeFromBuffer(contents), release, nil
}

var noopRelease = func() {}
