//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

	var releaseAdd, releaseDel func()
	offset := nodeOffset{node.Start, node.End}
	if s.readFromMemory {
		sn, err := s.segmentNodeFromBufferMmap(offset)
		if err != nil {
			return out, noopRelease, err
		}
		out.Deletions, releaseDel = bitmapBufPool.CloneToBuf(sn.Deletions())
		out.Additions, releaseAdd = bitmapBufPool.CloneToBuf(sn.Additions())
	} else {
		sn, release, err := s.segmentNodeFromBufferPread(offset, bitmapBufPool)
		if err != nil {
			return out, noopRelease, err
		}
		out.Deletions, releaseDel = bitmapBufPool.CloneToBuf(sn.Deletions())
		// reuse buffer of entire segment node.
		// node's data might get overwritten by changes of underlying additions bitmap.
		// overwrites should be safe, as other data is not used later on
		out.Additions, releaseAdd = sn.AdditionsUnlimited(), release
	}

	return out, func() { releaseAdd(); releaseDel() }, nil
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

	var sn *roaringset.SegmentNode
	offset := nodeOffset{node.Start, node.End}
	if s.readFromMemory {
		sn, err = s.segmentNodeFromBufferMmap(offset)
	} else {
		var release func()
		sn, release, err = s.segmentNodeFromBufferPread(offset, bitmapBufPool)
		defer release()
	}
	if err != nil {
		return err
	}

	input.Additions.
		AndNotConc(sn.Deletions(), concurrency.SROAR_MERGE).
		OrConc(sn.Additions(), concurrency.SROAR_MERGE)
	return nil
}

func (s *segment) segmentNodeFromBufferMmap(offset nodeOffset,
) (sn *roaringset.SegmentNode, err error) {
	return roaringset.NewSegmentNodeFromBuffer(s.contents[offset.start:offset.end]), nil
}

func (s *segment) segmentNodeFromBufferPread(offset nodeOffset, bitmapBufPool roaringset.BitmapBufPool,
) (sn *roaringset.SegmentNode, release func(), err error) {
	reader, readerRelease, err := s.bufferedReaderAt(offset.start, "roaringSetRead")
	if err != nil {
		return nil, noopRelease, err
	}
	defer readerRelease()

	ln := int(offset.end - offset.start)
	contents, release := bitmapBufPool.Get(ln)
	contents = contents[:ln]

	_, err = reader.Read(contents)
	if err != nil {
		release()
		return nil, noopRelease, err
	}
	return roaringset.NewSegmentNodeFromBuffer(contents), release, nil
}

var noopRelease = func() {}
