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
	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// roaringSetGet returns the node's additions bitmap, safe to mutate: cloned
// into a pooled buffer on the mmap path, backed by the pooled node buffer on
// the pread path. The node's deletions are deliberately not read: this
// method only runs for the oldest segment holding the key, whose tombstones
// cannot mask anything older; newer segments' deletions are applied by
// roaringSetMergeWith.
func (s *segment) roaringSetGet(key []byte, bitmapBufPool roaringset.BitmapBufPool,
) (bm *sroar.Bitmap, release func(), err error) {
	if err := segmentindex.CheckStrategyRoaringSet(s.strategy); err != nil {
		return nil, noopRelease, err
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return nil, noopRelease, lsmkv.NotFound
	}
	start, end, err := s.index.GetOffsets(key)
	if err != nil {
		return nil, noopRelease, err
	}

	offset := nodeOffset{start, end}
	if s.readFromMemory {
		sn, err := s.segmentNodeFromBufferMmap(offset)
		if err != nil {
			return nil, noopRelease, err
		}
		bm, release = sn.AdditionsCloneToBuf(bitmapBufPool)
	} else {
		sn, nodeRelease, err := s.segmentNodeFromBufferPread(offset, bitmapBufPool)
		if err != nil {
			return nil, noopRelease, err
		}
		// reuse buffer of entire segment node.
		// node's data might get overwritten by changes of underlying additions bitmap.
		// overwrites should be safe, as other data is not used later on
		bm, release = sn.AdditionsUnlimited(), nodeRelease
	}

	if bm == nil {
		// deletions-only node: additions become the mutable accumulator base
		// when layers are folded, so a non-nil, unshared bitmap is needed
		// even when the node holds none. It shares nothing with the node, so
		// the pooled node buffer of a pread read is released right away
		// instead of being held until the caller's release.
		if release != nil {
			release()
		}
		return sroar.NewBitmap(), noopRelease, nil
	}

	return bm, release, nil
}

// roaringSetMergeWith applies this segment's node for key onto additions in
// place: the node's deletions first (masking older segments' additions),
// then its own additions.
func (s *segment) roaringSetMergeWith(key []byte, additions *sroar.Bitmap, bitmapBufPool roaringset.BitmapBufPool, maxConc int,
) error {
	if err := segmentindex.CheckStrategyRoaringSet(s.strategy); err != nil {
		return err
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return nil
	}
	start, end, err := s.index.GetOffsets(key)
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			return nil
		}
		return err
	}

	var sn *roaringset.SegmentNode
	offset := nodeOffset{start, end}
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

	additions.
		AndNotConc(sn.Deletions(), maxConc).
		OrConc(sn.Additions(), maxConc)
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
