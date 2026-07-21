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

// emptyDeletions is a shared, immutable empty deletions bitmap returned instead
// of cloning an empty deletions set on every roaringset read. Downstream every
// consumer only reads a layer's Deletions (as an AndNot operand — see
// BitmapLayers.Flatten and layer merge), so sharing one instance is safe. Never
// mutate it.
var emptyDeletions = sroar.NewBitmap()

// cloneDeletionsToBuf clones del into a pooled buffer, unless del is empty — in
// which case it returns the shared empty bitmap and a noop release, avoiding a
// per-read clone (its pooled buffer + bitmap struct) for the common
// no-tombstone case.
func cloneDeletionsToBuf(bitmapBufPool roaringset.BitmapBufPool, del *sroar.Bitmap) (*sroar.Bitmap, func()) {
	if del.IsEmpty() {
		return emptyDeletions, noopRelease
	}
	return bitmapBufPool.CloneToBuf(del)
}

// returned bitmaps are cloned and safe to mutate
func (s *segment) roaringSetGet(key []byte, bitmapBufPool roaringset.BitmapBufPool,
) (l roaringset.BitmapLayer, release func(), err error) {
	out := roaringset.BitmapLayer{}

	if err := segmentindex.CheckStrategyRoaringSet(s.strategy); err != nil {
		return out, noopRelease, err
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return out, noopRelease, lsmkv.NotFound
	}
	start, end, err := s.index.GetOffsets(key)
	if err != nil {
		return out, noopRelease, err
	}

	var releaseAdd, releaseDel func()
	offset := nodeOffset{start, end}
	if s.readFromMemory {
		sn, err := s.segmentNodeFromBufferMmap(offset)
		if err != nil {
			return out, noopRelease, err
		}
		out.Deletions, releaseDel = cloneDeletionsToBuf(bitmapBufPool, sn.Deletions())
		out.Additions, releaseAdd = bitmapBufPool.CloneToBuf(sn.Additions())
	} else {
		sn, release, err := s.segmentNodeFromBufferPread(offset, bitmapBufPool)
		if err != nil {
			return out, noopRelease, err
		}
		out.Deletions, releaseDel = cloneDeletionsToBuf(bitmapBufPool, sn.Deletions())
		// reuse buffer of entire segment node.
		// node's data might get overwritten by changes of underlying additions bitmap.
		// overwrites should be safe, as other data is not used later on
		out.Additions, releaseAdd = sn.AdditionsUnlimited(), release
	}

	return out, func() { releaseAdd(); releaseDel() }, nil
}

func (s *segment) roaringSetMergeWith(key []byte, input roaringset.BitmapLayer, bitmapBufPool roaringset.BitmapBufPool, maxConc int,
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

	input.Additions.
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
