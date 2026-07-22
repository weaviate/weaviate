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

// cloneDeletionsToBuf clones del into a pooled buffer, or returns nil when del
// is empty (including nil — which is what SegmentNode.Deletions returns for an
// empty region). Deletions are only ever consumed as an AndNot operand (see
// BitmapLayers.Flatten, where the base layer's deletions are unused and every
// other layer's deletions feed AndNotConc), and that operand treats nil as
// empty, so no empty bitmap needs to be materialized for the common
// no-tombstone case. Using IsEmpty rather than a nil check keeps this correct
// regardless of how an empty del is represented. The release is nil (not a
// shared noop) when nothing was pooled, so combineReleases can drop it without
// allocating a wrapper closure.
func cloneDeletionsToBuf(bitmapBufPool roaringset.BitmapBufPool, del *sroar.Bitmap) (*sroar.Bitmap, func()) {
	if del.IsEmpty() {
		return nil, nil
	}
	return bitmapBufPool.CloneToBuf(del)
}

// cloneAdditionsToBuf clones add into a pooled buffer, unless add is empty — in
// which case it returns a fresh empty bitmap and a nil release. add may be nil
// (SegmentNode.Additions returns nil for an empty region). Unlike deletions,
// additions become the mutable accumulator base when layers are flattened, so a
// non-nil (and non-shared, as it is mutated) bitmap must be returned. The
// release is nil (not a shared noop) when nothing was pooled, so
// combineReleases can drop it without allocating a wrapper closure.
func cloneAdditionsToBuf(bitmapBufPool roaringset.BitmapBufPool, add *sroar.Bitmap) (*sroar.Bitmap, func()) {
	if add.IsEmpty() {
		return sroar.NewBitmap(), nil
	}
	return bitmapBufPool.CloneToBuf(add)
}

// combineReleases folds the additions and deletions release funcs into the
// single release the caller expects. Either may be nil (empty region, nothing
// pooled). A wrapper closure — which escapes and thus heap-allocates — is only
// built when both are non-nil; in the common no-tombstone case (deletions
// empty) the additions release is returned directly, and if neither pooled
// anything the shared noop is reused.
func combineReleases(releaseAdd, releaseDel func()) func() {
	switch {
	case releaseAdd == nil && releaseDel == nil:
		return noopRelease
	case releaseDel == nil:
		return releaseAdd
	case releaseAdd == nil:
		return releaseDel
	default:
		return func() { releaseAdd(); releaseDel() }
	}
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
		out.Additions, releaseAdd = cloneAdditionsToBuf(bitmapBufPool, sn.Additions())
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

	return out, combineReleases(releaseAdd, releaseDel), nil
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
