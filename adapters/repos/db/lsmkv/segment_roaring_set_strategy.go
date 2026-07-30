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

// combineReleases folds the additions and deletions releases into one. Either
// may be nil (empty region); the wrapper closure — which heap-allocates — is
// only built when both are non-nil.
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
func (s *segment) roaringSetGet(key []byte, bitmapBufPool roaringset.BitmapBufPool, addMinCap int,
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
		out.Deletions, releaseDel = sn.DeletionsCloneToBuf(bitmapBufPool)
		out.Additions, releaseAdd = sn.AdditionsCloneToBufWithMinCap(bitmapBufPool, addMinCap)
	} else {
		sn, release, err := s.segmentNodeFromBufferPread(offset, bitmapBufPool, addMinCap)
		if err != nil {
			return out, noopRelease, err
		}
		out.Deletions, releaseDel = sn.DeletionsCloneToBuf(bitmapBufPool)
		// reuse buffer of entire segment node.
		// node's data might get overwritten by changes of underlying additions bitmap.
		// overwrites should be safe, as other data is not used later on
		out.Additions, releaseAdd = sn.AdditionsUnlimited(), release
	}

	if out.Additions == nil {
		// deletions-only node: additions become the mutable accumulator base
		// when layers are folded, so a non-nil, unshared bitmap is needed
		// even when the node holds none
		out.Additions = sroar.NewBitmap()
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
		sn, release, err = s.segmentNodeFromBufferPread(offset, bitmapBufPool, 0)
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

// roaringSetNodeSize returns the byte size of the key's segment node, or 0
// when the segment does not hold the key.
func (s *segment) roaringSetNodeSize(key []byte) int {
	if s.strategy != segmentindex.StrategyRoaringSet {
		return 0
	}

	if s.useBloomFilter && !s.bloomFilter.Test(key) {
		return 0
	}
	start, end, err := s.index.GetOffsets(key)
	if err != nil {
		return 0
	}
	return int(end - start)
}

func (s *segment) segmentNodeFromBufferMmap(offset nodeOffset,
) (sn *roaringset.SegmentNode, err error) {
	return roaringset.NewSegmentNodeFromBuffer(s.contents[offset.start:offset.end]), nil
}

// minCap extends the pooled buffer beyond the node itself; the additions
// bitmap aliasing it (see AdditionsUnlimited) grows in place within that
// spare capacity.
func (s *segment) segmentNodeFromBufferPread(offset nodeOffset, bitmapBufPool roaringset.BitmapBufPool, minCap int,
) (sn *roaringset.SegmentNode, release func(), err error) {
	reader, readerRelease, err := s.bufferedReaderAt(offset.start, "roaringSetRead")
	if err != nil {
		return nil, noopRelease, err
	}
	defer readerRelease()

	ln := int(offset.end - offset.start)
	if minCap < ln {
		minCap = ln
	}
	contents, release := bitmapBufPool.Get(minCap)
	contents = contents[:ln]

	_, err = reader.Read(contents)
	if err != nil {
		release()
		return nil, noopRelease, err
	}
	return roaringset.NewSegmentNodeFromBuffer(contents), release, nil
}

var noopRelease = func() {}
