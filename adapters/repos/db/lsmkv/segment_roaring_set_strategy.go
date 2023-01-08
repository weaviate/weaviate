package lsmkv

import (
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/ent"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (i *segment) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	out := roaringset.BitmapLayer{}

	if i.strategy != SegmentStrategyRoaringSet {
		return out, fmt.Errorf("need strategy %s", StrategyRoaringSet)
	}

	if !i.bloomFilter.Test(key) {
		return out, ent.NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return out, ent.NotFound
		} else {
			return out, err
		}
	}

	sn := roaringset.NewSegmentNodeFromBuffer(i.contents[node.Start:node.End])

	// make sure that any data is copied before exiting this method, otherwise we
	// risk a SEGFAULT as described in
	// https://github.com/semi-technologies/weaviate/issues/1837
	out.Additions = sn.AdditionsWithCopy()
	out.Deletions = sn.DeletionsWithCopy()
	return out, nil
}
