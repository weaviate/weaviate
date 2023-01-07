package lsmkv

import (
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (i *segment) roaringSetGet(key []byte) (roaringSet, error) {
	out := roaringSet{}

	if i.strategy != SegmentStrategyRoaringSet {
		return out, fmt.Errorf("need strategy %s", StrategyRoaringSet)
	}

	if !i.bloomFilter.Test(key) {
		return out, NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return out, NotFound
		} else {
			return out, err
		}
	}

	sn := roaringset.NewSegmentNodeFromBuffer(i.contents[node.Start:node.End])

	// make sure that any data is copied before exiting this method, otherwise we
	// risk a SEGFAULT as described in
	// https://github.com/semi-technologies/weaviate/issues/1837
	out.additions = sn.AdditionsWithCopy()
	out.deletions = sn.DeletionsWithCopy()
	return out, nil
}
