//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/entities"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (i *segment) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	out := roaringset.BitmapLayer{}

	if i.strategy != segmentindex.StrategyRoaringSet {
		return out, fmt.Errorf("need strategy %s", StrategyRoaringSet)
	}

	if !i.bloomFilter.Test(key) {
		return out, entities.NotFound
	}

	node, err := i.index.Get(key)
	if err != nil {
		if err == segmentindex.NotFound {
			return out, entities.NotFound
		} else {
			return out, err
		}
	}

	sn := roaringset.NewSegmentNodeFromBuffer(i.contents[node.Start:node.End])

	// make sure that any data is copied before exiting this method, otherwise we
	// risk a SEGFAULT as described in
	// https://github.com/weaviate/weaviate/issues/1837
	out.Additions = sn.AdditionsWithCopy()
	out.Deletions = sn.DeletionsWithCopy()
	return out, nil
}
