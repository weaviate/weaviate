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

package roaringsetrange

import (
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

type MemtableCursor struct {
	nodes   []*MemtableNode
	nextPos int
}

func NewMemtableCursor(memtable *Memtable) *MemtableCursor {
	return &MemtableCursor{nodes: memtable.Nodes()}
}

func (c *MemtableCursor) First() (uint8, roaringset.BitmapLayer, bool) {
	c.nextPos = 0
	return c.Next()
}

func (c *MemtableCursor) Next() (uint8, roaringset.BitmapLayer, bool) {
	if c.nextPos >= len(c.nodes) {
		return 0, roaringset.BitmapLayer{}, false
	}

	mn := c.nodes[c.nextPos]
	c.nextPos++

	return mn.Key, roaringset.BitmapLayer{
		Additions: mn.Additions,
		Deletions: mn.Deletions,
	}, true
}
