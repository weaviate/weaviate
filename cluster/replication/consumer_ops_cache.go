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

package replication

import "sync"

type OpsCache struct {
	ops sync.Map
}

func NewOpsCache() *OpsCache {
	return &OpsCache{
		ops: sync.Map{},
	}
}

func (c *OpsCache) LoadOrStore(opId uint64) bool {
	_, ok := c.ops.LoadOrStore(opId, struct{}{})
	return ok
}

func (c *OpsCache) Remove(opId uint64) {
	c.ops.Delete(opId)
}
