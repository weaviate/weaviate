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

import (
	"context"
	"sync"
)

type OpsCache struct {
	// hasBeenCancelled is a map of opId to an empty struct
	hasBeenCancelled sync.Map
	// cancels is a map of opId to a cancel function
	cancels sync.Map
	// ops is a map of opId to an empty struct
	ops sync.Map
}

func NewOpsCache() *OpsCache {
	return &OpsCache{
		hasBeenCancelled: sync.Map{},
		cancels:          sync.Map{},
		ops:              sync.Map{},
	}
}

func (c *OpsCache) HasBeenCancelled(opId uint64) bool {
	_, ok := c.hasBeenCancelled.Load(opId)
	return ok
}

func (c *OpsCache) StoreHasBeenCancelled(opId uint64) {
	c.hasBeenCancelled.Store(opId, struct{}{})
}

func (c *OpsCache) LoadOrStore(opId uint64) bool {
	_, ok := c.ops.LoadOrStore(opId, struct{}{})
	return ok
}

func (c *OpsCache) Load(opId uint64) bool {
	_, ok := c.ops.Load(opId)
	return ok
}

func (c *OpsCache) LoadCancel(opId uint64) (context.CancelFunc, bool) {
	cancelAny, ok := c.cancels.Load(opId)
	if !ok {
		return nil, false
	}
	cancel, ok := cancelAny.(context.CancelFunc)
	if !ok {
		return nil, false
	}
	return cancel, true
}

func (c *OpsCache) StoreCancel(opId uint64, cancel context.CancelFunc) {
	c.cancels.Store(opId, cancel)
}

func (c *OpsCache) Cancel(opId uint64) bool {
	cancel, ok := c.LoadCancel(opId)
	if !ok {
		return false
	}
	cancel()
	return true
}

func (c *OpsCache) Remove(opId uint64) {
	c.hasBeenCancelled.Delete(opId)
	c.cancels.Delete(opId)
	c.ops.Delete(opId)
}
