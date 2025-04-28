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
