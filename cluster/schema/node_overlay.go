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

package schema

import (
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type shardKey struct {
	class string
	shard string
}

// NodeOverlay is this node's shard state outside RAFT: in memory, never
// snapshotted. Reads sit on the write path, so they take no lock and allocate
// nothing. Few shards are flagged, so writes copy the set.
type NodeOverlay struct {
	mu       sync.Mutex
	readOnly atomic.Pointer[map[shardKey]struct{}]
}

func (o *NodeOverlay) ReadOnly(class, shard string) bool {
	if o == nil {
		return false
	}
	set := o.readOnly.Load()
	if set == nil {
		return false
	}
	_, ok := (*set)[shardKey{class, shard}]
	return ok
}

func (o *NodeOverlay) SetReadOnly(class, shard string, readOnly bool) {
	o.update(func(set map[shardKey]struct{}) {
		if readOnly {
			set[shardKey{class, shard}] = struct{}{}
		} else {
			delete(set, shardKey{class, shard})
		}
	})
}

func (o *NodeOverlay) clearShards(class string, shards []string) {
	o.update(func(set map[shardKey]struct{}) {
		for _, shard := range shards {
			delete(set, shardKey{class, shard})
		}
	})
}

func (o *NodeOverlay) clearClass(class string) {
	o.update(func(set map[shardKey]struct{}) {
		maps.DeleteFunc(set, func(key shardKey, _ struct{}) bool { return key.class == class })
	})
}

// reset drops every user read-only flag, as a restart does.
func (o *NodeOverlay) reset() {
	o.update(func(set map[shardKey]struct{}) { clear(set) })
}

func (o *NodeOverlay) update(change func(map[shardKey]struct{})) {
	o.mu.Lock()
	defer o.mu.Unlock()

	next := make(map[shardKey]struct{})
	if set := o.readOnly.Load(); set != nil {
		maps.Copy(next, *set)
	}
	change(next)
	o.readOnly.Store(&next)
}

func (s *SchemaManager) NodeOverlay() *NodeOverlay {
	return s.overlay
}

// applyShardStatus runs on every node, owner or not.
func (s *SchemaManager) applyShardStatus(class, shard, status string) error {
	target, err := storagestate.ValidateStatus(strings.ToUpper(status))
	if err != nil {
		return fmt.Errorf("%s: %w", status, err)
	}
	switch target {
	case storagestate.StatusReadOnly:
		s.overlay.SetReadOnly(class, shard, true)
	case storagestate.StatusReady:
		s.overlay.SetReadOnly(class, shard, false)
	default:
	}
	return nil
}

func (s *schema) requireShard(class, shard string) error {
	s.mu.RLock()
	meta := s.classes[class]
	s.mu.RUnlock()
	if meta == nil {
		return fmt.Errorf("%w: %s", ErrClassNotFound, class)
	}
	return meta.RLockGuard(func(_ *models.Class, ss *sharding.State) error {
		if _, ok := ss.Physical[shard]; !ok {
			return fmt.Errorf("%w: %s", ErrShardNotFound, shard)
		}
		return nil
	})
}
