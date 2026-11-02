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
	"strings"
	"sync"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type shardKey struct {
	class string
	shard string
}

// NodeOverlay is node-local shard state, never snapshotted. Reads are on the write path: no locks, no allocs.
type NodeOverlay struct {
	readOnly sync.Map // shardKey to struct{}
}

func (o *NodeOverlay) ReadOnly(class, shard string) bool {
	if o == nil {
		return false
	}
	_, ok := o.readOnly.Load(shardKey{class, shard})
	return ok
}

func (o *NodeOverlay) SetReadOnly(class, shard string, readOnly bool) {
	if readOnly {
		o.readOnly.Store(shardKey{class, shard}, struct{}{})
	} else {
		o.readOnly.Delete(shardKey{class, shard})
	}
}

func (o *NodeOverlay) clearShards(class string, shards []string) {
	for _, shard := range shards {
		o.readOnly.Delete(shardKey{class, shard})
	}
}

func (o *NodeOverlay) clearClass(class string) {
	o.readOnly.Range(func(key, _ any) bool {
		if key.(shardKey).class == class {
			o.readOnly.Delete(key)
		}
		return true
	})
}

// reset drops every user read-only flag, as a restart does.
func (o *NodeOverlay) reset() {
	o.readOnly.Range(func(key, _ any) bool {
		o.readOnly.Delete(key)
		return true
	})
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
