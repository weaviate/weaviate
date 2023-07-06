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

package schema

import (
	"fmt"
	"sync"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// State is a cached copy of the schema that can also be saved into a remote
// storage, as specified by Repo
type State struct {
	ObjectSchema  *models.Schema `json:"object"`
	ShardingState map[string]*sharding.State
}

// NewState returns a new state with room for nClasses classes
func NewState(nClasses int) State {
	return State{
		ObjectSchema: &models.Schema{
			Classes: make([]*models.Class, 0, nClasses),
		},
		ShardingState: make(map[string]*sharding.State, nClasses),
	}
}

type schemaCache struct {
	sync.RWMutex
	State
}

// ShardOwner returns the node owner of the specified shard
func (s *schemaCache) ShardOwner(class, shard string) (string, error) {
	s.RLock()
	defer s.RUnlock()
	cls := s.ShardingState[class]
	if cls == nil {
		return "", fmt.Errorf("class not found")
	}
	x, ok := cls.Physical[shard]
	if !ok {
		return "", fmt.Errorf("shard not found")
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", fmt.Errorf("owner node not found")
	}
	return x.BelongsToNodes[0], nil
}

// ShardOwner returns the node owner of the specified shard
func (s *schemaCache) ShardReplicas(class, shard string) ([]string, error) {
	s.RLock()
	defer s.RUnlock()
	cls := s.ShardingState[class]
	if cls == nil {
		return nil, fmt.Errorf("class not found")
	}
	x, ok := cls.Physical[shard]
	if !ok {
		return nil, fmt.Errorf("shard not found")
	}
	return x.BelongsToNodes, nil
}

// TenantShard returns shard name for the provided tenant
func (s *schemaCache) TenantShard(class, tenant string) string {
	s.RLock()
	defer s.RUnlock()
	ss := s.ShardingState[class]
	if ss == nil {
		return ""
	}
	return ss.Shard(tenant, "")
}

// ShardFromUUID returns shard name of the provided uuid
func (s *schemaCache) ShardFromUUID(class string, uuid []byte) string {
	s.RLock()
	defer s.RUnlock()
	ss := s.ShardingState[class]
	if ss == nil {
		return ""
	}
	return ss.PhysicalShard(uuid)
}

func (s *schemaCache) CopyShardingState(className string) *sharding.State {
	s.RLock()
	defer s.RUnlock()
	pst := s.ShardingState[className]
	if pst != nil {
		st := pst.DeepCopy()
		pst = &st
	}

	return pst
}

// LockGuard provides convenient mechanism for owning mutex by function which mutates the state
func (s *schemaCache) LockGuard(mutate func()) {
	s.Lock()
	defer s.Unlock()
	mutate()
}

// RLockGuard provides convenient mechanism for owning mutex function which doesn't mutates the state
func (s *schemaCache) RLockGuard(reader func() error) error {
	s.RLock()
	defer s.RUnlock()
	return reader()
}

func (s *schemaCache) isEmpty() bool {
	s.RLock()
	defer s.RUnlock()
	return s.State.ObjectSchema == nil || len(s.State.ObjectSchema.Classes) == 0
}

func (s *schemaCache) setState(st State) {
	s.Lock()
	defer s.Unlock()
	s.State = st
}
