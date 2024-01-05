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

package schema

import (
	"errors"
	"fmt"
	"sync"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	errPropertyNotFound = errors.New("property not found")
	errClassNotFound    = errors.New("class not found")
	errShardNotFound    = errors.New("shard not found")
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
		return "", errClassNotFound
	}
	x, ok := cls.Physical[shard]
	if !ok {
		return "", errShardNotFound
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", fmt.Errorf("owner node not found")
	}
	return x.BelongsToNodes[0], nil
}

// ShardReplicas returns the nodes owning shard in class
func (s *schemaCache) ShardReplicas(class, shard string) ([]string, error) {
	s.RLock()
	defer s.RUnlock()
	cls := s.ShardingState[class]
	if cls == nil {
		return nil, errClassNotFound
	}
	x, ok := cls.Physical[shard]
	if !ok {
		return nil, errShardNotFound
	}
	return x.BelongsToNodes, nil
}

// TenantShard returns shard name for the provided tenant and its activity status
func (s *schemaCache) TenantShard(class, tenant string) (string, string) {
	s.RLock()
	defer s.RUnlock()
	ss := s.ShardingState[class]
	if ss == nil || !ss.PartitioningEnabled {
		return "", ""
	}

	if physical, ok := ss.Physical[tenant]; ok {
		return tenant, physical.ActivityStatus()
	}
	return "", ""
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

func (s *schemaCache) detachClass(name string) bool {
	s.Lock()
	defer s.Unlock()
	schema, ci := s.ObjectSchema, -1
	for i, cls := range schema.Classes {
		if cls.Class == name {
			ci = i
			break
		}
	}
	if ci == -1 {
		return false
	}

	nc := len(schema.Classes)
	schema.Classes[ci] = schema.Classes[nc-1]
	schema.Classes[nc-1] = nil // to prevent leaking this pointer.
	schema.Classes = schema.Classes[:nc-1]
	return true
}

func (s *schemaCache) deleteClassState(name string) {
	s.Lock()
	defer s.Unlock()
	delete(s.ShardingState, name)
}

func (s *schemaCache) unsafeFindClassIf(pred func(*models.Class) bool) *models.Class {
	for _, cls := range s.ObjectSchema.Classes {
		if pred(cls) {
			return cls
		}
	}
	return nil
}

func (s *schemaCache) unsafeFindClass(className string) *models.Class {
	for _, c := range s.ObjectSchema.Classes {
		if c.Class == className {
			return c
		}
	}
	return nil
}

func (s *schemaCache) addClass(c *models.Class, ss *sharding.State) {
	s.Lock()
	defer s.Unlock()

	s.ShardingState[c.Class] = ss
	s.ObjectSchema.Classes = append(s.ObjectSchema.Classes, c)
}

func (s *schemaCache) updateClass(u *models.Class, ss *sharding.State) error {
	s.Lock()
	defer s.Unlock()

	if c := s.unsafeFindClass(u.Class); c != nil {
		*c = *u
	} else {
		return errClassNotFound
	}
	if ss != nil {
		s.ShardingState[u.Class] = ss
	}
	return nil
}

func (s *schemaCache) addProperty(class string, p *models.Property) (models.Class, error) {
	s.Lock()
	defer s.Unlock()

	c := s.unsafeFindClass(class)
	if c == nil {
		return models.Class{}, errClassNotFound
	}

	// update all at once to prevent race condition with concurrent readers
	src := c.Properties
	dest := make([]*models.Property, len(src)+1)
	copy(dest, src)
	dest[len(src)] = p
	c.Properties = dest
	return *c, nil
}

func (s *schemaCache) mergeObjectProperty(class string, p *models.Property) (models.Class, error) {
	s.Lock()
	defer s.Unlock()

	c := s.unsafeFindClass(class)
	if c == nil {
		return models.Class{}, errClassNotFound
	}

	var prop *models.Property
	for i := range c.Properties {
		if c.Properties[i].Name == p.Name {
			prop = c.Properties[i]
			break
		}
	}
	if prop == nil {
		return models.Class{}, errPropertyNotFound
	}

	prop.NestedProperties, _ = schema.MergeRecursivelyNestedProperties(prop.NestedProperties, p.NestedProperties)
	return *c, nil
}

// readOnlySchema returns a read only schema
// Changing the schema outside this package might lead to undefined behavior.
func (s *schemaCache) readOnlySchema() *models.Schema {
	s.RLock()
	defer s.RUnlock()
	return shallowCopySchema(s.ObjectSchema)
}

func (s *schemaCache) readOnlyClass(name string) (*models.Class, error) {
	s.RLock()
	defer s.RUnlock()
	c := s.unsafeFindClass(name)
	if c == nil {
		return nil, errClassNotFound
	}
	cp := *c
	return &cp, nil
}

func (s *schemaCache) classExist(name string) bool {
	s.RLock()
	defer s.RUnlock()
	return s.unsafeFindClass(name) != nil
}

// ShallowCopySchema creates a shallow copy of existing classes
//
// This function assumes that class attributes are being overwritten.
// The properties attribute is the only one that might vary in size;
// therefore, we perform a shallow copy of the existing properties.
// This implementation assumes that individual properties are overwritten rather than partially updated
func shallowCopySchema(m *models.Schema) *models.Schema {
	cp := *m
	cp.Classes = make([]*models.Class, len(m.Classes))
	for i := 0; i < len(m.Classes); i++ {
		c := *m.Classes[i]
		cp.Classes[i] = &c
	}
	return &cp
}
