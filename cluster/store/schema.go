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

package store

import (
	"errors"
	"strings"
	"sync"

	command "github.com/weaviate/weaviate/cluster/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	errClassNotFound = errors.New("class not found")
	errClassExists   = errors.New("class already exits")
	errShardNotFound = errors.New("shard not found")
)

type ClassInfo struct {
	Exists            bool
	MultiTenancy      models.MultiTenancyConfig
	ReplicationFactor int
	Tenants           int
	Properties        int
}

type schema struct {
	nodeID      string
	shardReader shardReader
	sync.RWMutex
	Classes map[string]*metaClass
}

func (s *schema) ClassInfo(class string) (ci ClassInfo) {
	s.RLock()
	defer s.RUnlock()

	return s.Classes[class].ClassInfo()
}

// ClassEqual returns the name of an existing class with a similar name, and "" otherwise
// strings.EqualFold is used to compare classes
func (s *schema) ClassEqual(name string) string {
	s.RLock()
	defer s.RUnlock()
	for k := range s.Classes {
		if strings.EqualFold(k, name) {
			return k
		}
	}
	return ""
}

func (s *schema) MultiTenancy(class string) models.MultiTenancyConfig {
	return s.metaClass(class).MultiTenancyConfig()
}

// Read performs a read operation `reader` on the specified class and sharding state
func (s *schema) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	meta := s.metaClass(class)
	if meta == nil {
		return errClassNotFound
	}

	return meta.RLockGuard(reader)
}

func (s *schema) metaClass(class string) *metaClass {
	s.RLock()
	defer s.RUnlock()
	return s.Classes[class]
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (s *schema) ReadOnlyClass(class string) *models.Class {
	s.RLock()
	defer s.RUnlock()
	meta := s.Classes[class]
	if meta == nil {
		return nil
	}
	return meta.CloneClass()
}

// ReadOnlySchema returns a read only schema
// Changing the schema outside this package might lead to undefined behavior.
//
// it creates a shallow copy of existing classes
//
// This function assumes that class attributes are being overwritten.
// The properties attribute is the only one that might vary in size;
// therefore, we perform a shallow copy of the existing properties.
// This implementation assumes that individual properties are overwritten rather than partially updated
func (s *schema) ReadOnlySchema() models.Schema {
	cp := models.Schema{}
	s.RLock()
	defer s.RUnlock()
	cp.Classes = make([]*models.Class, len(s.Classes))
	i := 0
	for _, meta := range s.Classes {
		cp.Classes[i] = meta.CloneClass()
		i++
	}

	return cp
}

// ShardOwner returns the node owner of the specified shard
func (s *schema) ShardOwner(class, shard string) (string, error) {
	meta := s.metaClass(class)
	if meta == nil {
		return "", errClassNotFound
	}

	return meta.ShardOwner(shard)
}

// ShardFromUUID returns shard name of the provided uuid
func (s *schema) ShardFromUUID(class string, uuid []byte) string {
	meta := s.metaClass(class)
	if meta == nil {
		return ""
	}
	return meta.ShardFromUUID(uuid)
}

// ShardReplicas returns the replica nodes of a shard
func (s *schema) ShardReplicas(class, shard string) ([]string, error) {
	meta := s.metaClass(class)
	if meta == nil {
		return nil, errClassNotFound
	}
	return meta.ShardReplicas(shard)
}

// TenantShard returns shard name for the provided tenant and its activity status
func (s *schema) TenantShard(class, tenant string) (string, string) {
	s.RLock()
	defer s.RUnlock()

	meta := s.Classes[class]
	if meta == nil {
		return "", ""
	}

	return meta.TenantShard(tenant)
}

func (s *schema) CopyShardingState(class string) *sharding.State {
	meta := s.metaClass(class)
	if meta == nil {
		return nil
	}

	return meta.CopyShardingState()
}

func (s *schema) GetShardsStatus(class string) (models.ShardStatusList, error) {
	return s.shardReader.GetShardsStatus(class)
}

type shardReader interface {
	GetShardsStatus(class string) (models.ShardStatusList, error)
}

func NewSchema(nodeID string, shardReader shardReader) *schema {
	return &schema{
		nodeID:      nodeID,
		Classes:     make(map[string]*metaClass, 128),
		shardReader: shardReader,
	}
}

func (s *schema) len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.Classes)
}

func (s *schema) addClass(cls *models.Class, ss *sharding.State) error {
	s.Lock()
	defer s.Unlock()
	_, exists := s.Classes[cls.Class]
	if exists {
		return errClassExists
	}

	s.Classes[cls.Class] = &metaClass{Class: *cls, Sharding: *ss}
	return nil
}

// updateClass modifies existing class based on the givin update function
func (s *schema) updateClass(name string, f func(cls *models.Class, ss *sharding.State) error) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[name]
	if meta == nil {
		return errClassNotFound
	}
	return meta.LockGuard(f)
}

func (s *schema) deleteClass(name string) {
	s.Lock()
	defer s.Unlock()
	delete(s.Classes, name)
}

func (s *schema) addProperty(class string, props ...*models.Property) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return errClassNotFound
	}
	return meta.AddProperty(props...)
}

func (s *schema) addTenants(class string, req *command.AddTenantsRequest) error {
	req.Tenants = removeNilTenants(req.Tenants)
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return errClassNotFound
	}

	return meta.AddTenants(s.nodeID, req)
}

func (s *schema) deleteTenants(class string, req *command.DeleteTenantsRequest) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return errClassNotFound
	}

	return meta.DeleteTenants(req)
}

func (s *schema) updateTenants(class string, req *command.UpdateTenantsRequest) (n int, err error) {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return 0, errClassNotFound
	}

	return meta.UpdateTenants(s.nodeID, req)
}

func (s *schema) clear() {
	s.Lock()
	defer s.Unlock()
	for k := range s.Classes {
		delete(s.Classes, k)
	}
}
