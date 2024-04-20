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
	"fmt"
	"strings"
	"sync"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	entSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	errClassNotFound = errors.New("class not found")
	errClassExists   = errors.New("class already exists")
	errShardNotFound = errors.New("shard not found")
)

type ClassInfo struct {
	Exists            bool
	MultiTenancy      models.MultiTenancyConfig
	ReplicationFactor int
	Tenants           int
	Properties        int
	ClassVersion      uint64
	ShardVersion      uint64
}

func (ci *ClassInfo) Version() uint64 {
	return max(ci.ClassVersion, ci.ShardVersion)
}

type schema struct {
	nodeID      string
	shardReader shardReader
	sync.RWMutex
	Classes map[string]*metaClass
}

func (s *schema) ClassInfo(class string) ClassInfo {
	s.RLock()
	defer s.RUnlock()
	cl, ok := s.Classes[class]
	if !ok {
		return ClassInfo{}
	}
	return cl.ClassInfo()
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
	mtc, _ := s.metaClass(class).MultiTenancyConfig()
	return mtc
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
func (s *schema) ReadOnlyClass(class string) (*models.Class, uint64) {
	s.RLock()
	defer s.RUnlock()
	meta := s.Classes[class]
	if meta == nil {
		return nil, 0
	}
	return meta.CloneClass(), meta.ClassVersion
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
func (s *schema) ShardOwner(class, shard string) (string, uint64, error) {
	meta := s.metaClass(class)
	if meta == nil {
		return "", 0, errClassNotFound
	}

	return meta.ShardOwner(shard)
}

// ShardFromUUID returns shard name of the provided uuid
func (s *schema) ShardFromUUID(class string, uuid []byte) (string, uint64) {
	meta := s.metaClass(class)
	if meta == nil {
		return "", 0
	}
	return meta.ShardFromUUID(uuid)
}

// ShardReplicas returns the replica nodes of a shard
func (s *schema) ShardReplicas(class, shard string) ([]string, uint64, error) {
	meta := s.metaClass(class)
	if meta == nil {
		return nil, 0, errClassNotFound
	}
	return meta.ShardReplicas(shard)
}

// TenantsShards returns shard name for the provided tenant and its activity status
func (s *schema) TenantsShards(class string, tenants ...string) map[string]string {
	s.RLock()
	defer s.RUnlock()

	meta := s.Classes[class]
	if meta == nil {
		return nil
	}

	return meta.TenantsShards(class, tenants...)
}

func (s *schema) CopyShardingState(class string) (*sharding.State, uint64) {
	meta := s.metaClass(class)
	if meta == nil {
		return nil, 0
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

func (s *schema) addClass(cls *models.Class, ss *sharding.State, v uint64) error {
	s.Lock()
	defer s.Unlock()
	_, exists := s.Classes[cls.Class]
	if exists {
		return errClassExists
	}

	s.Classes[cls.Class] = &metaClass{Class: *cls, Sharding: *ss, ClassVersion: v, ShardVersion: v}
	return nil
}

// updateClass modifies existing class based on the givin update function
func (s *schema) updateClass(name string, f func(*metaClass) error) error {
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

func (s *schema) addProperty(class string, v uint64, props ...*models.Property) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return errClassNotFound
	}
	return meta.AddProperty(v, props...)
}

func (s *schema) addTenants(class string, v uint64, req *command.AddTenantsRequest) error {
	req.Tenants = removeNilTenants(req.Tenants)
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return errClassNotFound
	}

	return meta.AddTenants(s.nodeID, req, v)
}

func (s *schema) deleteTenants(class string, v uint64, req *command.DeleteTenantsRequest) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return errClassNotFound
	}

	return meta.DeleteTenants(req, v)
}

func (s *schema) updateTenants(class string, v uint64, req *command.UpdateTenantsRequest) (n int, err error) {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return 0, errClassNotFound
	}

	return meta.UpdateTenants(s.nodeID, req, v)
}

func (s *schema) getTenants(class string) ([]*models.Tenant, error) {
	s.RLock()
	// To avoid races between checking that multi tenancy is enabled and reading from the class itself,
	// ensure we fetch both using the same lock.
	// We will then read the tenants from the class using the more specific meta lock
	info := s.Classes[class].ClassInfo()
	meta := s.Classes[class]
	s.RUnlock()

	// Check that the class exists, and that multi tenancy is enabled
	if meta == nil {
		return []*models.Tenant{}, errClassNotFound
	}
	if !info.MultiTenancy.Enabled {
		return []*models.Tenant{}, fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}

	// Read tenants using the meta lock guard
	var res []*models.Tenant
	f := func(_ *models.Class, ss *sharding.State) error {
		res = make([]*models.Tenant, len(ss.Physical))
		i := 0
		for tenant := range ss.Physical {
			res[i] = &models.Tenant{
				Name:           tenant,
				ActivityStatus: entSchema.ActivityStatus(ss.Physical[tenant].Status),
			}
			i++
		}
		return nil
	}
	return res, meta.RLockGuard(f)
}

func (s *schema) States() map[string]ClassState {
	s.RLock()
	defer s.RUnlock()

	cs := make(map[string]ClassState, len(s.Classes))
	for _, c := range s.Classes {
		cs[c.Class.Class] = ClassState{
			Class:  c.Class,
			Shards: c.Sharding,
		}
	}

	return cs
}

func (s *schema) clear() {
	s.Lock()
	defer s.Unlock()
	for k := range s.Classes {
		delete(s.Classes, k)
	}
}
