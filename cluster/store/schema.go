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
	errClassNotFound       = errors.New("class not found")
	errClassExists         = errors.New("class already exists")
	errShardNotFound       = errors.New("shard not found")
	errSchemaVersionTooLow = errors.New("minimum requested schema version could not be reached")
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

type schema struct {
	nodeID      string
	shardReader shardReader
	sync.RWMutex
	Classes map[string]*metaClass
}

func (s *schema) ClassInfo(class string, version uint64) (ClassInfo, uint64) {
	s.RLock()
	defer s.RUnlock()
	cl, ok := s.Classes[class]
	if !ok {
		return ClassInfo{}, 0
	}
	return cl.ClassInfo(), cl.ClassVersion
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
func (s *schema) Read(class string, reader func(*models.Class, *sharding.State) error, requestedVersion uint64) error {
	meta := s.metaClass(class)
	if meta == nil {
		return errClassNotFound
	}

	if requestedVersion != 0 && meta.ClassVersion < requestedVersion {
		return errSchemaVersionTooLow
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
func (s *schema) ShardOwner(class, shard string) (string, error, uint64) {
	meta := s.metaClass(class)
	if meta == nil {
		return "", errClassNotFound, 0
	}

	res, err := meta.ShardOwner(shard)
	return res, err, meta.ShardVersion
}

// ShardFromUUID returns shard name of the provided uuid
func (s *schema) ShardFromUUID(class string, uuid []byte) (string, uint64) {
	meta := s.metaClass(class)
	if meta == nil {
		return "", 0
	}
	return meta.ShardFromUUID(uuid), meta.ShardVersion
}

// ShardReplicas returns the replica nodes of a shard
func (s *schema) ShardReplicas(class, shard string) ([]string, error, uint64) {
	meta := s.metaClass(class)
	if meta == nil {
		return nil, errClassNotFound, 0
	}
	res, err := meta.ShardReplicas(shard)
	return res, err, meta.ShardVersion
}

// TenantShard returns shard name for the provided tenant and its activity status
func (s *schema) TenantShard(class, tenant string) (string, string, uint64) {
	s.RLock()
	defer s.RUnlock()

	meta := s.Classes[class]
	if meta == nil {
		return "", "", 0
	}

	tenant, status := meta.TenantShard(tenant)
	return tenant, status, meta.ShardVersion
}

func (s *schema) CopyShardingState(class string) (*sharding.State, uint64) {
	meta := s.metaClass(class)
	if meta == nil {
		return nil, 0
	}

	return meta.CopyShardingState(), meta.ShardVersion
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

func (s *schema) getTenants(class string, after *string, limit *int64) ([]*models.Tenant, error) {
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
		if limit != nil {
			res = make([]*models.Tenant, *limit)
		} else {
			res = make([]*models.Tenant, len(ss.Physical))
		}
		foundAfter := after == nil
		i := 0
		for tenant := range ss.Physical {
			if !foundAfter {
				if tenant == *after || after == nil {
					foundAfter = true
				}
				continue
			}
			if i >= len(res) {
				break
			}
			res[i] = &models.Tenant{
				Name:           tenant,
				ActivityStatus: entSchema.ActivityStatus(ss.Physical[tenant].Status),
			}
			i++
		}
		res = res[:i] // Trim the slice to the actual number of tenants added
		return nil
	}
	return res, meta.RLockGuard(f)
}

func (s *schema) clear() {
	s.Lock()
	defer s.Unlock()
	for k := range s.Classes {
		delete(s.Classes, k)
	}
}
