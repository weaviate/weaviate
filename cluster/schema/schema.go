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
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	entSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	ErrClassExists   = errors.New("class already exists")
	ErrClassNotFound = errors.New("class not found")
	ErrShardNotFound = errors.New("shard not found")
	ErrMTDisabled    = errors.New("multi-tenancy is not enabled")
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

	// metrics
	// collectionsCount represents the number of collections on this specific node.
	collectionsCount *prometheus.GaugeVec

	// shardsCount represents the number of shards (of all collections) on this specific node.
	shardsCount *prometheus.GaugeVec
}

func NewSchema(nodeID string, shardReader shardReader, reg prometheus.Registerer) *schema {
	// this also registers the prometheus metrics with given `reg` in addition to just creating it.
	r := promauto.With(reg)

	s := &schema{
		nodeID:      nodeID,
		Classes:     make(map[string]*metaClass, 128),
		shardReader: shardReader,
		collectionsCount: r.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "schema_collections",
			Help:      "Number of collections per node",
		}, []string{"nodeID"}),
		shardsCount: r.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "schema_shards",
			Help:      "Number of shards per node with corresponding status",
		}, []string{"nodeID", "status"}), // status: HOT, WARM, COLD, FROZEN
		// TODO: Can we have `ConstLabels` of `nodeID`?
	}

	return s
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
		return ErrClassNotFound
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

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (s *schema) ReadOnlyClasses(classes ...string) map[string]versioned.Class {
	if len(classes) == 0 {
		return nil
	}

	vclasses := make(map[string]versioned.Class, len(classes))
	s.RLock()
	defer s.RUnlock()

	for _, class := range classes {
		meta := s.Classes[class]
		if meta == nil {
			continue
		}
		vclasses[class] = versioned.Class{Class: meta.CloneClass(), Version: meta.ClassVersion}
	}

	return vclasses
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
		return "", 0, ErrClassNotFound
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
		return nil, 0, ErrClassNotFound
	}
	return meta.ShardReplicas(shard)
}

// TenantsShards returns shard name for the provided tenant and its activity status
func (s *schema) TenantsShards(class string, tenants ...string) (map[string]string, uint64) {
	s.RLock()
	defer s.RUnlock()

	meta := s.Classes[class]
	if meta == nil {
		return nil, 0
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

func (s *schema) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	return s.shardReader.GetShardsStatus(class, tenant)
}

type shardReader interface {
	GetShardsStatus(class, tenant string) (models.ShardStatusList, error)
}

func (s *schema) len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.Classes)
}

func (s *schema) multiTenancyEnabled(class string) (bool, *metaClass, ClassInfo, error) {
	s.RLock()
	defer s.RUnlock()
	meta := s.Classes[class]
	info := s.Classes[class].ClassInfo()
	if meta == nil {
		return false, nil, ClassInfo{}, ErrClassNotFound
	}
	if !info.MultiTenancy.Enabled {
		return false, nil, ClassInfo{}, fmt.Errorf("%w for class %q", ErrMTDisabled, class)
	}
	return true, meta, info, nil
}

func (s *schema) addClass(cls *models.Class, ss *sharding.State, v uint64) error {
	s.Lock()
	defer s.Unlock()
	_, exists := s.Classes[cls.Class]
	if exists {
		return ErrClassExists
	}

	s.Classes[cls.Class] = &metaClass{
		Class: *cls, Sharding: *ss, ClassVersion: v, ShardVersion: v,
	}

	s.collectionsCount.WithLabelValues(s.nodeID).Inc()
	return nil
}

// updateClass modifies existing class based on the givin update function
func (s *schema) updateClass(name string, f func(*metaClass) error) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[name]
	if meta == nil {
		return ErrClassNotFound
	}
	return meta.LockGuard(f)
}

// replaceStatesNodeName it update the node name inside sharding states.
// WARNING: this shall be used in one node cluster environments only.
// because it will replace the shard node name if the node name got updated
// only if the replication factor is 1, otherwise it's no-op
func (s *schema) replaceStatesNodeName(new string) {
	s.Lock()
	defer s.Unlock()

	for _, meta := range s.Classes {
		meta.LockGuard(func(mc *metaClass) error {
			if meta.Class.ReplicationConfig.Factor > 1 {
				return nil
			}

			for idx := range meta.Sharding.Physical {
				cp := meta.Sharding.Physical[idx].DeepCopy()
				cp.BelongsToNodes = []string{new}
				meta.Sharding.Physical[idx] = cp
			}
			return nil
		})
	}
}

func (s *schema) deleteClass(name string) {
	s.Lock()
	defer s.Unlock()

	// since `delete(map, key)` is no-op if `key` doesn't exist, check before deleting
	// so that we can increment the `collectionsCount` correctly.
	if _, ok := s.Classes[name]; !ok {
		return
	}

	delete(s.Classes, name)
	s.collectionsCount.WithLabelValues(s.nodeID).Dec()
}

func (s *schema) addProperty(class string, v uint64, props ...*models.Property) error {
	s.Lock()
	defer s.Unlock()

	meta := s.Classes[class]
	if meta == nil {
		return ErrClassNotFound
	}
	return meta.AddProperty(v, props...)
}

func (s *schema) addTenants(class string, v uint64, req *command.AddTenantsRequest) error {
	req.Tenants = removeNilTenants(req.Tenants)

	ok, meta, info, err := s.multiTenancyEnabled(class)
	if !ok {
		return err
	}

	sc, err := meta.AddTenants(s.nodeID, req, int64(info.ReplicationFactor), v)
	if err != nil {
		return err
	}
	for status, count := range sc {
		s.shardsCount.WithLabelValues(s.nodeID, status).Add(float64(count))
	}

	return nil
}

func (s *schema) deleteTenants(class string, v uint64, req *command.DeleteTenantsRequest) error {
	ok, meta, _, err := s.multiTenancyEnabled(class)
	if !ok {
		return err
	}
	sc, err := meta.DeleteTenants(req, v)
	if err != nil {
		return err
	}

	for status, count := range sc {
		s.shardsCount.WithLabelValues(s.nodeID, status).Sub(float64(count))
	}

	return nil
}

func (s *schema) updateTenants(class string, v uint64, req *command.UpdateTenantsRequest) error {
	ok, meta, _, err := s.multiTenancyEnabled(class)
	if !ok {
		return err
	}
	// TODO: What happens if it returns error because of missingShards, but still some shards are updated in `s.Sharding.Physical`?
	// Basically. Is partial error possible?
	sc, err := meta.UpdateTenants(s.nodeID, req, v)
	for status, count := range sc {
		// count can be positive or negative.
		s.shardsCount.WithLabelValues(s.nodeID, status).Add(float64(count))
	}

	if err != nil {
		return err
	}

	return nil
}

func (s *schema) getTenants(class string, tenants []string) ([]*models.Tenant, error) {
	ok, meta, _, err := s.multiTenancyEnabled(class)
	if !ok {
		return nil, err
	}

	// Read tenants using the meta lock guard
	var res []*models.Tenant
	f := func(_ *models.Class, ss *sharding.State) error {
		if len(tenants) == 0 {
			res = make([]*models.Tenant, len(ss.Physical))
			i := 0
			for tenant := range ss.Physical {
				res[i] = makeTenant(tenant, entSchema.ActivityStatus(ss.Physical[tenant].Status))
				i++
			}
		} else {
			res = make([]*models.Tenant, 0, len(tenants))
			for _, tenant := range tenants {
				if status, ok := ss.Physical[tenant]; ok {
					res = append(res, makeTenant(tenant, entSchema.ActivityStatus(status.Status)))
				}
			}
		}
		return nil
	}
	return res, meta.RLockGuard(f)
}

func (s *schema) States() map[string]types.ClassState {
	s.RLock()
	defer s.RUnlock()

	cs := make(map[string]types.ClassState, len(s.Classes))
	for _, c := range s.Classes {
		cs[c.Class.Class] = types.ClassState{
			Class:  c.Class,
			Shards: c.Sharding,
		}
	}

	return cs
}

func (s *schema) MetaClasses() map[string]*metaClass {
	s.RLock()
	defer s.RUnlock()

	return s.Classes
}

func makeTenant(name, status string) *models.Tenant {
	return &models.Tenant{
		Name:           name,
		ActivityStatus: status,
	}
}
