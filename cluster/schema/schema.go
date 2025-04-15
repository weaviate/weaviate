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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
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

	// mu protects the `classes`
	mu      sync.RWMutex
	classes map[string]*metaClass

	// metrics
	// collectionsCount represents the number of collections on this specific node.
	collectionsCount prometheus.Gauge

	// shardsCount represents the number of shards (of all collections) on this specific node.
	shardsCount *prometheus.GaugeVec
}

func NewSchema(nodeID string, shardReader shardReader, reg prometheus.Registerer) *schema {
	// this also registers the prometheus metrics with given `reg` in addition to just creating it.
	r := promauto.With(reg)

	s := &schema{
		nodeID:      nodeID,
		classes:     make(map[string]*metaClass, 128),
		shardReader: shardReader,
		collectionsCount: r.NewGauge(prometheus.GaugeOpts{
			Namespace:   "weaviate",
			Name:        "schema_collections",
			Help:        "Number of collections per node",
			ConstLabels: prometheus.Labels{"nodeID": nodeID},
		}),
		shardsCount: r.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "weaviate",
			Name:        "schema_shards",
			Help:        "Number of shards per node with corresponding status",
			ConstLabels: prometheus.Labels{"nodeID": nodeID},
		}, []string{"status"}), // status: HOT, WARM, COLD, FROZEN
	}

	return s
}

func (s *schema) ClassInfo(class string) ClassInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cl, ok := s.classes[class]
	if !ok {
		return ClassInfo{}
	}
	return cl.ClassInfo()
}

// ClassEqual returns the name of an existing class with a similar name, and "" otherwise
// strings.EqualFold is used to compare classes
func (s *schema) ClassEqual(name string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k := range s.classes {
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.classes[class]
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (s *schema) ReadOnlyClass(class string) (*models.Class, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta := s.classes[class]
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, class := range classes {
		meta := s.classes[class]
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	cp.Classes = make([]*models.Class, len(s.classes))
	i := 0
	for _, meta := range s.classes {
		cp.Classes[i] = meta.CloneClass()
		i++
	}

	return cp
}

func (s *schema) CollectionsCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.classes)
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta := s.classes[class]
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.classes)
}

func (s *schema) multiTenancyEnabled(class string) (bool, *metaClass, ClassInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta := s.classes[class]
	if meta == nil {
		return false, nil, ClassInfo{}, ErrClassNotFound
	}
	info := s.classes[class].ClassInfo()
	if !info.MultiTenancy.Enabled {
		return false, nil, ClassInfo{}, fmt.Errorf("%w for class %q", ErrMTDisabled, class)
	}
	return true, meta, info, nil
}

func (s *schema) addClass(cls *models.Class, ss *sharding.State, v uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.classes[cls.Class]
	if exists {
		return ErrClassExists
	}

	s.classes[cls.Class] = &metaClass{
		Class: *cls, Sharding: *ss, ClassVersion: v, ShardVersion: v,
	}

	s.collectionsCount.Inc()

	for _, shard := range ss.Physical {
		s.shardsCount.WithLabelValues(shard.Status).Inc()
	}

	return nil
}

// updateClass modifies existing class based on the givin update function
func (s *schema) updateClass(name string, f func(*metaClass) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta := s.classes[name]
	if meta == nil {
		return ErrClassNotFound
	}
	return meta.LockGuard(f)
}

func (s *schema) deleteClass(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// since `delete(map, key)` is no-op if `key` doesn't exist, check before deleting
	// so that we can increment the `collectionsCount` correctly.
	class, ok := s.classes[name]
	if !ok {
		return false
	}

	// sc tracks number of shards in this collection to be deleted by status.
	sc := make(map[string]int)

	// need to decrement shards count on this class.
	for _, shard := range class.Sharding.Physical {
		sc[shard.Status]++
	}

	delete(s.classes, name)

	s.collectionsCount.Dec()
	for status, count := range sc {
		s.shardsCount.WithLabelValues(status).Sub(float64(count))
	}

	return true
}

// replaceClasses replaces the existing `schema.Classes` with given `classes`
// mainly used in cases like restoring the whole schema from backup or something.
func (s *schema) replaceClasses(classes map[string]*metaClass) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.collectionsCount.Sub(float64(len(s.classes)))
	for _, ss := range s.classes {
		for _, shard := range ss.Sharding.Physical {
			s.shardsCount.WithLabelValues(shard.Status).Dec()
		}
	}

	s.classes = classes

	s.collectionsCount.Add(float64(len(s.classes)))

	for _, ss := range s.classes {
		for _, shard := range ss.Sharding.Physical {
			s.shardsCount.WithLabelValues(shard.Status).Inc()
		}
	}
}

// replaceStatesNodeName it update the node name inside sharding states.
// WARNING: this shall be used in one node cluster environments only.
// because it will replace the shard node name if the node name got updated
// only if the replication factor is 1, otherwise it's no-op
func (s *schema) replaceStatesNodeName(new string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, meta := range s.classes {
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

func (s *schema) addProperty(class string, v uint64, props ...*models.Property) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta := s.classes[class]
	if meta == nil {
		return ErrClassNotFound
	}
	return meta.AddProperty(v, props...)
}

func (s *schema) addReplicaToShard(class string, v uint64, shard string, replica string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta := s.classes[class]
	if meta == nil {
		return ErrClassNotFound
	}
	return meta.AddReplicaToShard(v, shard, replica)
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
		s.shardsCount.WithLabelValues(status).Add(float64(count))
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
		s.shardsCount.WithLabelValues(status).Sub(float64(count))
	}

	return nil
}

func (s *schema) updateTenants(class string, v uint64, req *command.UpdateTenantsRequest) error {
	ok, meta, _, err := s.multiTenancyEnabled(class)
	if !ok {
		return err
	}
	sc, err := meta.UpdateTenants(s.nodeID, req, v)
	// partial update possible
	for status, count := range sc {
		// count can be positive or negative.
		s.shardsCount.WithLabelValues(status).Add(float64(count))
	}

	return err
}

func (s *schema) updateTenantsProcess(class string, v uint64, req *command.TenantProcessRequest) error {
	ok, meta, _, err := s.multiTenancyEnabled(class)
	if !ok {
		return err
	}

	sc, err := meta.UpdateTenantsProcess(s.nodeID, req, v)
	// partial update possible
	for status, count := range sc {
		// count can be positive or negative.
		s.shardsCount.WithLabelValues(status).Add(float64(count))
	}

	return err
}

func (s *schema) getTenants(class string, tenants []string) ([]*models.TenantResponse, error) {
	ok, meta, _, err := s.multiTenancyEnabled(class)
	if !ok {
		return nil, err
	}

	// Read tenants using the meta lock guard
	var res []*models.TenantResponse
	f := func(_ *models.Class, ss *sharding.State) error {
		if len(tenants) == 0 {
			res = make([]*models.TenantResponse, 0, len(ss.Physical))
			for tenant, physical := range ss.Physical {
				res = append(res, makeTenantResponse(tenant, physical))
			}
		} else {
			res = make([]*models.TenantResponse, 0, len(tenants))
			for _, tenant := range tenants {
				if physical, ok := ss.Physical[tenant]; ok {
					res = append(res, makeTenantResponse(tenant, physical))
				}
			}
		}
		return nil
	}
	return res, meta.RLockGuard(f)
}

func (s *schema) States() map[string]types.ClassState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cs := make(map[string]types.ClassState, len(s.classes))
	for _, c := range s.classes {
		cs[c.Class.Class] = types.ClassState{
			Class:  c.Class,
			Shards: c.Sharding,
		}
	}

	return cs
}

// MetaClasses is thread-safe and returns a deep copy of the meta classes and sharding states
func (s *schema) MetaClasses() map[string]*metaClass {
	s.mu.RLock()
	defer s.mu.RUnlock()

	classesCopy := make(map[string]*metaClass, len(s.classes))
	for k, v := range s.classes {
		v.RLock()
		classesCopy[k] = &metaClass{
			Class:        v.Class,
			ClassVersion: v.ClassVersion,
			Sharding:     v.Sharding.DeepCopy(),
			ShardVersion: v.ShardVersion,
		}
		v.RUnlock()
	}

	return classesCopy
}

func (s *schema) Restore(r io.Reader, parser Parser) error {
	snap := snapshot{}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %w", err)
	}
	for _, cls := range snap.Classes {
		if err := parser.ParseClass(&cls.Class); err != nil { // should not fail
			return fmt.Errorf("parsing class %q: %w", cls.Class.Class, err) // schema might be corrupted
		}
		cls.Sharding.SetLocalName(s.nodeID)
	}

	s.replaceClasses(snap.Classes)
	return nil
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *schema) Persist(sink raft.SnapshotSink) (err error) {
	// we don't need to lock here because, we call MetaClasses() which is thread-safe
	defer sink.Close()
	snap := snapshot{
		NodeID:     s.nodeID,
		SnapshotID: sink.ID(),
		Classes:    s.MetaClasses(),
	}
	if err := json.NewEncoder(sink).Encode(&snap); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	return nil
}

func (s *schema) Release() {
}

// makeTenant creates a tenant with the given name and status
func makeTenant(name, status string) models.Tenant {
	return models.Tenant{
		Name:           name,
		ActivityStatus: status,
	}
}

func makeTenantResponse(tenant string, physical sharding.Physical) *models.TenantResponse {
	// copy BelongsToNodes to avoid modification of the original slice
	cpy := make([]string, len(physical.BelongsToNodes))
	copy(cpy, physical.BelongsToNodes)

	return MakeTenantWithBelongsToNodes(tenant, entSchema.ActivityStatus(physical.Status), cpy)
}

// MakeTenantWithBelongsToNodes creates a tenant with the given name, status, and belongsToNodes
func MakeTenantWithBelongsToNodes(name, status string, belongsToNodes []string) *models.TenantResponse {
	return &models.TenantResponse{
		Tenant:         makeTenant(name, status),
		BelongsToNodes: belongsToNodes,
	}
}
