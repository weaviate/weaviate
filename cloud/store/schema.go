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

	"github.com/cenkalti/backoff/v4"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/cloud/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	"golang.org/x/exp/slices"
)

var (
	errClassNotFound = errors.New("class not found")
	errClassExists   = errors.New("class already exits")
	errShardNotFound = errors.New("shard not found")
)

type schema struct {
	nodeID      string
	shardReader shardReader
	sync.RWMutex
	Classes map[string]*metaClass
}

type shardReader interface {
	GetShardsStatus(class string) (models.ShardStatusList, error)
}

type metaClass struct {
	Class    models.Class
	Sharding sharding.State
}

func NewSchema(nodeID string, shardReader shardReader) *schema {
	return &schema{
		nodeID:      nodeID,
		Classes:     make(map[string]*metaClass, 128),
		shardReader: shardReader,
	}
}

func (s *schema) Len() int {
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

	s.Classes[cls.Class] = &metaClass{*cls, *ss}
	return nil
}

func (s *schema) updateClass(cls *models.Class, ss *sharding.State) error {
	s.Lock()
	defer s.Unlock()

	info := s.Classes[cls.Class]
	if info == nil {
		return errClassNotFound
	}

	if cls != nil {
		info.Class = *cls
	}
	if ss != nil {
		info.Sharding = *ss
	}

	return nil
}

func (s *schema) deleteClass(name string) {
	s.Lock()
	defer s.Unlock()
	delete(s.Classes, name)
}

func (s *schema) addProperty(class string, p models.Property) error {
	s.Lock()
	defer s.Unlock()

	info := s.Classes[class]
	if info == nil {
		return errClassNotFound
	}

	// update all at once to prevent race condition with concurrent readers
	src := info.Class.Properties
	dest := make([]*models.Property, len(src)+1)
	copy(dest, src)
	dest[len(src)] = &p
	info.Class.Properties = dest
	return nil
}

func (s *schema) addTenants(class string, req *command.AddTenantsRequest) error {
	req.Tenants = removeNilTenants(req.Tenants)
	s.Lock()
	defer s.Unlock()

	info := s.Classes[class]
	if info == nil {
		return errClassNotFound
	}

	ps := info.Sharding.Physical

	for i, t := range req.Tenants {
		if _, ok := ps[t.Name]; ok {
			req.Tenants[i] = nil // already exists
			continue
		}

		p := sharding.Physical{Name: t.Name, Status: t.Status, BelongsToNodes: t.Nodes}
		info.Sharding.Physical[t.Name] = p
		if !slices.Contains(t.Nodes, s.nodeID) {
			req.Tenants[i] = nil // is owner by another node
		}
	}
	req.Tenants = removeNilTenants(req.Tenants)
	return nil
}

func (s *schema) deleteTenants(class string, req *command.DeleteTenantsRequest) error {
	s.Lock()
	defer s.Unlock()

	info := s.Classes[class]
	if info == nil {
		return errClassNotFound
	}

	for _, name := range req.Tenants {
		info.Sharding.DeletePartition(name)
	}
	return nil
}

func (s *schema) updateTenants(class string, req *command.UpdateTenantsRequest) (n int, err error) {
	s.Lock()
	defer s.Unlock()

	info := s.Classes[class]
	if info == nil {
		return 0, errClassNotFound
	}

	missingShards := []string{}
	ps := info.Sharding.Physical
	for i, u := range req.Tenants {

		p, ok := ps[u.Name]
		if !ok {
			missingShards = append(missingShards, u.Name)
			req.Tenants[i] = nil
			continue
		}
		if p.ActivityStatus() == u.Status {
			req.Tenants[i] = nil
			continue
		}
		copy := p.DeepCopy()
		copy.Status = u.Status
		if u.Nodes != nil && len(u.Nodes) >= 0 {
			copy.BelongsToNodes = u.Nodes
		}
		ps[u.Name] = copy
		if !slices.Contains(copy.BelongsToNodes, s.nodeID) {
			req.Tenants[i] = nil
		}
		n++
	}
	if len(missingShards) > 0 {
		err = fmt.Errorf("%w: %v", errShardNotFound, missingShards)
	}

	req.Tenants = removeNilTenants(req.Tenants)
	return
}

func (s *schema) clear() {
	s.Lock()
	defer s.Unlock()
	for k := range s.Classes {
		delete(s.Classes, k)
	}
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

type ClassInfo struct {
	Exists            bool
	MultiTenancy      models.MultiTenancyConfig
	ReplicationFactor int
	Tenants           int
	Properties        int
}

func (s *schema) ClassInfo(class string) (ci ClassInfo) {
	s.RLock()
	defer s.RUnlock()

	i := s.Classes[class]
	if i == nil {
		return
	}

	ci.Exists = true
	ci.Properties = len(i.Class.Properties)
	ci.MultiTenancy = parseMultiTenancyConfig(i)
	ci.ReplicationFactor = 1
	if i.Class.ReplicationConfig != nil && i.Class.ReplicationConfig.Factor > 1 {
		ci.ReplicationFactor = int(i.Class.ReplicationConfig.Factor)
	}
	ci.Tenants = len(i.Sharding.Physical)
	return ci
}

func (s *schema) MultiTenancy(class string) models.MultiTenancyConfig {
	info, _ := s.ReadMetaClass(class)
	return parseMultiTenancyConfig(info)
}

func parseMultiTenancyConfig(class *metaClass) (cfg models.MultiTenancyConfig) {
	if class == nil || class.Class.MultiTenancyConfig == nil {
		return
	}
	cfg = *class.Class.MultiTenancyConfig
	return
}

// Read
func (s *schema) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	info, err := s.ReadMetaClass(class)
	if err != nil {
		return errClassNotFound
	}

	s.Lock()
	defer s.Unlock()

	return reader(&info.Class, &info.Sharding)
}

func (s *schema) ReadMetaClass(class string) (*metaClass, error) {
	var (
		info   *metaClass
		exists bool
	)
	backoff.Retry(func() error {
		s.RLock()
		defer s.RUnlock()

		info, exists = s.Classes[class]
		if !exists {
			return fmt.Errorf("class %s not found locally", class)
		}
		return nil
	}, utils.NewBackoff())

	if info == nil {
		return nil, fmt.Errorf("class %s not found locally", class)
	}
	return info, nil
}

func (s *schema) ReadOnlyClass(class string) *models.Class {
	info, err := s.ReadMetaClass(class)
	if err != nil {
		return nil
	}
	return &info.Class
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
		c := meta.Class
		cp.Classes[i] = &c
		i++
	}

	return cp
}

// ShardOwner returns the node owner of the specified shard
func (s *schema) ShardOwner(class, shard string) (string, error) {
	i, err := s.ReadMetaClass(class)
	if err != nil {
		return "", errClassNotFound
	}

	x, ok := i.Sharding.Physical[shard]
	if !ok {
		return "", errShardNotFound
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", fmt.Errorf("owner node not found")
	}
	return x.BelongsToNodes[0], nil
}

// ShardFromUUID returns shard name of the provided uuid
func (s *schema) ShardFromUUID(class string, uuid []byte) string {
	i, err := s.ReadMetaClass(class)
	if err != nil {
		return ""
	}
	return i.Sharding.PhysicalShard(uuid)
}

// ShardOwner returns the node owner of the specified shard
func (s *schema) ShardReplicas(class, shard string) ([]string, error) {
	info, err := s.ReadMetaClass(class)
	if err != nil {
		return nil, errClassNotFound
	}

	x, ok := info.Sharding.Physical[shard]
	if !ok {
		return nil, errShardNotFound
	}
	return x.BelongsToNodes, nil
}

// TenantShard returns shard name for the provided tenant and its activity status
func (s *schema) TenantShard(class, tenant string) (string, string) {
	var exTenant, status string
	info, err := s.ReadMetaClass(class)
	if err != nil || !info.Sharding.PartitioningEnabled {
		return exTenant, status
	}

	backoff.Retry(func() error {
		physical, ok := info.Sharding.Physical[tenant]
		if !ok {
			return fmt.Errorf("doesn't exists")
		}
		exTenant = tenant
		status = physical.ActivityStatus()
		return nil
	}, utils.NewBackoff())

	return exTenant, status
}

func (s *schema) CopyShardingState(class string) *sharding.State {
	info, err := s.ReadMetaClass(class)
	if err != nil {
		return nil
	}

	st := info.Sharding.DeepCopy()
	return &st
}

func (s *schema) GetShardsStatus(class string) (models.ShardStatusList, error) {
	return s.shardReader.GetShardsStatus(class)
}
