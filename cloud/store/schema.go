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

package store

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	command "github.com/weaviate/weaviate/cloud/proto/cluster"
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
	nodeID string
	sync.RWMutex
	Classes     snapshot `json:"classes"`
	shardReader shardReader
}

type shardReader interface {
	GetShardsStatus(class string) (models.ShardStatusList, error)
}

type snapshot map[string]*metaClass

type metaClass struct {
	Class    models.Class
	Sharding sharding.State
}

func NewSchema(nodeID string, shardReader shardReader) *schema {
	return &schema{
		nodeID:      nodeID,
		Classes:     make(snapshot, 128),
		shardReader: shardReader,
	}
}

func (f *schema) addClass(cls *models.Class, ss *sharding.State) error {
	f.Lock()
	defer f.Unlock()
	info := f.Classes[cls.Class]
	if info != nil {
		return errClassExists
	}
	f.Classes[cls.Class] = &metaClass{*cls, *ss}
	return nil
}

func (f *schema) updateClass(u *models.Class, ss *sharding.State) error {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[u.Class]
	if info == nil {
		return errClassNotFound
	}
	if u != nil {
		info.Class = *u
	}
	if ss != nil {
		info.Sharding = *ss
	}

	return nil
}

func (f *schema) deleteClass(name string) {
	f.Lock()
	defer f.Unlock()
	delete(f.Classes, name)
}

func (f *schema) addProperty(class string, p models.Property) error {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[class]
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

func (f *schema) addTenants(class string, req *command.AddTenantsRequest) error {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[class]
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
		if !slices.Contains[string](t.Nodes, f.nodeID) {
			req.Tenants[i] = nil // is owner by another node
		}
	}
	req.Tenants = removeNilTenants(req.Tenants)
	return nil
}

func (f *schema) deleteTenants(class string, req *command.DeleteTenantsRequest) error {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[class]
	if info == nil {
		return errClassNotFound
	}
	for _, name := range req.Tenants {
		info.Sharding.DeletePartition(name)
	}
	return nil
}

func (f *schema) updateTenants(class string, req *command.UpdateTenantsRequest) (n int, err error) {
	f.Lock()
	defer f.Unlock()

	info := f.Classes[class]
	if info == nil {
		return 0, errClassNotFound
	}
	missingShards := []string{}
	ps := info.Sharding.Physical
	for i, u := range req.Tenants {
		p, ok := ps[u.Name]
		if !ok {
			missingShards = append(missingShards, u.Name)
			continue
		}
		if p.ActivityStatus() == u.Status {
			req.Tenants[i] = nil
			continue
		}
		copy := p.DeepCopy()
		copy.Status = u.Status
		if len(u.Nodes) >= 0 {
			copy.BelongsToNodes = u.Nodes
		}
		ps[u.Name] = copy
		if !slices.Contains[string](copy.BelongsToNodes, f.nodeID) {
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

type ClassInfo struct {
	Exists            bool
	MultiTenancy      bool
	ReplicationFactor int
	Tenants           int
}

func (f *schema) ClassInfo(class string) (ci ClassInfo) {
	f.RLock()
	defer f.RUnlock()

	i := f.Classes[class]
	if i == nil {
		return
	}
	ci.Exists = true
	ci.MultiTenancy = i.Class.MultiTenancyConfig != nil && i.Class.MultiTenancyConfig.Enabled
	ci.ReplicationFactor = 1
	if i.Class.ReplicationConfig != nil && i.Class.ReplicationConfig.Factor > 1 {
		ci.ReplicationFactor = int(i.Class.ReplicationConfig.Factor)
	}
	ci.Tenants = len(i.Sharding.Physical)
	return ci
}

func (f *schema) MultiTenancy(class string) bool {
	f.RLock()
	defer f.RUnlock()

	i := f.Classes[class]
	return i != nil && i.Class.MultiTenancyConfig != nil && i.Class.MultiTenancyConfig.Enabled
}

// Read
func (f *schema) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	f.RLock()
	defer f.RUnlock()

	info := f.Classes[class]
	if info == nil {
		return errClassNotFound
	}

	return reader(&info.Class, &info.Sharding)
}

func (f *schema) ReadOnlyClass(class string) *models.Class {
	f.RLock()
	defer f.RUnlock()
	info := f.Classes[class]
	if info == nil {
		return nil
	}
	cp := info.Class
	return &cp
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
func (f *schema) ReadOnlySchema() models.Schema {
	cp := models.Schema{}
	f.RLock()
	defer f.RUnlock()
	cp.Classes = make([]*models.Class, len(f.Classes))
	i := 0
	for _, meta := range f.Classes {
		c := meta.Class
		cp.Classes[i] = &c
		i++
	}

	return cp
}

// ClassEqual returns the name of an existing class with a similar name, and "" otherwise
// strings.EqualFold is used to compare classes
func (f *schema) ClassEqual(name string) string {
	f.RLock()
	defer f.RUnlock()
	for k := range f.Classes {
		if strings.EqualFold(k, name) {
			return k
		}
	}
	return ""
}

// ShardOwner returns the node owner of the specified shard
func (f *schema) ShardOwner(class, shard string) (string, error) {
	f.RLock()
	defer f.RUnlock()

	i := f.Classes[class]
	if i == nil {
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
func (f *schema) ShardFromUUID(class string, uuid []byte) string {
	f.RLock()
	defer f.RUnlock()
	i := f.Classes[class]
	if i == nil {
		return ""
	}
	return i.Sharding.PhysicalShard(uuid)
}

// ShardOwner returns the node owner of the specified shard
func (f *schema) ShardReplicas(class, shard string) ([]string, error) {
	f.RLock()
	defer f.RUnlock()

	i := f.Classes[class]
	if i == nil {
		return nil, errClassNotFound
	}
	x, ok := i.Sharding.Physical[shard]
	if !ok {
		return nil, errShardNotFound
	}
	return x.BelongsToNodes, nil
}

// TenantShard returns shard name for the provided tenant and its activity status
func (f *schema) TenantShard(class, tenant string) (string, string) {
	f.RLock()
	defer f.RUnlock()

	i := f.Classes[class]
	if i == nil || !i.Sharding.PartitioningEnabled {
		return "", ""
	}

	if physical, ok := i.Sharding.Physical[tenant]; ok {
		return tenant, physical.ActivityStatus()
	}
	return "", ""
}

func (f *schema) CopyShardingState(class string) *sharding.State {
	f.RLock()
	defer f.RUnlock()

	i := f.Classes[class]
	if i == nil {
		return nil
	}

	st := i.Sharding.DeepCopy()
	return &st
}

func (f *schema) GetShardsStatus(class string) (models.ShardStatusList, error) {
	return f.shardReader.GetShardsStatus(class)
}
