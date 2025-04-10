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

package sharding

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"

	"github.com/spaolacci/murmur3"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

const shardNameLength = 12

type State struct {
	IndexID             string              `json:"indexID"` // for monitoring, reporting purposes. Does not influence the shard-calculations
	Config              config.Config       `json:"config"`
	Physical            map[string]Physical `json:"physical"`
	Virtual             []Virtual           `json:"virtual"`
	PartitioningEnabled bool                `json:"partitioningEnabled"`

	// different for each node, not to be serialized
	localNodeName string // TODO: localNodeName is static it is better to store just once
}

// MigrateFromOldFormat checks if the old (pre-v1.17) format was used and
// migrates it into the new format for backward-compatibility with all classes
// created before v1.17
func (s *State) MigrateFromOldFormat() {
	for shardName, shard := range s.Physical {
		if shard.LegacyBelongsToNodeForBackwardCompat != "" && len(shard.BelongsToNodes) == 0 {
			shard.BelongsToNodes = []string{
				shard.LegacyBelongsToNodeForBackwardCompat,
			}
			shard.LegacyBelongsToNodeForBackwardCompat = ""
		}
		s.Physical[shardName] = shard
	}
}

type Virtual struct {
	Name               string  `json:"name"`
	Upper              uint64  `json:"upper"`
	OwnsPercentage     float64 `json:"ownsPercentage"`
	AssignedToPhysical string  `json:"assignedToPhysical"`
}

type Physical struct {
	Name           string   `json:"name"`
	OwnsVirtual    []string `json:"ownsVirtual,omitempty"`
	OwnsPercentage float64  `json:"ownsPercentage"`

	LegacyBelongsToNodeForBackwardCompat string   `json:"belongsToNode,omitempty"`
	BelongsToNodes                       []string `json:"belongsToNodes,omitempty"`

	Status string `json:"status,omitempty"`
}

// BelongsToNode for backward-compatibility when there was no replication. It
// always returns the first node of the list
func (p Physical) BelongsToNode() string {
	return p.BelongsToNodes[0]
}

func (s *State) AddReplicaToShard(shard string, replica string) error {
	phys, ok := s.Physical[shard]
	if !ok {
		return fmt.Errorf("could not find shard %s", shard)
	}
	if err := phys.AddReplica(replica); err != nil {
		return err
	}
	s.Physical[shard] = phys
	return nil
}

func (p *Physical) AddReplica(replica string) error {
	if slices.Contains(p.BelongsToNodes, replica) {
		return fmt.Errorf("replica %s already exists", replica)
	}
	p.BelongsToNodes = append(p.BelongsToNodes, replica)
	return nil
}

// AdjustReplicas shrinks or extends the replica set (p.BelongsToNodes)
func (p *Physical) AdjustReplicas(count int, nodes cluster.NodeSelector) error {
	if count < 0 {
		return fmt.Errorf("negative replication factor: %d", count)
	}
	// let's be defensive here and make sure available replicas are unique.
	available := make(map[string]bool)
	for _, n := range p.BelongsToNodes {
		available[n] = true
	}
	// a == b should be always true except in case of bug
	if b, a := len(p.BelongsToNodes), len(available); b > a {
		p.BelongsToNodes = p.BelongsToNodes[:a]
		i := 0
		for n := range available {
			p.BelongsToNodes[i] = n
			i++
		}
	}
	if count < len(p.BelongsToNodes) { // less replicas wanted
		p.BelongsToNodes = p.BelongsToNodes[:count]
		return nil
	}

	names := nodes.StorageCandidates()
	if count > len(names) {
		return fmt.Errorf("not enough storage replicas: found %d want %d", len(names), count)
	}

	// make sure included nodes are unique
	for _, n := range names {
		if !available[n] {
			p.BelongsToNodes = append(p.BelongsToNodes, n)
			available[n] = true
		}
		if len(available) == count {
			break
		}
	}

	return nil
}

func (p *Physical) ActivityStatus() string {
	return schema.ActivityStatus(p.Status)
}

func InitState(id string, config config.Config, nodeLocalName string, names []string, replFactor int64, partitioningEnabled bool) (*State, error) {
	if f, n := replFactor, len(names); f > int64(n) {
		return nil, fmt.Errorf("could not find enough weaviate nodes for replication: %d available, %d requested", len(names), f)
	}

	out := &State{
		Config:              config,
		IndexID:             id,
		localNodeName:       nodeLocalName,
		PartitioningEnabled: partitioningEnabled,
	}

	if partitioningEnabled {
		out.Physical = make(map[string]Physical, 128)
		return out, nil
	}

	if err := out.initPhysical(names, replFactor); err != nil {
		return nil, err
	}
	out.initVirtual()
	out.distributeVirtualAmongPhysical()

	return out, nil
}

// Shard returns the shard name if it exits and empty string otherwise
func (s *State) Shard(partitionKey, objectID string) string {
	if s.PartitioningEnabled {
		if _, ok := s.Physical[partitionKey]; ok {
			return partitionKey // will change in the future
		}
		return ""
	}
	return s.PhysicalShard([]byte(objectID))
}

func (s *State) PhysicalShard(in []byte) string {
	if len(s.Physical) == 0 {
		panic("no physical shards present")
	}

	if len(s.Virtual) == 0 {
		panic("no virtual shards present")
	}

	h := murmur3.New64()
	h.Write(in)
	token := h.Sum64()

	virtual := s.virtualByToken(token)

	return virtual.AssignedToPhysical
}

// CountPhysicalShards return a count of physical shards
func (s *State) CountPhysicalShards() int {
	return len(s.Physical)
}

func (s *State) AllPhysicalShards() []string {
	var names []string
	for _, physical := range s.Physical {
		names = append(names, physical.Name)
	}

	sort.Slice(names, func(a, b int) bool {
		return names[a] < names[b]
	})

	return names
}

func (s *State) AllPhysicalShardsAndReplicas() map[string][]string {
	shardsToReplicas := make(map[string][]string, len(s.Physical))
	for _, physical := range s.Physical {
		shardsToReplicas[physical.Name] = physical.BelongsToNodes
	}
	return shardsToReplicas
}

func (s *State) AllLocalPhysicalShards() []string {
	var names []string
	for _, physical := range s.Physical {
		if s.IsLocalShard(physical.Name) {
			names = append(names, physical.Name)
		}
	}

	sort.Slice(names, func(a, b int) bool {
		return names[a] < names[b]
	})

	return names
}

func (s *State) SetLocalName(name string) {
	s.localNodeName = name
}

func (s *State) IsLocalShard(name string) bool {
	for _, node := range s.Physical[name].BelongsToNodes {
		if node == s.localNodeName {
			return true
		}
	}

	return false
}

// initPhysical assigns shards to nodes according to the following rules:
//
//   - The starting point of the ring is random
//   - Shard N+1's first node is the right neighbor of shard N's first node
//   - If a shard has multiple nodes (replication) they are always the right
//     neighbors of the first node of that shard
//
// Example with 3 nodes, 2 shards, replicationFactor=2:
//
// Shard 1: Node1, Node2
// Shard 2: Node2, Node3
//
// Example with 3 nodes, 3 shards, replicationFactor=3:
//
// Shard 1: Node1, Node2, Node3
// Shard 2: Node2, Node3, Node1
// Shard 3: Node3, Node1, Node2
//
// Example with 12 nodes, 3 shards, replicationFactor=5:
//
// Shard 1: Node7, Node8, Node9, Node10, Node 11
// Shard 2: Node8, Node9, Node10, Node 11, Node 12
// Shard 3: Node9, Node10, Node11, Node 12, Node 1
func (s *State) initPhysical(nodes []string, replFactor int64) error {
	if len(nodes) == 0 {
		return fmt.Errorf("there is no nodes provided, can't initiate state for empty node list")
	}
	it, err := cluster.NewNodeIterator(nodes, cluster.StartAfter)
	if err != nil {
		return err
	}
	it.SetStartNode(nodes[len(nodes)-1])

	s.Physical = map[string]Physical{}

	nodeSet := make(map[string]bool)
	for i := 0; i < s.Config.DesiredCount; i++ {
		name := generateShardName()
		shard := Physical{Name: name}
		shard.BelongsToNodes = make([]string, 0, replFactor)
		for { // select shard
			node := it.Next()
			if len(nodeSet) == len(nodes) { // this is a new round
				for k := range nodeSet {
					delete(nodeSet, k)
				}
			}
			if !nodeSet[node] {
				nodeSet[node] = true
				shard.BelongsToNodes = append(shard.BelongsToNodes, node)
				break
			}
		}

		for i := replFactor; i > 1; i-- {
			shard.BelongsToNodes = append(shard.BelongsToNodes, it.Next())
		}

		s.Physical[name] = shard
	}

	return nil
}

// GetPartitions based on the specified shards, available nodes, and replFactor
// It doesn't change the internal state
// TODO-RAFT: Ensure this function is higherorder, if the repartition result is changed, this will result in
// inconsistency when applying old log entry for add tenants
func (s State) GetPartitions(nodes []string, shards []string, replFactor int64) (map[string][]string, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("list of storage nodes is empty")
	}
	if replFactor > int64(len(nodes)) {
		return nil, fmt.Errorf("not enough replicas: found %d want %d", len(nodes), replFactor)
	}
	it, err := cluster.NewNodeIterator(nodes, cluster.StartAfter)
	if err != nil {
		return nil, err
	}
	it.SetStartNode(nodes[len(nodes)-1])
	partitions := make(map[string][]string, len(shards))
	nodeSet := make(map[string]bool)
	for _, name := range shards {
		if existedShard, exists := s.Physical[name]; exists &&
			existedShard.Status != models.TenantActivityStatusFROZEN &&
			existedShard.Status != models.TenantActivityStatusFREEZING {
			continue
		}
		owners := make([]string, 0, replFactor)
		for { // select shard
			node := it.Next()
			if len(nodeSet) == len(nodes) { // this is a new round
				for k := range nodeSet {
					delete(nodeSet, k)
				}
			}
			if !nodeSet[node] {
				nodeSet[node] = true
				owners = append(owners, node)
				break
			}
		}

		for i := replFactor; i > 1; i-- {
			owners = append(owners, it.Next())
		}

		partitions[name] = owners
	}

	return partitions, nil
}

// AddPartition to physical shards
func (s *State) AddPartition(name string, nodes []string, status string) Physical {
	p := Physical{
		Name:           name,
		BelongsToNodes: nodes,
		OwnsPercentage: 1.0,
		Status:         status,
	}
	s.Physical[name] = p
	return p
}

// DeletePartition to physical shards. Return `true` if given partition is
// actually deleted.
func (s *State) DeletePartition(name string) (string, bool) {
	t, ok := s.Physical[name]
	if !ok {
		return "", false
	}
	status := t.Status
	delete(s.Physical, name)
	return status, true
}

// ApplyNodeMapping replaces node names with their new value form nodeMapping in s.
// If s.LegacyBelongsToNodeForBackwardCompat is non empty, it will also perform node name replacement if present in nodeMapping.
func (s *State) ApplyNodeMapping(nodeMapping map[string]string) {
	if len(nodeMapping) == 0 {
		return
	}

	for k, v := range s.Physical {
		if v.LegacyBelongsToNodeForBackwardCompat != "" {
			if newNodeName, ok := nodeMapping[v.LegacyBelongsToNodeForBackwardCompat]; ok {
				v.LegacyBelongsToNodeForBackwardCompat = newNodeName
			}
		}

		for i, nodeName := range v.BelongsToNodes {
			if newNodeName, ok := nodeMapping[nodeName]; ok {
				v.BelongsToNodes[i] = newNodeName
			}
		}

		s.Physical[k] = v
	}
}

func (s *State) initVirtual() {
	count := s.Config.DesiredVirtualCount
	s.Virtual = make([]Virtual, count)

	for i := range s.Virtual {
		name := generateShardName()
		h := murmur3.New64()
		h.Write([]byte(name))
		s.Virtual[i] = Virtual{Name: name, Upper: h.Sum64()}
	}

	sort.Slice(s.Virtual, func(a, b int) bool {
		return s.Virtual[a].Upper < s.Virtual[b].Upper
	})

	for i := range s.Virtual {
		var tokenCount uint64
		if i == 0 {
			tokenCount = s.Virtual[0].Upper + (math.MaxUint64 - s.Virtual[len(s.Virtual)-1].Upper)
		} else {
			tokenCount = s.Virtual[i].Upper - s.Virtual[i-1].Upper
		}
		s.Virtual[i].OwnsPercentage = float64(tokenCount) / float64(math.MaxUint64)

	}
}

// this is a primitive distribution that only works for initializing. Once we
// want to support dynamic sharding, we need to come up with something better
// than this
func (s *State) distributeVirtualAmongPhysical() {
	ids := make([]string, len(s.Virtual))
	for i, v := range s.Virtual {
		ids[i] = v.Name
	}

	rand.Shuffle(len(s.Virtual), func(a, b int) {
		ids[a], ids[b] = ids[b], ids[a]
	})

	physicalIDs := make([]string, 0, len(s.Physical))
	for name := range s.Physical {
		physicalIDs = append(physicalIDs, name)
	}

	for i, vid := range ids {
		pickedPhysical := physicalIDs[i%len(physicalIDs)]

		virtual := s.VirtualByName(vid)
		virtual.AssignedToPhysical = pickedPhysical
		physical := s.Physical[pickedPhysical]
		physical.OwnsVirtual = append(physical.OwnsVirtual, vid)
		physical.OwnsPercentage += virtual.OwnsPercentage
		s.Physical[pickedPhysical] = physical
	}
}

// uses linear search, but should only be used during shard init and update
// operations, not in regular
func (s *State) VirtualByName(name string) *Virtual {
	for i := range s.Virtual {
		if s.Virtual[i].Name == name {
			return &s.Virtual[i]
		}
	}

	return nil
}

func (s *State) virtualByToken(token uint64) *Virtual {
	for i := range s.Virtual {
		if token > s.Virtual[i].Upper {
			continue
		}

		return &s.Virtual[i]
	}

	return &s.Virtual[0]
}

const shardNameChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateShardName() string {
	b := make([]byte, shardNameLength)
	for i := range b {
		b[i] = shardNameChars[rand.Intn(len(shardNameChars))]
	}

	return string(b)
}

func (s State) DeepCopy() State {
	var virtualCopy []Virtual

	physicalCopy := make(map[string]Physical, len(s.Physical))
	for name, shard := range s.Physical {
		physicalCopy[name] = shard.DeepCopy()
	}

	if len(s.Virtual) > 0 {
		virtualCopy = make([]Virtual, len(s.Virtual))
	}
	for i, virtual := range s.Virtual {
		virtualCopy[i] = virtual.DeepCopy()
	}

	return State{
		localNodeName:       s.localNodeName,
		IndexID:             s.IndexID,
		Config:              s.Config.DeepCopy(),
		Physical:            physicalCopy,
		Virtual:             virtualCopy,
		PartitioningEnabled: s.PartitioningEnabled,
	}
}

func (p Physical) DeepCopy() Physical {
	var ownsVirtualCopy []string
	if len(p.OwnsVirtual) > 0 {
		ownsVirtualCopy = make([]string, len(p.OwnsVirtual))
		copy(ownsVirtualCopy, p.OwnsVirtual)
	}

	belongsCopy := make([]string, len(p.BelongsToNodes))
	copy(belongsCopy, p.BelongsToNodes)

	return Physical{
		Name:           p.Name,
		OwnsVirtual:    ownsVirtualCopy,
		OwnsPercentage: p.OwnsPercentage,
		BelongsToNodes: belongsCopy,
		Status:         p.Status,
	}
}

func (v Virtual) DeepCopy() Virtual {
	return Virtual{
		Name:               v.Name,
		Upper:              v.Upper,
		OwnsPercentage:     v.OwnsPercentage,
		AssignedToPhysical: v.AssignedToPhysical,
	}
}
