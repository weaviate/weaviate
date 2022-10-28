//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sharding

import (
	"fmt"
	"math"
	"math/rand"
	"sort"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/spaolacci/murmur3"
)

const shardNameLength = 12

type State struct {
	IndexID  string              `json:"indexID"` // for monitoring, reporting purposes. Does not influence the shard-calculations
	Config   Config              `json:"config"`
	Physical map[string]Physical `json:"physical"`
	Virtual  []Virtual           `json:"virtual"`

	// different for each node, not to be serialized
	localNodeName string
}

type Virtual struct {
	Name               string  `json:"name"`
	Upper              uint64  `json:"upper"`
	OwnsPercentage     float64 `json:"ownsPercentage"`
	AssignedToPhysical string  `json:"assignedToPhysical"`
}

type Physical struct {
	Name           string   `json:"name"`
	OwnsVirtual    []string `json:"ownsVirtual"`
	OwnsPercentage float64  `json:"ownsPercentage"`

	// TODO: migrate existing classes on READ
	BelongsToNodes []string `json:"belongsToNodes"`
}

// BelongsToNode for backward-compatibility when there was no replication. It
// always returns the first node of the list
func (p Physical) BelongsToNode() string {
	return p.BelongsToNodes[0]
}

// Adjust Replicas uses a NodeIterator to add new nodes (scale out) or remove
// existing nodes (scale in) from the "BelongsToNodes" mappings. This is used
// as part of dynamically changing the replication factor. This method
// basically controls where a shard will land in the cluster. If we want to add
// some kind of node bias while scaling in the future, it would probably go
// here.
func (p *Physical) AdjustReplicas(count int, nodes nodes) error {
	it, err := cluster.NewNodeIterator(nodes, cluster.StartAfter)
	if err != nil {
		return err
	}

	it.SetStartNode(p.BelongsToNodes[len(p.BelongsToNodes)-1])

	if count < len(p.BelongsToNodes) {
		if count < 0 {
			return errors.Errorf("cannot scale below 0, got %d", count)
		}
		p.BelongsToNodes = p.BelongsToNodes[:count]
		return nil
	}

	for len(p.BelongsToNodes) < count {
		p.BelongsToNodes = append(p.BelongsToNodes, it.Next())
	}

	return nil
}

type nodes interface {
	AllNames() []string
	LocalName() string
}

func InitState(id string, config Config, nodes nodes) (*State, error) {
	out := &State{Config: config, IndexID: id, localNodeName: nodes.LocalName()}

	if err := out.initPhysical(nodes); err != nil {
		return nil, err
	}

	if err := out.initVirtual(); err != nil {
		return nil, err
	}

	if err := out.distributeVirtualAmongPhysical(); err != nil {
		return nil, err
	}

	return out, nil
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

// CountPhysicalShards return a count of pysical shards
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

func (s *State) AllLocalPhysicalShards() []string {
	var names []string
	for _, physical := range s.Physical {
		if s.IsShardLocal(physical.Name) {
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

func (s *State) IsShardLocal(name string) bool {
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
func (s *State) initPhysical(nodes nodes) error {
	it, err := cluster.NewNodeIterator(nodes, cluster.StartRandom)
	if err != nil {
		return err
	}

	s.Physical = map[string]Physical{}

	for i := 0; i < s.Config.DesiredCount; i++ {
		name := generateShardName()
		shard := Physical{Name: name}
		node := it.Next()
		shard.BelongsToNodes = []string{node}
		if s.Config.Replicas > 1 {
			// create a second node iterator and start after the already assigned
			// one, this way we can identify our next n right neighbors without
			// affecting the root iterator which will determine the next shard
			replicationIter, err := cluster.NewNodeIterator(nodes, cluster.StartAfter)
			if err != nil {
				return fmt.Errorf("assign replication nodes: %w", err)
			}

			replicationIter.SetStartNode(node)
			// the first node is already assigned, we only need to assign the
			// additional nodes
			for i := s.Config.Replicas; i > 1; i-- {
				shard.BelongsToNodes = append(shard.BelongsToNodes, replicationIter.Next())
			}
		}

		s.Physical[name] = shard
	}

	return nil
}

func (s *State) initVirtual() error {
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

	return nil
}

// this is a primitive distribution that only works for initializing. Once we
// want to support dynamic sharding, we need to come up with something better
// than this
func (s *State) distributeVirtualAmongPhysical() error {
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

		virtual := s.virtualByName(vid)
		virtual.AssignedToPhysical = pickedPhysical
		physical := s.Physical[pickedPhysical]
		physical.OwnsVirtual = append(physical.OwnsVirtual, vid)
		physical.OwnsPercentage += virtual.OwnsPercentage
		s.Physical[pickedPhysical] = physical
	}

	return nil
}

// uses linear search, but should only be used during shard init and udpate
// operations, not in regular
func (s *State) virtualByName(name string) *Virtual {
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
	physicalCopy := map[string]Physical{}
	for name, shard := range s.Physical {
		physicalCopy[name] = shard.DeepCopy()
	}

	virtualCopy := make([]Virtual, len(s.Virtual))
	for i, virtual := range s.Virtual {
		virtualCopy[i] = virtual.DeepCopy()
	}

	return State{
		localNodeName: s.localNodeName,
		IndexID:       s.localNodeName,
		Config:        s.Config.DeepCopy(),
		Physical:      physicalCopy,
		Virtual:       virtualCopy,
	}
}

func (c Config) DeepCopy() Config {
	return Config{
		VirtualPerPhysical:  c.VirtualPerPhysical,
		DesiredCount:        c.DesiredCount,
		ActualCount:         c.ActualCount,
		DesiredVirtualCount: c.DesiredVirtualCount,
		ActualVirtualCount:  c.ActualVirtualCount,
		Key:                 c.Key,
		Strategy:            c.Strategy,
		Function:            c.Function,
		Replicas:            c.Replicas,
	}
}

func (p Physical) DeepCopy() Physical {
	ownsVirtualCopy := make([]string, len(p.OwnsVirtual))
	copy(ownsVirtualCopy, p.OwnsVirtual)

	belongsCopy := make([]string, len(p.BelongsToNodes))
	copy(belongsCopy, p.BelongsToNodes)

	return Physical{
		Name:           p.Name,
		OwnsVirtual:    ownsVirtualCopy,
		OwnsPercentage: p.OwnsPercentage,
		BelongsToNodes: belongsCopy,
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
