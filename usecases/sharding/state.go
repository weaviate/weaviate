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
	"math"
	"math/rand"
	"sort"

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
	BelongsToNode  string   `json:"belongsToNode"`
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
	return s.Physical[name].BelongsToNode == s.localNodeName
}

func (s *State) initPhysical(nodes nodes) error {
	it, err := cluster.NewNodeIterator(nodes, cluster.StartRandom)
	if err != nil {
		return err
	}

	s.Physical = map[string]Physical{}

	for i := 0; i < s.Config.DesiredCount; i++ {
		name := generateShardName()
		s.Physical[name] = Physical{Name: name, BelongsToNode: it.Next()}
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
