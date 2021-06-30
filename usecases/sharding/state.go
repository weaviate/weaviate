package sharding

import (
	"math"
	"math/rand"
	"sort"

	"github.com/spaolacci/murmur3"
)

const shardNameLength = 12

type State struct {
	indexID  string // for monitoring, reporting purposes. Does not influence the shard-calculations
	config   Config
	physical map[string]Physical
	virtual  []Virtual
}

type Virtual struct {
	Name               string
	Upper              uint64
	OwnsPercentage     float64
	AssignedToPhysical string
}

type Physical struct {
	Name           string
	OwnsVirtual    []string
	OwnsPercentage float64
}

func InitState(id string, config Config) (*State, error) {
	out := &State{config: config, indexID: id}

	if err := out.initPhysical(); err != nil {
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

func (s *State) initPhysical() error {
	s.physical = map[string]Physical{}

	for i := 0; i < s.config.DesiredCount; i++ {
		name := generateShardName()
		s.physical[name] = Physical{Name: name}
	}

	return nil
}

func (s *State) initVirtual() error {
	count := s.config.DesiredVirtualCount
	s.virtual = make([]Virtual, count)

	for i := range s.virtual {
		name := generateShardName()
		h := murmur3.New64()
		h.Write([]byte(name))
		s.virtual[i] = Virtual{Name: name, Upper: h.Sum64()}
	}

	sort.Slice(s.virtual, func(a, b int) bool {
		return s.virtual[a].Upper < s.virtual[b].Upper
	})

	for i := range s.virtual {
		var tokenCount uint64
		if i == 0 {
			tokenCount = s.virtual[0].Upper + (math.MaxUint64 - s.virtual[len(s.virtual)-1].Upper)
		} else {
			tokenCount = s.virtual[i].Upper - s.virtual[i-1].Upper
		}
		s.virtual[i].OwnsPercentage = float64(tokenCount) / float64(math.MaxUint64)

	}

	return nil
}

// this is a primitive distribution that only works for initializing. Once we
// want to support dynamic sharding, we need to come up with something better
// than this
func (s *State) distributeVirtualAmongPhysical() error {
	ids := make([]string, len(s.virtual))
	for i, v := range s.virtual {
		ids[i] = v.Name
	}

	rand.Shuffle(len(s.virtual), func(a, b int) {
		ids[a], ids[b] = ids[b], ids[a]
	})

	physicalIDs := make([]string, 0, len(s.physical))
	for name := range s.physical {
		physicalIDs = append(physicalIDs, name)
	}

	for i, vid := range ids {
		pickedPhysical := physicalIDs[i%len(physicalIDs)]

		virtual := s.virtualByName(vid)
		virtual.AssignedToPhysical = pickedPhysical
		physical := s.physical[pickedPhysical]
		physical.OwnsVirtual = append(physical.OwnsVirtual, vid)
		physical.OwnsPercentage += virtual.OwnsPercentage
		s.physical[pickedPhysical] = physical
	}

	return nil
}

// uses linear search, but should only be used during shard init and udpate
// operations, not in regular
func (s *State) virtualByName(name string) *Virtual {
	for i := range s.virtual {
		if s.virtual[i].Name == name {
			return &s.virtual[i]
		}
	}

	return nil
}

const shardNameChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateShardName() string {
	b := make([]byte, shardNameLength)
	for i := range b {
		b[i] = shardNameChars[rand.Intn(len(shardNameChars))]
	}

	return string(b)
}
