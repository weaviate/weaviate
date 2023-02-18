package userindex

import (
	"fmt"
	"sync"
)

// Status tracks all running indexes for the user-facing API as well as
// prevents starting up duplicate indexes
type Status struct {
	lock    sync.Mutex
	indexes []Index
}

type Index struct {
	ID      string
	shards  []string
	Status  string
	Paths   []string
	Type    string
	Subject string
	Reason  string
}

func (s *Status) Register(shard string, newInd Index) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	for i, ind := range s.indexes {
		if ind.ID == newInd.ID {
			return s.indexes[i].merge(shard, newInd)
		}
	}

	newInd.shards = append(newInd.shards, shard)
	s.indexes = append(s.indexes, newInd)
	return nil
}

func (s *Status) List() []Index {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.indexes
}

func (ind *Index) merge(newShard string, newInd Index) error {
	for _, shard := range ind.shards {
		if shard == newShard {
			return fmt.Errorf("duplicate index for shard %s: %s",
				newShard, ind.ID)
		}
	}

	fmt.Printf("about to amend")

	ind.shards = append(ind.shards, newShard)
	ind.Paths = append(ind.Paths, newInd.Paths...)

	return nil
}

const StatusReady = "ready"
