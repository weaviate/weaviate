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

func (s *Status) RemoveShard(shardName string) {
	i := 0
	for j := range s.indexes {
		keep := s.indexes[j].removeShard(shardName)
		if !keep {
			continue
		}

		if i != j {
			s.indexes[i] = s.indexes[j]
		}
		i++
	}

	s.indexes = s.indexes[:i]
}

func (ind *Index) removeShard(shardName string) bool {
	pos := ind.shardPos(shardName)
	if pos < 0 {
		return true
	}

	if len(ind.shards) == 1 && pos == 0 {
		// no need to remove the shard, the whole entry can be dropped
		return false
	}

	ind.shards = append(ind.shards[:pos], ind.shards[pos+1:]...)
	ind.Paths = append(ind.Paths[:pos], ind.Paths[pos+1:]...)
	return true
}

func (ind *Index) shardPos(needle string) int {
	for i, hay := range ind.shards {
		if hay == needle {
			return i
		}
	}

	return -1
}

const StatusReady = "ready"
