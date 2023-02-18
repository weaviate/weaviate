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

	"github.com/weaviate/weaviate/entities/models"
)

// Status tracks all running indexes for the user-facing API as well as
// prevents starting up duplicate indexes
type Status struct {
	lock    sync.Mutex
	indexes []Index
}

type Index struct {
	ID      string
	Shards  []string
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

	newInd.Shards = append(newInd.Shards, shard)
	s.indexes = append(s.indexes, newInd)
	return nil
}

func (s *Status) List() []Index {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.indexes
}

func (ind *Index) merge(newShard string, newInd Index) error {
	for _, shard := range ind.Shards {
		if shard == newShard {
			return fmt.Errorf("duplicate index for shard %s: %s",
				newShard, ind.ID)
		}
	}

	fmt.Printf("about to amend")

	ind.Shards = append(ind.Shards, newShard)
	ind.Paths = append(ind.Paths, newInd.Paths...)

	return nil
}

func (s *Status) RemoveShard(shardName string) {
	s.lock.Lock()
	defer s.lock.Unlock()

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

	if len(ind.Shards) == 1 && pos == 0 {
		// no need to remove the shard, the whole entry can be dropped
		return false
	}

	ind.Shards = append(ind.Shards[:pos], ind.Shards[pos+1:]...)
	ind.Paths = append(ind.Paths[:pos], ind.Paths[pos+1:]...)
	return true
}

func (ind *Index) shardPos(needle string) int {
	for i, hay := range ind.Shards {
		if hay == needle {
			return i
		}
	}

	return -1
}

func (s *Status) ToSwagger() *models.IndexStatusList {
	s.lock.Lock()
	defer s.lock.Unlock()

	out := &models.IndexStatusList{}

	out.Total = int64(len(s.indexes))
	out.Indexes = make([]*models.IndexStatus, len(s.indexes))
	for i, ind := range s.indexes {
		out.Indexes[i] = ind.ToSwagger()
		if len(ind.Shards) > int(out.ShardCount) {
			out.ShardCount = int64(len(ind.Shards))
		}
	}

	return out
}

func (ind Index) ToSwagger() *models.IndexStatus {
	return &models.IndexStatus{
		ID:      ind.ID,
		Paths:   ind.Paths,
		Reason:  ind.Reason,
		Status:  ind.Status,
		Subject: ind.Subject,
		Type:    ind.Type,
	}
}

const StatusReady = "ready"
