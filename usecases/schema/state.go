//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// The pre-RAFT schema snapshot and the store that persisted it, kept together
// because SchemaStore exists only to save and load State.

// State is a cached copy of the schema that can also be saved into a remote
// storage, as specified by Repo
type State struct {
	ObjectSchema  *models.Schema `json:"object"`
	ShardingState map[string]*sharding.State
}

// NewState returns a new state with room for nClasses classes
func NewState(nClasses int) State {
	return State{
		ObjectSchema: &models.Schema{
			Classes: make([]*models.Class, 0, nClasses),
		},
		ShardingState: make(map[string]*sharding.State, nClasses),
	}
}

func (s State) EqualEnough(other *State) bool {
	// Same number of classes
	eqClassLen := len(s.ObjectSchema.Classes) == len(other.ObjectSchema.Classes)
	if !eqClassLen {
		return false
	}

	// Same sharding state length
	eqSSLen := len(s.ShardingState) == len(other.ShardingState)
	if !eqSSLen {
		return false
	}

	for cls, ss1ss := range s.ShardingState {
		// Same sharding state keys
		ss2ss, ok := other.ShardingState[cls]
		if !ok {
			return false
		}

		// Same number of physical shards
		eqPhysLen := len(ss1ss.Physical) == len(ss2ss.Physical)
		if !eqPhysLen {
			return false
		}

		for shard, ss1phys := range ss1ss.Physical {
			// Same physical shard contents and status
			ss2phys, ok := ss2ss.Physical[shard]
			if !ok {
				return false
			}
			eqActivStat := ss1phys.ActivityStatus() == ss2phys.ActivityStatus()
			if !eqActivStat {
				return false
			}
		}
	}

	return true
}

// SchemaStore is responsible for persisting the schema
// by providing support for both partial and complete schema updates
// Deprecated: instead schema now is persistent via RAFT
// see : usecase/schema/handler.go & cluster/store/store.go
// Load and save are left to support backward compatibility
type SchemaStore interface {
	// Save saves the complete schema to the persistent storage
	Save(ctx context.Context, schema State) error

	// Load loads the complete schema from the persistent storage
	Load(context.Context) (State, error)
}

// KeyValuePair is used to serialize shards updates
type KeyValuePair struct {
	Key   string
	Value []byte
}

// ClassPayload is used to serialize class updates
type ClassPayload struct {
	Name          string
	Metadata      []byte
	ShardingState []byte
	Shards        []KeyValuePair
	ReplaceShards bool
	Error         error
}
