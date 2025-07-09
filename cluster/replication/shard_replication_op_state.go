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

package replication

import (
	"errors"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

var ErrMaxErrorsReached = errors.New("max errors reached")

const (
	MaxErrors = 50
)

// State is the status of a shard replication operation
type State struct {
	// State is the current state of the shard replication operation
	State api.ShardReplicationState
	// Errors is the list of errors that occurred during this state
	Errors []string
}

// StateHistory is the history of the state changes of the shard replication operation
// Defining this as a type allows us to define methods on it
type StateHistory []State

// ShardReplicationOpStatus is the status of a shard replication operation as well as the history of the state changes and their associated errors (if any)
type ShardReplicationOpStatus struct {
	// SchemaVersion is the minimum schema version that the shard replication operation can safely proceed with
	// It's necessary to track this because the schema version is not always the same across multiple nodes due to EC issues with RAFT.
	// By communicating it with remote nodes, we can ensure that they will wait for the schema version to be the same or greater before proceeding with the operation.
	SchemaVersion uint64

	// Current is the current state of the shard replication operation
	Current State

	// ShouldCancel is a flag indicating that the operation should be cancelled at the earliest possible time
	ShouldCancel bool
	// ShouldDelete is a flag indicating that the operation should be cancelled at the earliest possible time and then deleted
	ShouldDelete bool
	// UnCancellable is a flag indicating that an operation is not capable of being cancelled.
	// E.g., an op is not cancellable if it is in the DEHYDRATING state after the replica has been added to the sharding state.
	UnCancellable bool

	// History is the history of the state changes of the shard replication operation
	History StateHistory
}

// NewShardReplicationStatus creates a new ShardReplicationOpStatus initialized with the given state and en empty history
func NewShardReplicationStatus(state api.ShardReplicationState) ShardReplicationOpStatus {
	return ShardReplicationOpStatus{
		Current: State{
			State: state,
		},
		History: []State{},
	}
}

// AddError adds an error to the current state of the shard replication operation
func (s *ShardReplicationOpStatus) AddError(error string) error {
	if len(s.Current.Errors) >= MaxErrors {
		return ErrMaxErrorsReached
	}
	s.Current.Errors = append(s.Current.Errors, error)
	return nil
}

// ChangeState changes the state of the shard replication operation to the next state and keeps the previous state in the history
func (s *ShardReplicationOpStatus) ChangeState(nextState api.ShardReplicationState) {
	s.History = append(s.History, s.Current)
	s.Current = State{
		State:  nextState,
		Errors: []string{},
	}
}

// GetCurrent returns the current state and errors of the shard replication operation
func (s *ShardReplicationOpStatus) GetCurrent() State {
	return s.Current
}

// GetCurrentState returns the current state of the shard replication operation
func (s *ShardReplicationOpStatus) GetCurrentState() api.ShardReplicationState {
	return s.Current.State
}

func (s *ShardReplicationOpStatus) TriggerCancellation() {
	s.ShouldCancel = true
	s.ShouldDelete = false
}

func (s *ShardReplicationOpStatus) CompleteCancellation() {
	s.ShouldCancel = false
	s.ShouldDelete = false
	s.ChangeState(api.CANCELLED)
}

func (s *ShardReplicationOpStatus) TriggerDeletion() {
	s.ShouldCancel = true
	s.ShouldDelete = true
}

// OnlyCancellation returns true if ShouldCancel is true and ShouldDelete is false
func (s *ShardReplicationOpStatus) OnlyCancellation() bool {
	return s.ShouldCancel && !s.ShouldDelete
}

// ShouldCleanup returns true if the current state is not READY
func (s *ShardReplicationOpStatus) ShouldCleanup() bool {
	return s.GetCurrentState() != api.READY && s.GetCurrentState() != api.DEHYDRATING
}

// GetHistory returns the history of the state changes of the shard replication operation
func (s *ShardReplicationOpStatus) GetHistory() StateHistory {
	return s.History
}

// ToAPIFormat converts the State to the API format
func (s State) ToAPIFormat() api.ReplicationDetailsState {
	return api.ReplicationDetailsState{
		State:  s.State.String(),
		Errors: s.Errors,
	}
}

// ToAPIFormat converts the StateHistory to the API format
func (sh StateHistory) ToAPIFormat() []api.ReplicationDetailsState {
	states := make([]api.ReplicationDetailsState, len(sh))
	for i, s := range sh {
		states[i] = s.ToAPIFormat()
	}
	return states
}

// ShardReplicationOpAndStatus is a struct that contains a ShardReplicationOp and a ShardReplicationOpStatus
type ShardReplicationOpAndStatus struct {
	Op     ShardReplicationOp
	Status ShardReplicationOpStatus
}

// NewShardReplicationOpAndStatus creates a new ShardReplicationOpAndStatus from op and status
func NewShardReplicationOpAndStatus(op ShardReplicationOp, status ShardReplicationOpStatus) ShardReplicationOpAndStatus {
	return ShardReplicationOpAndStatus{
		Op:     op,
		Status: status,
	}
}
