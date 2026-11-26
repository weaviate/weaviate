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

package replication

import (
	"errors"
	"fmt"
	"slices"

	"github.com/weaviate/weaviate/cluster/replication/types"
)

// PlanState is the lifecycle state of a rebalance plan.
type PlanState string

// The ten plan states. No command applied in this package sets RUNNING,
// STOPPING, COMPLETED or FAILED.
const (
	PlanStatePlanning         PlanState = "PLANNING"
	PlanStateProposed         PlanState = "PROPOSED"
	PlanStateNotNeeded        PlanState = "NOT_NEEDED"
	PlanStateNoAcceptablePlan PlanState = "NO_ACCEPTABLE_PLAN"
	PlanStatePlanningFailed   PlanState = "PLANNING_FAILED"
	PlanStateRunning          PlanState = "RUNNING"
	PlanStateStopping         PlanState = "STOPPING"
	PlanStateStopped          PlanState = "STOPPED"
	PlanStateCompleted        PlanState = "COMPLETED"
	PlanStateFailed           PlanState = "FAILED"
)

func (s PlanState) isEnd() bool {
	switch s {
	case PlanStateCompleted, PlanStateStopped, PlanStateFailed,
		PlanStateNotNeeded, PlanStateNoAcceptablePlan, PlanStatePlanningFailed:
		return true
	default:
		return false
	}
}

func (s PlanState) isPlanningOutcome() bool {
	switch s {
	case PlanStateProposed, PlanStateNotNeeded, PlanStateNoAcceptablePlan, PlanStatePlanningFailed:
		return true
	default:
		return false
	}
}

var (
	// ErrPlanNotFound means no plan exists or the stored plan has another ID.
	ErrPlanNotFound = errors.New("rebalance plan not found")
	// ErrPlanExists means create found a plan that is not finished at the command's timestamp.
	ErrPlanExists = errors.New("an unfinished rebalance plan exists")
	// ErrPlanTransition means the command is not allowed from the plan's current
	// state, or the requested outcome is not a planning outcome.
	ErrPlanTransition = errors.New("rebalance plan state transition not allowed")
	// ErrPlanStaleRun means the completion comes from a planning run that is no
	// longer current: one from an older Raft term, or another run in the recorded term.
	ErrPlanStaleRun = errors.New("rebalance planning run is stale")
)

// PlanNodeBalance is one node's measured or projected load in a plan.
type PlanNodeBalance struct {
	Node            string  `json:"node"`
	DiskUtilisation float64 `json:"diskUtilisation"`
	ReplicaCount    int     `json:"replicaCount"`
}

// Plan is the cluster's single rebalance plan, replicated through Raft.
type Plan struct {
	ID    string    `json:"id"`
	State PlanState `json:"state"`

	SourceNodes []string `json:"sourceNodes"`
	TargetNodes []string `json:"targetNodes"`
	Collections []string `json:"collections"`
	Namespaces  []string `json:"namespaces"`

	CreatedAtUnixMs int64 `json:"createdAtUnixMs"`

	// RunID and RunTerm identify the planning run whose completion was applied.
	RunID   string `json:"runId"`
	RunTerm uint64 `json:"runTerm"`

	// MeasuredAtUnixMs, Nodes and Reason come from the applied planning run.
	MeasuredAtUnixMs int64                            `json:"measuredAtUnixMs"`
	Nodes            []types.RebalanceNodeMeasurement `json:"nodes"`
	Reason           string                           `json:"reason"`

	Baseline            []PlanNodeBalance `json:"baseline"`
	Goal                []PlanNodeBalance `json:"goal"`
	MoveCount           int               `json:"moveCount"`
	PlanHash            string            `json:"planHash"`
	ApproveBeforeUnixMs int64             `json:"approveBeforeUnixMs"`
}

// clone returns a Plan that shares no memory with p. GetPlan and snapshotPlan
// return clones, so no caller holds a pointer into memory the FSM owns.
// snapshotOps follows the same rule for ops.
func (p Plan) clone() Plan {
	p.SourceNodes = slices.Clone(p.SourceNodes)
	p.TargetNodes = slices.Clone(p.TargetNodes)
	p.Collections = slices.Clone(p.Collections)
	p.Namespaces = slices.Clone(p.Namespaces)
	p.Nodes = slices.Clone(p.Nodes)
	p.Baseline = slices.Clone(p.Baseline)
	p.Goal = slices.Clone(p.Goal)
	return p
}

// Finished reports whether the plan is in an end state, or is PROPOSED and
// nowUnixMs has reached ApproveBeforeUnixMs. Callers applying a command pass
// the command's timestamp as nowUnixMs, never the local clock.
func (p Plan) Finished(nowUnixMs int64) bool {
	return p.State.isEnd() || (p.State == PlanStateProposed && nowUnixMs >= p.ApproveBeforeUnixMs)
}

// CreatePlanRequest is the payload of the create-plan command.
type CreatePlanRequest struct {
	PlanID          string   `json:"planId"`
	SourceNodes     []string `json:"sourceNodes"`
	TargetNodes     []string `json:"targetNodes"`
	Collections     []string `json:"collections"`
	Namespaces      []string `json:"namespaces"`
	CreatedAtUnixMs int64    `json:"createdAtUnixMs"`
}

// CompletePlanningRequest is the payload of the complete-planning command.
type CompletePlanningRequest struct {
	PlanID              string                           `json:"planId"`
	RunID               string                           `json:"runId"`
	RunTerm             uint64                           `json:"runTerm"`
	Outcome             PlanState                        `json:"outcome"`
	MeasuredAtUnixMs    int64                            `json:"measuredAtUnixMs"`
	Nodes               []types.RebalanceNodeMeasurement `json:"nodes"`
	Reason              string                           `json:"reason"`
	Baseline            []PlanNodeBalance                `json:"baseline"`
	Goal                []PlanNodeBalance                `json:"goal"`
	MoveCount           int                              `json:"moveCount"`
	PlanHash            string                           `json:"planHash"`
	ApproveBeforeUnixMs int64                            `json:"approveBeforeUnixMs"`
}

// StopPlanRequest is the payload of the stop-plan command.
type StopPlanRequest struct {
	PlanID string `json:"planId"`
}

// DeletePlanRequest is the payload of the delete-plan command.
type DeletePlanRequest struct {
	PlanID string `json:"planId"`
}

// CreatePlan stores a new PLANNING plan. It refuses while an unfinished plan
// exists and replaces a finished one.
func (s *ShardReplicationFSM) CreatePlan(req *CreatePlanRequest) error {
	s.planLock.Lock()
	defer s.planLock.Unlock()

	if s.plan != nil && !s.plan.Finished(req.CreatedAtUnixMs) {
		return fmt.Errorf("plan %q is %s: %w", s.plan.ID, s.plan.State, ErrPlanExists)
	}
	s.replacePlanLocked(&Plan{
		ID:              req.PlanID,
		State:           PlanStatePlanning,
		SourceNodes:     slices.Clone(req.SourceNodes),
		TargetNodes:     slices.Clone(req.TargetNodes),
		Collections:     slices.Clone(req.Collections),
		Namespaces:      slices.Clone(req.Namespaces),
		CreatedAtUnixMs: req.CreatedAtUnixMs,
	})
	return nil
}

// CompletePlanning applies a planning run's outcome. logTerm is the Raft term of
// the log entry carrying the command; a run whose recorded term differs from it
// was started by a deposed leader.
func (s *ShardReplicationFSM) CompletePlanning(logTerm uint64, req *CompletePlanningRequest) error {
	s.planLock.Lock()
	defer s.planLock.Unlock()

	plan, err := s.planByIDLocked(req.PlanID)
	if err != nil {
		return err
	}
	if req.RunTerm != logTerm {
		return fmt.Errorf("plan %q: run term %d, entry term %d: %w", req.PlanID, req.RunTerm, logTerm, ErrPlanStaleRun)
	}
	if plan.RunID != "" && req.RunTerm == plan.RunTerm && req.RunID != plan.RunID {
		return fmt.Errorf("plan %q: run %q in term %d, recorded run %q: %w", req.PlanID, req.RunID, req.RunTerm, plan.RunID, ErrPlanStaleRun)
	}
	if plan.State != PlanStatePlanning {
		return fmt.Errorf("plan %q: complete planning from %s: %w", req.PlanID, plan.State, ErrPlanTransition)
	}
	if !req.Outcome.isPlanningOutcome() {
		return fmt.Errorf("plan %q: %q is not a planning outcome: %w", req.PlanID, req.Outcome, ErrPlanTransition)
	}

	next := plan.clone()
	next.State = req.Outcome
	next.RunID = req.RunID
	next.RunTerm = req.RunTerm
	next.MeasuredAtUnixMs = req.MeasuredAtUnixMs
	next.Nodes = slices.Clone(req.Nodes)
	next.Reason = req.Reason
	next.Baseline = slices.Clone(req.Baseline)
	next.Goal = slices.Clone(req.Goal)
	next.MoveCount = req.MoveCount
	next.PlanHash = req.PlanHash
	next.ApproveBeforeUnixMs = req.ApproveBeforeUnixMs
	s.replacePlanLocked(&next)
	return nil
}

// StopPlan moves the plan to STOPPED, only from PLANNING.
func (s *ShardReplicationFSM) StopPlan(req *StopPlanRequest) error {
	s.planLock.Lock()
	defer s.planLock.Unlock()

	plan, err := s.planByIDLocked(req.PlanID)
	if err != nil {
		return err
	}
	if plan.State != PlanStatePlanning {
		return fmt.Errorf("plan %q: stop from %s: %w", req.PlanID, plan.State, ErrPlanTransition)
	}
	next := plan.clone()
	next.State = PlanStateStopped
	s.replacePlanLocked(&next)
	return nil
}

// DeletePlan removes the plan, only from PROPOSED or an end state.
func (s *ShardReplicationFSM) DeletePlan(req *DeletePlanRequest) error {
	s.planLock.Lock()
	defer s.planLock.Unlock()

	plan, err := s.planByIDLocked(req.PlanID)
	if err != nil {
		return err
	}
	if plan.State != PlanStateProposed && !plan.State.isEnd() {
		return fmt.Errorf("plan %q: delete from %s: %w", req.PlanID, plan.State, ErrPlanTransition)
	}
	s.replacePlanLocked(nil)
	return nil
}

// GetPlan returns a clone of the current plan, sharing no memory with FSM
// state, or false when none exists.
func (s *ShardReplicationFSM) GetPlan() (Plan, bool) {
	s.planLock.Lock()
	defer s.planLock.Unlock()

	if s.plan == nil {
		return Plan{}, false
	}
	return s.plan.clone(), true
}

// planByIDLocked returns the stored plan when its ID is id. Callers hold s.planLock.
func (s *ShardReplicationFSM) planByIDLocked(id string) (*Plan, error) {
	if s.plan == nil || s.plan.ID != id {
		return nil, fmt.Errorf("plan %q: %w", id, ErrPlanNotFound)
	}
	return s.plan, nil
}

// replacePlanLocked stores p and resets the gauge to one series for p's state,
// or no series when p is nil. Every write publishes a new plan through this
// function; a published plan is never mutated in place. The caller must not
// keep using p. Callers hold s.planLock.
func (s *ShardReplicationFSM) replacePlanLocked(p *Plan) {
	s.plan = p
	s.plansByStateGauge.Reset()
	if p != nil {
		s.plansByStateGauge.WithLabelValues(string(p.State)).Set(1)
	}
}

func (s *ShardReplicationFSM) snapshotPlan() *Plan {
	s.planLock.Lock()
	defer s.planLock.Unlock()

	if s.plan == nil {
		return nil
	}
	p := s.plan.clone()
	return &p
}

// restorePlan replaces the plan with p from a snapshot; nil clears it. Callers
// do not hold s.opsLock.
func (s *ShardReplicationFSM) restorePlan(p *Plan) {
	s.planLock.Lock()
	defer s.planLock.Unlock()

	s.replacePlanLocked(p)
}
