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
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/replication/types"
	clusterTypes "github.com/weaviate/weaviate/cluster/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	rebalanceLogAction = "replication_rebalance"

	// capacityTolerance is the largest relative spread, (max−min)/max, allowed
	// between listed nodes' disk totals and between their memory limits.
	capacityTolerance = 0.05
	// balanceTolerance is the largest standard deviation of disk utilisation and
	// of count ratio for which the baseline counts as balanced.
	balanceTolerance   = 0.05
	measurementTimeout = 30 * time.Second

	// TODO(PR 2): replace with the approval window. Until moves exist, a
	// PROPOSED plan never expires; it stays until deleted.
	proposedApproveBeforeUnixMs int64 = math.MaxInt64

	measurementUnavailable = "measurement unavailable"
	notMeasured            = "not measured"

	// Values of the runs_total result label when the run is cancelled or the
	// proposal is refused or fails. A proposal that lands uses its outcome's
	// plan state.
	planningResultCancelled = "CANCELLED"
	planningResultStopped   = "STOPPED"
	planningResultStale     = "STALE"
	planningResultError     = "ERROR"
)

// RebalanceProposer is the Raft surface the planner needs. *cluster.Raft
// satisfies it.
type RebalanceProposer interface {
	CompleteRebalancePlanning(ctx context.Context, req *CompletePlanningRequest) error
}

// RebalancePlannerParams are the dependencies of a RebalancePlanner. Every
// field except Measure is required; NewRebalancePlanner rejects a nil one.
type RebalancePlannerParams struct {
	Logger     *logrus.Logger
	FSM        *ShardReplicationFSM
	Proposer   RebalanceProposer
	Clock      clockwork.Clock
	Registerer prometheus.Registerer

	// Measure reads one node's resources. When nil, every run ends
	// PLANNING_FAILED with the reason "measurement unavailable".
	Measure func(ctx context.Context, node string) (types.RebalanceNodeMeasurement, error)
	// States returns every collection's sharding state.
	States func() map[string]clusterTypes.ClassState
	// CurrentTerm is the Raft term the run records; the FSM refuses a completion
	// whose log entry has another term.
	CurrentTerm func() uint64
	// ReadyToPlan is IsLeader() && Ready() && FSMHasCaughtUp().
	ReadyToPlan func() bool
	// Enabled is true when both replica movement and rebalancing are enabled.
	Enabled func() bool
}

// RebalancePlanner measures the plan's listed nodes and completes planning
// with a verdict. Its loop runs on every node; a pass on a follower returns at
// the ReadyToPlan gate.
type RebalancePlanner struct {
	logger *logrus.Entry
	p      RebalancePlannerParams
	wake   chan struct{}

	runs     *prometheus.CounterVec
	duration prometheus.Histogram
}

func NewRebalancePlanner(p RebalancePlannerParams) (*RebalancePlanner, error) {
	switch {
	case p.Logger == nil:
		return nil, errors.New("rebalance planner: Logger is required")
	case p.FSM == nil:
		return nil, errors.New("rebalance planner: FSM is required")
	case p.Proposer == nil:
		return nil, errors.New("rebalance planner: Proposer is required")
	case p.Clock == nil:
		return nil, errors.New("rebalance planner: Clock is required")
	case p.Registerer == nil:
		return nil, errors.New("rebalance planner: Registerer is required")
	case p.States == nil:
		return nil, errors.New("rebalance planner: States is required")
	case p.CurrentTerm == nil:
		return nil, errors.New("rebalance planner: CurrentTerm is required")
	case p.ReadyToPlan == nil:
		return nil, errors.New("rebalance planner: ReadyToPlan is required")
	case p.Enabled == nil:
		return nil, errors.New("rebalance planner: Enabled getter is required")
	}

	auto := promauto.With(p.Registerer)
	return &RebalancePlanner{
		logger: p.Logger.WithFields(logrus.Fields{
			"component": rebalanceLogAction,
			"action":    rebalanceLogAction,
		}),
		p:    p,
		wake: make(chan struct{}, 1),
		runs: auto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "replication_rebalance_planning_runs_total",
			Help:      "Total number of rebalance planning runs, by outcome or by why the outcome did not land",
		}, []string{"result"}),
		duration: auto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "weaviate",
			Name:      "replication_rebalance_planning_duration_seconds",
			Help:      "Duration of rebalance planning runs, from measurement start to the proposal's return",
			Buckets:   []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60},
		}),
	}, nil
}

// Wake asks Run for a planning pass. It never blocks: a wake that arrives
// while one is already pending is dropped, and the pending one covers it.
func (rp *RebalancePlanner) Wake() {
	select {
	case rp.wake <- struct{}{}:
	default:
	}
}

// Run handles one wake at a time until ctx is cancelled.
func (rp *RebalancePlanner) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rp.wake:
		}
		rp.RunOnce(ctx)
	}
}

// RunOnce plans the current plan once. It does nothing unless both flags are
// on, this node is ready to plan, and the plan is PLANNING.
func (rp *RebalancePlanner) RunOnce(ctx context.Context) {
	if !rp.p.Enabled() {
		return
	}
	// Read the term before the leadership check. If this node loses
	// leadership after the check, the run keeps the old term, and the FSM
	// refuses it as stale.
	term := rp.p.CurrentTerm()
	if !rp.p.ReadyToPlan() {
		return
	}
	plan, ok := rp.p.FSM.GetPlan()
	if !ok || plan.State != PlanStatePlanning {
		return
	}

	start := rp.p.Clock.Now()
	req := &CompletePlanningRequest{
		PlanID:           plan.ID,
		RunID:            uuid.NewString(),
		RunTerm:          term,
		MeasuredAtUnixMs: start.UnixMilli(),
	}
	logger := rp.logger.WithFields(logrus.Fields{
		"planId":  req.PlanID,
		"runId":   req.RunID,
		"runTerm": req.RunTerm,
	})
	logger.Info("rebalance planning started")

	rp.assess(ctx, plan, req)
	var result string
	if err := ctx.Err(); err != nil {
		// The measurements of a cancelled run reflect the cancellation, not
		// the nodes.
		logger.Infof("rebalance planning cancelled before proposing %s: %v", req.Outcome, err)
		result = planningResultCancelled
	} else {
		result = rp.propose(ctx, req, logger)
	}

	rp.runs.WithLabelValues(result).Inc()
	rp.duration.Observe(rp.p.Clock.Since(start).Seconds())
}

// assess measures the listed nodes and sets req's outcome, reason, rows and baseline.
func (rp *RebalancePlanner) assess(ctx context.Context, plan Plan, req *CompletePlanningRequest) {
	nodes := slices.Concat(plan.SourceNodes, plan.TargetNodes)

	if rp.p.Measure == nil {
		req.Nodes = make([]types.RebalanceNodeMeasurement, len(nodes))
		for i, node := range nodes {
			req.Nodes[i] = types.RebalanceNodeMeasurement{Node: node, Error: measurementUnavailable}
		}
		req.Outcome, req.Reason = PlanStatePlanningFailed, measurementUnavailable
		return
	}

	req.Nodes = rp.measure(ctx, nodes)
	if reason := unmetPrecondition(req.Nodes); reason != "" {
		req.Outcome, req.Reason = PlanStatePlanningFailed, reason
		return
	}

	req.Baseline = baseline(req.Nodes, rp.p.States())
	if balanced(req.Baseline) {
		req.Outcome = PlanStateNotNeeded
		return
	}
	req.Outcome = PlanStateProposed
	req.ApproveBeforeUnixMs = proposedApproveBeforeUnixMs
}

// measure reads every node in parallel under one measurementTimeout. Row i belongs to nodes[i].
func (rp *RebalancePlanner) measure(ctx context.Context, nodes []string) []types.RebalanceNodeMeasurement {
	ctx, cancel := clockwork.WithTimeout(ctx, rp.p.Clock, measurementTimeout)
	defer cancel()

	rows := make([]types.RebalanceNodeMeasurement, len(nodes))
	eg := enterrors.NewErrorGroupWrapper(rp.logger)
	for i, node := range nodes {
		// If Measure panics, the row keeps this notMeasured error.
		rows[i] = types.RebalanceNodeMeasurement{Node: node, Error: notMeasured}
		eg.Go(func() error {
			m, err := rp.p.Measure(ctx, node)
			if err != nil {
				rows[i].Error = err.Error()
				return nil
			}
			m.Node = node
			rows[i] = m
			return nil
		}, node)
	}
	// Every goroutine returns nil, so Wait reports only a recovered panic,
	// which the wrapper has already logged.
	_ = eg.Wait()
	return rows
}

// unmetPrecondition returns why the measurements cannot be planned on, or ""
// when every precondition holds.
func unmetPrecondition(rows []types.RebalanceNodeMeasurement) string {
	if len(rows) == 0 {
		// CreatePlan accepts empty node lists, and slices.Min below panics on an empty slice.
		return "no nodes listed"
	}
	// Each node goes only into the first group whose check it fails. The
	// groups are reported in the same order, so the reported group names
	// every node that fails its check.
	var errored, noDisk, noWarning, unlimited []string
	for _, r := range rows {
		switch {
		case r.Error != "":
			errored = append(errored, fmt.Sprintf("%s: %s", r.Node, r.Error))
		case r.DiskTotalBytes <= 0:
			noDisk = append(noDisk, r.Node)
		case r.DiskWarningPercentage <= 0:
			// A non-positive warning percentage disables the disk warning
			// (adapters/repos/db/resource_use.go), and disk utilisation
			// divides by it.
			noWarning = append(noWarning, r.Node)
		case r.MemoryLimitBytes == math.MaxInt64:
			unlimited = append(unlimited, r.Node)
		}
	}
	if len(errored) > 0 {
		return "could not measure nodes due to error: " + strings.Join(errored, ", ")
	}
	if len(noDisk) > 0 {
		return "no disk capacity on nodes: " + strings.Join(noDisk, ", ")
	}
	if len(noWarning) > 0 {
		return "disk usage warning disabled on nodes: " + strings.Join(noWarning, ", ")
	}
	if len(unlimited) > 0 {
		return "no memory limit on nodes: " + strings.Join(unlimited, ", ")
	}

	field := func(get func(types.RebalanceNodeMeasurement) int64) []int64 {
		out := make([]int64, len(rows))
		for i, r := range rows {
			out[i] = get(r)
		}
		return out
	}
	if !withinCapacityTolerance(field(func(r types.RebalanceNodeMeasurement) int64 { return r.DiskTotalBytes })) {
		return fmt.Sprintf("disk capacity differs by more than %.0f%% across nodes", capacityTolerance*100)
	}
	if !withinCapacityTolerance(field(func(r types.RebalanceNodeMeasurement) int64 { return r.MemoryLimitBytes })) {
		return fmt.Sprintf("memory limit differs by more than %.0f%% across nodes", capacityTolerance*100)
	}
	warning := field(func(r types.RebalanceNodeMeasurement) int64 { return r.DiskWarningPercentage })
	if slices.Min(warning) != slices.Max(warning) {
		return "disk warning percentage differs across nodes"
	}
	return ""
}

// withinCapacityTolerance reports whether (max−min)/max ≤ capacityTolerance.
// values is non-empty.
func withinCapacityTolerance(values []int64) bool {
	lo, hi := slices.Min(values), slices.Max(values)
	return float64(hi-lo) <= capacityTolerance*float64(hi)
}

// baseline returns one row per measured node. A shard counts once on every
// listed node in its BelongsToNodes; FROZEN shards hold no local data and are
// skipped.
func baseline(rows []types.RebalanceNodeMeasurement, states map[string]clusterTypes.ClassState) []PlanNodeBalance {
	replicas := make(map[string]int, len(rows))
	for _, r := range rows {
		replicas[r.Node] = 0
	}
	for _, state := range states {
		for _, shard := range state.Shards.Physical {
			if shard.ActivityStatus() == models.TenantActivityStatusFROZEN {
				continue
			}
			for _, node := range shard.BelongsToNodes {
				if _, listed := replicas[node]; listed {
					replicas[node]++
				}
			}
		}
	}

	out := make([]PlanNodeBalance, len(rows))
	for i, r := range rows {
		used := float64(r.DiskTotalBytes - r.DiskAvailableBytes)
		usable := float64(r.DiskTotalBytes) * float64(r.DiskWarningPercentage) / 100
		out[i] = PlanNodeBalance{
			Node:            r.Node,
			DiskUtilisation: used / usable,
			ReplicaCount:    replicas[r.Node],
		}
	}
	return out
}

// balanced reports whether the standard deviations of disk utilisation and of
// count ratio (replicas over the mean replica count, 0 when that mean is 0)
// are both within balanceTolerance.
func balanced(rows []PlanNodeBalance) bool {
	disk := make([]float64, len(rows))
	counts := make([]float64, len(rows))
	for i, r := range rows {
		disk[i] = r.DiskUtilisation
		counts[i] = float64(r.ReplicaCount)
	}
	ratios := make([]float64, len(rows))
	if m := mean(counts); m != 0 {
		for i, c := range counts {
			ratios[i] = c / m
		}
	}
	return stddev(disk) <= balanceTolerance && stddev(ratios) <= balanceTolerance
}

func mean(xs []float64) float64 {
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// stddev is the population standard deviation of xs.
func stddev(xs []float64) float64 {
	m := mean(xs)
	sum := 0.0
	for _, x := range xs {
		sum += (x - m) * (x - m)
	}
	return math.Sqrt(sum / float64(len(xs)))
}

// propose completes planning with req and returns the runs_total result.
func (rp *RebalancePlanner) propose(ctx context.Context, req *CompletePlanningRequest, logger *logrus.Entry) string {
	err := rp.p.Proposer.CompleteRebalancePlanning(ctx, req)
	switch {
	case err == nil:
		logger.WithFields(logrus.Fields{
			"outcome": req.Outcome,
			"reason":  req.Reason,
		}).Infof("rebalance planning ended %s", req.Outcome)
		return string(req.Outcome)
	case errors.Is(err, ErrPlanTransition), errors.Is(err, ErrPlanNotFound):
		logger.Infof("rebalance planning outcome %s discarded, the plan was stopped or replaced: %v", req.Outcome, err)
		return planningResultStopped
	case errors.Is(err, ErrPlanStaleRun):
		logger.Warnf("rebalance planning outcome %s discarded, the run is stale: %v", req.Outcome, err)
		return planningResultStale
	default:
		logger.Errorf("could not complete rebalance planning with %s: %v", req.Outcome, err)
		return planningResultError
	}
}
