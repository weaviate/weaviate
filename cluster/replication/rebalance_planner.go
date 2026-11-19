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
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	rebalanceLogAction = "replication_rebalance"

	// capacityTolerance is the largest relative spread, (max−min)/max, allowed
	// between listed nodes' disk totals and between their memory limits.
	capacityTolerance = 0.05
	// balanceTolerance is the largest standard deviation of disk utilisation and
	// of count ratio for which the baseline counts as balanced.
	balanceTolerance = 0.05

	// measurementTimeout limits each collection attempt on a node.
	measurementTimeout = 30 * time.Second
	// A failed collection round is retried after a backoff. The backoff starts
	// at collectionBackoffMin and doubles up to collectionBackoffMax.
	collectionBackoffMin = time.Second
	collectionBackoffMax = 30 * time.Second
	// collectionLimit is how long a node may keep failing collection rounds in
	// a row before the plan ends PLANNING_FAILED.
	collectionLimit = 10 * time.Minute

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

// rebalanceProposer is the Raft surface the planner needs. *cluster.Raft
// satisfies it.
type rebalanceProposer interface {
	CompleteRebalancePlanning(ctx context.Context, req *CompletePlanningRequest) error
}

// rebalanceSchemaReader holds the schema reads the planner makes.
// schema.SchemaReader satisfies it.
type rebalanceSchemaReader interface {
	ReadSchema(reader func(models.Class, uint64)) error
	Read(class string, retryIfClassNotFound bool, reader func(*models.Class, *sharding.State) error) error
}

// rebalanceDrafting is the draft task surface the planner needs. *cluster.Raft
// satisfies it. RebalanceDraftTasks reads this node's FSM.
type rebalanceDrafting interface {
	AddRebalanceDraftTask(ctx context.Context, planID string, nodes []string) error
	TerminateRebalanceDraftTask(ctx context.Context, planID string, version uint64) error
	RebalanceDraftTasks() []DraftTask
}

// DraftUnitStatus is the status of one listed node's draft unit.
type DraftUnitStatus string

const (
	DraftUnitPending    DraftUnitStatus = "PENDING"
	DraftUnitInProgress DraftUnitStatus = "IN_PROGRESS"
	DraftUnitCompleted  DraftUnitStatus = "COMPLETED"
	DraftUnitFailed     DraftUnitStatus = "FAILED"
)

// DraftTask is a rebalance draft task as one node's FSM holds it. The task ID
// is the plan ID.
type DraftTask struct {
	PlanID   string
	Version  uint64
	Terminal bool
	Error    string
	// Units holds each listed node's unit status, keyed by node name.
	Units map[string]DraftUnitStatus
}

// RebalancePlannerParams are the dependencies of a RebalancePlanner. Every
// field except Measure is required; NewRebalancePlanner rejects a nil one.
type RebalancePlannerParams struct {
	Logger     *logrus.Logger
	FSM        *ShardReplicationFSM
	Proposer   rebalanceProposer
	Drafting   rebalanceDrafting
	Clock      clockwork.Clock
	Registerer prometheus.Registerer

	// Measure reads one node's resources and its draft for task taskID at
	// version. When nil, every run that reaches collection ends
	// PLANNING_FAILED with the reason "measurement unavailable".
	Measure func(ctx context.Context, node, taskID string, version uint64) (types.RebalanceNodeMeasurement, error)
	// Schema lists the collections and reads their sharding states.
	Schema rebalanceSchemaReader
	// CurrentTerm is the Raft term the run records; the FSM refuses a completion
	// whose log entry has another term.
	CurrentTerm func() uint64
	// ReadyToPlan is IsLeader() && Ready() && FSMHasCaughtUp().
	ReadyToPlan func() bool
	// Enabled is true when both replica movement and rebalancing are enabled.
	Enabled func() bool
}

// RebalancePlanner drafts and collects the plan's listed nodes and completes
// planning with a verdict. Its loop runs on every node; a pass on a follower
// returns at the ReadyToPlan gate.
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
	case p.Drafting == nil:
		return nil, errors.New("rebalance planner: Drafting is required")
	case p.Clock == nil:
		return nil, errors.New("rebalance planner: Clock is required")
	case p.Registerer == nil:
		return nil, errors.New("rebalance planner: Registerer is required")
	case p.Schema == nil:
		return nil, errors.New("rebalance planner: Schema is required")
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
			Help:      "Duration of rebalance planning runs, from collection start to the proposal's return",
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

// RunOnce takes the current plan one step. It does nothing unless this node is
// ready to plan. First it ends every running draft task whose plan is not the
// PLANNING plan. Then, with both flags on and the plan PLANNING, it acts on the
// plan's latest draft task:
//   - no task: it starts one.
//   - every unit COMPLETED: it collects the drafts and completes planning.
//   - task ended with a unit not COMPLETED: it fails the plan.
//   - otherwise it waits.
func (rp *RebalancePlanner) RunOnce(ctx context.Context) {
	// Read the term before the leadership check. If this node loses
	// leadership after the check, the run keeps the old term, and the FSM
	// refuses it as stale.
	term := rp.p.CurrentTerm()
	if !rp.p.ReadyToPlan() {
		return
	}
	plan, ok := rp.p.FSM.GetPlan()
	planningID := ""
	if ok && plan.State == PlanStatePlanning {
		planningID = plan.ID
	}

	// The sweep runs even with a flag off, so a stop made after a flag is
	// turned off still ends drafting. If a terminate fails, RunOnce returns
	// before it adds a task. So no new task starts while an older one may
	// still run.
	tasks := rp.p.Drafting.RebalanceDraftTasks()
	if !rp.sweep(ctx, tasks, planningID) {
		return
	}

	if !rp.p.Enabled() || planningID == "" {
		return
	}

	logger := rp.logger.WithField("planId", plan.ID)
	task, found := latestDraftTask(tasks, plan.ID)
	if !found {
		if err := rp.p.Drafting.AddRebalanceDraftTask(ctx, plan.ID, slices.Concat(plan.SourceNodes, plan.TargetNodes)); err != nil {
			logger.Errorf("could not start rebalance drafting: %v", err)
			return
		}
		logger.Info("rebalance drafting started")
		return
	}
	unfinished := unfinishedNodes(task)
	if len(unfinished) > 0 && !task.Terminal {
		// The task's next unit change or its end wakes the planner again.
		return
	}

	start := rp.p.Clock.Now()
	req := &CompletePlanningRequest{
		PlanID:           plan.ID,
		RunID:            uuid.NewString(),
		RunTerm:          term,
		MeasuredAtUnixMs: start.UnixMilli(),
	}
	logger = logger.WithFields(logrus.Fields{
		"runId":   req.RunID,
		"runTerm": req.RunTerm,
	})
	logger.Info("rebalance planning started")

	halt := ""
	if len(unfinished) > 0 {
		req.Outcome = PlanStatePlanningFailed
		req.Reason = fmt.Sprintf("drafting did not finish on nodes: %s: %s", strings.Join(unfinished, ", "), task.Error)
	} else {
		halt = rp.assess(ctx, plan, task, req, logger)
	}
	var result string
	switch err := ctx.Err(); {
	case err != nil:
		// The measurements of a cancelled run reflect the cancellation, not
		// the nodes.
		logger.Infof("rebalance planning cancelled before proposing: %v", err)
		result = planningResultCancelled
	case halt != "":
		result = halt
	default:
		result = rp.propose(ctx, req, logger)
	}

	rp.runs.WithLabelValues(result).Inc()
	rp.duration.Observe(rp.p.Clock.Since(start).Seconds())
}

// sweep terminates every running draft task whose plan ID is not planningID.
// It returns false at the first terminate that fails.
func (rp *RebalancePlanner) sweep(ctx context.Context, tasks []DraftTask, planningID string) bool {
	for _, task := range tasks {
		if task.Terminal || task.PlanID == planningID {
			continue
		}
		logger := rp.logger.WithFields(logrus.Fields{"planId": task.PlanID, "version": task.Version})
		if err := rp.p.Drafting.TerminateRebalanceDraftTask(ctx, task.PlanID, task.Version); err != nil {
			logger.Errorf("could not end rebalance drafting of a plan that is no longer planning: %v", err)
			return false
		}
		logger.Info("rebalance drafting ended, the plan is no longer planning")
	}
	return true
}

// latestDraftTask returns the highest-version task of planID.
func latestDraftTask(tasks []DraftTask, planID string) (DraftTask, bool) {
	var latest DraftTask
	found := false
	for _, task := range tasks {
		if task.PlanID == planID && (!found || task.Version > latest.Version) {
			latest, found = task, true
		}
	}
	return latest, found
}

// unfinishedNodes returns the sorted nodes whose unit is not COMPLETED.
func unfinishedNodes(task DraftTask) []string {
	var nodes []string
	for node, status := range task.Units {
		if status != DraftUnitCompleted {
			nodes = append(nodes, node)
		}
	}
	slices.Sort(nodes)
	return nodes
}

// assess collects the listed nodes' drafts and sets req's outcome, reason,
// rows, measurement time and baseline, and returns "". If collection halts,
// assess sets no outcome and returns collect's runs_total result.
func (rp *RebalancePlanner) assess(ctx context.Context, plan Plan, task DraftTask, req *CompletePlanningRequest,
	logger *logrus.Entry,
) string {
	nodes := slices.Concat(plan.SourceNodes, plan.TargetNodes)

	if rp.p.Measure == nil {
		req.Nodes = make([]types.RebalanceNodeMeasurement, len(nodes))
		for i, node := range nodes {
			req.Nodes[i] = types.RebalanceNodeMeasurement{Node: node, Error: measurementUnavailable}
		}
		req.Outcome, req.Reason = PlanStatePlanningFailed, measurementUnavailable
		return ""
	}

	if halt := rp.collect(ctx, plan.ID, task, nodes, req, logger); halt != "" {
		return halt
	}
	// Draft sizes never enter the Raft log.
	for i := range req.Nodes {
		req.Nodes[i].Shards = nil
	}
	if req.Outcome != "" {
		return ""
	}
	if reason := unmetPrecondition(req.Nodes); reason != "" {
		req.Outcome, req.Reason = PlanStatePlanningFailed, reason
		return ""
	}

	replicas, err := rp.countReplicas(req.Nodes)
	if err != nil {
		req.Outcome, req.Reason = PlanStatePlanningFailed, err.Error()
		return ""
	}
	req.Baseline = baseline(req.Nodes, replicas)
	if balanced(req.Baseline) {
		req.Outcome = PlanStateNotNeeded
		return ""
	}
	req.Outcome = PlanStateProposed
	req.ApproveBeforeUnixMs = proposedApproveBeforeUnixMs
	return ""
}

// collect runs collection rounds until a round reads every node. Each round
// sets req.Nodes and req.MeasuredAtUnixMs. If a node keeps failing rounds for
// collectionLimit, collect sets the PLANNING_FAILED outcome instead, and
// req.Nodes keeps the last round's rows. Both cases return "".
// collect can also halt with no outcome set. It then returns the runs_total
// result. CANCELLED means ctx ended. STOPPED means that after a backoff the
// plan is no longer planID in PLANNING. STALE means that after a backoff this
// node is no longer ready to plan.
func (rp *RebalancePlanner) collect(ctx context.Context, planID string, task DraftTask, nodes []string,
	req *CompletePlanningRequest, logger *logrus.Entry,
) string {
	start := rp.p.Clock.Now()
	failingSince := make(map[string]time.Time, len(nodes))
	backoff := collectionBackoffMin
	for round := 1; ; round++ {
		roundStart := rp.p.Clock.Now()
		req.Nodes, req.MeasuredAtUnixMs = rp.measure(ctx, nodes, task), roundStart.UnixMilli()

		var stalled, failures []string
		for _, r := range req.Nodes {
			if r.Error == "" {
				delete(failingSince, r.Node)
				continue
			}
			failures = append(failures, r.Node+": "+r.Error)
			since, failing := failingSince[r.Node]
			if !failing {
				since = roundStart
				failingSince[r.Node] = since
			}
			if rp.p.Clock.Since(since) >= collectionLimit {
				stalled = append(stalled, r.Node)
			}
		}
		switch {
		case len(failingSince) == 0:
			if round > 1 {
				logger.WithFields(logrus.Fields{
					"rounds":  round,
					"elapsed": rp.p.Clock.Since(start).String(),
				}).Info("rebalance draft collection succeeded after failed rounds")
			}
			return ""
		case ctx.Err() != nil:
			return planningResultCancelled
		case len(stalled) > 0:
			req.Outcome = PlanStatePlanningFailed
			req.Reason = "could not collect drafts from nodes: " + strings.Join(stalled, ", ")
			return ""
		}

		logger.WithFields(logrus.Fields{
			"round":   round,
			"elapsed": rp.p.Clock.Since(start).String(),
			"backoff": backoff.String(),
		}).Warnf("rebalance draft collection round failed: %s", strings.Join(failures, "; "))
		if !rp.sleep(ctx, backoff) {
			return planningResultCancelled
		}
		backoff = min(2*backoff, collectionBackoffMax)
		if current, ok := rp.p.FSM.GetPlan(); !ok || current.ID != planID || current.State != PlanStatePlanning {
			logger.Info("rebalance planning ended without an outcome, the plan was stopped or replaced")
			return planningResultStopped
		}
		if !rp.p.ReadyToPlan() {
			logger.Info("rebalance planning ended without an outcome, this node is no longer ready to plan")
			return planningResultStale
		}
	}
}

// sleep waits d on the planner's clock. It returns false if ctx ends first.
func (rp *RebalancePlanner) sleep(ctx context.Context, d time.Duration) bool {
	timer := rp.p.Clock.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.Chan():
		return true
	}
}

// measure reads every node's resources and its draft of task, in parallel and
// under one measurementTimeout. Row i belongs to nodes[i].
func (rp *RebalancePlanner) measure(ctx context.Context, nodes []string, task DraftTask) []types.RebalanceNodeMeasurement {
	ctx, cancel := clockwork.WithTimeout(ctx, rp.p.Clock, measurementTimeout)
	defer cancel()

	rows := make([]types.RebalanceNodeMeasurement, len(nodes))
	eg := enterrors.NewErrorGroupWrapper(rp.logger)
	for i, node := range nodes {
		// If Measure panics, the row keeps this notMeasured error.
		rows[i] = types.RebalanceNodeMeasurement{Node: node, Error: notMeasured}
		eg.Go(func() error {
			m, err := rp.p.Measure(ctx, node, task.PlanID, task.Version)
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
// when every precondition holds. Callers pass only rows without an error.
func unmetPrecondition(rows []types.RebalanceNodeMeasurement) string {
	if len(rows) == 0 {
		// CreatePlan accepts empty node lists, and slices.Min below panics on an empty slice.
		return "no nodes listed"
	}
	// Each node goes only into the first group whose check it fails. The
	// groups are reported in the same order, so the reported group names
	// every node that fails its check.
	var noDisk, noWarning, unlimited []string
	for _, r := range rows {
		switch {
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

// countReplicas returns the replica count of every measured node. A shard
// counts once on every listed node in its BelongsToNodes; FROZEN shards hold no
// local data and are skipped. A collection deleted after it was listed is
// skipped; any other read error is returned.
func (rp *RebalancePlanner) countReplicas(rows []types.RebalanceNodeMeasurement) (map[string]int, error) {
	replicas := make(map[string]int, len(rows))
	for _, r := range rows {
		replicas[r.Node] = 0
	}

	var classes []string
	if err := rp.p.Schema.ReadSchema(func(c models.Class, _ uint64) {
		classes = append(classes, c.Class)
	}); err != nil {
		return nil, fmt.Errorf("could not list collections: %w", err)
	}
	for _, class := range classes {
		// The sharding state is live. It is read-locked only for the duration
		// of this callback, so every shard is counted inside it.
		err := rp.p.Schema.Read(class, false, func(_ *models.Class, state *sharding.State) error {
			for _, shard := range state.Physical {
				if shard.ActivityStatus() == models.TenantActivityStatusFROZEN {
					continue
				}
				for _, node := range shard.BelongsToNodes {
					if _, listed := replicas[node]; listed {
						replicas[node]++
					}
				}
			}
			return nil
		})
		if errors.Is(err, schema.ErrClassNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("could not read collection %q: %w", class, err)
		}
	}
	return replicas, nil
}

// baseline returns one row per measured node, with that node's count from replicas.
func baseline(rows []types.RebalanceNodeMeasurement, replicas map[string]int) []PlanNodeBalance {
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
