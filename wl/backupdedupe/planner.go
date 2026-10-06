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

// Package backupdedupe plans deduplicated backups: it checkpoints replicas, proves their convergence and designates one archiving node per shard.
// It is Weaviate-licensed (wl/LICENSE-WEAVIATE), unlike the BSD-3-Clause code outside wl/, and is only constructed on licensed nodes.
package backupdedupe

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// Checkpointer proves per-shard replica convergence; implemented by *db.DB.
type Checkpointer interface {
	// ShardReplicas returns shard name -> replica node names for class.
	ShardReplicas(ctx context.Context, class string) (map[string][]string, error)
	// IsAsyncReplicationEnabled is true when async replication keeps replicas consistent (also for RF=1, where it is irrelevant).
	IsAsyncReplicationEnabled(ctx context.Context, class string) bool
	CreateAsyncCheckpoints(ctx context.Context, class string, cutoffMs int64, shards []string) error
	DeleteAsyncCheckpoints(ctx context.Context, class string, shards []string) error
	GetAsyncCheckpointNodeStatuses(ctx context.Context, class string, shards []string) (map[string][]replica.AsyncCheckpointNodeStatus, error)
}

const (
	// Must exceed checkpoint-create fan-out latency: shards reject a past cutoff.
	_DedupeCutoffLead               = 10 * time.Second
	_DedupePollInterval             = 3 * time.Second
	_DefaultDedupeConvergenceBudget = 60 * time.Second
	_DedupeCleanupTimeout           = 10 * time.Second
	// Classes whose checkpoint RPCs run at once; each call already fans out per shard and replica.
	_DedupeClassConcurrency = 16
	// Caps the cleanup wall time so one hung replica can't pin the op slot for hours on wide backups.
	_DedupeMaxCleanupBudget = 2 * time.Minute
	// Caps the per-class-wave share of planning's hard deadline.
	_DedupeMaxFanoutAllowance = 5 * time.Minute
	// Headroom over lead+budget for the create/status fan-outs; the resulting deadline is planning's hard stop.
	_DedupePlanningSlack = 30 * time.Second
	// Mirrors the API's documented maximum, which is otherwise enforced only by generated swagger validation.
	_MaxDedupeConvergenceBudget = 600 * time.Second
)

// sentinelError keeps the sentinels constant: Go initialises a package-level var on every node at startup, licensed or not.
type sentinelError string

func (e sentinelError) Error() string { return string(e) }

const (
	// ErrNilCheckpointer is New's refusal of a Config without a Checkpointer.
	ErrNilCheckpointer sentinelError = "backupdedupe: nil checkpointer"
	// ErrNilLogger is New's refusal of a Config without a Logger.
	ErrNilLogger sentinelError = "backupdedupe: nil logger"
)

// Config configures a Planner; a zero duration takes its default.
type Config struct {
	Checkpointer      Checkpointer
	Logger            logrus.FieldLogger
	CutoffLead        time.Duration
	PollInterval      time.Duration
	ConvergenceBudget time.Duration
	PlanningSlack     time.Duration
	CleanupTimeout    time.Duration
}

// Planner is the backup.DedupePlanner of a licensed node.
type Planner struct {
	checkpointer      Checkpointer
	log               logrus.FieldLogger
	cutoffLead        time.Duration
	pollInterval      time.Duration
	convergenceBudget time.Duration
	planningSlack     time.Duration
	cleanupTimeout    time.Duration
}

// New returns a Planner, or an error instead of a Planner that would panic on its first call.
func New(cfg Config) (*Planner, error) {
	if isNil(cfg.Checkpointer) {
		return nil, ErrNilCheckpointer
	}
	if isNil(cfg.Logger) {
		return nil, ErrNilLogger
	}
	return &Planner{
		checkpointer:      cfg.Checkpointer,
		log:               cfg.Logger,
		cutoffLead:        orDefault(cfg.CutoffLead, _DedupeCutoffLead),
		pollInterval:      orDefault(cfg.PollInterval, _DedupePollInterval),
		convergenceBudget: orDefault(cfg.ConvergenceBudget, _DefaultDedupeConvergenceBudget),
		planningSlack:     orDefault(cfg.PlanningSlack, _DedupePlanningSlack),
		cleanupTimeout:    orDefault(cfg.CleanupTimeout, _DedupeCleanupTimeout),
	}, nil
}

func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// PlanDesignatedShards designates one archiving node per convergence-proven shard; failures only downgrade shards to all-replica fallback, and checkpoints are deleted before returning (archiving needs no live checkpoint).
// Designations only ever name members of participants: a designated non-participant would archive nothing while every replica skips.
// cancelled reports the operation's external cancel signal; nil means never cancelled.
func (p *Planner) PlanDesignatedShards(ctx context.Context, classes []string, budget time.Duration,
	participants map[string]struct{}, preferred map[string]map[string]string, cancelled func() bool,
) *backup.DedupePlan {
	defer func(begin time.Time) {
		monitoring.GetMetrics().BackupDedupePlanningDurations.Observe(float64(time.Since(begin).Milliseconds()))
	}(time.Now())
	if cancelled == nil {
		cancelled = func() bool { return false }
	}
	plan := &backup.DedupePlan{
		Designations: make(map[string]map[string]string),
		Replicas:     make(map[string]map[string][]string),
	}
	// Registered first so it runs last, after checkpoint cleanup: one line per run, whatever the class or tenant count.
	sum := &planSummary{classes: len(classes), missingByNode: map[string]int{}}
	defer func(begin time.Time) { p.logSummary(sum, plan, time.Since(begin), cancelled()) }(time.Now())
	if budget <= 0 {
		budget = p.convergenceBudget
	}
	budget = min(budget, _MaxDedupeConvergenceBudget)
	// Hard deadline: the request ctx has none, and a wedged peer RPC would otherwise stall planning while the op slot blocks every subsequent backup.
	ctx, cancel := context.WithTimeout(ctx, p.cutoffLead+budget+p.planningSlack+dedupeFanoutAllowance(len(classes)))
	defer cancel()
	// A user Cancel only flags the slot; propagate it into the ctx so in-flight checkpointer RPCs unblock.
	watchDone := make(chan struct{})
	defer close(watchDone)
	enterrors.GoWrapper(func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-watchDone:
				return
			case <-t.C:
				if cancelled() {
					cancel()
					return
				}
			}
		}
	}, p.log)

	candidates := make(map[string][]string, len(classes))
	for i, class := range classes {
		enabled := p.checkpointer.IsAsyncReplicationEnabled(ctx, class)
		// A dead ctx may have answered the check, so the class counts as unresolved rather than ineligible.
		if cancelled() || ctx.Err() != nil {
			p.stopEligibility(ctx, sum, plan, candidates, len(classes)-i, cancelled)
			return plan
		}
		if !enabled {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("class_ineligible").Inc()
			sum.asyncDisabled++
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				Debug("replica dedupe: class skipped, async replication not enabled")
			continue
		}
		replicasByShard, err := p.checkpointer.ShardReplicas(ctx, class)
		if err != nil && (cancelled() || (ctx.Err() != nil && errors.Is(err, ctx.Err()))) {
			p.stopEligibility(ctx, sum, plan, candidates, len(classes)-i, cancelled)
			return plan
		}
		if err != nil {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("class_ineligible").Inc()
			sum.replicas.add(class, 0, err)
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				Debugf("replica dedupe: class falls back to all-replica backup: %v", err)
			continue
		}
		var shards []string
		for shard, nodes := range replicasByShard {
			if len(uniqueNonEmpty(nodes)) >= 2 {
				shards = append(shards, shard)
				if plan.Replicas[class] == nil {
					plan.Replicas[class] = make(map[string][]string, len(replicasByShard))
				}
				plan.Replicas[class][shard] = nodes
			}
		}
		if len(shards) > 0 {
			sort.Strings(shards)
			candidates[class] = shards
		}
	}
	for _, shards := range candidates {
		plan.CandidateShards += len(shards)
	}
	if len(candidates) == 0 {
		return plan
	}

	candidateClasses := make([]string, 0, len(candidates))
	for class := range candidates {
		candidateClasses = append(candidateClasses, class)
	}
	sort.Strings(candidateClasses)

	cutoffs := make(map[string]int64, len(candidates))
	created := make(map[string][]string, len(candidates))
	// Registered before any create so a panic still deletes what exists.
	defer func() { p.deleteCheckpoints(ctx, created) }()
	results := p.createCheckpoints(ctx, candidateClasses, candidates)
	// A user Cancel seen here voids every create outcome, failures included.
	aborted := cancelled()
	for i, class := range candidateClasses {
		res := results[i]
		// A panicked create may have left checkpoints behind.
		if res.err == nil || res.panicked {
			created[class] = candidates[class]
		}
		if aborted {
			continue
		}
		// Planning's ctx died first: counted once below as a deadline fallback, not per class as an RPC failure.
		if res.err != nil && !res.panicked && ctx.Err() != nil && errors.Is(res.err, ctx.Err()) {
			aborted = true
			continue
		}
		if res.err != nil {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("create_rpc_failed").Add(float64(len(candidates[class])))
			sum.create.add(class, len(candidates[class]), res.err)
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				Debugf("replica dedupe: class falls back to all-replica backup: create checkpoints: %v", res.err)
			delete(candidates, class)
			continue
		}
		cutoffs[class] = res.cutoffMs
	}
	plan.Cutoffs = cutoffs
	if aborted || cancelled() {
		p.abortPlanning(ctx, sum, plan, candidates, cancelled)
		return plan
	}
	if len(candidates) == 0 {
		monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("fallback").Add(float64(plan.Fallback()))
		return plan
	}

	var latestCutoffMs int64
	for _, cutoffMs := range cutoffs {
		latestCutoffMs = max(latestCutoffMs, cutoffMs)
	}
	if !sleepUnlessCancelled(ctx, time.UnixMilli(latestCutoffMs), cancelled) {
		p.abortPlanning(ctx, sum, plan, candidates, cancelled)
		return plan
	}

	converged := p.pollConvergence(ctx, sum, candidates, plan.Replicas, cutoffs, budget, cancelled)
	// A user Cancel kills the backup, even after a converging final pass: it designates nothing and records no outcome.
	if cancelled() {
		p.abortPlanning(ctx, sum, plan, candidates, cancelled)
		return plan
	}

	loads := make(map[string]int)
	classNames := make([]string, 0, len(converged))
	for class := range converged {
		classNames = append(classNames, class)
	}
	sort.Strings(classNames)
	for _, class := range classNames {
		designations, sticky := assignDesignations(converged[class], loads, participants, preferred[class])
		plan.Designations[class] = designations
		p.log.WithField("action", backup.OpCreate).WithField("class", class).
			WithField("designated", len(designations)).
			WithField("sticky", sticky).
			WithField("fallback", len(candidates[class])-len(designations)).
			Debug("replica dedupe: class planning complete")
	}
	monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("designated").Add(float64(plan.Designated()))
	monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("fallback").Add(float64(plan.Fallback()))
	return plan
}

// stopEligibility aborts planning inside the class-eligibility loop.
// Unresolved classes have no known shard count, so only resolved candidates count as planning_deadline shards; the summary names the unresolved classes.
func (p *Planner) stopEligibility(ctx context.Context, sum *planSummary, plan *backup.DedupePlan, candidates map[string][]string, unresolved int, cancelled func() bool) {
	for _, shards := range candidates {
		plan.CandidateShards += len(shards)
	}
	sum.deadlineClasses += unresolved
	p.abortPlanning(ctx, sum, plan, candidates, cancelled)
}

// abortPlanning accounts every remaining candidate as fallen back when planning stops before the cutoff.
func (p *Planner) abortPlanning(ctx context.Context, sum *planSummary, plan *backup.DedupePlan, candidates map[string][]string, cancelled func() bool) {
	sum.stopped, sum.stopErr = true, ctx.Err()
	// A user Cancel kills the whole backup; anything else is the planning deadline silently degrading every candidate.
	if cancelled() {
		return
	}
	remaining := 0
	for _, shards := range candidates {
		remaining += len(shards)
	}
	sum.deadlineShards += remaining
	monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("planning_deadline").Add(float64(remaining))
	monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("fallback").Add(float64(plan.Fallback()))
}

// failureTally counts one failure category across classes, keeping the first error as the example.
type failureTally struct {
	classes, shards int
	firstClass      string
	firstErr        error
}

func (t *failureTally) add(class string, shards int, err error) {
	t.classes++
	t.shards += shards
	if t.firstErr == nil {
		t.firstClass, t.firstErr = class, err
	}
}

// planSummary collects one planning run's outcomes so it logs once, never once per class or tenant.
type planSummary struct {
	classes, asyncDisabled   int
	replicas, create, status failureTally
	missing, unconverged     int
	missingByNode            map[string]int
	deadlineShards           int
	deadlineClasses          int
	stopped                  bool
	stopErr                  error
}

// _DedupeSummaryMaxNodes caps the nodes named in the summary line.
const _DedupeSummaryMaxNodes = 5

// logSummary emits the run's single summary: Info when nothing degraded, Warn naming each failure category once otherwise.
func (p *Planner) logSummary(sum *planSummary, plan *backup.DedupePlan, took time.Duration, cancelled bool) {
	log := p.log.WithField("action", backup.OpCreate).
		WithField("classes", sum.classes).
		WithField("candidate_shards", plan.CandidateShards).
		WithField("designated", plan.Designated()).
		WithField("fallback", plan.Fallback()).
		WithField("took", took.String())
	if sum.asyncDisabled > 0 {
		log = log.WithField("async_disabled_classes", sum.asyncDisabled)
	}
	var problems []string
	for _, t := range []struct {
		what string
		t    failureTally
	}{{"replica lookup", sum.replicas}, {"checkpoint create", sum.create}, {"checkpoint status", sum.status}} {
		if t.t.classes > 0 {
			problems = append(problems, fmt.Sprintf("%s failed for %d classes (%d shards), first: class %q: %s",
				t.what, t.t.classes, t.t.shards, t.t.firstClass, replica.TruncatedError(t.t.firstErr)))
		}
	}
	if sum.missing > 0 {
		problems = append(problems, fmt.Sprintf("checkpoint missing for %d shards on %s", sum.missing, topNodes(sum.missingByNode, _DedupeSummaryMaxNodes)))
	}
	if sum.unconverged > 0 {
		problems = append(problems, fmt.Sprintf("%d shards did not converge within the budget", sum.unconverged))
	}
	switch {
	case sum.stopped && cancelled:
		log.Info("replica dedupe: planning cancelled")
		return
	case sum.stopped && sum.deadlineClasses > 0:
		problems = append(problems, fmt.Sprintf("planning deadline hit, %d shards fall back, %d classes unresolved: %v",
			sum.deadlineShards, sum.deadlineClasses, sum.stopErr))
	case sum.stopped:
		problems = append(problems, fmt.Sprintf("planning deadline hit, %d shards fall back: %v", sum.deadlineShards, sum.stopErr))
	}
	if len(problems) == 0 {
		log.Info("replica dedupe: planning complete")
		return
	}
	log.Warnf("replica dedupe: planning complete with fallbacks: %s", strings.Join(problems, "; "))
}

// topNodes renders the n nodes with the highest counts, busiest first, plus how many were left out.
func topNodes(counts map[string]int, n int) string {
	nodes := make([]string, 0, len(counts))
	for node := range counts {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if counts[nodes[i]] != counts[nodes[j]] {
			return counts[nodes[i]] > counts[nodes[j]]
		}
		return nodes[i] < nodes[j]
	})
	parts := make([]string, 0, min(n, len(nodes))+1)
	for i, node := range nodes {
		if i == n {
			parts = append(parts, fmt.Sprintf("+%d more nodes", len(nodes)-n))
			break
		}
		parts = append(parts, fmt.Sprintf("%s (%d shards)", node, counts[node]))
	}
	return strings.Join(parts, ", ")
}

// dedupeFanoutAllowance is planning's deadline headroom for the per-class create and status waves.
func dedupeFanoutAllowance(classes int) time.Duration {
	return min(time.Duration(dedupeClassWaves(classes))*time.Second, _DedupeMaxFanoutAllowance)
}

// dedupeCleanupBudget bounds the whole checkpoint cleanup: one per-class timeout per wave, capped.
func dedupeCleanupBudget(classes int, perClass time.Duration) time.Duration {
	return min(time.Duration(dedupeClassWaves(classes))*perClass, _DedupeMaxCleanupBudget)
}

func dedupeClassWaves(classes int) int {
	return (classes + _DedupeClassConcurrency - 1) / _DedupeClassConcurrency
}

type checkpointCreateResult struct {
	cutoffMs int64
	err      error
	panicked bool
}

// createCheckpoints creates each class's checkpoints concurrently; a panic becomes that class's error with panicked set.
func (p *Planner) createCheckpoints(ctx context.Context, classes []string, candidates map[string][]string) []checkpointCreateResult {
	results := make([]checkpointCreateResult, len(classes))
	eg := enterrors.NewErrorGroupWrapper(p.log)
	eg.SetLimit(_DedupeClassConcurrency)
	for i, class := range classes {
		shards := candidates[class]
		eg.Go(func() error {
			res := &results[i]
			// Planning already stopped: nothing to create, and an attempt would only burn a local fan-out.
			if err := ctx.Err(); err != nil {
				res.err = err
				return nil
			}
			returned := false
			res.err = enterrors.RunRecovered(p.log, func() error {
				// Taken right before the call: a queued class must not inherit a cutoff that the lead no longer covers.
				res.cutoffMs = time.Now().Add(p.cutoffLead).UnixMilli()
				err := p.checkpointer.CreateAsyncCheckpoints(ctx, class, res.cutoffMs, shards)
				returned = true
				return err
			})
			res.panicked = !returned
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		p.log.WithField("action", backup.OpCreate).Warnf("replica dedupe: create checkpoints fan-out: %v", err)
	}
	return results
}

// deleteCheckpoints deletes every created class's checkpoints concurrently, ignoring cancellation but bounded by dedupeCleanupBudget overall and cleanupTimeout per class.
func (p *Planner) deleteCheckpoints(ctx context.Context, created map[string][]string) {
	if len(created) == 0 {
		return
	}
	budgetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dedupeCleanupBudget(len(created), p.cleanupTimeout))
	defer cancel()
	var (
		mu         sync.Mutex
		failed     int
		firstClass string
		firstErr   error
	)
	eg := enterrors.NewErrorGroupWrapper(p.log)
	eg.SetLimit(_DedupeClassConcurrency)
	for class, shards := range created {
		eg.Go(func() error {
			classCtx, cancelClass := context.WithTimeout(budgetCtx, p.cleanupTimeout)
			defer cancelClass()
			err := p.checkpointer.DeleteAsyncCheckpoints(classCtx, class, shards)
			if err == nil {
				return nil
			}
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				Debugf("replica dedupe: delete checkpoints: %v", err)
			mu.Lock()
			defer mu.Unlock()
			failed++
			if firstErr == nil {
				firstClass, firstErr = class, err
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		p.log.WithField("action", backup.OpCreate).Warnf("replica dedupe: delete checkpoints fan-out: %v", err)
	}
	if failed > 0 {
		p.log.WithField("action", backup.OpCreate).WithField("failed", failed).WithField("classes", len(created)).
			Warnf("replica dedupe: delete checkpoints failed for %d of %d classes, first error: class %q: %s",
				failed, len(created), firstClass, replica.TruncatedError(firstErr))
	}
}

type checkpointStatusResult struct {
	statuses map[string][]replica.AsyncCheckpointNodeStatus
	err      error
}

// fetchCheckpointStatuses fetches every class's statuses concurrently; a panic becomes that class's error.
func (p *Planner) fetchCheckpointStatuses(ctx context.Context, classes []string, shardNames [][]string) []checkpointStatusResult {
	results := make([]checkpointStatusResult, len(classes))
	eg := enterrors.NewErrorGroupWrapper(p.log)
	eg.SetLimit(_DedupeClassConcurrency)
	for i, class := range classes {
		eg.Go(func() error {
			res := &results[i]
			// Planning already stopped: a dead-ctx call only burns a fan-out.
			if err := ctx.Err(); err != nil {
				res.err = err
				return nil
			}
			res.err = enterrors.RunRecovered(p.log, func() error {
				var err error
				res.statuses, err = p.checkpointer.GetAsyncCheckpointNodeStatuses(ctx, class, shardNames[i])
				return err
			})
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		p.log.WithField("action", backup.OpCreate).Warnf("replica dedupe: checkpoint status fan-out: %v", err)
	}
	return results
}

// pollConvergence polls until every candidate shard converges or drops, returning class -> shard -> replicas for converged shards.
func (p *Planner) pollConvergence(ctx context.Context, sum *planSummary, candidates map[string][]string,
	replicas map[string]map[string][]string, cutoffs map[string]int64, budget time.Duration, cancelled func() bool,
) map[string]map[string][]string {
	converged := make(map[string]map[string][]string)
	pending := make(map[string]map[string]struct{}, len(candidates))
	for class, shards := range candidates {
		pending[class] = make(map[string]struct{}, len(shards))
		for _, shard := range shards {
			pending[class][shard] = struct{}{}
		}
	}

	deadline := time.Now().Add(budget)
	aborted := false
	for firstPoll := true; len(pending) > 0; firstPoll = false {
		classes := make([]string, 0, len(pending))
		for class := range pending {
			classes = append(classes, class)
		}
		sort.Strings(classes)
		shardNamesByClass := make([][]string, len(classes))
		for i, class := range classes {
			shardNames := make([]string, 0, len(pending[class]))
			for shard := range pending[class] {
				shardNames = append(shardNames, shard)
			}
			sort.Strings(shardNames)
			shardNamesByClass[i] = shardNames
		}
		results := p.fetchCheckpointStatuses(ctx, classes, shardNamesByClass)
		// A user Cancel seen here voids the pass: no status failure or missing checkpoint is recorded.
		if cancelled() {
			aborted = true
			break
		}
		for i, class := range classes {
			shards, shardNames := pending[class], shardNamesByClass[i]
			statuses, err := results[i].statuses, results[i].err
			// Planning's ctx died first: the class stays pending and is accounted below as a deadline, not a status failure.
			if err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				aborted = true
				continue
			}
			if err != nil {
				monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("status_failed").Add(float64(len(shards)))
				sum.status.add(class, len(shards), err)
				p.log.WithField("action", backup.OpCreate).WithField("class", class).
					Debugf("replica dedupe: class falls back to all-replica backup: checkpoint status: %v", err)
				delete(pending, class)
				continue
			}
			missing := 0
			for _, shard := range shardNames {
				entries := statuses[shard]
				if convergedReplicaSet(entries, replicas[class][shard], cutoffs[class]) {
					if converged[class] == nil {
						converged[class] = make(map[string][]string)
					}
					converged[class][shard] = replicas[class][shard]
					delete(shards, shard)
					continue
				}
				// Checkpoint membership is final after create, so an entry absent on the first poll never appears later; only root equality is worth polling for.
				if !firstPoll {
					continue
				}
				if lacking := missingReplicasAtCutoff(entries, replicas[class][shard], cutoffs[class]); len(lacking) > 0 {
					missing++
					for _, node := range lacking {
						sum.missingByNode[node]++
					}
					p.log.WithField("action", backup.OpCreate).WithField("class", class).WithField("shard", shard).
						Debug("replica dedupe: shard falls back, checkpoint missing on at least one replica")
					delete(shards, shard)
				}
			}
			if missing > 0 {
				monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("checkpoint_missing").Add(float64(missing))
				sum.missing += missing
				p.log.WithField("action", backup.OpCreate).WithField("class", class).WithField("shards", missing).
					Debug("replica dedupe: shards fall back to all-replica backup, checkpoint missing on at least one replica")
			}
			if len(shards) == 0 {
				delete(pending, class)
			}
		}
		if aborted || len(pending) == 0 || time.Now().After(deadline) {
			break
		}
		if !sleepUnlessCancelled(ctx, time.Now().Add(p.pollInterval), cancelled) {
			break
		}
	}
	// A cancel or deadline in the poll sleep, or behind a status call that swallowed it, stops planning just like a dead-ctx status call.
	if len(pending) > 0 && (ctx.Err() != nil || cancelled()) {
		aborted = true
	}
	if aborted {
		sum.stopped, sum.stopErr = true, ctx.Err()
		if cancelled() {
			return converged
		}
	}
	reason := "not_converged"
	if aborted {
		reason = "planning_deadline"
	}
	for class, shards := range pending {
		if len(shards) > 0 {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues(reason).Add(float64(len(shards)))
			if aborted {
				sum.deadlineShards += len(shards)
			} else {
				sum.unconverged += len(shards)
			}
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				WithField("unconverged", len(shards)).
				Debug("replica dedupe: unconverged shards fall back to all-replica backup")
		}
	}
	return converged
}

// convergedReplicaSet is true when entries prove every replica identical at the cutoff; absent entries never mean agreement.
func convergedReplicaSet(entries []replica.AsyncCheckpointNodeStatus, replicas []string, wantCutoffMs int64) bool {
	replicaSet := uniqueNonEmpty(replicas)
	if len(replicaSet) < 2 {
		return false
	}
	byNode := make(map[string]replica.AsyncCheckpointNodeStatus, len(entries))
	for _, e := range entries {
		if _, ok := replicaSet[e.Node]; !ok {
			return false
		}
		if prev, ok := byNode[e.Node]; ok &&
			(prev.Root != e.Root || prev.CutoffMs != e.CutoffMs || prev.CreatedAt.UnixMilli() != e.CreatedAt.UnixMilli()) {
			return false
		}
		if e.CutoffMs != wantCutoffMs {
			return false
		}
		byNode[e.Node] = e
	}
	if len(byNode) != len(replicaSet) {
		return false
	}
	var first replica.AsyncCheckpointNodeStatus
	seen := false
	for _, e := range byNode {
		if !seen {
			first = e
			seen = true
			continue
		}
		// Millisecond precision: remote entries round-trip through created_at_ms, the local one keeps nanoseconds.
		if e.Root != first.Root || e.CreatedAt.UnixMilli() != first.CreatedAt.UnixMilli() {
			return false
		}
	}
	return first.Root != (hashtree.Digest{})
}

// replicaSetCompleteAtCutoff is true when every replica has an entry at the expected cutoff.
func replicaSetCompleteAtCutoff(entries []replica.AsyncCheckpointNodeStatus, replicas []string, cutoffMs int64) bool {
	return len(missingReplicasAtCutoff(entries, replicas, cutoffMs)) == 0
}

// missingReplicasAtCutoff returns the replicas without an entry at the expected cutoff.
func missingReplicasAtCutoff(entries []replica.AsyncCheckpointNodeStatus, replicas []string, cutoffMs int64) []string {
	at := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if e.CutoffMs == cutoffMs {
			at[e.Node] = struct{}{}
		}
	}
	var missing []string
	for node := range uniqueNonEmpty(replicas) {
		if _, ok := at[node]; !ok {
			missing = append(missing, node)
		}
	}
	return missing
}

// assignDesignations picks one archiving node per shard: an eligible preferred (base) designee outranks balance since only it can skip unchanged files, the rest go least-loaded (lexicographic ties, loads shared across classes).
// Shards with fewer than two participant replicas get no designation: naming a non-participant would orphan the shard, and a lone participant gains nothing.
func assignDesignations(shardReplicas map[string][]string, loads map[string]int, participants map[string]struct{}, preferred map[string]string) (map[string]string, int) {
	shards := make([]string, 0, len(shardReplicas))
	for shard := range shardReplicas {
		shards = append(shards, shard)
	}
	sort.Strings(shards)

	eligible := make(map[string][]string, len(shards))
	for _, shard := range shards {
		nodes := make([]string, 0, len(shardReplicas[shard]))
		for node := range uniqueNonEmpty(shardReplicas[shard]) {
			if _, ok := participants[node]; ok {
				nodes = append(nodes, node)
			}
		}
		if len(nodes) < 2 {
			continue
		}
		sort.Strings(nodes)
		eligible[shard] = nodes
	}

	out := make(map[string]string, len(shards))
	sticky := 0
	// sticky picks first so the least-loaded picks see their load
	for _, shard := range shards {
		want := preferred[shard]
		if want == "" || !slices.Contains(eligible[shard], want) {
			continue
		}
		loads[want]++
		out[shard] = want
		sticky++
	}
	for _, shard := range shards {
		nodes := eligible[shard]
		if nodes == nil {
			continue
		}
		if _, done := out[shard]; done {
			continue
		}
		best := nodes[0]
		for _, node := range nodes[1:] {
			if loads[node] < loads[best] {
				best = node
			}
		}
		loads[best]++
		out[shard] = best
	}
	return out, sticky
}

func uniqueNonEmpty(nodes []string) map[string]struct{} {
	set := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n != "" {
			set[n] = struct{}{}
		}
	}
	return set
}

// sleepUnlessCancelled is sleepUntil plus the operation's external cancel signal, polled once per second so Cancel works during planning.
func sleepUnlessCancelled(ctx context.Context, t time.Time, cancelled func() bool) bool {
	for {
		if cancelled() {
			return false
		}
		next := time.Now().Add(time.Second)
		if next.After(t) {
			next = t
		}
		if !sleepUntil(ctx, next) {
			return false
		}
		if !time.Now().Before(t) {
			return true
		}
	}
}

// sleepUntil blocks until t or ctx cancellation; false on cancellation.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
