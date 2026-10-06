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
	"reflect"
	"slices"
	"sort"
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
}

// Planner is the backup.DedupePlanner of a licensed node.
type Planner struct {
	checkpointer      Checkpointer
	log               logrus.FieldLogger
	cutoffLead        time.Duration
	pollInterval      time.Duration
	convergenceBudget time.Duration
	planningSlack     time.Duration
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
// cancelled reports that the backup is ending, by a user cancel or the end of the caller's flow; nil means never cancelled.
// Once cancelled is true, planning returns early and records no fallback.
func (p *Planner) PlanDesignatedShards(ctx context.Context, classes []string, budget time.Duration,
	participants map[string]struct{}, preferred map[string]map[string]string, cancelled func() bool,
) *backup.DedupePlan {
	defer func(begin time.Time) {
		monitoring.GetMetrics().BackupDedupePlanningDurations.Observe(float64(time.Since(begin).Milliseconds()))
	}(time.Now())
	if cancelled == nil {
		cancelled = func() bool { return false }
	}
	if budget <= 0 {
		budget = p.convergenceBudget
	}
	budget = min(budget, _MaxDedupeConvergenceBudget)
	// Hard deadline: the request ctx has none, and a wedged peer RPC would otherwise stall planning while the op slot blocks every subsequent backup.
	// Scaled per class so wide backups' serial create fan-outs don't eat the convergence budget.
	ctx, cancel := context.WithTimeout(ctx, p.cutoffLead+budget+p.planningSlack+time.Duration(len(classes))*time.Second)
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
	plan := &backup.DedupePlan{
		Designations: make(map[string]map[string]string),
		Replicas:     make(map[string]map[string][]string),
	}

	candidates := make(map[string][]string, len(classes))
	for _, class := range classes {
		if !p.checkpointer.IsAsyncReplicationEnabled(ctx, class) {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("class_ineligible").Inc()
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				Info("replica dedupe: class skipped, async replication not enabled")
			continue
		}
		replicasByShard, err := p.checkpointer.ShardReplicas(ctx, class)
		if err != nil {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("class_ineligible").Inc()
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				Warnf("replica dedupe: class falls back to all-replica backup: %v", err)
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
	created := make([]string, 0, len(candidates))
	// Registered before any create so a panic mid-loop still deletes what exists; a fresh timeout per class keeps one slow node from starving later deletes.
	defer func() {
		base := context.WithoutCancel(ctx)
		for _, class := range created {
			cleanupCtx, cancelCleanup := context.WithTimeout(base, _DedupeCleanupTimeout)
			err := p.checkpointer.DeleteAsyncCheckpoints(cleanupCtx, class, candidates[class])
			cancelCleanup()
			if err != nil {
				p.log.WithField("action", backup.OpCreate).WithField("class", class).
					Warnf("replica dedupe: delete checkpoints: %v", err)
			}
		}
	}()
	for _, class := range candidateClasses {
		// Per-class cutoff: one shared cutoff would let serial creates on wide backups overrun the lead and silently reject later classes.
		cutoffMs := time.Now().Add(p.cutoffLead).UnixMilli()
		if err := p.checkpointer.CreateAsyncCheckpoints(ctx, class, cutoffMs, candidates[class]); err != nil {
			if cancelled() {
				return plan
			}
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("create_rpc_failed").Add(float64(len(candidates[class])))
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				Warnf("replica dedupe: class falls back to all-replica backup: create checkpoints: %v", err)
			delete(candidates, class)
			continue
		}
		cutoffs[class] = cutoffMs
		created = append(created, class)
	}
	plan.Cutoffs = cutoffs
	if len(candidates) == 0 {
		return plan
	}

	var latestCutoffMs int64
	for _, cutoffMs := range cutoffs {
		latestCutoffMs = max(latestCutoffMs, cutoffMs)
	}
	if !sleepUnlessCancelled(ctx, time.UnixMilli(latestCutoffMs), cancelled) {
		// A cancel kills the whole backup; anything else is the planning deadline silently degrading every candidate.
		if !cancelled() {
			remaining := 0
			for _, shards := range candidates {
				remaining += len(shards)
			}
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("planning_deadline").Add(float64(remaining))
			monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("fallback").Add(float64(plan.Fallback()))
			p.log.WithField("action", backup.OpCreate).
				Warnf("replica dedupe: planning aborted before cutoff, %d shards fall back to all-replica backup: %v", remaining, ctx.Err())
		}
		return plan
	}

	converged := p.pollConvergence(ctx, candidates, plan.Replicas, cutoffs, budget, cancelled)
	if cancelled() {
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
			Info("replica dedupe: planning complete")
	}
	monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("designated").Add(float64(plan.Designated()))
	monitoring.GetMetrics().BackupDedupeShards.WithLabelValues("fallback").Add(float64(plan.Fallback()))
	return plan
}

// pollConvergence polls until every candidate shard converges or drops, returning class -> shard -> replicas for converged shards.
func (p *Planner) pollConvergence(ctx context.Context, candidates map[string][]string,
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
	for firstPoll := true; len(pending) > 0; firstPoll = false {
		for class, shards := range pending {
			shardNames := make([]string, 0, len(shards))
			for shard := range shards {
				shardNames = append(shardNames, shard)
			}
			sort.Strings(shardNames)

			statuses, err := p.checkpointer.GetAsyncCheckpointNodeStatuses(ctx, class, shardNames)
			if cancelled() {
				return converged
			}
			if err != nil {
				monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("status_failed").Add(float64(len(shards)))
				p.log.WithField("action", backup.OpCreate).WithField("class", class).
					Warnf("replica dedupe: class falls back to all-replica backup: checkpoint status: %v", err)
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
				if firstPoll && !replicaSetCompleteAtCutoff(entries, replicas[class][shard], cutoffs[class]) {
					missing++
					p.log.WithField("action", backup.OpCreate).WithField("class", class).WithField("shard", shard).
						Debug("replica dedupe: shard falls back, checkpoint missing on at least one replica")
					delete(shards, shard)
				}
			}
			if missing > 0 {
				monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("checkpoint_missing").Add(float64(missing))
				p.log.WithField("action", backup.OpCreate).WithField("class", class).WithField("shards", missing).
					Info("replica dedupe: shards fall back to all-replica backup, checkpoint missing on at least one replica")
			}
			if len(shards) == 0 {
				delete(pending, class)
			}
		}
		if len(pending) == 0 || time.Now().After(deadline) {
			break
		}
		if !sleepUnlessCancelled(ctx, time.Now().Add(p.pollInterval), cancelled) {
			break
		}
	}
	if cancelled() {
		return converged
	}
	for class, shards := range pending {
		if len(shards) > 0 {
			monitoring.GetMetrics().BackupDedupeFallbacks.WithLabelValues("not_converged").Add(float64(len(shards)))
			p.log.WithField("action", backup.OpCreate).WithField("class", class).
				WithField("unconverged", len(shards)).
				Info("replica dedupe: unconverged shards fall back to all-replica backup")
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
	at := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if e.CutoffMs == cutoffMs {
			at[e.Node] = struct{}{}
		}
	}
	for node := range uniqueNonEmpty(replicas) {
		if _, ok := at[node]; !ok {
			return false
		}
	}
	return true
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
