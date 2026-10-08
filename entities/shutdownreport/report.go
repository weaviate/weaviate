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

// Package shutdownreport collects what a node shutdown did and logs it as one
// summary line plus one warning per kind of anomaly.
package shutdownreport

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// Each timed shutdown step is logged as <name>_ms.
const (
	StepBatchDrain         = "batch_drain"
	StepLeaveCluster       = "leave_cluster"
	StepWaitGRPCStop       = "wait_grpc_stop"
	StepDrainExports       = "drain_exports"
	StepStopTelemetry      = "stop_telemetry"
	StepCloseTaskScheduler = "close_task_scheduler"
	StepPreDBSteps         = "pre_db_steps"
	StepInternalClose      = "internal_close"
	StepClusterClose       = "cluster_close"
	StepCloseDBUsers       = "close_db_users"
	StepCloseModules       = "close_modules"
	StepCloseConnManagers  = "close_conn_managers"
	StepShutdownOTEL       = "shutdown_otel"
	StepFlushSentry        = "flush_sentry"
	StepDB                 = "db"
)

var allSteps = []string{
	StepBatchDrain, StepLeaveCluster, StepWaitGRPCStop, StepDrainExports, StepStopTelemetry,
	StepCloseTaskScheduler, StepPreDBSteps, StepInternalClose, StepClusterClose, StepCloseDBUsers,
	StepCloseModules, StepCloseConnManagers, StepShutdownOTEL, StepFlushSentry, StepDB,
}

// clientDrain names the drain of client requests, which starts in Begin and
// ends in RESTDrained, two callbacks Time cannot wrap as one step. Its logged
// duration ends at the later of RESTDrained and GRPCStopped.
const clientDrain = "client_drain"

// Thresholds are the durations above which Log warns about a phase.
type Thresholds struct {
	ClientDrain, InternalClose, DB, Total time.Duration
}

func DefaultThresholds() Thresholds {
	return Thresholds{
		ClientDrain:   10 * time.Second,
		InternalClose: 5 * time.Second,
		DB:            20 * time.Second,
		Total:         30 * time.Second,
	}
}

// Report collects what one shutdown did. Every method is safe on a nil *Report and then
// records nothing, so callers whose ctx carries no report pay a nil check.
type Report struct {
	node string

	mu          sync.Mutex
	begun       time.Time
	restDrained time.Duration
	grpcStopped time.Duration
	steps       map[string]time.Duration
	running     map[string]struct{}
	clients     clientOutcome

	slowestIndexes, slowestShards slowest

	indexesTotal, indexesDone              atomic.Int64
	shardsStarted, shardsTornDown          atomic.Int64
	shardsStillInUse, hashtreeDumpsSkipped atomic.Int64
	buckets                                [bucketOutcomes]atomic.Int64
}

// BucketOutcome is how a bucket's shutdown ended.
type BucketOutcome int

const (
	BucketFlushed BucketOutcome = iota
	BucketWALReused
	BucketCtxExpired
	BucketFailed
	bucketOutcomes
)

// clientOutcome is how the REST and gRPC servers stopped serving clients.
type clientOutcome struct {
	grpcStopTook     time.Duration
	grpcForcedStop   bool
	restForcedClose  bool
	restStillRunning int64

	restUnavailable, grpcUnavailable, grpcWebUnavailable int64
}

func New(node string) *Report {
	return &Report{
		node:    node,
		steps:   map[string]time.Duration{},
		running: map[string]struct{}{},
	}
}

type reportKey struct{}

// With returns a ctx carrying r, which From returns.
func With(ctx context.Context, r *Report) context.Context {
	return context.WithValue(ctx, reportKey{}, r)
}

// From returns the report set by With, or nil.
func From(ctx context.Context) *Report {
	r, _ := ctx.Value(reportKey{}).(*Report)
	return r
}

// Begin starts the clock. Only the first call counts.
func (r *Report) Begin() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.begun.IsZero() {
		return
	}
	r.begun = time.Now()
	r.running[clientDrain] = struct{}{}
}

// RESTDrained marks the moment the REST servers stopped serving.
func (r *Report) RESTDrained() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restDrained = r.sinceBegin()
	delete(r.running, clientDrain)
}

// GRPCStopped records how long the gRPC server took to stop and whether the
// stop had to be forced.
func (r *Report) GRPCStopped(took time.Duration, forced bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.grpcStopped = r.sinceBegin()
	r.clients.grpcStopTook = took
	r.clients.grpcForcedStop = forced
}

// CallsUnavailable records the calls answered as unavailable, both those
// refused after shutdown began and those cut while running.
func (r *Report) CallsUnavailable(rest, grpc, grpcWeb int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients.restUnavailable = rest
	r.clients.grpcUnavailable = grpc
	r.clients.grpcWebUnavailable = grpcWeb
}

// RESTForceClosed records that the HTTP servers were closed while still busy,
// and how many handlers were still running after the wait for them.
func (r *Report) RESTForceClosed(stillRunning int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients.restForcedClose = true
	r.clients.restStillRunning = stillRunning
}

// Time runs fn and records its duration under step, also when fn panics.
func (r *Report) Time(step string, fn func()) {
	defer r.StartPhase(step)()
	fn()
}

// StartPhase marks step as running until the returned func is called, which
// records its duration.
func (r *Report) StartPhase(step string) (end func()) {
	if r == nil {
		return func() {}
	}
	start := time.Now()
	r.mu.Lock()
	r.running[step] = struct{}{}
	r.mu.Unlock()
	return func() {
		took := time.Since(start)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.steps[step] = took
		delete(r.running, step)
	}
}

func (r *Report) SetIndexTotal(n int) {
	if r == nil {
		return
	}
	r.indexesTotal.Store(int64(n))
}

func (r *Report) IndexDone(id string, took time.Duration) {
	if r == nil {
		return
	}
	r.indexesDone.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slowestIndexes.add(took, func() string { return id })
}

func (r *Report) ShardTeardownStarted() {
	if r == nil {
		return
	}
	r.shardsStarted.Add(1)
}

// ShardTornDown counts a teardown ShardTeardownStarted counted, failed or not.
func (r *Report) ShardTornDown(index, shard string, took time.Duration) {
	if r == nil {
		return
	}
	r.shardsTornDown.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slowestShards.add(took, func() string { return index + "/" + shard })
}

// ShardStillInUse counts a shard whose shutdown gave up because requests still
// held it.
func (r *Report) ShardStillInUse() {
	if r == nil {
		return
	}
	r.shardsStillInUse.Add(1)
}

// HashtreeDumpSkipped counts a shard that shut down without saving its
// hashtree.
func (r *Report) HashtreeDumpSkipped() {
	if r == nil {
		return
	}
	r.hashtreeDumpsSkipped.Add(1)
}

func (r *Report) BucketDone(o BucketOutcome) {
	if r == nil {
		return
	}
	r.buckets[o].Add(1)
}

// Log writes the summary line, then the warnings.
func (r *Report) Log(logger logrus.FieldLogger, th Thresholds) {
	if r == nil {
		return
	}
	logger = logger.WithFields(logrus.Fields{"action": "shutdown_report", "node": r.node})
	s := r.summary()
	logger.WithFields(s.fields()).Info("shutdown finished")
	for _, w := range s.warnings(th) {
		logger.WithFields(w.fields).Warn(w.msg)
	}
}

// sinceBegin must be called with r.mu held.
func (r *Report) sinceBegin() time.Duration {
	if r.begun.IsZero() {
		return 0
	}
	return time.Since(r.begun)
}

func (r *Report) summary() summary {
	// Loaded before started, so a teardown finishing in between cannot make
	// shards_in_teardown negative.
	tornDown := r.shardsTornDown.Load()
	db := dbOutcome{
		indexesTotal:         r.indexesTotal.Load(),
		indexesDone:          r.indexesDone.Load(),
		shardsTornDown:       tornDown,
		shardsInTeardown:     r.shardsStarted.Load() - tornDown,
		shardsStillInUse:     r.shardsStillInUse.Load(),
		hashtreeDumpsSkipped: r.hashtreeDumpsSkipped.Load(),
	}
	for o := range db.buckets {
		db.buckets[o] = r.buckets[o].Load()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	_, dbDone := r.steps[StepDB]
	_, dbRunning := r.running[StepDB]
	db.reached = dbDone || dbRunning
	db.slowestIndexes = r.slowestIndexes.entries()
	db.slowestShards = r.slowestShards.entries()
	return summary{
		total:       r.sinceBegin(),
		clientDrain: max(r.restDrained, r.grpcStopped),
		steps:       maps.Clone(r.steps),
		clients:     r.clients,
		db:          db,
	}
}

type summary struct {
	total, clientDrain time.Duration
	steps              map[string]time.Duration
	clients            clientOutcome
	db                 dbOutcome
}

// dbOutcome is what DB.Shutdown did to the indexes and shards it held.
type dbOutcome struct {
	reached bool

	indexesTotal, indexesDone                          int64
	shardsTornDown, shardsInTeardown, shardsStillInUse int64
	hashtreeDumpsSkipped                               int64
	buckets                                            [bucketOutcomes]int64
	slowestIndexes, slowestShards                      []timedName
}

type timedName struct {
	name string
	took time.Duration
}

// slowest holds the three longest durations added, longest first.
type slowest [3]timedName

// add calls name only when took enters the three, so most callers allocate
// nothing.
func (s *slowest) add(took time.Duration, name func() string) {
	i := slices.IndexFunc(s[:], func(e timedName) bool { return e.name == "" || took > e.took })
	if i < 0 {
		return
	}
	copy(s[i+1:], s[i:len(s)-1])
	s[i] = timedName{name: name(), took: took}
}

func (s *slowest) entries() []timedName {
	n := slices.IndexFunc(s[:], func(e timedName) bool { return e.name == "" })
	if n < 0 {
		n = len(s)
	}
	return slices.Clone(s[:n])
}

// formatSlowest renders entries as name=1.23s,…
func formatSlowest(entries []timedName) string {
	var b strings.Builder
	for i, e := range entries {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(e.name)
		b.WriteByte('=')
		b.WriteString(threeDigits(e.took).String())
	}
	return b.String()
}

// threeDigits rounds d to three significant digits, so 1.234567s logs as 1.23s.
func threeDigits(d time.Duration) time.Duration {
	unit := time.Duration(1)
	for d >= 1000*unit {
		unit *= 10
	}
	return d.Round(unit)
}

type warning struct {
	msg    string
	fields logrus.Fields
}

// fields emits every step key, 0 for a step that did not run, so the key set
// is the same on every shutdown.
func (s summary) fields() logrus.Fields {
	f := logrus.Fields{
		"total_ms":        s.total.Milliseconds(),
		"client_drain_ms": s.clientDrain.Milliseconds(),

		"grpc_stop_ms":                s.clients.grpcStopTook.Milliseconds(),
		"grpc_forced_stop":            s.clients.grpcForcedStop,
		"rest_forced_close":           s.clients.restForcedClose,
		"rest_handlers_still_running": s.clients.restStillRunning,
		"rest_unavailable":            s.clients.restUnavailable,
		"grpc_unavailable":            s.clients.grpcUnavailable,
		"grpc_web_unavailable":        s.clients.grpcWebUnavailable,

		"db_reached":             s.db.reached,
		"indexes_done":           s.db.indexesDone,
		"indexes_total":          s.db.indexesTotal,
		"shards_torn_down":       s.db.shardsTornDown,
		"shards_in_teardown":     s.db.shardsInTeardown,
		"shards_still_in_use":    s.db.shardsStillInUse,
		"hashtree_dumps_skipped": s.db.hashtreeDumpsSkipped,
		"buckets_flushed":        s.db.buckets[BucketFlushed],
		"buckets_wal_reused":     s.db.buckets[BucketWALReused],
		"buckets_ctx_expired":    s.db.buckets[BucketCtxExpired],
		"buckets_failed":         s.db.buckets[BucketFailed],
		"slowest_indexes":        formatSlowest(s.db.slowestIndexes),
		"slowest_shards":         formatSlowest(s.db.slowestShards),
	}
	for _, step := range allSteps {
		f[step+"_ms"] = s.steps[step].Milliseconds()
	}
	return f
}

func (s summary) warnings(th Thresholds) []warning {
	var warns []warning
	if w, ok := s.phaseWarning(th); ok {
		warns = append(warns, w)
	}
	if s.clients.grpcForcedStop {
		warns = append(warns, warning{
			msg:    "gRPC server stop was forced",
			fields: logrus.Fields{"grpc_stop_ms": s.clients.grpcStopTook.Milliseconds()},
		})
	}
	if s.clients.restForcedClose {
		warns = append(warns, warning{
			msg:    "REST server close was forced",
			fields: logrus.Fields{"rest_handlers_still_running": s.clients.restStillRunning},
		})
	}
	if s.db.shardsStillInUse > 0 {
		warns = append(warns, warning{
			msg:    "shutdown skipped shards still in use",
			fields: logrus.Fields{"shards_still_in_use": s.db.shardsStillInUse},
		})
	}
	ctxExpired, failed := s.db.buckets[BucketCtxExpired], s.db.buckets[BucketFailed]
	if s.db.hashtreeDumpsSkipped > 0 || ctxExpired > 0 || failed > 0 {
		warns = append(warns, warning{
			msg: "shutdown skipped flushes or snapshots",
			fields: logrus.Fields{
				"hashtree_dumps_skipped": s.db.hashtreeDumpsSkipped,
				"buckets_ctx_expired":    ctxExpired,
				"buckets_failed":         failed,
			},
		})
	}
	// DB.Shutdown runs inside the cluster close, so a cluster close that never
	// entered it left the indexes open.
	if _, clusterClosed := s.steps[StepClusterClose]; clusterClosed && !s.db.reached {
		warns = append(warns, warning{msg: "shutdown did not close the database"})
	}
	return warns
}

func (s summary) phaseWarning(th Thresholds) (warning, bool) {
	over := logrus.Fields{}
	addIfOver := func(phase string, took, threshold time.Duration) {
		if took > threshold {
			over[phase+"_ms"] = took.Milliseconds()
			over[phase+"_threshold_ms"] = threshold.Milliseconds()
		}
	}
	addIfOver(clientDrain, s.clientDrain, th.ClientDrain)
	addIfOver(StepInternalClose, s.steps[StepInternalClose], th.InternalClose)
	addIfOver(StepDB, s.steps[StepDB], th.DB)
	addIfOver("total", s.total, th.Total)
	if len(over) == 0 {
		return warning{}, false
	}
	if s.total > th.Total {
		if step, took := s.slowestStep(); step != "" {
			over["slowest_step"] = step
			over["slowest_step_ms"] = took.Milliseconds()
		}
	}
	return warning{msg: "shutdown phases took longer than their threshold", fields: over}, true
}

// slowestStep skips pre_db_steps, which only groups other steps, and counts
// cluster_close without the DB close it contains. Ties go to the name that
// sorts first.
func (s summary) slowestStep() (string, time.Duration) {
	var slowest string
	var slowestTook time.Duration
	for _, step := range slices.Sorted(maps.Keys(s.steps)) {
		took := s.steps[step]
		switch step {
		case StepPreDBSteps:
			continue
		case StepClusterClose:
			took -= s.steps[StepDB]
		}
		if took > slowestTook {
			slowest, slowestTook = step, took
		}
	}
	return slowest, slowestTook
}
