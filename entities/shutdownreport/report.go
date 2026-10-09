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

	enterrors "github.com/weaviate/weaviate/entities/errors"
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

// clientDrain stays in running from Begin until RESTDrained, a span Time cannot
// wrap. Its logged duration is the later of RESTDrained and GRPCStopped.
const clientDrain = "client_drain"

// FromEnv uses these when SHUTDOWN_HEARTBEAT_AFTER and SHUTDOWN_HEARTBEAT_INTERVAL are unset.
// The first heartbeat comes well before Kubernetes' default 30s terminationGracePeriodSeconds.
const (
	DefaultHeartbeatAfter    = 15 * time.Second
	DefaultHeartbeatInterval = 10 * time.Second
)

// Thresholds are the durations above which Log warns about a phase.
type Thresholds struct {
	ClientDrain   time.Duration `json:"client_drain"`
	InternalClose time.Duration `json:"internal_close"`
	DB            time.Duration `json:"db"`
	Total         time.Duration `json:"total"`
}

// DefaultThresholds apply where a SHUTDOWN_SLOW_LOG_*_THRESHOLD variable is unset.
func DefaultThresholds() Thresholds {
	return Thresholds{
		ClientDrain:   15 * time.Second,
		InternalClose: 10 * time.Second,
		DB:            30 * time.Second,
		Total:         30 * time.Second,
	}
}

// Report collects what one shutdown did. Every method is safe on a nil *Report and then
// records nothing, so callers whose ctx carries no report pay a nil check.
type Report struct {
	node string

	mu    sync.Mutex
	begun time.Time
	steps map[string]time.Duration
	// running names the steps in progress, which StartHeartbeat logs.
	running map[string]struct{}

	clients Clients
	indexes Indexes
	shards  Shards
	buckets Buckets
}

// Clients records how the REST and gRPC servers stopped serving clients.
type Clients struct {
	r *Report
	// r.mu guards the fields below, since restDrained and grpcStopped are
	// measured from r.begun.
	restDrained, grpcStopped time.Duration
	outcome                  clientOutcome
}

// Indexes records what DB.Shutdown did to the indexes it held.
type Indexes struct {
	total, done atomic.Int64

	mu      sync.Mutex
	current string
	slowest slowest
}

// Shards records the shard teardowns.
type Shards struct {
	started, tornDown, stillInUse, hashtreeDumpsSkipped atomic.Int64

	mu      sync.Mutex
	slowest slowest
}

// Buckets counts bucket shutdowns by outcome.
type Buckets struct {
	counts [bucketOutcomes]atomic.Int64
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
	r := &Report{
		node:    node,
		steps:   map[string]time.Duration{},
		running: map[string]struct{}{},
	}
	r.clients.r = r
	return r
}

// Clients, Indexes, Shards and Buckets return nil on a nil *Report, and their
// methods then record nothing.
func (r *Report) Clients() *Clients {
	if r == nil {
		return nil
	}
	return &r.clients
}

func (r *Report) Indexes() *Indexes {
	if r == nil {
		return nil
	}
	return &r.indexes
}

func (r *Report) Shards() *Shards {
	if r == nil {
		return nil
	}
	return &r.shards
}

func (r *Report) Buckets() *Buckets {
	if r == nil {
		return nil
	}
	return &r.buckets
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
func (c *Clients) RESTDrained() {
	if c == nil {
		return
	}
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	c.restDrained = c.r.sinceBegin()
	delete(c.r.running, clientDrain)
}

// GRPCStopped records how long the gRPC server took to stop and whether the
// stop had to be forced.
func (c *Clients) GRPCStopped(took time.Duration, forced bool) {
	if c == nil {
		return
	}
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	c.grpcStopped = c.r.sinceBegin()
	c.outcome.grpcStopTook = took
	c.outcome.grpcForcedStop = forced
}

// CallsUnavailable records the calls answered as unavailable, both those
// refused after shutdown began and those cut while running.
func (c *Clients) CallsUnavailable(rest, grpc, grpcWeb int64) {
	if c == nil {
		return
	}
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	c.outcome.restUnavailable = rest
	c.outcome.grpcUnavailable = grpc
	c.outcome.grpcWebUnavailable = grpcWeb
}

// RESTForceClosed records that the HTTP servers were closed while still busy,
// and how many handlers were still running after the wait for them.
func (c *Clients) RESTForceClosed(stillRunning int64) {
	if c == nil {
		return
	}
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	c.outcome.restForcedClose = true
	c.outcome.restStillRunning = stillRunning
}

// Time runs fn and records its duration under step, also when fn panics.
func (r *Report) Time(step string, fn func()) {
	defer r.StartStep(step)()
	fn()
}

// StartStep marks step as running until the returned func is called, which
// records its duration.
func (r *Report) StartStep(step string) (end func()) {
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

func (ix *Indexes) SetTotal(n int) {
	if ix == nil {
		return
	}
	ix.total.Store(int64(n))
}

// Started records the index DB.Shutdown is closing, which the heartbeat logs.
func (ix *Indexes) Started(id string) {
	if ix == nil {
		return
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.current = id
}

func (ix *Indexes) Done(id string, took time.Duration) {
	if ix == nil {
		return
	}
	ix.done.Add(1)
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.slowest.add(took, func() string { return id })
	ix.current = ""
}

func (s *Shards) TeardownStarted() {
	if s == nil {
		return
	}
	s.started.Add(1)
}

// TornDown counts a teardown TeardownStarted counted, failed or not.
func (s *Shards) TornDown(index, shard string, took time.Duration) {
	if s == nil {
		return
	}
	s.tornDown.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slowest.add(took, func() string { return index + "/" + shard })
}

// StillInUse counts a shard whose shutdown gave up because requests still
// held it.
func (s *Shards) StillInUse() {
	if s == nil {
		return
	}
	s.stillInUse.Add(1)
}

// HashtreeDumpSkipped counts a shard that shut down without saving its
// hashtree.
func (s *Shards) HashtreeDumpSkipped() {
	if s == nil {
		return
	}
	s.hashtreeDumpsSkipped.Add(1)
}

func (b *Buckets) Done(o BucketOutcome) {
	if b == nil {
		return
	}
	b.counts[o].Add(1)
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

// StartHeartbeat logs "shutdown still running" first when the after duration
// has passed, then every interval, until stop is called. after == 0 disables
// it. A line is Info until the shutdown has run longer than warnAfter.
func (r *Report) StartHeartbeat(logger logrus.FieldLogger, after, interval, warnAfter time.Duration,
	gauges map[string]func() int64,
) (stop func()) {
	if r == nil || after == 0 {
		return func() {}
	}
	logger = logger.WithFields(logrus.Fields{"action": "shutdown_report", "node": r.node})
	done, exited := make(chan struct{}), make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(exited)
		timer := time.NewTimer(after)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			fields, elapsed := r.heartbeatFields(gauges)
			level := logrus.InfoLevel
			if elapsed > warnAfter {
				level = logrus.WarnLevel
			}
			logger.WithFields(fields).Log(level, "shutdown still running")
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}, logger)
	return sync.OnceFunc(func() {
		close(done)
		<-exited
	})
}

func (r *Report) heartbeatFields(gauges map[string]func() int64) (logrus.Fields, time.Duration) {
	tornDown, inTeardown := r.shards.counts()
	var buckets int64
	for o := range r.buckets.counts {
		buckets += r.buckets.counts[o].Load()
	}
	f := logrus.Fields{
		"shards_torn_down":   tornDown,
		"shards_in_teardown": inTeardown,
		"indexes_done":       r.indexes.done.Load(),
		"buckets_done":       buckets,
	}
	if total := r.indexes.total.Load(); total > 0 {
		f["indexes_total"] = total
	}
	for name, gauge := range gauges {
		f[name] = gauge()
	}

	r.indexes.mu.Lock()
	f["current_index"] = r.indexes.current
	r.indexes.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	elapsed := r.sinceBegin()
	f["elapsed_ms"] = elapsed.Milliseconds()
	f["running"] = strings.Join(slices.Sorted(maps.Keys(r.running)), ",")
	return f, elapsed
}

// counts loads tornDown before started, so a teardown finishing in between
// cannot make inTeardown negative.
func (s *Shards) counts() (tornDown, inTeardown int64) {
	tornDown = s.tornDown.Load()
	return tornDown, s.started.Load() - tornDown
}

// sinceBegin must be called with r.mu held.
func (r *Report) sinceBegin() time.Duration {
	if r.begun.IsZero() {
		return 0
	}
	return time.Since(r.begun)
}

func (r *Report) summary() summary {
	tornDown, inTeardown := r.shards.counts()
	db := dbOutcome{
		indexesTotal:         r.indexes.total.Load(),
		indexesDone:          r.indexes.done.Load(),
		shardsTornDown:       tornDown,
		shardsInTeardown:     inTeardown,
		shardsStillInUse:     r.shards.stillInUse.Load(),
		hashtreeDumpsSkipped: r.shards.hashtreeDumpsSkipped.Load(),
	}
	for o := range db.buckets {
		db.buckets[o] = r.buckets.counts[o].Load()
	}
	r.indexes.mu.Lock()
	db.slowestIndexes = r.indexes.slowest.entries()
	r.indexes.mu.Unlock()
	r.shards.mu.Lock()
	db.slowestShards = r.shards.slowest.entries()
	r.shards.mu.Unlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	_, dbDone := r.steps[StepDB]
	_, dbRunning := r.running[StepDB]
	db.reached = dbDone || dbRunning
	return summary{
		total:       r.sinceBegin(),
		clientDrain: max(r.clients.restDrained, r.clients.grpcStopped),
		steps:       maps.Clone(r.steps),
		clients:     r.clients.outcome,
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

// slowestStep skips pre_db_steps, which spans the steps before the DB close, and
// counts cluster_close net of its DB close. client_drain counts whole, batch_drain
// and wait_grpc_stop inside it included. Ties go to the name that sorts first.
func (s summary) slowestStep() (string, time.Duration) {
	steps := map[string]time.Duration{clientDrain: s.clientDrain}
	maps.Copy(steps, s.steps)
	var slowest string
	var slowestTook time.Duration
	for _, step := range slices.Sorted(maps.Keys(steps)) {
		took := steps[step]
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
