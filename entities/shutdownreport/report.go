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
	"sync"
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

// clientDrain is in the running set from Begin until RESTDrained.
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
	steps       map[string]time.Duration
	running     map[string]struct{}
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
	r.mu.Lock()
	defer r.mu.Unlock()
	return summary{
		total:       r.sinceBegin(),
		clientDrain: r.restDrained,
		steps:       maps.Clone(r.steps),
	}
}

type summary struct {
	total, clientDrain time.Duration
	steps              map[string]time.Duration
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
	}
	for _, step := range allSteps {
		f[step+"_ms"] = s.steps[step].Milliseconds()
	}
	return f
}

func (s summary) warnings(th Thresholds) []warning {
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
		return nil
	}
	if s.total > th.Total {
		if step, took := s.slowestStep(); step != "" {
			over["slowest_step"] = step
			over["slowest_step_ms"] = took.Milliseconds()
		}
	}
	return []warning{{msg: "shutdown phases took longer than their threshold", fields: over}}
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
