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

package shutdownreport

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runningSteps(r *Report) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Sorted(maps.Keys(r.running))
}

func TestReportConcurrentAggregation(t *testing.T) {
	const workers = 64
	tests := []struct {
		name      string
		heartbeat bool
		work      func(r *Report, i int)
		check     func(t *testing.T, s summary)
	}{
		{
			name: "shard and index counts",
			work: func(r *Report, i int) {
				took := time.Duration(i+1) * time.Millisecond
				r.Shards().TeardownStarted()
				r.Shards().TornDown("Movies", fmt.Sprintf("s%d", i), took)
				r.Indexes().Done(fmt.Sprintf("Class%d", i), took)
				r.Shards().StillInUse()
			},
			check: func(t *testing.T, s summary) {
				assert.Equal(t, int64(workers), s.db.shardsTornDown)
				assert.Zero(t, s.db.shardsInTeardown)
				assert.Equal(t, int64(workers), s.db.shardsStillInUse)
				assert.Equal(t, int64(workers), s.db.indexesDone)
				assert.Equal(t, []timedName{
					{"Movies/s63", 64 * time.Millisecond},
					{"Movies/s62", 63 * time.Millisecond},
					{"Movies/s61", 62 * time.Millisecond},
				}, s.db.slowestShards)
				assert.Equal(t, []timedName{
					{"Class63", 64 * time.Millisecond},
					{"Class62", 63 * time.Millisecond},
					{"Class61", 62 * time.Millisecond},
				}, s.db.slowestIndexes)
			},
		},
		{
			name:      "heartbeat reads while workers record",
			heartbeat: true,
			work: func(r *Report, i int) {
				r.Indexes().Started(fmt.Sprintf("Class%d", i))
				r.Time(fmt.Sprintf("step_%d", i), func() {
					r.Shards().TeardownStarted()
					r.Shards().TornDown("Movies", fmt.Sprintf("s%d", i), time.Millisecond)
					r.Buckets().Done(BucketFlushed)
				})
			},
			check: func(t *testing.T, s summary) {
				assert.Zero(t, s.db.shardsInTeardown)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New("node-1")
			logger, hook := test.NewNullLogger()
			stop := func() {}
			if tt.heartbeat {
				stop = r.StartHeartbeat(logger, time.Millisecond, time.Millisecond, time.Hour, nil)
			}
			// Workers keep recording until two heartbeats have logged, so -race sees
			// heartbeatFields read mid-write.
			deadline := time.Now().Add(5 * time.Second)
			var wg sync.WaitGroup
			for i := range workers {
				wg.Go(func() {
					tt.work(r, i)
					for tt.heartbeat && len(hook.AllEntries()) < 2 && time.Now().Before(deadline) {
						tt.work(r, i)
					}
				})
			}
			stopWithin(t, wg.Wait, 5*time.Second)
			stopWithin(t, stop, 5*time.Second)

			tt.check(t, r.summary())
			assert.Empty(t, runningSteps(r))
			if tt.heartbeat {
				assert.GreaterOrEqual(t, len(hook.AllEntries()), 2, "heartbeats")
			}
		})
	}
}

func TestHeartbeat(t *testing.T) {
	const after, interval = 10 * time.Second, 5 * time.Second
	// runBlocked starts run in a goroutine and returns a release func, which the
	// blocked func passed to run waits for.
	runBlocked := func(run func(blocked func())) func() {
		release, ended := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(ended)
			run(func() { <-release })
		}()
		synctest.Wait()
		return func() {
			close(release)
			<-ended
		}
	}
	tests := []struct {
		name       string
		disabled   bool
		setup      func(r *Report) (release func())
		gauges     map[string]func() int64
		warnAfter  time.Duration
		wantLevels []logrus.Level
		wantFields logrus.Fields
		wantAbsent []string
	}{
		{name: "after of zero disables it", disabled: true},
		{
			name: "names the running step, current index and counts",
			setup: func(r *Report) func() {
				r.Indexes().Started("Movies")
				for range 3 {
					r.Shards().TeardownStarted()
				}
				r.Shards().TornDown("Movies", "s0", time.Second)
				r.Buckets().Done(BucketFlushed)
				r.Buckets().Done(BucketFailed)
				return runBlocked(func(blocked func()) { r.Time(StepClusterClose, blocked) })
			},
			gauges: map[string]func() int64{
				"grpc_calls_running":    func() int64 { return 2 },
				"rest_requests_running": func() int64 { return 1 },
			},
			wantFields: logrus.Fields{
				"running": "cluster_close", "current_index": "Movies",
				"shards_in_teardown": int64(2), "shards_torn_down": int64(1), "buckets_done": int64(2),
				"grpc_calls_running": int64(2), "rest_requests_running": int64(1),
			},
			wantAbsent: []string{"indexes_total"},
		},
		{
			name: "index total once set",
			setup: func(r *Report) func() {
				r.Indexes().SetTotal(4)
				r.Indexes().Done("Movies", time.Second)
				return func() {}
			},
			wantFields: logrus.Fields{"indexes_total": int64(4), "indexes_done": int64(1)},
		},
		{
			name: "a closed index is no longer current",
			setup: func(r *Report) func() {
				r.Indexes().Started("Movies")
				r.Indexes().Done("Movies", time.Second)
				return func() {}
			},
			wantFields: logrus.Fields{"current_index": ""},
		},
		{
			name: "nested steps are both running",
			setup: func(r *Report) func() {
				return runBlocked(func(blocked func()) {
					r.Time(StepPreDBSteps, func() { r.Time(StepLeaveCluster, blocked) })
				})
			},
			wantFields: logrus.Fields{"running": "leave_cluster,pre_db_steps"},
		},
		{
			name: "elapsed time and client drain after Begin",
			setup: func(r *Report) func() {
				r.Begin()
				return func() {}
			},
			wantFields: logrus.Fields{"running": "client_drain", "elapsed_ms": after.Milliseconds()},
		},
		{
			name: "Info until the shutdown runs longer than warnAfter, then Warn",
			setup: func(r *Report) func() {
				r.Begin()
				return func() {}
			},
			warnAfter:  after,
			wantLevels: []logrus.Level{logrus.InfoLevel, logrus.WarnLevel},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctestWithin(t, func(t *testing.T) {
				r := New("node-1")
				release := func() {}
				if tt.setup != nil {
					release = tt.setup(r)
				}
				defer release()
				heartbeatAfter, wantFirst := after, 1
				if tt.disabled {
					heartbeatAfter, wantFirst = 0, 0
				}
				warnAfter, wantLevels := tt.warnAfter, tt.wantLevels
				if warnAfter == 0 {
					warnAfter, wantLevels = time.Hour, []logrus.Level{logrus.InfoLevel, logrus.InfoLevel}
				}
				logger, hook := test.NewNullLogger()
				stop := r.StartHeartbeat(logger, heartbeatAfter, interval, warnAfter, tt.gauges)

				lines := func(elapsed time.Duration) int {
					time.Sleep(elapsed)
					synctest.Wait()
					return len(hook.AllEntries())
				}
				assert.Equal(t, 0, lines(after-time.Millisecond), "before after")
				assert.Equal(t, wantFirst, lines(time.Millisecond), "at after")
				assert.Equal(t, 2*wantFirst, lines(interval), "one interval later")

				stopWithin(t, stop, interval)
				stopWithin(t, stop, interval)
				assert.Equal(t, 2*wantFirst, lines(3*interval), "after stop")

				if wantFirst == 0 || !assert.Len(t, hook.AllEntries(), len(wantLevels)) {
					return
				}
				for i, want := range wantLevels {
					assert.Equal(t, want.String(), hook.AllEntries()[i].Level.String(), "line %d", i)
				}
				e := hook.AllEntries()[0]
				assert.Equal(t, "shutdown still running", e.Message)
				assert.Equal(t, "shutdown_report", e.Data["action"])
				assert.Equal(t, "node-1", e.Data["node"])
				for k, v := range tt.wantFields {
					assert.Equal(t, v, e.Data[k], k)
				}
				for _, k := range tt.wantAbsent {
					assert.NotContains(t, e.Data, k)
				}
			})
		})
	}
}

// A shutdown shorter than after must neither wait for the first beat nor log one.
func TestHeartbeatStoppedBeforeFirstBeat(t *testing.T) {
	synctestWithin(t, func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		stop := New("node-1").StartHeartbeat(logger, time.Second, time.Second, time.Hour, nil)
		time.Sleep(time.Second / 2)

		start := time.Now()
		stop()
		assert.Zero(t, time.Since(start), "stop waited for the first beat")
		time.Sleep(3 * time.Second)
		synctest.Wait()
		assert.Empty(t, hook.AllEntries())
	})
}

// synctestWithin runs f in a synctest bubble under a real-time bound, because a
// goroutine deadlocked on a mutex never advances the fake clock.
func synctestWithin(t *testing.T, f func(t *testing.T)) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		synctest.Test(t, f)
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("synctest bubble did not return")
	}
}

func stopWithin(t *testing.T, stop func(), d time.Duration) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(d):
		t.Fatal("stop did not return")
	}
}

// blockingHook holds the logging goroutine inside a heartbeat line until release closes.
type blockingHook struct{ release chan struct{} }

func (h blockingHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h blockingHook) Fire(*logrus.Entry) error {
	<-h.release
	return nil
}

// A heartbeat line must not land after the summary, so stop waits for one being written.
func TestHeartbeatStopWaitsForLineInProgress(t *testing.T) {
	synctestWithin(t, func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		hook := blockingHook{release: make(chan struct{})}
		logger.AddHook(hook)
		stop := New("node-1").StartHeartbeat(logger, time.Second, time.Second, time.Hour, nil)
		time.Sleep(time.Second)
		synctest.Wait()

		stopped := make(chan struct{})
		go func() {
			stop()
			close(stopped)
		}()
		synctest.Wait()
		select {
		case <-stopped:
			t.Error("stop returned while a heartbeat line was being written")
		default:
		}
		close(hook.release)
		<-stopped
	})
}

func TestReportTimePanickingStep(t *testing.T) {
	synctestWithin(t, func(t *testing.T) {
		r := New("node-1")
		func() {
			defer func() { _ = recover() }()
			r.Time(StepClusterClose, func() {
				time.Sleep(3 * time.Second)
				panic("step failed")
			})
		}()

		assert.Equal(t, 3*time.Second, r.summary().steps[StepClusterClose])
		assert.Empty(t, runningSteps(r))
	})
}

func TestReportClock(t *testing.T) {
	tests := []struct {
		name            string
		run             func(t *testing.T, r *Report)
		wantTotal       int64
		wantClientDrain int64
		wantDB          int64
		wantDBReached   bool
	}{
		{
			name: "a second Begin does not restart the clock",
			run: func(t *testing.T, r *Report) {
				r.Begin()
				time.Sleep(5 * time.Second)
				r.Begin()
				time.Sleep(time.Second)
			},
			wantTotal: 6000,
		},
		{
			name: "client drain ends at RESTDrained",
			run: func(t *testing.T, r *Report) {
				r.Begin()
				assert.Equal(t, []string{clientDrain}, runningSteps(r))
				time.Sleep(2 * time.Second)
				r.Clients().RESTDrained()
				assert.Empty(t, runningSteps(r))
				time.Sleep(time.Second)
			},
			wantTotal:       3000,
			wantClientDrain: 2000,
		},
		{
			name: "client drain ends at the later gRPC stop",
			run: func(t *testing.T, r *Report) {
				r.Begin()
				time.Sleep(2 * time.Second)
				r.Clients().RESTDrained()
				time.Sleep(2 * time.Second)
				r.Clients().GRPCStopped(time.Second, false)
				time.Sleep(time.Second)
			},
			wantTotal:       5000,
			wantClientDrain: 4000,
		},
		{
			name: "db phase",
			run: func(t *testing.T, r *Report) {
				r.Begin()
				end := r.StartStep(StepDB)
				time.Sleep(7 * time.Second)
				end()
			},
			wantTotal:     7000,
			wantDB:        7000,
			wantDBReached: true,
		},
		{
			name: "db entered but not returned",
			run: func(t *testing.T, r *Report) {
				r.Begin()
				r.StartStep(StepDB)
				time.Sleep(time.Second)
			},
			wantTotal:     1000,
			wantDBReached: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctestWithin(t, func(t *testing.T) {
				r := New("node-1")
				tt.run(t, r)
				f := r.summary().fields()
				assert.Equal(t, tt.wantTotal, f["total_ms"])
				assert.Equal(t, tt.wantClientDrain, f["client_drain_ms"])
				assert.Equal(t, tt.wantDB, f["db_ms"])
				assert.Equal(t, tt.wantDBReached, f["db_reached"])
			})
		})
	}
}

func TestReportSummaryBounded(t *testing.T) {
	wantKeys := []string{
		"action", "batch_drain_ms", "buckets_ctx_expired", "buckets_failed", "buckets_flushed",
		"buckets_wal_reused", "client_drain_ms", "close_conn_managers_ms", "close_db_users_ms",
		"close_modules_ms", "close_task_scheduler_ms", "cluster_close_ms", "db_ms", "db_reached",
		"drain_exports_ms", "flush_sentry_ms", "grpc_forced_stop", "grpc_stop_ms", "grpc_unavailable",
		"grpc_web_unavailable", "hashtree_dumps_skipped", "indexes_done", "indexes_total",
		"internal_close_ms", "leave_cluster_ms", "node", "pre_db_steps_ms", "rest_forced_close",
		"rest_handlers_still_running", "rest_unavailable", "shards_in_teardown", "shards_still_in_use",
		"shards_torn_down", "shutdown_otel_ms", "slowest_indexes", "slowest_shards", "stop_telemetry_ms",
		"total_ms", "wait_grpc_stop_ms",
	}
	tornDown := func(n int) func(r *Report) {
		return func(r *Report) {
			for i := range n {
				r.Shards().TeardownStarted()
				r.Shards().TornDown("Movies", fmt.Sprintf("s%d", i), time.Duration(i+1)*time.Millisecond)
			}
		}
	}
	tests := []struct {
		name       string
		record     func(r *Report)
		wantFields logrus.Fields
		wantWarns  int
	}{
		{
			name: "client outcome",
			record: func(r *Report) {
				r.Clients().GRPCStopped(21*time.Second, true)
				r.Clients().CallsUnavailable(1, 2, 3)
			},
			wantFields: logrus.Fields{
				"grpc_stop_ms": int64(21000), "grpc_forced_stop": true,
				"rest_unavailable": int64(1), "grpc_unavailable": int64(2), "grpc_web_unavailable": int64(3),
			},
			wantWarns: 1,
		},
		{
			name: "bucket outcomes",
			record: func(r *Report) {
				for o, n := range []int{BucketFlushed: 1, BucketWALReused: 2, BucketCtxExpired: 3, BucketFailed: 4} {
					for range n {
						r.Buckets().Done(BucketOutcome(o))
					}
				}
			},
			wantFields: logrus.Fields{
				"buckets_flushed": int64(1), "buckets_wal_reused": int64(2),
				"buckets_ctx_expired": int64(3), "buckets_failed": int64(4),
			},
			wantWarns: 1,
		},
		{name: "no shard", record: tornDown(0), wantFields: logrus.Fields{"slowest_shards": ""}},
		{
			name:       "one shard",
			record:     tornDown(1),
			wantFields: logrus.Fields{"shards_torn_down": int64(1), "slowest_shards": "Movies/s0=1ms"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New("node-1")
			r.Begin()
			tt.record(r)
			logger, hook := test.NewNullLogger()
			r.Log(logger, DefaultThresholds())

			var infos, warns []*logrus.Entry
			for _, e := range hook.AllEntries() {
				assert.Equal(t, "node-1", e.Data["node"], "%s line", e.Message)
				assert.Equal(t, "shutdown_report", e.Data["action"], "%s line", e.Message)
				if e.Level == logrus.InfoLevel {
					infos = append(infos, e)
				} else {
					warns = append(warns, e)
				}
			}
			require.Len(t, infos, 1)
			assert.Equal(t, "shutdown finished", infos[0].Message)
			assert.Equal(t, wantKeys, slices.Sorted(maps.Keys(infos[0].Data)))
			for k, v := range tt.wantFields {
				assert.Equal(t, v, infos[0].Data[k], k)
			}
			assert.Len(t, warns, tt.wantWarns)
		})
	}
}

func TestReportWarnings(t *testing.T) {
	th := DefaultThresholds()
	totalOver := th.Total + time.Millisecond
	totalOverFields := func(slowest string, took int64) logrus.Fields {
		return logrus.Fields{
			"phases_over": "total",
			"total_ms":    totalOver.Milliseconds(), "total_threshold_ms": th.Total.Milliseconds(),
			"slowest_step": slowest, "slowest_step_ms": took,
		}
	}
	skipped := func(hashtreeDumps, ctxExpired, failed int64) warning {
		return warning{msg: "shutdown skipped flushes or snapshots", fields: logrus.Fields{
			"hashtree_dumps_skipped": hashtreeDumps, "buckets_ctx_expired": ctxExpired, "buckets_failed": failed,
		}}
	}
	tests := []struct {
		name       string
		s          summary
		wantPhases logrus.Fields
		wantOthers []warning
	}{
		{name: "client drain at threshold", s: summary{clientDrain: th.ClientDrain}},
		{
			name: "internal close at threshold",
			s:    summary{steps: map[string]time.Duration{StepInternalClose: th.InternalClose}},
		},
		{name: "db at threshold", s: summary{steps: map[string]time.Duration{StepDB: th.DB}}},
		{
			name: "every phase over, listed in a fixed order",
			s: summary{
				clientDrain: th.ClientDrain + time.Millisecond, total: totalOver,
				steps: map[string]time.Duration{
					StepInternalClose: th.InternalClose + time.Millisecond,
					StepDB:            th.DB + time.Millisecond, StepClusterClose: th.DB + time.Millisecond,
				},
				db: dbOutcome{reached: true},
			},
			wantPhases: logrus.Fields{
				"phases_over":     "client_drain,internal_close,db,total",
				"client_drain_ms": int64(15001), "client_drain_threshold_ms": int64(15000),
				"internal_close_ms": int64(10001), "internal_close_threshold_ms": int64(10000),
				"db_ms": int64(30001), "db_threshold_ms": int64(30000),
				"total_ms": totalOver.Milliseconds(), "total_threshold_ms": th.Total.Milliseconds(),
				"slowest_step": StepDB, "slowest_step_ms": int64(30001),
			},
		},
		{
			name: "total at threshold has no slowest step",
			s:    summary{total: th.Total, steps: map[string]time.Duration{StepLeaveCluster: 25 * time.Second}},
		},
		{
			name: "slowest step skips the group that contains it",
			s: summary{total: totalOver, steps: map[string]time.Duration{
				StepPreDBSteps: 20 * time.Second, StepLeaveCluster: 19 * time.Second,
			}},
			wantPhases: totalOverFields(StepLeaveCluster, 19000),
		},
		{
			name: "cluster close is counted without the db",
			s: summary{total: totalOver, steps: map[string]time.Duration{
				StepClusterClose: 10 * time.Second, StepDB: time.Second, StepLeaveCluster: 5 * time.Second,
			}, db: dbOutcome{reached: true}},
			wantPhases: totalOverFields(StepClusterClose, 9000),
		},
		{
			name: "client drain under its threshold is named over a shorter db",
			s: summary{total: totalOver, clientDrain: 14 * time.Second, steps: map[string]time.Duration{
				StepBatchDrain: time.Second, StepClusterClose: 8 * time.Second, StepDB: 8 * time.Second,
			}, db: dbOutcome{reached: true}},
			wantPhases: totalOverFields(clientDrain, 14000),
		},
		{
			name: "tie goes to the name that sorts first",
			s: summary{total: totalOver, steps: map[string]time.Duration{
				StepStopTelemetry: 4 * time.Second, StepDrainExports: 4 * time.Second,
			}},
			wantPhases: totalOverFields(StepDrainExports, 4000),
		},
		{
			name: "forced gRPC stop",
			s:    summary{clients: clientOutcome{grpcForcedStop: true, grpcStopTook: 21 * time.Second}},
			wantOthers: []warning{{
				msg: "gRPC server stop was forced", fields: logrus.Fields{"grpc_stop_ms": int64(21000)},
			}},
		},
		{
			name: "forced REST close warns with no handler left running",
			s:    summary{clients: clientOutcome{restForcedClose: true}},
			wantOthers: []warning{{
				msg: "REST server close was forced", fields: logrus.Fields{"rest_handlers_still_running": int64(0)},
			}},
		},
		{
			name: "shards still in use warn once",
			s:    summary{db: dbOutcome{shardsStillInUse: 1000}},
			wantOthers: []warning{{
				msg: "shutdown skipped shards still in use", fields: logrus.Fields{"shards_still_in_use": int64(1000)},
			}},
		},
		{
			name:       "skipped hashtree dump",
			s:          summary{db: dbOutcome{hashtreeDumpsSkipped: 1}},
			wantOthers: []warning{skipped(1, 0, 0)},
		},
		{
			name:       "buckets whose ctx expired",
			s:          summary{db: dbOutcome{buckets: [bucketOutcomes]int64{BucketCtxExpired: 2}}},
			wantOthers: []warning{skipped(0, 2, 0)},
		},
		{
			name:       "buckets that failed",
			s:          summary{db: dbOutcome{buckets: [bucketOutcomes]int64{BucketFailed: 3}}},
			wantOthers: []warning{skipped(0, 0, 3)},
		},
		{
			name: "flushed and WAL-reused buckets do not warn",
			s:    summary{db: dbOutcome{buckets: [bucketOutcomes]int64{BucketFlushed: 5, BucketWALReused: 6}}},
		},
		{
			name:       "cluster close that never reached the db",
			s:          summary{steps: map[string]time.Duration{StepClusterClose: time.Second}},
			wantOthers: []warning{{msg: "shutdown did not close the database"}},
		},
		{
			name: "cluster close that reached the db",
			s: summary{
				steps: map[string]time.Duration{StepClusterClose: time.Second, StepDB: time.Second},
				db:    dbOutcome{reached: true},
			},
		},
		{
			name: "every kind warns, in order",
			s: summary{
				total: totalOver, steps: map[string]time.Duration{StepLeaveCluster: time.Second, StepClusterClose: time.Millisecond},
				clients: clientOutcome{grpcForcedStop: true, grpcStopTook: 21 * time.Second, restForcedClose: true, restStillRunning: 2},
				db:      dbOutcome{shardsStillInUse: 3, hashtreeDumpsSkipped: 4},
			},
			wantPhases: totalOverFields(StepLeaveCluster, 1000),
			wantOthers: []warning{
				{msg: "gRPC server stop was forced", fields: logrus.Fields{"grpc_stop_ms": int64(21000)}},
				{msg: "REST server close was forced", fields: logrus.Fields{"rest_handlers_still_running": int64(2)}},
				{msg: "shutdown skipped shards still in use", fields: logrus.Fields{"shards_still_in_use": int64(3)}},
				skipped(4, 0, 0),
				{msg: "shutdown did not close the database"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var want []warning
			if tt.wantPhases != nil {
				want = append(want, warning{msg: "shutdown phases took longer than their threshold", fields: tt.wantPhases})
			}
			want = append(want, tt.wantOthers...)
			assert.Equal(t, want, tt.s.warnings(th))
		})
	}
}

func TestNilReport(t *testing.T) {
	for name, r := range map[string]*Report{
		"nil":               nil,
		"ctx carrying none": From(context.Background()),
	} {
		t.Run(name, func(t *testing.T) {
			r.Begin()
			r.Clients().RESTDrained()
			ran := false
			r.Time(StepLeaveCluster, func() { ran = true })
			r.StartStep(StepDB)()
			r.Clients().GRPCStopped(time.Second, true)
			r.Clients().CallsUnavailable(1, 2, 3)
			r.Clients().RESTForceClosed(1)
			r.Indexes().SetTotal(1)
			r.Indexes().Started("Movies")
			r.Indexes().Done("Movies", time.Second)
			r.Shards().TeardownStarted()
			r.Shards().TornDown("Movies", "s0", time.Second)
			r.Shards().StillInUse()
			r.Shards().HashtreeDumpSkipped()
			r.Buckets().Done(BucketFailed)
			logger, hook := test.NewNullLogger()
			r.StartHeartbeat(logger, time.Nanosecond, time.Nanosecond, time.Hour, nil)()
			r.Log(logger, DefaultThresholds())

			assert.True(t, ran, "Time must still run the step")
			assert.Empty(t, hook.AllEntries())
		})
	}
}

func TestFormatSlowest(t *testing.T) {
	tests := []struct {
		name    string
		entries []timedName
		want    string
	}{
		{name: "keeps three significant digits", entries: []timedName{{"a", 1234567 * time.Microsecond}}, want: "a=1.23s"},
		{name: "rounds up", entries: []timedName{{"a", 9999 * time.Microsecond}}, want: "a=10ms"},
		{name: "short durations stay exact", entries: []timedName{{"a", 999}}, want: "a=999ns"},
		{
			name:    "comma separated in order",
			entries: []timedName{{"a", 3 * time.Second}, {"b", 2 * time.Second}},
			want:    "a=3s,b=2s",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, formatSlowest(tt.entries))
		})
	}
}
