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
	tests := []struct {
		name    string
		workers int
	}{
		{name: "every worker times its own step", workers: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := New("node-1")
			var wg sync.WaitGroup
			for i := range tt.workers {
				wg.Go(func() { r.Time(fmt.Sprintf("step_%d", i), func() {}) })
			}
			wg.Wait()

			s := r.summary()
			assert.Len(t, s.steps, tt.workers)
			assert.Empty(t, runningSteps(r))
		})
	}
}

func TestReportTime(t *testing.T) {
	tests := []struct {
		name   string
		panics bool
	}{
		{name: "step returns"},
		{name: "step panics", panics: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := New("node-1")
				func() {
					defer func() { _ = recover() }()
					r.Time(StepClusterClose, func() {
						time.Sleep(3 * time.Second)
						if tt.panics {
							panic("step failed")
						}
					})
				}()

				assert.Equal(t, 3*time.Second, r.summary().steps[StepClusterClose])
				assert.Empty(t, runningSteps(r))
			})
		})
	}
}

func TestReportClock(t *testing.T) {
	tests := []struct {
		name            string
		run             func(t *testing.T, r *Report)
		wantTotal       int64
		wantClientDrain int64
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
				r.RESTDrained()
				assert.Empty(t, runningSteps(r))
				time.Sleep(time.Second)
			},
			wantTotal:       3000,
			wantClientDrain: 2000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := New("node-1")
				tt.run(t, r)
				f := r.summary().fields()
				assert.Equal(t, tt.wantTotal, f["total_ms"])
				assert.Equal(t, tt.wantClientDrain, f["client_drain_ms"])
			})
		})
	}
}

func TestReportSummaryBounded(t *testing.T) {
	wantKeys := []string{
		"action", "batch_drain_ms", "client_drain_ms", "close_conn_managers_ms", "close_db_users_ms",
		"close_modules_ms", "close_task_scheduler_ms", "cluster_close_ms", "db_ms", "drain_exports_ms",
		"flush_sentry_ms", "internal_close_ms", "leave_cluster_ms", "node", "pre_db_steps_ms",
		"shutdown_otel_ms", "stop_telemetry_ms", "total_ms", "wait_grpc_stop_ms",
	}
	tests := []struct {
		name      string
		elapsed   time.Duration
		wantWarns int
	}{
		{name: "idle shutdown", elapsed: time.Second},
		{name: "total over its threshold", elapsed: DefaultThresholds().Total + time.Second, wantWarns: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := New("node-1")
				r.Begin()
				r.Time(StepLeaveCluster, func() { time.Sleep(tt.elapsed) })
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
				assert.Len(t, warns, tt.wantWarns)
			})
		})
	}
}

func TestReportWarnings(t *testing.T) {
	th := DefaultThresholds()
	totalOver := th.Total + time.Millisecond
	totalOverFields := func(slowest string, took int64) logrus.Fields {
		return logrus.Fields{
			"total_ms": totalOver.Milliseconds(), "total_threshold_ms": th.Total.Milliseconds(),
			"slowest_step": slowest, "slowest_step_ms": took,
		}
	}
	tests := []struct {
		name string
		s    summary
		want logrus.Fields
	}{
		{name: "nothing over", s: summary{total: time.Second}},
		{name: "client drain at threshold", s: summary{clientDrain: th.ClientDrain}},
		{
			name: "client drain over",
			s:    summary{clientDrain: th.ClientDrain + time.Millisecond},
			want: logrus.Fields{"client_drain_ms": int64(10001), "client_drain_threshold_ms": int64(10000)},
		},
		{
			name: "internal close at threshold",
			s:    summary{steps: map[string]time.Duration{StepInternalClose: th.InternalClose}},
		},
		{
			name: "internal close over",
			s:    summary{steps: map[string]time.Duration{StepInternalClose: th.InternalClose + time.Millisecond}},
			want: logrus.Fields{"internal_close_ms": int64(5001), "internal_close_threshold_ms": int64(5000)},
		},
		{name: "db at threshold", s: summary{steps: map[string]time.Duration{StepDB: th.DB}}},
		{
			name: "db over",
			s:    summary{steps: map[string]time.Duration{StepDB: th.DB + time.Millisecond}},
			want: logrus.Fields{"db_ms": int64(20001), "db_threshold_ms": int64(20000)},
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
			want: totalOverFields(StepLeaveCluster, 19000),
		},
		{
			name: "slow db is named over cluster close",
			s: summary{total: totalOver, steps: map[string]time.Duration{
				StepClusterClose: 10 * time.Second, StepDB: 8 * time.Second,
			}},
			want: totalOverFields(StepDB, 8000),
		},
		{
			name: "cluster close is counted without the db",
			s: summary{total: totalOver, steps: map[string]time.Duration{
				StepClusterClose: 10 * time.Second, StepDB: time.Second, StepLeaveCluster: 5 * time.Second,
			}},
			want: totalOverFields(StepClusterClose, 9000),
		},
		{
			name: "tie goes to the name that sorts first",
			s: summary{total: totalOver, steps: map[string]time.Duration{
				StepStopTelemetry: 4 * time.Second, StepDrainExports: 4 * time.Second,
			}},
			want: totalOverFields(StepDrainExports, 4000),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warns := tt.s.warnings(th)
			if tt.want == nil {
				assert.Empty(t, warns)
				return
			}
			require.Len(t, warns, 1)
			assert.Equal(t, "shutdown phases took longer than their threshold", warns[0].msg)
			assert.Equal(t, tt.want, warns[0].fields)
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
			r.RESTDrained()
			ran := false
			r.Time(StepLeaveCluster, func() { ran = true })
			r.StartPhase(StepDB)()
			logger, hook := test.NewNullLogger()
			r.Log(logger, DefaultThresholds())

			assert.True(t, ran, "Time must still run the step")
			assert.Empty(t, hook.AllEntries())
		})
	}
}
