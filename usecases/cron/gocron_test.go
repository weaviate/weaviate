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

package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	gocron "github.com/netresearch/go-cron"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/entities/cron"
	namespacecleanup "github.com/weaviate/weaviate/usecases/namespace_cleanup"
	objectttl "github.com/weaviate/weaviate/usecases/object_ttl"
)

// followerService answers IsLeader false without panicking. Store.raft is nil
// and unexported, so no fixture built here can answer true.
func followerService() *cluster.Service {
	return &cluster.Service{Raft: cluster.NewRaft(nil, &cluster.Store{}, nil)}
}

func newTestCrons(t *testing.T) (*Crons, *gocron.Cron, context.CancelFunc) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	crons, err := NewCrons(ctx, logger, intervalConfig(time.Minute))
	require.NoError(t, err)
	return crons, initGoCron(ctx, gocron.DiscardLogger), cancel
}

func TestCrons_InitJobs(t *testing.T) {
	t.Run("a follower's tick never reaches the cleanup coordinator", func(t *testing.T) {
		tests := []struct {
			name string
			wire func(*Crons, *gocron.Cron, *namespacecleanup.Coordinator) error
			want int64
		}{
			{
				name: "initJobs gates the cleanup job on leadership",
				wire: func(crons *Crons, cr *gocron.Cron, co *namespacecleanup.Coordinator) error {
					return crons.initJobs(cr, followerService(), &objectttl.Coordinator{}, co)
				},
				want: 0,
			},
			{
				// The control: the same counter reads 1 once a gate lets the
				// tick through, so a zero above means the gate denied it
				// rather than that nothing ran.
				name: "a job every node runs reaches the coordinator",
				wire: func(crons *Crons, cr *gocron.Cron, co *namespacecleanup.Coordinator) error {
					return crons.namespaceCleanup.Init(cr, cron.RunOnEveryNode, co)
				},
				want: 1,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				crons, cr, cancel := newTestCrons(t)
				defer cancel()

				var listDeletingCalls atomic.Int64
				require.NoError(t, tt.wire(crons, cr,
					nonNilCoordinator(t, stubLister{listDeletingCalls: &listDeletingCalls})))
				require.Eventually(t, func() bool {
					return cr.EntryByName(namespaceCleanupJobName).Valid()
				}, 2*time.Second, 10*time.Millisecond, "cleanup job should have been registered")

				cr.EntryByName(namespaceCleanupJobName).Run()

				assert.Equal(t, tt.want, listDeletingCalls.Load(),
					"the tick must reach Coordinator.Tick only when its gate allows it")

				cancel()
				crons.namespaceCleanup.wait()
			})
		}
	})

	t.Run("a coordinator one arm refuses leaves cleanup unregistered", func(t *testing.T) {
		tests := []struct {
			name    string
			ttl     *objectttl.Coordinator
			cleanup bool
			wantErr string
		}{
			{name: "objects ttl", ttl: nil, cleanup: true, wantErr: "init objects ttl cron"},
			{name: "namespace cleanup", ttl: &objectttl.Coordinator{}, wantErr: "init namespace cleanup cron"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				crons, cr, cancel := newTestCrons(t)
				defer cancel()

				var cleanup *namespacecleanup.Coordinator
				if tt.cleanup {
					cleanup = nonNilCoordinator(t, stubLister{})
				}
				require.ErrorContains(t,
					crons.initJobs(cr, followerService(), tt.ttl, cleanup), tt.wantErr)

				requireNoGoroutine(t, crons.namespaceCleanup.cronsRegistration)
				assert.False(t, cr.EntryByName(namespaceCleanupJobName).Valid(),
					"cleanup must not register once an earlier job failed")
				assert.False(t, cr.EntryByName(objectsTTLJobName).Valid(),
					"the ttl job must stay unregistered; the fixture coordinator carries no schedule")
			})
		}
	})
}

func TestGoCronInit(t *testing.T) {
	t.Run("cron accepts different schedule formats", func(t *testing.T) {
		cr := initGoCron(context.Background(), gocron.DiscardLogger)

		t.Run("job with valid schudule is added", func(t *testing.T) {
			schedules := []string{
				"@every 1m",
				"0 16 * * *",
				"0 0 16 * * *",
				"0 */2 * * *",
				"1 0 */3 * * *",
				"30 14 25 12 * 2027",
				"0 30 14 25 12 * 2027",
				"0 30 14 25 12 *",
				"30 14 25 12 *",
			}

			for _, schedule := range schedules {
				t.Run(schedule, func(t *testing.T) {
					entryId, err := cr.AddFunc(schedule, func() {})

					require.NoError(t, err)
					require.NotZero(t, entryId)
				})
			}
		})

		t.Run("job with invalid schedule is not added", func(t *testing.T) {
			schedules := []string{
				"0 16 * *",
				"0 0 30 14 25 12 * 2027",
				"a b c d e",
			}

			for _, schedule := range schedules {
				t.Run(schedule, func(t *testing.T) {
					entryId, err := cr.AddFunc(schedule, func() {})

					require.Error(t, err)
					require.Zero(t, entryId)
				})
			}
		})
	})
}
