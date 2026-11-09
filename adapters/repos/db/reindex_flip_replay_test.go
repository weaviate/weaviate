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

package db

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Pins the schema flip's verdict on replay through the real task FSM, reindex
// detector and schema FSM. A flip naming no run was written by a released
// binary, which admitted it unconditionally, so head must too, whatever head's
// rules replay the run's other entries to. A flip naming its run lands only
// while that run is SWAPPING, so a stopped run's late flip cannot land after
// its outcome was recorded.
func TestReplayedMigrationFlipVerdict(t *testing.T) {
	at := time.UnixMilli(1_700_000_000_000).UnixMilli()
	type step func(m *distributedtask.Manager) error
	apply := func(sub any) *cmd.ApplyRequest {
		b, err := json.Marshal(sub)
		require.NoError(t, err)
		return &cmd.ApplyRequest{SubCommand: b}
	}
	prep := func(id, node string) step {
		return func(m *distributedtask.Manager) error {
			return m.RecordPreparationCompleteAck(apply(&cmd.RecordDistributedTaskPreparationCompleteAckRequest{
				Namespace: ReindexNamespace, Id: id, Version: 1, NodeId: node, Success: true, AckedAtUnixMillis: at,
			}))
		}
	}
	swapAck := func(id, node string, ok bool) step {
		return func(m *distributedtask.Manager) error {
			r := &cmd.RecordDistributedTaskPostCompletionAckRequest{
				Namespace: ReindexNamespace, Id: id, Version: 1, NodeId: node, Success: ok, AckedAtUnixMillis: at,
			}
			if !ok {
				r.Error = "synthetic failure"
			}
			return m.RecordPostCompletionAck(apply(r))
		}
	}
	cancel := func(id string) step {
		return func(m *distributedtask.Manager) error {
			return m.CancelTask(apply(&cmd.CancelDistributedTaskRequest{
				Namespace: ReindexNamespace, Id: id, Version: 1, CancelledAtUnixMillis: at,
			}))
		}
	}
	// Both released PREP acks: run T leaves PREPARING for SWAPPING.
	swapping := []step{prep("T", "n1"), prep("T", "n2")}

	for _, tc := range []struct {
		name string
		runs []string // each a run on C.p, its units all completed, so PREPARING
		// steps are log entries; a refused one is skipped, as the FSM apply does.
		steps      []step
		wantStatus distributedtask.TaskStatus // of run T
		// flipRun names the run in the flip entry; empty is a released flip.
		flipRun    string
		wantRefuse bool
	}{
		{
			name: "released cancel raced into PREPARING, then the released flip",
			runs: []string{"T"}, steps: append([]step{cancel("T")}, swapping...),
			wantStatus: distributedtask.TaskStatusCancelled,
		},
		{
			name:       "released second swap ack from one node, then the released flip",
			runs:       []string{"T"},
			steps:      append(swapping, swapAck("T", "n2", true), swapAck("T", "n2", false), swapAck("T", "n1", true)),
			wantStatus: distributedtask.TaskStatusFailed,
		},
		{name: "released flip with no task", wantStatus: ""},
		{
			name: "flip naming its SWAPPING run", runs: []string{"T"}, steps: swapping,
			wantStatus: distributedtask.TaskStatusSwapping, flipRun: "T",
		},
		{
			name: "flip naming its FAILED run", runs: []string{"T"},
			steps:      append(swapping, swapAck("T", "n2", false)),
			wantStatus: distributedtask.TaskStatusFailed, flipRun: "T", wantRefuse: true,
		},
		{
			name: "flip naming its CANCELLED run", runs: []string{"T"}, steps: []step{cancel("T")},
			wantStatus: distributedtask.TaskStatusCancelled, flipRun: "T", wantRefuse: true,
		},
		{
			name: "flip naming its FAILED run while another run on the property is SWAPPING", runs: []string{"T", "U"},
			steps:      append(swapping, swapAck("T", "n2", false), prep("U", "n1"), prep("U", "n2")),
			wantStatus: distributedtask.TaskStatusFailed, flipRun: "T", wantRefuse: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			sm := clusterschema.NewSchemaManager("n1", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())
			require.NoError(t, sm.AddClass(apply(&cmd.AddClassRequest{
				Class: &models.Class{Class: "C", Properties: []*models.Property{
					{Name: "p", DataType: []string{"text"}, Tokenization: models.PropertyTokenizationWord},
				}},
				State: &sharding.State{Physical: map[string]sharding.Physical{}},
			}), "n1", true, false))

			logger, _ := logrustest.NewNullLogger()
			m := distributedtask.NewManager(distributedtask.ManagerParameters{
				Logger:                  logger,
				SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{ReindexNamespace: ReindexDetector{}},
			})
			sm.SetMutationGuard(m)

			payload, err := json.Marshal(ReindexTaskPayload{
				Collection: "C", MigrationType: ReindexTypeChangeTokenization,
				Properties: []string{"p"}, TargetTokenization: models.PropertyTokenizationField,
			})
			require.NoError(t, err)
			for _, id := range tc.runs {
				units := map[string]string{"u1": "n1", "u2": "n2"}
				var specs []*cmd.UnitSpec
				for u := range units {
					specs = append(specs, &cmd.UnitSpec{Id: u})
				}
				require.NoError(t, m.AddTask(apply(&cmd.AddDistributedTaskRequest{
					Namespace: ReindexNamespace, Id: id, Payload: payload, UnitSpecs: specs,
					NeedsPreparationBarrier: true, SubmittedAtUnixMillis: at,
				}), 1))
				for u, n := range units {
					require.NoError(t, m.RecordUnitCompletion(apply(&cmd.RecordDistributedTaskUnitCompletionRequest{
						Namespace: ReindexNamespace, Id: id, Version: 1, NodeId: n, UnitId: u, FinishedAtUnixMillis: at,
					})))
				}
			}
			for _, s := range tc.steps {
				_ = s(m)
			}
			if tc.wantStatus != "" {
				tasks, err := m.ListDistributedTasks(t.Context())
				require.NoError(t, err)
				var status distributedtask.TaskStatus
				for _, task := range tasks[ReindexNamespace] {
					if task.ID == "T" {
						status = distributedtask.TaskStatus(task.Status)
					}
				}
				require.Equal(t, tc.wantStatus, status)
			}

			// Built as raw JSON: these are the bytes on the log, whichever binary wrote them.
			flip := map[string]any{
				"Property":              &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: models.PropertyTokenizationField},
				"FieldsToUpdate":        []string{cmd.PropertyFieldTokenization},
				"FromInFlightMigration": true,
			}
			if tc.flipRun != "" {
				flip["MigrationRun"] = map[string]any{"namespace": ReindexNamespace, "id": tc.flipRun, "version": 1}
			}
			err = sm.UpdateProperty(&cmd.ApplyRequest{
				Type: cmd.ApplyRequest_TYPE_UPDATE_PROPERTY, Class: "C", Version: 2, SubCommand: apply(flip).SubCommand,
			}, true, false)
			if tc.wantRefuse {
				require.ErrorIs(t, err, clusterschema.ErrBadRequest)
				require.ErrorContains(t, err, "cutover write is refused")
				return
			}
			require.NoError(t, err)
			require.Equal(t, models.PropertyTokenizationField, sm.NewSchemaReader().ReadOnlyClass("C").Properties[0].Tokenization)
		})
	}
}
