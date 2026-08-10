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
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestOnTaskCompleted_TerminalRepairGuidance pins which terminal outcomes
// reach the operator with "your buckets are inverted, rebuild the property".
//
// The message asserts data corruption as fact, so it must not fire for a
// cancel that did nothing. Cancel is offered at every non-terminal status,
// including STARTED where a barrier migration has written nothing at all —
// and STARTED is the status [ReindexGateRemedy] tells the operator it is safe
// to cancel at, so an unconditional message would turn the PR's own advice
// into a false corruption alarm. FAILED carries its own evidence: a unit died
// mid-work.
func TestOnTaskCompleted_TerminalRepairGuidance(t *testing.T) {
	const (
		propName  = "descr"
		indexType = "searchable"
		tracker   = "searchable_retokenize_descr_1"
	)

	cases := []struct {
		name string
		// sentinels the tracker dir carries when the task terminalizes;
		// nil means the task never got past STARTED on this node.
		sentinels    []string
		status       distributedtask.TaskStatus
		wantGuidance bool
	}{
		{
			name:         "cancelled at STARTED, nothing on disk",
			sentinels:    nil,
			status:       distributedtask.TaskStatusCancelled,
			wantGuidance: false,
		},
		{
			name:         "cancelled with a started-only generation",
			sentinels:    []string{"started.mig"},
			status:       distributedtask.TaskStatusCancelled,
			wantGuidance: false,
		},
		{
			name:         "cancelled after the generation merged",
			sentinels:    []string{"started.mig", "merged.mig"},
			status:       distributedtask.TaskStatusCancelled,
			wantGuidance: true,
		},
		{
			name:         "cancelled after the swap committed",
			sentinels:    []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"},
			status:       distributedtask.TaskStatusCancelled,
			wantGuidance: true,
		},
		{
			// A unit died mid-work whatever the disk shows, so FAILED does
			// not have to earn the message.
			name:         "failed with nothing on disk",
			sentinels:    nil,
			status:       distributedtask.TaskStatusFailed,
			wantGuidance: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CancelGuidance_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			if len(tc.sentinels) > 0 {
				mkTrackerDir(t, shard.pathLSM(), tracker, tc.sentinels...)
			}

			logger, hook := logrustest.NewNullLogger()
			p := &ReindexProvider{
				logger:       logger,
				serverCtx:    context.Background(),
				db:           &DB{indices: map[string]*Index{indexID(schema.ClassName(className)): idx}},
				payloads:     make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
				reindexTasks: make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric),
			}

			payload, err := json.Marshal(ReindexTaskPayload{
				Collection:         className,
				MigrationType:      ReindexTypeChangeTokenization,
				Properties:         []string{propName},
				TargetTokenization: "field",
			})
			require.NoError(t, err)

			require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_guidance", Version: 1},
				Status:         tc.status,
				Payload:        payload,
			}))

			var repairCommands int
			for _, entry := range hook.AllEntries() {
				if _, ok := entry.Data["repair_command"]; ok {
					repairCommands++
				}
			}
			if tc.wantGuidance {
				require.Equal(t, 1, repairCommands,
					"expected one repair_command entry, got the log: %v", hook.AllEntries())
				return
			}
			require.Zero(t, repairCommands,
				"a cancel that left nothing promotable must not claim the buckets are inverted")
		})
	}
}

// A provider with no local store cannot look, and silence about data that
// may be inverted is the worse error of the two.
func TestPromotableReindexStateOnThisNode_NoLocalStoreAnswersYes(t *testing.T) {
	p := &ReindexProvider{}
	require.True(t, p.promotableReindexStateOnThisNode(&ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	}))
}
