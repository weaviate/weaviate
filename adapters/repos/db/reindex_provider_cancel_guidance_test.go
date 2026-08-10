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
// log "your buckets are inverted, rebuild the property": FAILED always (a
// unit died mid-work), CANCELLED only once something actually merged — a
// STARTED cancel wrote nothing, so an unconditional message would falsely
// claim corruption on the status [ReindexGateRemedy] calls safe to cancel.
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
		sentinels []string
		status    distributedtask.TaskStatus
		// migrationType defaults to change-tokenization when empty.
		migrationType ReindexMigrationType
		// properties defaults to the single propName when nil.
		properties   []string
		wantGuidance bool
		// notInLog must appear in no entry the terminal path emits.
		notInLog []string
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
		{
			// A format-only migration flips no schema, so a cancel cannot
			// leave buckets inverted against one — neither branch applies.
			name:          "cancelled format-only migration",
			status:        distributedtask.TaskStatusCancelled,
			migrationType: ReindexTypeRepairFilterable,
			notInLog:      []string{"nothing to repair", "still pre-migration"},
		},
		{
			// Reserved whole-collection shape: nothing on disk to look at
			// per property, so the check cannot clear the cancel.
			name:       "cancelled whole-collection migration",
			status:     distributedtask.TaskStatusCancelled,
			properties: []string{},
			notInLog:   []string{"nothing to repair"},
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

			migrationType := tc.migrationType
			if migrationType == "" {
				migrationType = ReindexTypeChangeTokenization
			}
			properties := tc.properties
			if properties == nil {
				properties = []string{propName}
			}
			payload, err := json.Marshal(ReindexTaskPayload{
				Collection:         className,
				MigrationType:      migrationType,
				Properties:         properties,
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
				for _, unwanted := range tc.notInLog {
					require.NotContains(t, entry.Message, unwanted)
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

// Every condition under which the check cannot look answers yes: silence
// about data that may be inverted is the worse error of the two.
func TestPromotableReindexStateOnThisNode_AnswersYesWhenItCannotLook(t *testing.T) {
	cases := []struct {
		name     string
		provider *ReindexProvider
		payload  ReindexTaskPayload
	}{
		{
			name:     "no local store to read",
			provider: &ReindexProvider{},
			payload: ReindexTaskPayload{
				Collection:    "C",
				MigrationType: ReindexTypeChangeTokenization,
				Properties:    []string{"name"},
			},
		},
		{
			name:     "a migration type this build does not know",
			provider: &ReindexProvider{db: &DB{}},
			payload: ReindexTaskPayload{
				Collection:    "C",
				MigrationType: "a-type-from-a-newer-node",
				Properties:    []string{"name"},
			},
		},
		{
			name:     "no property to look under",
			provider: &ReindexProvider{db: &DB{}},
			payload: ReindexTaskPayload{
				Collection:    "C",
				MigrationType: ReindexTypeChangeTokenization,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, tc.provider.promotableReindexStateOnThisNode(&tc.payload))
		})
	}
}

// The CANCELLED evidence gate reads tracker dirs that the local worker is
// still writing unless cleanup drained it first. When the drain times out
// the gate's answer is unusable, so the guidance is emitted anyway — a
// missed warning about inverted buckets costs more than a false one.
func TestOnTaskCompleted_DrainTimeoutStillWarnsOnCancel(t *testing.T) {
	const propName = "descr"

	ctx := testCtx()
	className := "CancelDrain_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	desc := distributedtask.TaskDescriptor{ID: "T_drain", Version: 1}

	// A cancelled server context makes the drain's deadline expire on entry,
	// standing in for a worker that outlives the timeout.
	serverCtx, cancelServer := context.WithCancel(context.Background())
	cancelServer()

	logger, hook := logrustest.NewNullLogger()
	p := &ReindexProvider{
		logger:       logger,
		serverCtx:    serverCtx,
		db:           &DB{indices: map[string]*Index{indexID(schema.ClassName(className)): idx}},
		payloads:     make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
		reindexTasks: make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric),
		runningHandles: map[distributedtask.TaskDescriptor]*reindexTaskHandle{
			desc: {cancel: func() {}, doneCh: make(chan struct{})},
		},
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
		TaskDescriptor: desc,
		Status:         distributedtask.TaskStatusCancelled,
		Payload:        payload,
	}))

	var repairCommands int
	for _, entry := range hook.AllEntries() {
		if _, ok := entry.Data["repair_command"]; ok {
			repairCommands++
		}
	}
	require.Equal(t, 1, repairCommands,
		"a drain that did not finish leaves the on-disk check unusable, "+
			"so the warning must not be suppressed: %v", hook.AllEntries())
}
