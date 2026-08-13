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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/schema"
)

// Pins that FAILED always logs repair guidance, but CANCELLED only once
// something actually merged — a STARTED cancel wrote nothing.
func TestOnTaskCompleted_TerminalRepairGuidance(t *testing.T) {
	const (
		propName  = "descr"
		indexType = "searchable"
		tracker   = "searchable_retokenize_descr_1"
	)

	cases := []struct {
		name string
		// tracker dir to create; defaults to the per-property one.
		tracker string
		// sentinels the tracker dir carries when the task terminalizes;
		// nil means the task never got past STARTED on this node.
		sentinels []string
		status    distributedtask.TaskStatus
		// migrationType defaults to change-tokenization when empty.
		migrationType ReindexMigrationType
		// properties defaults to the single propName when nil.
		properties        []string
		wantRepairCommand bool
		// wantInLog must each appear in some entry the terminal path emits.
		wantInLog []string
		// notInLog must appear in no entry the terminal path emits.
		notInLog []string
	}{
		{
			name:              "cancelled at STARTED, nothing on disk",
			sentinels:         nil,
			status:            distributedtask.TaskStatusCancelled,
			wantRepairCommand: false,
			// Checks the phrase shared by both repair-guidance variants, so
			// this also catches the guidance firing without evidence on the
			// arm the repair_command counter can't see.
			wantInLog: []string{
				"no promotable generation on this node",
				// The gate covers what THIS task left behind, not what the
				// next restart will do overall — a finished migration on
				// another property can still leave something to promote.
				"this task left nothing for the next restart to promote here",
			},
			notInLog: []string{
				"canonical inverted bucket",
				"the next restart would promote nothing here",
			},
		},
		{
			name:              "cancelled with a started-only generation",
			sentinels:         []string{"started.mig"},
			status:            distributedtask.TaskStatusCancelled,
			wantRepairCommand: false,
		},
		{
			name:              "cancelled after the generation merged",
			sentinels:         []string{"started.mig", "merged.mig"},
			status:            distributedtask.TaskStatusCancelled,
			wantRepairCommand: true,
		},
		{
			name:              "cancelled after the swap committed",
			sentinels:         []string{"started.mig", "merged.mig", "swapped.mig", "tidied.mig"},
			status:            distributedtask.TaskStatusCancelled,
			wantRepairCommand: true,
		},
		{
			// change-algorithm's generation lives in a class-level tracker dir,
			// not under <prefix>_<prop>; the evidence check must look there too.
			name:              "cancelled change-algorithm after the class-level generation merged",
			tracker:           MigrationDirSearchableMapToBlockmax + genSuffix(1),
			sentinels:         []string{"started.mig", "merged.mig"},
			status:            distributedtask.TaskStatusCancelled,
			migrationType:     ReindexTypeChangeAlgorithm,
			wantRepairCommand: true,
		},
		{
			// A unit died mid-work whatever the disk shows, so FAILED does
			// not have to earn the message.
			name:              "failed with nothing on disk",
			sentinels:         nil,
			status:            distributedtask.TaskStatusFailed,
			wantRepairCommand: true,
		},
		{
			// A format-only migration flips no schema, so a cancel cannot
			// leave buckets inverted against one — neither branch applies.
			name:          "cancelled format-only migration",
			status:        distributedtask.TaskStatusCancelled,
			migrationType: ReindexTypeRepairFilterable,
			notInLog:      []string{"no promotable generation"},
		},
		{
			// Reserved whole-collection shape: no property to render a
			// repair call for, so the operator gets the generic runbook
			// pointer instead of a copy-pasteable command.
			name:       "cancelled whole-collection migration",
			status:     distributedtask.TaskStatusCancelled,
			properties: []string{},
			wantInLog:  []string{"manual repair guidance not available"},
			notInLog:   []string{"no promotable generation"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shard, idx := newReindexTestShard(t, "CancelGuidance", propName)
			className := string(idx.Config.ClassName)

			if len(tc.sentinels) > 0 {
				dir := tc.tracker
				if dir == "" {
					dir = tracker
				}
				mkTrackerDir(t, shard.pathLSM(), dir, tc.sentinels...)
			}

			logger, hook := logrustest.NewNullLogger()
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(schema.ClassName(className)): idx}},
				nil, logger, "n1", nil, context.Background())

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
				UnitToShard:        map[string]string{"u1": shard.Name()},
			})
			require.NoError(t, err)

			require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_guidance", Version: 1},
				Status:         tc.status,
				Payload:        payload,
			}))

			var repairCommands int
			var allMessages string
			for _, entry := range hook.AllEntries() {
				if _, ok := entry.Data["repair_command"]; ok {
					repairCommands++
				}
				allMessages += entry.Message + "\n"
				for _, unwanted := range tc.notInLog {
					require.NotContains(t, entry.Message, unwanted)
				}
			}
			for _, want := range tc.wantInLog {
				require.Contains(t, allMessages, want)
			}
			if tc.wantRepairCommand {
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

// When the drain times out, the CANCELLED evidence gate's answer is
// unusable, so guidance is emitted anyway (a missed warning costs more).
func TestOnTaskCompleted_DrainTimeoutStillWarnsOnCancel(t *testing.T) {
	const propName = "descr"

	shard, idx := newReindexTestShard(t, "CancelDrain", propName)
	className := string(idx.Config.ClassName)

	desc := distributedtask.TaskDescriptor{ID: "T_drain", Version: 1}

	// A cancelled server context makes the drain's deadline expire on entry,
	// standing in for a worker that outlives the timeout.
	serverCtx, cancelServer := context.WithCancel(context.Background())
	cancelServer()

	logger, hook := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(schema.ClassName(className)): idx}},
		nil, logger, "n1", nil, serverCtx)
	structuralInvariantInjectHandle(p, desc)

	payload, err := json.Marshal(ReindexTaskPayload{
		Collection:         className,
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{propName},
		TargetTokenization: "field",
		UnitToShard:        map[string]string{"u1": shard.Name()},
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
