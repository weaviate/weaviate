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

package rest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/backup"
)

func quietLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return logger
}

func busy(kind, id string) backup.NodeActivity {
	return backup.NodeActivity{Busy: true, Kind: kind, ID: id}
}

// probeFromMap answers per node; a node with no entry answers idle.
func probeFromMap(answers map[string]backup.NodeActivity, faults map[string]error,
) func(context.Context, string) (backup.NodeActivity, error) {
	return func(_ context.Context, node string) (backup.NodeActivity, error) {
		if err, ok := faults[node]; ok {
			return backup.NodeActivity{}, err
		}
		return answers[node], nil
	}
}

func TestScanBackupActivity(t *testing.T) {
	boom := errors.New("connection refused")

	tests := []struct {
		name        string
		nodes       []string
		answers     map[string]backup.NodeActivity
		faults      map[string]error
		wantVerdict backupActivityVerdict
		wantKind    string
		wantNode    string
	}{
		{
			name:        "nobody to ask",
			wantVerdict: backupActivityClear,
		},
		{
			name:        "every node idle",
			nodes:       []string{"n1", "n2", "n3"},
			wantVerdict: backupActivityClear,
		},
		{
			name:        "one node backing up",
			nodes:       []string{"n1", "n2"},
			answers:     map[string]backup.NodeActivity{"n2": busy(backup.NodeActivityKindBackup, "b1")},
			wantVerdict: backupActivityBusy,
			wantKind:    backup.NodeActivityKindBackup,
			wantNode:    "n2",
		},
		{
			name:        "one node restoring",
			nodes:       []string{"n1"},
			answers:     map[string]backup.NodeActivity{"n1": busy(backup.NodeActivityKindRestore, "r1")},
			wantVerdict: backupActivityBusy,
			wantKind:    backup.NodeActivityKindRestore,
			wantNode:    "n1",
		},
		{
			name:        "one node unreachable",
			nodes:       []string{"n1", "n2"},
			faults:      map[string]error{"n1": boom},
			wantVerdict: backupActivityUnreachable,
			wantNode:    "n1",
		},
		{
			name:  "busy outranks unreachable, whichever answered first",
			nodes: []string{"n1", "n2"},
			// n1 is the earlier slot, so a fold that took the first non-clear
			// answer would publish 503 instead of the 409 this must produce.
			faults:      map[string]error{"n1": boom},
			answers:     map[string]backup.NodeActivity{"n2": busy(backup.NodeActivityKindBackup, "b1")},
			wantVerdict: backupActivityBusy,
			wantKind:    backup.NodeActivityKindBackup,
			wantNode:    "n2",
		},
		{
			name:        "a node too old to serve the route passes",
			nodes:       []string{"n1", "n2"},
			faults:      map[string]error{"n1": clients.ErrNodeActivityUnsupported},
			wantVerdict: backupActivityClear,
		},
		{
			name:  "a node too old to serve the route does not clear a busy sibling",
			nodes: []string{"n1", "n2"},
			faults: map[string]error{
				"n1": clients.ErrNodeActivityUnsupported,
			},
			answers:     map[string]backup.NodeActivity{"n2": busy(backup.NodeActivityKindBackup, "b1")},
			wantVerdict: backupActivityBusy,
			wantKind:    backup.NodeActivityKindBackup,
			wantNode:    "n2",
		},
		{
			name:        "a wrapped unsupported answer still passes",
			nodes:       []string{"n1"},
			faults:      map[string]error{"n1": errors.Join(errors.New("node activity"), clients.ErrNodeActivityUnsupported)},
			wantVerdict: backupActivityClear,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := scanBackupActivity(context.Background(), tt.nodes,
				probeFromMap(tt.answers, tt.faults), quietLogger())

			assert.Equal(t, tt.wantVerdict, scan.verdict)
			assert.Equal(t, tt.wantKind, scan.kind)
			assert.Equal(t, tt.wantNode, scan.node)
		})
	}
}

// A prober that dies before writing its slot must read as a node that could not
// answer. The zero value of a result slot is "clear", so an unseeded fan-out
// would admit the submission the dead prober was asked to guard.
func TestScanBackupActivityWithADeadProber(t *testing.T) {
	scan := scanBackupActivity(context.Background(), []string{"n1"},
		func(context.Context, string) (backup.NodeActivity, error) { panic("prober died") },
		quietLogger())

	assert.Equal(t, backupActivityUnreachable, scan.verdict)
	assert.Equal(t, "n1", scan.node)
	assert.ErrorIs(t, scan.fault, errProbeLeftNoAnswer)
}

func TestRefuseOnLocalBackupActivity(t *testing.T) {
	tests := []struct {
		name        string
		activity    backup.NodeActivity
		wantVerdict backupActivityVerdict
		wantKind    string
	}{
		{name: "idle", wantVerdict: backupActivityClear},
		{
			name:        "coordinating or taking part in a backup",
			activity:    busy(backup.NodeActivityKindBackup, "b1"),
			wantVerdict: backupActivityBusy,
			wantKind:    backup.NodeActivityKindBackup,
		},
		{
			name:        "coordinating or taking part in a restore",
			activity:    busy(backup.NodeActivityKindRestore, "r1"),
			wantVerdict: backupActivityBusy,
			wantKind:    backup.NodeActivityKindRestore,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := refuseOnLocalBackupActivity(tt.activity)

			assert.Equal(t, tt.wantVerdict, scan.verdict)
			assert.Equal(t, tt.wantKind, scan.kind)
		})
	}
}

// TestOpenSubmitBackupGateOrder pins the rung order. A local slot read after
// the hold would refuse the capture running on this very node, and a fan-out
// before the hold leaves a window where a capture starts on a peer that has
// already answered.
func TestOpenSubmitBackupGateOrder(t *testing.T) {
	tests := []struct {
		name       string
		local      backup.NodeActivity
		cluster    backupActivityScan
		wantSteps  []string
		wantScan   backupActivityScan
		wantHeldAt int
	}{
		{
			name:      "this node is busy, so no gate is closed and no peer is asked",
			local:     busy(backup.NodeActivityKindBackup, "b1"),
			wantSteps: []string{"local"},
			wantScan:  backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindBackup, id: "b1"},
		},
		{
			name:      "this node is idle, so the gate closes and then the peers are asked",
			cluster:   backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindRestore, node: "n2"},
			wantSteps: []string{"local", "hold", "cluster"},
			wantScan:  backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindRestore, node: "n2"},
		},
		{
			name:      "the whole cluster is clear",
			wantSteps: []string{"local", "hold", "cluster"},
			wantScan:  backupActivityScan{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var steps []string
			released := false

			release, scan := openSubmitBackupGate(
				func() backup.NodeActivity { steps = append(steps, "local"); return tt.local },
				func() func() {
					steps = append(steps, "hold")
					return func() { released = true }
				},
				func() backupActivityScan { steps = append(steps, "cluster"); return tt.cluster })

			assert.Equal(t, tt.wantSteps, steps)
			assert.Equal(t, tt.wantScan, scan)

			require.NotNil(t, release, "a refused submission must still have a release to defer")
			release()
			assert.Equal(t, tt.wantSteps[len(tt.wantSteps)-1] != "local", released,
				"the hold is released only when it was raised")
		})
	}
}

func TestBackupActivityRefusal(t *testing.T) {
	h := &indexesHandlers{appState: &state.State{Logger: quietLogger()}}

	tests := []struct {
		name     string
		scan     backupActivityScan
		wantCode int
		wantBody string
	}{
		{name: "clear admits", scan: backupActivityScan{}},
		{
			name:     "a backup",
			scan:     backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindBackup, id: "backup-42", node: "node-3"},
			wantCode: http.StatusConflict,
			wantBody: "reindex blocked: a backup is running in the cluster; retry after it finishes",
		},
		{
			name:     "a restore",
			scan:     backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindRestore, id: "backup-42", node: "node-3"},
			wantCode: http.StatusConflict,
			wantBody: "reindex blocked: a restore is running in the cluster; retry after it finishes",
		},
		{
			name:     "a kind off the wire this build cannot name",
			scan:     backupActivityScan{verdict: backupActivityBusy, kind: "offload\nnode-3", id: "backup-42"},
			wantCode: http.StatusConflict,
			wantBody: "reindex blocked: a backup is running in the cluster; retry after it finishes",
		},
		{
			name:     "a node that did not answer",
			scan:     backupActivityScan{verdict: backupActivityUnreachable, node: "node-3", fault: errors.New("connection refused")},
			wantCode: http.StatusServiceUnavailable,
			wantBody: "cannot confirm the cluster is free of backups: a node did not answer the " +
				"backup-activity probe; retry once every node is reachable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := h.backupActivityRefusal(nil, "Books", tt.scan)
			if tt.wantCode == 0 {
				assert.Nil(t, resp)
				return
			}

			code, body := statusOf(t, resp)
			require.Len(t, body.Error, 1)
			assert.Equal(t, tt.wantCode, code)
			// Equality, not a substring: every leak this redaction exists to
			// stop is an addition to the body, which a substring still passes.
			assert.Equal(t, tt.wantBody, body.Error[0].Message)
		})
	}
}

func TestOtherNodes(t *testing.T) {
	tests := []struct {
		name  string
		all   []string
		local string
		want  []string
	}{
		{name: "not in a cluster yet", want: []string{}},
		{name: "alone", all: []string{"n1"}, local: "n1", want: []string{}},
		{name: "three nodes", all: []string{"n1", "n2", "n3"}, local: "n2", want: []string{"n1", "n3"}},
		{name: "local name unknown", all: []string{"n1", "n2"}, want: []string{"n1", "n2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, otherNodes(tt.all, tt.local))
		})
	}
}
