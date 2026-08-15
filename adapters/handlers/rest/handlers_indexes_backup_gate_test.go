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
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/reindex"
)

func quietLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return logger
}

func busy(kind, id string) backup.NodeActivity {
	return backup.NodeActivity{Answered: true, Busy: true, Kind: kind, ID: id}
}

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

// A prober that dies must leave an answer that refuses, not one that admits.
func TestScanBackupActivityWithADeadProber(t *testing.T) {
	scan := scanBackupActivity(context.Background(), []string{"n1"},
		func(context.Context, string) (backup.NodeActivity, error) { panic("prober died") },
		quietLogger())

	assert.Equal(t, backupActivityUnreachable, scan.verdict)
	assert.Equal(t, "n1", scan.node)
	assert.ErrorIs(t, scan.fault, errProbeLeftNoAnswer)
}

// Pins the rung order: the local slots, then the hold, then the fan-out.
func TestOpenSubmitBackupGateOrder(t *testing.T) {
	tests := []struct {
		name      string
		local     backup.NodeActivity
		cluster   backupActivityScan
		wantSteps []string
		wantScan  backupActivityScan
	}{
		{
			name:      "this node is busy, so no gate is closed and no peer is asked",
			local:     busy(backup.NodeActivityKindBackup, "b1"),
			wantSteps: []string{"local"},
			wantScan:  backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindBackup, id: "b1"},
		},
		{
			name:      "this node is taking part in a restore",
			local:     busy(backup.NodeActivityKindRestore, "r1"),
			wantSteps: []string{"local"},
			wantScan:  backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindRestore, id: "r1"},
		},
		{
			name:      "this node is idle, so the gate closes and then the peers are asked",
			local:     backup.NodeActivity{Answered: true},
			cluster:   backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindRestore, node: "n2"},
			wantSteps: []string{"local", "hold", "cluster"},
			wantScan:  backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindRestore, node: "n2"},
		},
		{
			name:      "the whole cluster is clear",
			local:     backup.NodeActivity{Answered: true},
			wantSteps: []string{"local", "hold", "cluster"},
			wantScan:  backupActivityScan{},
		},
		{
			// Nothing said this node is free, so no hold may be raised on it.
			name:      "this node's own slots left no answer",
			wantSteps: []string{"local"},
			wantScan:  backupActivityScan{verdict: backupActivityUnreachable, fault: errProbeLeftNoAnswer},
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
	tests := []struct {
		name        string
		scan        backupActivityScan
		wantCode    int
		wantBody    string
		wantVerdict string
	}{
		{name: "clear admits", scan: backupActivityScan{}},
		{
			name:        "a backup",
			scan:        backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindBackup, id: "backup-42", node: "node-3"},
			wantCode:    http.StatusConflict,
			wantBody:    "reindex blocked: a backup is running in the cluster; retry after it finishes",
			wantVerdict: reindex.VerdictBackupBusy,
		},
		{
			name:        "a restore",
			scan:        backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindRestore, id: "backup-42", node: "node-3"},
			wantCode:    http.StatusConflict,
			wantBody:    "reindex blocked: a restore is running in the cluster; retry after it finishes",
			wantVerdict: reindex.VerdictRestoreBusy,
		},
		{
			name:        "a kind off the wire this build cannot name",
			scan:        backupActivityScan{verdict: backupActivityBusy, kind: "offload\nnode-3", id: "backup-42"},
			wantCode:    http.StatusConflict,
			wantBody:    "reindex blocked: a backup is running in the cluster; retry after it finishes",
			wantVerdict: reindex.VerdictBackupBusy,
		},
		{
			name:     "a node that did not answer",
			scan:     backupActivityScan{verdict: backupActivityUnreachable, node: "node-3", fault: errors.New("connection refused")},
			wantCode: http.StatusServiceUnavailable,
			wantBody: "cannot confirm the cluster is free of backups: a node did not answer the " +
				"backup-activity probe; retry once every node is reachable",
			wantVerdict: reindex.VerdictUnreachable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewPedanticRegistry()
			h := &indexesHandlers{appState: &state.State{
				Logger:             quietLogger(),
				ReindexGateMetrics: reindex.NewGateMetrics(registry, nil),
			}}

			resp := h.backupActivityRefusal(nil, "Books", tt.scan)
			if tt.wantCode == 0 {
				assert.Nil(t, resp)
				return
			}

			code, body := statusOf(t, resp)
			require.Len(t, body.Error, 1)
			assert.Equal(t, tt.wantCode, code)
			// Equality, not substring: every leak this stops adds to the body.
			assert.Equal(t, tt.wantBody, body.Error[0].Message)
			assert.Equal(t, 1.0, refusalCount(t, registry, tt.wantVerdict))
		})
	}
}

// Also asserts every other series stayed at zero: they all exist from
// construction, so "the right one went up" is only half the claim.
func refusalCount(t *testing.T, registry *prometheus.Registry, verdict string) float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)

	want := map[string]string{"gate": reindex.GateSubmit, "verdict": verdict}
	var found float64
	matched := false
	for _, family := range families {
		if family.GetName() != "weaviate_reindex_gate_refusals_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			value := metric.GetCounter().GetValue()
			if reflect.DeepEqual(labels, want) {
				found, matched = value, true
				continue
			}
			assert.Zerof(t, value, "one refusal also incremented %v", labels)
		}
	}
	require.Truef(t, matched, "the gate wrote no %v series", want)
	return found
}

// Pins "nobody else holds a backup" apart from "this node cannot establish who
// is in the cluster".
func TestPeersToProbe(t *testing.T) {
	tests := []struct {
		name            string
		all             []string
		local           string
		wantPeers       []string
		wantEstablished bool
	}{
		{
			name:            "alone, and the view says so",
			all:             []string{"n1"},
			local:           "n1",
			wantPeers:       []string{},
			wantEstablished: true,
		},
		{
			name:            "three nodes",
			all:             []string{"n1", "n2", "n3"},
			local:           "n2",
			wantPeers:       []string{"n1", "n3"},
			wantEstablished: true,
		},
		{
			name:  "the member list is empty, so the view has not converged",
			local: "n1",
		},
		{
			name:  "this node is missing from a populated list, so members were reaped",
			all:   []string{"n2", "n3"},
			local: "n1",
		},
		{
			name: "no local name to check against",
			all:  []string{"n1", "n2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peers, established := peersToProbe(tt.all, tt.local)

			assert.Equal(t, tt.wantEstablished, established)
			assert.Equal(t, tt.wantPeers, peers)
		})
	}
}

// Whether a peer is capturing does not depend on this node's own wiring, so a
// gate that cannot ask must answer the same as one whose peers did not answer.
// The two faults still have to be told apart: missing wiring is a bug here, an
// unusable member list is a cluster to wait for.
func TestScanClusterBackupActivityWithoutTheWiringToAsk(t *testing.T) {
	tests := []struct {
		name      string
		appState  *state.State
		wantFault error
	}{
		{name: "no prober", appState: &state.State{Logger: quietLogger(), Cluster: &cluster.State{}}, wantFault: errClusterProbeUnwired},
		{name: "no cluster handle to read a member list from", appState: &state.State{Logger: quietLogger(), ClusterBackupActivity: staticProber{}}, wantFault: errClusterProbeUnwired},
		{name: "a member list that does not name this node", appState: &state.State{Logger: quietLogger(), ClusterBackupActivity: staticProber{}, Cluster: &cluster.State{}}, wantFault: errClusterViewUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := (&indexesHandlers{appState: tt.appState}).scanClusterBackupActivity(context.Background())

			assert.Equal(t, backupActivityUnreachable, scan.verdict)
			assert.ErrorIs(t, scan.fault, tt.wantFault)
		})
	}
}

// A refusal is the line an auditor most wants attributed, and the success line
// in the same flow already carries the caller.
func TestBackupActivityRefusalNamesThePrincipal(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	h := &indexesHandlers{appState: &state.State{Logger: logger}}

	h.logBackupActivityRefusal(&models.Principal{Username: "alice"}, "Books", "busy",
		backupActivityScan{verdict: backupActivityBusy, kind: backup.NodeActivityKindBackup})

	require.Len(t, hook.AllEntries(), 1)
	assert.Equal(t, "alice", hook.AllEntries()[0].Data["principal"])
}

// staticProber answers idle for every peer, so a test that reaches the fan-out
// can be told apart from one that refused before it.
type staticProber struct{}

func (staticProber) NodeActivity(context.Context, string) (backup.NodeActivity, error) {
	return backup.NodeActivity{}, nil
}

// The local slots decide before any peer is asked, and on a single node they
// are the only rung there is.
func TestSubmitGateReadsTheLocalRung(t *testing.T) {
	tests := []struct {
		name        string
		local       backup.NodeActivity
		wantVerdict backupActivityVerdict
		wantKind    string
		wantFault   error
	}{
		{
			name:        "this node is capturing",
			local:       busy(backup.NodeActivityKindBackup, "b1"),
			wantVerdict: backupActivityBusy,
			wantKind:    backup.NodeActivityKindBackup,
		},
		{
			name:        "this node is restoring",
			local:       busy(backup.NodeActivityKindRestore, "r1"),
			wantVerdict: backupActivityBusy,
			wantKind:    backup.NodeActivityKindRestore,
		},
		{
			// Nothing here is wired to answer for the peers, and the refusal
			// that produces is what proves the gate went past the local rung
			// rather than answering from it.
			name:        "idle, so the peers are asked too",
			local:       backup.NodeActivity{Answered: true},
			wantVerdict: backupActivityUnreachable,
			wantFault:   errClusterProbeUnwired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &indexesHandlers{appState: &state.State{
				Logger:         quietLogger(),
				BackupActivity: fixedActivity{activity: tt.local},
			}}

			_, scan := openSubmitBackupGate(h.localBackupActivity,
				func() func() { return h.markSubmitInProgress("Books") },
				func() backupActivityScan { return h.scanClusterBackupActivity(context.Background()) })

			assert.Equal(t, tt.wantVerdict, scan.verdict)
			assert.Equal(t, tt.wantKind, scan.kind)
			assert.ErrorIs(t, scan.fault, tt.wantFault)
		})
	}
}

type fixedActivity struct{ activity backup.NodeActivity }

func (f fixedActivity) Activity() backup.NodeActivity { return f.activity }

// A nil probe must read as "not wired" rather than panic.
func TestSetBackupActivityKeepsANilProbeNil(t *testing.T) {
	appState := &state.State{}
	appState.SetBackupActivity(nil)

	require.Nil(t, appState.BackupActivity)
	h := &indexesHandlers{appState: appState}
	assert.NotPanics(t, func() { assert.False(t, h.localBackupActivity().Busy) })
}

// Refused is a no-op on a nil receiver, so dropping the install line leaves
// every gate refusing correctly and reporting nothing. No gate test covers the
// install: each injects its own metrics by hand.
func TestInstallReindexGateLookupsWiresTheMetrics(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	// The gauge reads the hold registry off the DB, so the provider has to be
	// bound to the same one the gates read.
	repo := &db.DB{}
	appState := &state.State{
		Logger: quietLogger(),
		ReindexProvider: db.NewReindexProvider(repo, nil, nil, quietLogger(), "node-1", nil,
			context.Background()),
		ServerConfig: &config.WeaviateConfig{},
	}

	installReindexGateLookups(appState, repo, context.Background(), registry)

	require.NotNil(t, appState.ReindexGateMetrics, "the handlers were left counting into nothing")
	appState.ReindexGateMetrics.Refused(reindex.GateSubmit, reindex.VerdictBackupBusy)
	assert.Equal(t, 1.0, refusalCount(t, registry, reindex.VerdictBackupBusy))

	series := map[string]int{}
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		series[family.GetName()] = len(family.GetMetric())
	}
	// Gathered, so the gauges really read the provider at scrape time.
	assert.Equal(t, 2, series["weaviate_reindex_open_holds"], "one gauge per hold kind")
}
