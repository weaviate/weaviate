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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/backup"
)

// nodeBehavior is how a fake node answers the backup activity probe.
type nodeBehavior struct {
	activity    backup.NodeActivity
	notFound    bool
	unreachable bool
}

var (
	nodeIdle        = nodeBehavior{}
	nodeOldVersion  = nodeBehavior{notFound: true}
	nodeUnreachable = nodeBehavior{unreachable: true}
)

func nodeBusy(kind, id string) nodeBehavior {
	return nodeBehavior{activity: backup.NodeActivity{Busy: true, Kind: kind, ID: id}}
}

type fakeActivityResolver map[string]string

func (r fakeActivityResolver) NodeHostname(nodeName string) (string, bool) {
	host, ok := r[nodeName]
	return host, ok
}

// startActivityCluster spins up one httptest server per node; unreachable nodes get closed immediately.
func startActivityCluster(t *testing.T, behaviors map[string]nodeBehavior) fakeActivityResolver {
	t.Helper()

	resolver := fakeActivityResolver{}
	for node, behavior := range behaviors {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if behavior.notFound {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(backup.NewNodeActivityResponse(behavior.activity))
		}))
		if behavior.unreachable {
			server.Close()
		} else {
			t.Cleanup(server.Close)
		}

		parsed, err := url.Parse(server.URL)
		require.NoError(t, err)
		resolver[node] = parsed.Host
	}
	return resolver
}

func TestScanBackupActivity(t *testing.T) {
	tests := []struct {
		name            string
		nodes           []string
		behaviors       map[string]nodeBehavior
		wantBusyNode    string
		wantKind        string
		wantID          string
		wantUnreachable string
	}{
		{
			name:      "all idle",
			nodes:     []string{"node1", "node2", "node3"},
			behaviors: map[string]nodeBehavior{"node1": nodeIdle, "node2": nodeIdle, "node3": nodeIdle},
		},
		{
			name:         "one busy with a backup",
			nodes:        []string{"node1", "node2"},
			behaviors:    map[string]nodeBehavior{"node1": nodeIdle, "node2": nodeBusy(backup.NodeActivityKindBackup, "node2-backup")},
			wantBusyNode: "node2",
			wantKind:     backup.NodeActivityKindBackup,
			wantID:       "node2-backup",
		},
		{
			name:         "one busy with a restore",
			nodes:        []string{"node1", "node2"},
			behaviors:    map[string]nodeBehavior{"node1": nodeIdle, "node2": nodeBusy(backup.NodeActivityKindRestore, "node2-restore")},
			wantBusyNode: "node2",
			wantKind:     backup.NodeActivityKindRestore,
			wantID:       "node2-restore",
		},
		{
			name:            "one unreachable",
			nodes:           []string{"node1", "node2"},
			behaviors:       map[string]nodeBehavior{"node1": nodeIdle, "node2": nodeUnreachable},
			wantUnreachable: "node2",
		},
		{
			name:         "busy outranks unreachable",
			nodes:        []string{"node1", "node2", "node3"},
			behaviors:    map[string]nodeBehavior{"node1": nodeUnreachable, "node2": nodeBusy(backup.NodeActivityKindBackup, "node2-backup"), "node3": nodeIdle},
			wantBusyNode: "node2",
			wantKind:     backup.NodeActivityKindBackup,
			wantID:       "node2-backup",
			// the unreachable node is still recorded; the responder decides
			wantUnreachable: "node1",
		},
		{
			name:      "node without the route counts as free",
			nodes:     []string{"node1", "node2"},
			behaviors: map[string]nodeBehavior{"node1": nodeOldVersion, "node2": nodeIdle},
		},
		{
			name:         "lowest index wins when two nodes are busy",
			nodes:        []string{"node1", "node2", "node3"},
			behaviors:    map[string]nodeBehavior{"node1": nodeIdle, "node2": nodeBusy(backup.NodeActivityKindRestore, "node2-restore"), "node3": nodeBusy(backup.NodeActivityKindBackup, "node3-backup")},
			wantBusyNode: "node2",
			wantKind:     backup.NodeActivityKindRestore,
			wantID:       "node2-restore",
		},
		{
			name:            "unresolvable node counts as unreachable",
			nodes:           []string{"node1", "ghost"},
			behaviors:       map[string]nodeBehavior{"node1": nodeIdle},
			wantUnreachable: "ghost",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := startActivityCluster(t, tt.behaviors)
			logger, _ := test.NewNullLogger()
			prober := clients.NewClusterBackupActivity(http.DefaultClient, resolver)

			scan := scanBackupActivity(context.Background(), tt.nodes, prober, logger)

			assert.Equal(t, tt.wantBusyNode, scan.BusyNode)
			assert.Equal(t, tt.wantKind, scan.Activity.Kind)
			assert.Equal(t, tt.wantID, scan.Activity.ID)
			assert.Equal(t, tt.wantUnreachable, scan.UnreachableNode)
			if tt.wantUnreachable != "" {
				assert.Error(t, scan.UnreachableErr)
			}
		})
	}
}

// TestScanBackupActivityDeterministic pins list order over answer order.
func TestScanBackupActivityDeterministic(t *testing.T) {
	resolver := startActivityCluster(t, map[string]nodeBehavior{
		"node1": nodeIdle,
		"node2": nodeBusy(backup.NodeActivityKindBackup, "node2-backup"),
		"node3": nodeBusy(backup.NodeActivityKindRestore, "node3-restore"),
	})
	logger, _ := test.NewNullLogger()
	prober := clients.NewClusterBackupActivity(http.DefaultClient, resolver)

	for i := 0; i < 20; i++ {
		scan := scanBackupActivity(context.Background(), []string{"node1", "node2", "node3"}, prober, logger)
		require.Equal(t, "node2", scan.BusyNode)
	}
}

// Checks refusal wording and that privileged detail (node names, backup IDs,
// addresses) never leaks into a response requiring only update_collections.
func TestBackupActivityResponder(t *testing.T) {
	principal := &models.Principal{Username: "alice"}
	probeErr := &url.Error{
		Op:  "Get",
		URL: "http://10.42.7.13:7947/backups/node-activity",
		Err: errors.New("dial tcp 10.42.7.13:7947: connect: connection refused"),
	}

	t.Run("clear", func(t *testing.T) {
		assert.Nil(t, backupActivityResponder(principal, backupActivityScan{}))
	})

	tests := []struct {
		name         string
		scan         backupActivityScan
		wantConflict bool
		wantMsg      string
	}{
		{
			name: "busy",
			scan: backupActivityScan{
				BusyNode: "weaviate-2",
				Activity: backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindRestore, ID: "nightly-2026-08-04"},
			},
			wantConflict: true,
			wantMsg:      "reindex blocked: a restore is running in the cluster; retry after it finishes",
		},
		{
			name:    "unreachable",
			scan:    backupActivityScan{UnreachableNode: "weaviate-2", UnreachableErr: probeErr},
			wantMsg: "reindex blocked: cannot confirm the cluster is free of backups; retry once every node answers",
		},
		{
			name: "busy outranks unreachable",
			scan: backupActivityScan{
				BusyNode:        "weaviate-2",
				Activity:        backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindBackup, ID: "nightly-2026-08-04"},
				UnreachableNode: "weaviate-3",
				UnreachableErr:  probeErr,
			},
			wantConflict: true,
			wantMsg:      "reindex blocked: a backup is running in the cluster; retry after it finishes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responder := backupActivityResponder(principal, tt.scan)

			var payload *models.ErrorResponse
			if tt.wantConflict {
				conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
				require.Truef(t, ok, "expected a 409 responder, got %T", responder)
				payload = conflict.Payload
			} else {
				unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
				require.Truef(t, ok, "expected a 503 responder, got %T", responder)
				payload = unavailable.Payload
			}

			msg := errorMessage(t, payload)
			assert.Equal(t, tt.wantMsg, msg)
			for _, secret := range []string{"weaviate-2", "weaviate-3", "nightly-2026-08-04", "10.42.7.13", "7947"} {
				assert.NotContains(t, msg, secret)
			}
		})
	}
}

// panickingProber panics for the named nodes and reports idle for the rest.
type panickingProber struct{ panicOn map[string]struct{} }

func (p panickingProber) NodeActivity(_ context.Context, node string) (backup.NodeActivity, error) {
	if _, ok := p.panicOn[node]; ok {
		panic("prober blew up on " + node)
	}
	return backup.NodeActivity{}, nil
}

// A prober goroutine that panics is recovered by GoWrapper, and the deferred
// wg.Done runs during the unwinding, so wg.Wait returns normally over a result
// slot that was never written. An unwritten slot must not read as a node with
// no backup running: the scan would report the whole cluster clear on evidence
// it never got.
func TestScanBackupActivityCountsAPanickingProbeAsUnreachable(t *testing.T) {
	tests := []struct {
		name            string
		nodes           []string
		panicOn         []string
		wantUnreachable string
	}{
		{
			name:            "one node's probe panics",
			nodes:           []string{"n1", "n2", "n3"},
			panicOn:         []string{"n2"},
			wantUnreachable: "n2",
		},
		{
			name:            "every probe panics",
			nodes:           []string{"n1"},
			panicOn:         []string{"n1"},
			wantUnreachable: "n1",
		},
		{
			name:            "the lowest-index panic wins, matching the unreachable rule",
			nodes:           []string{"n1", "n2", "n3"},
			panicOn:         []string{"n2", "n3"},
			wantUnreachable: "n2",
		},
		{
			name:    "no panic still reads as clear",
			nodes:   []string{"n1", "n2"},
			panicOn: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			panicOn := map[string]struct{}{}
			for _, n := range tt.panicOn {
				panicOn[n] = struct{}{}
			}
			logger, _ := test.NewNullLogger()

			scan := scanBackupActivity(context.Background(), tt.nodes, panickingProber{panicOn: panicOn}, logger)

			assert.Equal(t, tt.wantUnreachable, scan.UnreachableNode)
			assert.Empty(t, scan.BusyNode)
			if tt.wantUnreachable == "" {
				assert.Nil(t, backupActivityResponder(&models.Principal{Username: "alice"}, scan))
				return
			}
			require.Error(t, scan.UnreachableErr)
			responder := backupActivityResponder(&models.Principal{Username: "alice"}, scan)
			unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
			require.Truef(t, ok, "a probe that never reported must answer 503, got %T", responder)
			assert.Equal(t,
				"reindex blocked: cannot confirm the cluster is free of backups; retry once every node answers",
				errorMessage(t, unavailable.Payload))
		})
	}
}

func errorMessage(t *testing.T, payload *models.ErrorResponse) string {
	t.Helper()
	require.NotNil(t, payload)
	require.Len(t, payload.Error, 1)
	return payload.Error[0].Message
}
