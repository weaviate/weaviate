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

// startActivityCluster spins up one httptest server per node and returns a
// resolver pointing at them; unreachable nodes get a server closed immediately.
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
			json.NewEncoder(w).Encode(behavior.activity)
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

// TestScanBackupActivityDeterministic pins that the reported node follows
// list order, not answer order.
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

func TestBackupActivityResponder(t *testing.T) {
	principal := &models.Principal{Username: "alice"}

	t.Run("clear", func(t *testing.T) {
		assert.Nil(t, backupActivityResponder(principal, backupActivityScan{}))
	})

	t.Run("busy", func(t *testing.T) {
		responder := backupActivityResponder(principal, backupActivityScan{
			BusyNode: "node2",
			Activity: backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindRestore, ID: "restore-9"},
		})

		conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
		require.True(t, ok, "expected a 409 responder, got %T", responder)
		msg := errorMessage(t, conflict.Payload)
		assert.Equal(t, "reindex blocked: a restore is running in the cluster; retry after it finishes", msg)
	})

	t.Run("unreachable", func(t *testing.T) {
		responder := backupActivityResponder(principal, backupActivityScan{
			UnreachableNode: "node3",
			UnreachableErr:  assert.AnError,
		})

		unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
		require.True(t, ok, "expected a 503 responder, got %T", responder)
		msg := errorMessage(t, unavailable.Payload)
		assert.Equal(t, "reindex blocked: cannot confirm the cluster is free of backups; retry once every node answers", msg)
	})

	t.Run("busy outranks unreachable", func(t *testing.T) {
		responder := backupActivityResponder(principal, backupActivityScan{
			BusyNode:        "node2",
			Activity:        backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1"},
			UnreachableNode: "node3",
			UnreachableErr:  assert.AnError,
		})

		conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
		require.True(t, ok, "expected a 409 responder, got %T", responder)
		assert.Contains(t, errorMessage(t, conflict.Payload), "a backup is running in the cluster")
	})
}

// Reaching this handler needs update_collections on one collection. Node names
// sit behind read_nodes, backup IDs behind read_backups, and the probe's
// transport error carries the peer's internal address — none may reach the body.
func TestBackupActivityResponderWithholdsPrivilegedDetail(t *testing.T) {
	principal := &models.Principal{Username: "alice"}
	probeErr := &url.Error{
		Op:  "Get",
		URL: "http://10.42.7.13:7947/backups/node-activity",
		Err: errors.New("dial tcp 10.42.7.13:7947: connect: connection refused"),
	}

	tests := []struct {
		name string
		scan backupActivityScan
	}{
		{
			name: "busy node",
			scan: backupActivityScan{
				BusyNode: "weaviate-2",
				Activity: backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindBackup, ID: "nightly-2026-08-04"},
			},
		},
		{
			name: "unreachable node",
			scan: backupActivityScan{UnreachableNode: "weaviate-2", UnreachableErr: probeErr},
		},
		{
			name: "busy and unreachable",
			scan: backupActivityScan{
				BusyNode:        "weaviate-2",
				Activity:        backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindRestore, ID: "nightly-2026-08-04"},
				UnreachableNode: "weaviate-3",
				UnreachableErr:  probeErr,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responder := backupActivityResponder(principal, tt.scan)
			require.NotNil(t, responder)

			var payload *models.ErrorResponse
			switch r := responder.(type) {
			case *schema.SchemaObjectsIndexesUpdateConflict:
				payload = r.Payload
			case *schema.SchemaObjectsIndexesUpdateServiceUnavailable:
				payload = r.Payload
			default:
				t.Fatalf("unexpected responder %T", responder)
			}

			msg := errorMessage(t, payload)
			for _, secret := range []string{"weaviate-2", "weaviate-3", "nightly-2026-08-04", "10.42.7.13", "7947"} {
				assert.NotContains(t, msg, secret)
			}
		})
	}
}

func errorMessage(t *testing.T, payload *models.ErrorResponse) string {
	t.Helper()
	require.NotNil(t, payload)
	require.Len(t, payload.Error, 1)
	return payload.Error[0].Message
}
