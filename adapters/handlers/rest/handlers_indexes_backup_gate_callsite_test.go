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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/usecases/backup"
)

// fixedActivityProber answers the backup activity probe from a static map.
type fixedActivityProber map[string]backup.NodeActivity

func (p fixedActivityProber) NodeActivity(_ context.Context, nodeName string) (backup.NodeActivity, error) {
	return p[nodeName], nil
}

// fixedMembership is the node list the gate fans out over.
type fixedMembership []string

func (m fixedMembership) AllNames() []string { return m }

// LocalName reports the first entry as this node.
func (m fixedMembership) LocalName() string {
	if len(m) == 0 {
		return ""
	}
	return m[0]
}

// TestUpdateIndexRefusesWhileBackupRuns drives the full submission handler,
// not just the gate, so a dropped call to it fails here too.
func TestUpdateIndexRefusesWhileBackupRuns(t *testing.T) {
	// No task service: the gate is the next thing the handler reaches.
	h := submissionHandlers(t, nil, fixedActivityProber{fixtureNode: backup.NodeActivity{
		Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1",
	}})

	responder := submitReindex(h)

	conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
	require.Truef(t, ok, "a running backup must be refused with 409, got %T", responder)
	require.Equal(t,
		"reindex blocked: a backup is running in the cluster; retry after it finishes",
		errorMessage(t, conflict.Payload))
}

// TestUpdateIndexWithoutClusterServiceIsUnavailable pins a 503 instead of a nil-deref panic.
func TestUpdateIndexWithoutClusterServiceIsUnavailable(t *testing.T) {
	// No prober: the gate allows submission and the missing task service answers.
	h := submissionHandlers(t, nil, nil)

	responder := submitReindex(h)

	unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
	require.Truef(t, ok, "expected 503, got %T", responder)
	require.Equal(t, "cluster service unavailable; cannot submit reindex task",
		errorMessage(t, unavailable.Payload))
}

// TestUpdateIndexWithoutClusterMembershipIsUnavailable pins a 503, not a
// nil-deref panic, when cluster membership is nil (a real state before a node joins).
func TestUpdateIndexWithoutClusterMembershipIsUnavailable(t *testing.T) {
	h := submissionHandlers(t, nil, nil)
	h.cluster = nil

	responder := submitReindex(h)

	unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
	require.Truef(t, ok, "expected 503, got %T", responder)
	require.Equal(t, "cluster service unavailable; cannot submit reindex task",
		errorMessage(t, unavailable.Payload))
}
