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

// A missing cluster service is a 503, and it is reached through a gate that ran
// and cleared: the probe comes first, so a wired cluster with no task service
// still gets the refusal rather than a nil-deref panic.
func TestUpdateIndexWithoutClusterServiceIsUnavailable(t *testing.T) {
	prober := &countingProber{}
	h := submissionHandlers(t, nil, prober)

	responder := submitReindex(h)

	unavailable, ok := responder.(*schema.SchemaObjectsIndexesUpdateServiceUnavailable)
	require.Truef(t, ok, "expected 503, got %T", responder)
	require.Equal(t, "cluster service unavailable; cannot submit reindex task",
		errorMessage(t, unavailable.Payload))

	prober.mu.Lock()
	defer prober.mu.Unlock()
	require.Equal(t, 1, prober.calls,
		"the pre-commit gate runs before the task service is needed; only it can have probed")
}

// Cluster membership is nil before a node joins. There is no one to probe then,
// so the gate fails open — the same posture as an unwired probe — and the
// submission proceeds instead of dereferencing the missing membership.
func TestUpdateIndexWithoutClusterMembershipSkipsTheBackupGate(t *testing.T) {
	svc := &raceTaskService{}
	// Would refuse the submission if it were ever asked.
	prober := fixedActivityProber{fixtureNode: backup.NodeActivity{
		Busy: true, Kind: backup.NodeActivityKindBackup, ID: "backup-1",
	}}
	h := submissionHandlers(t, svc, prober)
	h.cluster = nil

	responder := submitReindex(h)

	_, ok := responder.(*schema.SchemaObjectsIndexesUpdateAccepted)
	require.Truef(t, ok, "with no membership there is nothing to probe; expected the submission to proceed, got %T", responder)
	require.Equal(t, 1, svc.adds)
}
