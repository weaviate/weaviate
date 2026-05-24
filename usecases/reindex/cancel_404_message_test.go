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

package reindex

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// stubCancelCluster returns no in-flight tasks so [Service.Cancel]
// always takes the "nothing to cancel" branch.
type stubCancelCluster struct{ ClusterService }

func (stubCancelCluster) ListDistributedTasks(context.Context) (map[string][]*distributedtask.Task, error) {
	return map[string][]*distributedtask.Task{}, nil
}

// TestCancel_NotFound_MessageContract pins the structured-404 body the
// acceptance suite at [test/acceptance/reindex_backup] enforces.
//
// Why duplicate the acceptance assertion at the unit tier:
//
//   - The acceptance reproducer takes ~4 minutes (Docker compose + RAFT
//     bootstrap). A 200ms unit reproducer means the next refactor that
//     touches this message fails locally before push.
//   - When [test/acceptance/reindex_backup] regressed because a refactor
//     dropped the "GET /v1/schema/..." substring, the audit found there
//     was no faster-tier coverage of the message contract. This test
//     closes that gap.
func TestCancel_NotFound_MessageContract(t *testing.T) {
	const (
		collection = "ReindexBackup_CancelNoTask"
		property   = "body"
		indexType  = "searchable"
	)

	svc := New(Deps{Cluster: stubCancelCluster{}}, logrus.New())
	_, err := svc.Cancel(context.Background(), collection, property, indexType, "tester")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrNotFound),
		"cancel-with-no-task must wrap ErrNotFound so the handler maps it to 404")

	msg := err.Error()
	// These substrings are the contract the operator-facing 404 body
	// carries. Each is independently load-bearing — see the assertion
	// list in test/acceptance/reindex_backup/suite_test.go:
	// testCancelOnNoInFlightReturns404.
	assert.True(t, strings.Contains(msg, collection),
		"404 message must name the collection, got: %s", msg)
	assert.True(t, strings.Contains(msg, property),
		"404 message must name the property, got: %s", msg)
	assert.True(t, strings.Contains(msg, indexType),
		"404 message must name the indexType, got: %s", msg)
	assert.True(t, strings.Contains(msg, "no in-flight reindex task to cancel"),
		"404 message must explain why nothing was canceled, got: %s", msg)
	assert.True(t, strings.Contains(msg, "GET /v1/schema/"),
		"404 message must point operators at the state-inspection endpoint, got: %s", msg)
}
