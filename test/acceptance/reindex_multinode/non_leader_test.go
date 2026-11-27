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

package reindex_multinode

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// testNonLeaderSubmitStatusCancel drives submit, status and cancel through a node that is not the RAFT
// leader, so the submit and cancel checks read the leader's task list over the header query.
func testNonLeaderSubmitStatusCancel(t *testing.T, compose *docker.DockerCompose) {
	className := "MultiNodeNonLeader"
	uri := restURIOf(compose, (raftLeaderIndex(t, compose)+1)%3+1)

	createCollection(t, compose, uri, className, 3, 3, textProps("title", "body"))
	defer deleteCollection(t, uri, className)
	importObjects(t, uri, className, testDocuments)

	finished := reindexhelpers.RebuildIndex(t, uri, className, "title", "filterable")
	reindexhelpers.AwaitReindexViaIndexes(t, uri, className, "title", "filterable", reindexhelpers.WithTimeout(180*time.Second))
	reindexhelpers.AwaitReindexFinished(t, uri, finished, reindexhelpers.WithTimeout(180*time.Second))

	cancelledID := reindexhelpers.RebuildIndex(t, uri, className, "body", "filterable")
	cancelled, arm := cancelReindexProperty(t, uri, className, "body", "filterable", cancelledID)
	t.Logf("cancel through the non-leader answered %s", arm)
	status := awaitTaskTerminal(t, uri, cancelledID, 180*time.Second)
	if cancelled {
		require.Equal(t, "CANCELLED", status, "a cancel the non-leader accepted must stop the run")
	} else {
		require.Equal(t, "FINISHED", status, "a run the cancel missed must still finish")
	}
}
