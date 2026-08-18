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

package multi_node

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiclient "github.com/weaviate/weaviate/client"
	clcluster "github.com/weaviate/weaviate/client/cluster"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// TestSchemaWritesAfterLeaderFailover pins that a node which has just won an
// election adjudicates schema writes against what the cluster has committed,
// not against whatever its own FSM happened to hold at the moment it took
// office. Raft reports leadership as soon as the election is won, so a leader
// can be serving while still replaying entries it inherited; PreApplyFilter
// reads the in-memory schema map, and Store.Execute drains the FSM before it
// judges. A duplicate admitted here means that drain is gone.
//
// This is a contract test, not a deterministic reproduction: the drain window
// is milliseconds wide and nothing exposed to an out-of-process test can widen
// it, so this passes with the drain removed more often than not. The mechanism
// itself is pinned deterministically by
// TestProposeBarrier_RefusesADuplicateTheFSMHasNotApplied in package cluster,
// which blocks the FSM through the indexer mock. What this adds is the real
// election, real replication and real request timing that a unit test cannot.
func TestSchemaWritesAfterLeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		// The cluster may be mid-election; docker stop is forceful either way.
		_ = compose.Terminate(context.Background())
	}()

	// Keyed by raft node id, which is CLUSTER_HOSTNAME ("weaviate-N") and is
	// what /v1/cluster/statistics reports as Statistics.Name.
	clients := make(map[string]*apiclient.Weaviate, 3)
	for i := 1; i <= 3; i++ {
		clients[fmt.Sprintf("weaviate-%d", i-1)] = raftNodeClient(compose.GetWeaviateNode(i).URI())
	}
	firstNode := clients["weaviate-0"]

	const (
		seeded = "FailoverSeeded"
		fresh  = "FailoverFresh"
	)

	t.Run("seed a class every node has committed", func(t *testing.T) {
		require.NoError(t, createClassOn(ctx, firstNode, seeded))

		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for name, c := range clients {
				assert.NoError(ct, getClassOn(ctx, c, seeded),
					"node %s has not applied %s yet", name, seeded)
			}
		}, 60*time.Second, time.Second)
	})

	var oldLeader string
	t.Run("identify the leader", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			name, err := raftLeaderName(ctx, firstNode)
			if !assert.NoError(ct, err) {
				return
			}
			assert.NotEmpty(ct, name)
			oldLeader = name
		}, 60*time.Second, time.Second)
		require.Contains(t, clients, oldLeader, "leader is not one of the three nodes")
	})

	// A failure above leaves oldLeader empty, and every step below is about that
	// node; stop here rather than cascading into misleading failures.
	require.NotEmpty(t, oldLeader, "no leader identified")

	// A node that stays up, so leadership can still be read after the stop.
	var survivor *apiclient.Weaviate
	for name, c := range clients {
		if name != oldLeader {
			survivor = c
			break
		}
	}
	require.NotNil(t, survivor)

	t.Run("stop the leader", func(t *testing.T) {
		require.NoError(t, compose.StopNode(ctx, raftNodeSuffix(t, oldLeader), nil))
	})

	var newLeader *apiclient.Weaviate
	t.Run("a survivor takes over", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			name, err := raftLeaderName(ctx, survivor)
			if !assert.NoError(ct, err) {
				return
			}
			if !assert.NotEmpty(ct, name) {
				return
			}
			assert.NotEqual(ct, oldLeader, name, "the stopped node is still reported as leader")
			newLeader = clients[name]
		}, 90*time.Second, time.Second)
		require.NotNil(t, newLeader)
	})

	t.Run("the new leader refuses a class the cluster already has", func(t *testing.T) {
		// Not EventuallyWithT: a success is the bug, so it has to fail the test
		// outright rather than be retried away. Only errors that mean "not
		// serving yet" are worth another attempt.
		want := fmt.Sprintf("class name %s already exists", seeded)
		deadline := time.Now().Add(90 * time.Second)
		for {
			err := createClassOn(ctx, newLeader, seeded)
			if err == nil {
				t.Fatalf("the new leader admitted a duplicate of %q: it judged the propose "+
					"against an FSM it had not drained", seeded)
			}
			if strings.Contains(swaggerErrText(err), want) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the new leader never refused the duplicate; last response: %s",
					swaggerErrText(err))
			}
			time.Sleep(200 * time.Millisecond)
		}
	})

	t.Run("the new leader still admits a class the cluster does not have", func(t *testing.T) {
		// The refusal above is only meaningful if the leader is not simply
		// rejecting everything it is handed.
		require.NoError(t, createClassOn(ctx, newLeader, fresh))
	})

	t.Run("the restarted node converges on both classes", func(t *testing.T) {
		suffix := raftNodeSuffix(t, oldLeader)
		require.NoError(t, compose.StartNode(ctx, suffix))

		// StartNode re-maps the container's published port, so the client built
		// before the stop points at an address that is no longer served.
		restarted := raftNodeClient(compose.GetWeaviateNode(suffix + 1).URI())
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, class := range []string{seeded, fresh} {
				assert.NoError(ct, getClassOn(ctx, restarted, class),
					"restarted node %s is missing %s", oldLeader, class)
			}
		}, 120*time.Second, 2*time.Second)
	})
}

func raftNodeClient(hostPort string) *apiclient.Weaviate {
	transport := httptransport.New(hostPort, "/v1", []string{"http"})
	return apiclient.New(transport, strfmt.Default)
}

// raftNodeSuffix turns a raft node id ("weaviate-1") into the container suffix
// StopNode and StartNode take.
func raftNodeSuffix(t *testing.T, nodeID string) int {
	t.Helper()

	var n int
	_, err := fmt.Sscanf(nodeID, "weaviate-%d", &n)
	require.NoError(t, err, "unexpected raft node id %q", nodeID)
	return n
}

// raftLeaderName reports which node c believes is the raft leader. A node that
// is down reports no raft block at all, so it cannot be named here.
func raftLeaderName(ctx context.Context, c *apiclient.Weaviate) (string, error) {
	params := clcluster.NewClusterGetStatisticsParams().WithContext(ctx)
	resp, err := c.Cluster.ClusterGetStatistics(params, nil)
	if err != nil {
		return "", err
	}
	if resp.Payload == nil {
		return "", fmt.Errorf("empty statistics payload")
	}
	for _, s := range resp.Payload.Statistics {
		if s != nil && s.Raft != nil && s.Raft.State == "Leader" {
			return s.Name, nil
		}
	}
	return "", fmt.Errorf("no node reports raft state Leader")
}

func createClassOn(ctx context.Context, c *apiclient.Weaviate, class string) error {
	params := clschema.NewSchemaObjectsCreateParams().
		WithContext(ctx).
		WithObjectClass(&models.Class{
			Class:             class,
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 3},
		})
	_, err := c.Schema.SchemaObjectsCreate(params, nil)
	return err
}

func getClassOn(ctx context.Context, c *apiclient.Weaviate, class string) error {
	params := clschema.NewSchemaObjectsGetParams().WithContext(ctx).WithClassName(class)
	_, err := c.Schema.SchemaObjectsGet(params, nil)
	return err
}

// swaggerErrText renders a generated-client error so the API's message can be
// matched. The typed errors carry the payload only in their Payload field, and
// Error() prints the status code alone.
func swaggerErrText(err error) string {
	b, marshalErr := json.MarshalIndent(err, "", " ")
	if marshalErr != nil {
		return err.Error()
	}
	return fmt.Sprintf("%s %s", err.Error(), b)
}
