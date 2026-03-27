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

package clusterapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/objects"
)

// captureReplicator wraps fakeReplicator and records the VObjects delivered to
// OverwriteObjects so integration tests can assert on the fully-deserialized
// payload that the server handler produces from the client's wire encoding.
type captureReplicator struct {
	fakeReplicator

	mu            sync.Mutex
	receivedIdx   string
	receivedShard string
	receivedObjs  []*objects.VObject
	responses     []types.RepairResponse
}

func newCaptureReplicator(responses []types.RepairResponse) *captureReplicator {
	return &captureReplicator{
		fakeReplicator: *newFakeReplicator(false),
		responses:      responses,
	}
}

// OverwriteObjects shadows fakeReplicator.OverwriteObjects to capture inputs.
func (c *captureReplicator) OverwriteObjects(_ context.Context, index, shard string, vobjects []*objects.VObject) ([]types.RepairResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.receivedIdx = index
	c.receivedShard = shard
	c.receivedObjs = vobjects
	return c.responses, nil
}

func (c *captureReplicator) received() (string, string, []*objects.VObject) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.receivedIdx, c.receivedShard, c.receivedObjs
}

// TestOverwriteObjectsClientServerIntegration wires the real replicationClient
// (adapters/clients) directly against the real putOverwriteObjects HTTP handler
// (adapters/handlers/rest/clusterapi) via an in-process httptest.Server.
//
// This is the only test that exercises the full encoding path end-to-end:
// MarshalV2 → zstd compress → HTTP PUT → decompress → UnmarshalV2 → shard call,
// catching any mismatch between the client and server that unit tests on each
// side in isolation cannot detect.
func TestOverwriteObjectsClientServerIntegration(t *testing.T) {
	t.Parallel()

	now := time.Now()

	input := []*objects.VObject{
		{
			LatestObject: &models.Object{
				ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
				Class:              "Article",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(),
				Tenant:             "tenant-1",
				Properties: map[string]interface{}{
					"title": "integration test",
					"count": float64(7),
				},
			},
			Vector:          []float32{0.1, 0.2, 0.3},
			Vectors:         map[string][]float32{"named": {0.4, 0.5}},
			StaleUpdateTime: now.UnixMilli(),
			Version:         3,
		},
		{
			// Deleted tombstone: no LatestObject, only metadata.
			ID:              strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242"),
			Deleted:         true,
			StaleUpdateTime: now.Add(time.Minute).UnixMilli(),
			Version:         9,
		},
	}

	expectedResponses := []types.RepairResponse{
		{ID: "73f2eb5f-5abf-447a-81ca-74b1dd168241", UpdateTime: now.Add(time.Hour).UnixMilli()},
		{ID: "73f2eb5f-5abf-447a-81ca-74b1dd168242", Deleted: true},
	}

	cap := newCaptureReplicator(expectedResponses)
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(
		cap,
		clusterapi.NewNoopAuthHandler(),
		func() bool { return false },
		cluster.RequestQueueConfig{},
		logger,
		func() bool { return true },
	)
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	defer server.Close()

	// Use the real replication client with the test server's HTTP client so
	// requests are routed to the in-process server without TLS.
	c := clients.NewReplicationClient(server.Client())

	// server.URL is "http://host:port"; strip the scheme for the client which
	// constructs the URL itself.
	host := server.URL[len("http://"):]
	resp, err := c.OverwriteObjects(context.Background(), host, "Article", "S1", input)
	require.NoError(t, err)
	require.Len(t, resp, 2)
	assert.Equal(t, expectedResponses[0].ID, resp[0].ID)
	assert.Equal(t, expectedResponses[1].ID, resp[1].ID)
	assert.Equal(t, expectedResponses[1].Deleted, resp[1].Deleted)

	// Assert the server handler deserialized the wire payload correctly.
	idx, shard, received := cap.received()
	assert.Equal(t, "Article", idx)
	assert.Equal(t, "S1", shard)
	require.Len(t, received, 2)

	r0 := received[0]
	require.NotNil(t, r0.LatestObject)
	assert.Equal(t, input[0].LatestObject.ID, r0.LatestObject.ID)
	assert.Equal(t, input[0].LatestObject.Class, r0.LatestObject.Class)
	assert.Equal(t, input[0].LatestObject.Tenant, r0.LatestObject.Tenant)
	assert.Equal(t, input[0].LatestObject.CreationTimeUnix, r0.LatestObject.CreationTimeUnix)
	assert.Equal(t, input[0].LatestObject.LastUpdateTimeUnix, r0.LatestObject.LastUpdateTimeUnix)
	assert.Equal(t, input[0].LatestObject.Properties, r0.LatestObject.Properties)
	assert.Equal(t, input[0].Vector, r0.Vector)
	assert.Equal(t, input[0].Vectors, r0.Vectors)
	assert.Equal(t, input[0].Version, r0.Version)
	assert.Equal(t, input[0].StaleUpdateTime, r0.StaleUpdateTime)

	r1 := received[1]
	assert.True(t, r1.Deleted)
	assert.Equal(t, input[1].ID, r1.ID)
	assert.Equal(t, input[1].Version, r1.Version)
	assert.Equal(t, input[1].StaleUpdateTime, r1.StaleUpdateTime)
}
