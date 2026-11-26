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

package namespace_license

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/license"
)

const adminUser, adminKey = "admin-user", "admin-key"

// ns is the namespace every namespaced request in this package names.
const ns = "ns1"

// licenseText is the refusal of a node with NAMESPACES_ENABLED and no license
// key.
var licenseText = license.Required("namespaces").Error()

// stopGrace bounds how long RestartAt waits for the node to stop.
const stopGrace = 10 * time.Second

// newCompose returns the single-node cluster the test starts. The key file lets
// the test take the license away and give it back across restarts. Verbose
// node status counts flushed segments only, so the node flushes every memtable
// that has been dirty for 2s, however small its commit log.
func newCompose() *docker.Compose {
	return docker.New().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithRBAC().
		WithRbacRoots(adminUser).
		WithDbUsers().
		WithNamespaces().
		WithLicenseKeyFile().
		WithMCP().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("OBJECTS_TTL_DELETE_SCHEDULE", "@every 1s").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "2").
		WithWeaviateEnv("PERSISTENCE_MAX_REUSE_WAL_SIZE", "0")
}

// start starts c, points the REST and MCP helpers at it and terminates it when
// the test ends.
func start(t *testing.T, c *docker.Compose) *docker.DockerCompose {
	t.Helper()
	compose, err := c.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, compose.Terminate(context.Background()))
	})
	helper.SetupClient(compose.GetWeaviate().URI())
	return compose
}

// restart restarts the node and points the REST and MCP helpers and a new gRPC
// client at the ports it comes back on.
func restart(t *testing.T, compose *docker.DockerCompose) pb.WeaviateClient {
	t.Helper()
	grace := stopGrace
	require.NoError(t, compose.RestartAt(context.Background(), 0, &grace))
	helper.SetupClient(compose.GetWeaviate().URI())
	return grpcClient(t, compose)
}

func grpcClient(t *testing.T, compose *docker.DockerCompose) pb.WeaviateClient {
	t.Helper()
	conn, err := helper.CreateGrpcConnectionClient(compose.GetWeaviate().GrpcURI())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return helper.CreateGrpcWeaviateClient(conn)
}

func authCtx(t *testing.T, key string) context.Context {
	return metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+key)
}

// send sends a REST request as key and returns the status and the body.
func send(t *testing.T, key, method, path string, body any) (int, string) {
	t.Helper()
	var reqBody io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, helper.GetWeaviateURL()+path, reqBody)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// assertLicenseRefusal sends a REST request as key and asserts the license
// 403.
func assertLicenseRefusal(t *testing.T, key, method, path string, body any) {
	t.Helper()
	code, respBody := send(t, key, method, path, body)
	assert.Equal(t, http.StatusForbidden, code, respBody)
	assert.Contains(t, respBody, licenseText)
}

type restRequest struct {
	name, method, path string
	body               any
}

// dataRequests returns the REST data requests the licensed and unlicensed
// phases send on class. The batch insert writes id.
func dataRequests(class string, id strfmt.UUID) []restRequest {
	return []restRequest{
		{"get class", http.MethodGet, "/v1/schema/" + class, nil},
		{"list objects", http.MethodGet, "/v1/objects?class=" + class, nil},
		{"batch objects", http.MethodPost, "/v1/batch/objects", map[string]any{
			"objects": []any{map[string]any{"class": class, "id": id, "properties": map[string]any{"title": "Heat"}}},
		}},
		{"bm25 search", http.MethodPost, "/v1/search/" + class + "/bm25", map[string]any{"query": "heat"}},
		{"aggregate", http.MethodPost, "/v1/aggregate/" + class, map[string]any{}},
	}
}

type grpcCall struct {
	name string
	call func(ctx context.Context) error
}

// grpcCalls returns the unary gRPC calls the licensed and unlicensed phases
// send on class, and TenantsGet on the multi-tenant tenantClass. BatchDelete's
// filter matches no object.
func grpcCalls(client pb.WeaviateClient, class, tenantClass string) []grpcCall {
	return []grpcCall{
		{"Search", func(ctx context.Context) error {
			_, err := client.Search(ctx, &pb.SearchRequest{
				Collection: class, Limit: 1, Uses_123Api: true, Uses_125Api: true, Uses_127Api: true,
			})
			return err
		}},
		{"Aggregate", func(ctx context.Context) error {
			_, err := client.Aggregate(ctx, &pb.AggregateRequest{Collection: class, ObjectsCount: true})
			return err
		}},
		{"TenantsGet", func(ctx context.Context) error {
			_, err := client.TenantsGet(ctx, &pb.TenantsGetRequest{Collection: tenantClass})
			return err
		}},
		{"BatchDelete", func(ctx context.Context) error {
			_, err := client.BatchDelete(ctx, &pb.BatchDeleteRequest{
				Collection: class,
				Filters: &pb.Filters{
					Operator:  pb.Filters_OPERATOR_EQUAL,
					TestValue: &pb.Filters_ValueText{ValueText: "no such title"},
					Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "title"}},
				},
			})
			return err
		}},
	}
}

func batchObject(class string, id strfmt.UUID) *pb.BatchObject {
	return &pb.BatchObject{Uuid: id.String(), Collection: class}
}

type referenceBatch struct {
	name string
	send func(t *testing.T, key string) []string
}

// referenceBatches returns the REST, gRPC BatchReferences and BatchStream ways
// to add a reference from id to itself through the property related of class.
// Each sends it as key and returns the per-reference errors.
func referenceBatches(client pb.WeaviateClient, class string, id strfmt.UUID) []referenceBatch {
	ref := &pb.BatchReference{Name: "related", FromCollection: class, FromUuid: id.String(), ToCollection: &class, ToUuid: id.String()}
	return []referenceBatch{
		{"REST batch references", func(t *testing.T, key string) []string {
			resp, err := helper.Client(t).Batch.BatchReferencesCreate(
				batch.NewBatchReferencesCreateParams().WithBody([]*models.BatchReference{{
					From: strfmt.URI("weaviate://localhost/" + class + "/" + id.String() + "/related"),
					To:   strfmt.URI("weaviate://localhost/" + class + "/" + id.String()),
				}}),
				helper.CreateAuth(key))
			require.NoError(t, err)
			var errs []string
			for _, row := range resp.Payload {
				if row.Result != nil && row.Result.Errors != nil {
					for _, e := range row.Result.Errors.Error {
						errs = append(errs, e.Message)
					}
				}
			}
			return errs
		}},
		{"gRPC BatchReferences", func(t *testing.T, key string) []string {
			resp, err := client.BatchReferences(authCtx(t, key), &pb.BatchReferencesRequest{References: []*pb.BatchReference{ref}})
			require.NoError(t, err)
			var errs []string
			for _, e := range resp.Errors {
				errs = append(errs, e.Error)
			}
			return errs
		}},
		{"BatchStream with only references", func(t *testing.T, key string) []string {
			var errs []string
			for _, e := range runOpenReferenceStream(t, client, key, ref) {
				errs = append(errs, e.Error)
			}
			return errs
		}},
	}
}

// runBatchStream sends data on a new BatchStream as key and stops the stream.
// It returns the per-item errors, and the error the stream ended with, nil
// where it ended cleanly.
func runBatchStream(t *testing.T, client pb.WeaviateClient, key string, data *pb.BatchStreamRequest_Data) ([]*pb.BatchStreamReply_Results_Error, error) {
	t.Helper()
	stream := startBatchStream(t, client, key)

	// Send answers io.EOF once the server has ended the stream, and Recv
	// below returns the reason.
	for _, msg := range []*pb.BatchStreamRequest{
		{Message: &pb.BatchStreamRequest_Data_{Data: data}},
		{Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}}},
	} {
		if err := stream.Send(msg); err != nil {
			require.ErrorIs(t, err, io.EOF)
			break
		}
	}
	require.NoError(t, stream.CloseSend())

	var errs []*pb.BatchStreamReply_Results_Error
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return errs, nil
		}
		if err != nil {
			return errs, err
		}
		errs = append(errs, msg.GetResults().GetErrors()...)
	}
}

// runOpenReferenceStream sends ref twice on one BatchStream as key, waiting for
// each message's results, and returns the first message's errors. The second
// message gets results only if the stream stayed open after the first.
func runOpenReferenceStream(t *testing.T, client pb.WeaviateClient, key string, ref *pb.BatchReference) []*pb.BatchStreamReply_Results_Error {
	t.Helper()
	stream := startBatchStream(t, client, key)
	data := &pb.BatchStreamRequest_Data{
		References: &pb.BatchStreamRequest_Data_References{Values: []*pb.BatchReference{ref}},
	}
	var first []*pb.BatchStreamReply_Results_Error
	for i := range 2 {
		require.NoError(t, stream.Send(&pb.BatchStreamRequest{Message: &pb.BatchStreamRequest_Data_{Data: data}}))
		for {
			msg, err := stream.Recv()
			require.NoError(t, err, "the stream must stay open")
			if res := msg.GetResults(); len(res.GetErrors())+len(res.GetSuccesses()) > 0 {
				if i == 0 {
					first = res.GetErrors()
				}
				break
			}
		}
	}
	require.NoError(t, stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}},
	}))
	require.NoError(t, stream.CloseSend())
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return first
		}
		require.NoError(t, err)
	}
}

// startBatchStream opens a BatchStream as key and waits for its Started reply.
func startBatchStream(t *testing.T, client pb.WeaviateClient, key string) pb.Weaviate_BatchStreamClient {
	t.Helper()
	stream, err := client.BatchStream(authCtx(t, key))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.BatchStreamRequest{
		Message: &pb.BatchStreamRequest_Start_{Start: &pb.BatchStreamRequest_Start{}},
	}))
	started, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, started.GetStarted())
	return stream
}

// readDataRole is a global role that lets a namespaced user read the objects
// of every collection in its own namespace.
func readDataRole(name string) *models.Role {
	return &models.Role{
		Name: authorization.String(name),
		Permissions: []*models.Permission{
			helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection("*").Permission(),
		},
	}
}

// requireObjectCount waits until GET /v1/nodes?output=verbose counts want
// objects in class.
func requireObjectCount(t *testing.T, class string, want int64, timeout time.Duration) {
	t.Helper()
	verbose := "verbose"
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Nodes.NodesGet(
			nodes.NewNodesGetParams().WithOutput(&verbose), helper.CreateAuth(adminKey))
		if !assert.NoError(c, err) {
			return
		}
		var got int64
		for _, node := range resp.Payload.Nodes {
			for _, shard := range node.Shards {
				if shard.Class == class {
					got += shard.ObjectCount
				}
			}
		}
		assert.Equal(c, want, got)
	}, timeout, 500*time.Millisecond)
}

// mcpHybridSearch runs a BM25-only MCP hybrid search on class as key.
func mcpHybridSearch(t *testing.T, key, class string) error {
	t.Helper()
	alpha := 0.0
	var resp *search.QueryHybridResp
	return helper.CallToolOnce(t.Context(), t, "weaviate-query-hybrid", &search.QueryHybridArgs{
		CollectionName: class, Query: "heat", Alpha: &alpha,
	}, &resp, key)
}

// unlicensedWarnings counts the namespaces startup warnings in the node's log,
// which holds every boot of its container.
func unlicensedWarnings(t *testing.T, compose *docker.DockerCompose) int {
	t.Helper()
	reader, err := compose.GetWeaviate().Container().Logs(t.Context())
	require.NoError(t, err)
	defer reader.Close()

	var warnings int
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		var entry struct{ Action, Feature, Level string }
		if json.Unmarshal(scanner.Bytes(), &entry) != nil {
			continue
		}
		if entry.Action == "startup" && entry.Feature == "namespaces" && entry.Level == "warning" {
			warnings++
		}
	}
	require.NoError(t, scanner.Err())
	return warnings
}
