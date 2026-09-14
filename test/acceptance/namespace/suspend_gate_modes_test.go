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

package namespace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// Two shapes separate the request-rejection gate's refusal from the shard-load
// guard's, which answers the same request with the same sentinel.
//
// Mode A sends a search to a node holding no replica while the home node is
// stopped. ExecuteForEachShard takes its remote arm there, so GetShard is never
// entered and the shard guard cannot answer. Without the gate the answer is a
// transport failure, and with it the namespace refusal. Search verbs only.
//
// Mode B sends the request to the node holding the shard, with every node up.
// getOptInitLocalShard checks the namespace above the shard-map read, so the
// shard guard refuses there too and most Mode B rows are refusal coverage. The
// count aggregate is the exception, because its replica fan-out drops the
// guard's error while this gate refuses the whole request at authorization.

// gatePair is a suspended namespace and an active one holding the same class
// shape, so every refusal row has a control that differs only in the state.
type gatePair struct {
	suspendedNS    string
	activeNS       string
	suspendedClass string
	activeClass    string
	// The two alias fields are set by newGatePairWithAliases and are empty
	// otherwise. aliasOntoSuspended is named in the active namespace and targets
	// the suspended namespace's class. aliasOntoActive is the mirror.
	aliasOntoSuspended string
	aliasOntoActive    string
}

const gateSearchTitle = "a searchable title"

// newGatePair creates both namespaces on the given home nodes, seeds one object
// into each class, then suspends the first namespace. Every write happens before
// the suspend, because a suspended namespace turns schema and object writes away.
func newGatePair(t *testing.T, suspendedHome, activeHome string) gatePair {
	t.Helper()
	return newGatePairWith(t, suspendedHome, activeHome, false)
}

// newGatePairWithAliases is newGatePair with the two cross-namespace aliases
// linked as well. Only the rows that search through an alias take it: the link
// costs two creates and two retry-looped repoints on every fixture that asks
// for it.
func newGatePairWithAliases(t *testing.T, suspendedHome, activeHome string) gatePair {
	t.Helper()
	return newGatePairWith(t, suspendedHome, activeHome, true)
}

func newGatePairWith(t *testing.T, suspendedHome, activeHome string, withAliases bool) gatePair {
	t.Helper()
	pair := gatePair{suspendedNS: uniqueNS(), activeNS: uniqueNS()}
	const class = "Gated"

	for _, ns := range []struct{ name, home string }{
		{pair.suspendedNS, suspendedHome},
		{pair.activeNS, activeHome},
	} {
		helper.CreateNamespaceWithHomeNode(t, ns.name, ns.home, adminKey)
		t.Cleanup(func() { helper.DeleteNamespace(t, ns.name, adminKey) })

		key := createNamespacedUser(t, "u1", ns.name, adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, ns.name+":u1", adminKey) })

		setupClassInNs1(t, ns.name, class, key)
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class:      class,
			Properties: map[string]any{"title": gateSearchTitle},
		}, key)
		require.NoError(t, err)

		if withAliases {
			// Each namespace gets an alias onto its own class first, because AddAlias
			// refuses a global principal and a namespaced one cannot name another
			// namespace. The repoint below is what makes them cross-namespace.
			helper.CreateAliasAuth(t, &models.Alias{Alias: "GatedAlias", Class: class}, key)
			t.Cleanup(func() { helper.DeleteAliasWithAuthz(t, ns.name+":GatedAlias", helper.CreateAuth(adminKey)) })
		}
	}

	pair.suspendedClass = pair.suspendedNS + ":" + class
	pair.activeClass = pair.activeNS + ":" + class

	if withAliases {
		pair.aliasOntoSuspended = pair.activeNS + ":GatedAlias"
		pair.aliasOntoActive = pair.suspendedNS + ":GatedAlias"

		// UpdateAlias qualifies rather than confines, so the root operator is the one
		// principal that can point an alias at another namespace's class.
		retryOnAliasLag(t, func() error {
			_, err := helper.UpdateAliasAuthWithReturn(t, pair.aliasOntoSuspended, pair.suspendedClass, adminKey)
			return err
		})
		retryOnAliasLag(t, func() error {
			_, err := helper.UpdateAliasAuthWithReturn(t, pair.aliasOntoActive, pair.activeClass, adminKey)
			return err
		})
	}

	helper.SuspendNamespace(t, pair.suspendedNS, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, pair.suspendedNS, adminKey) })
	return pair
}

// withHomeNodeStopped is Mode A. It takes homeNode away for the duration of body,
// and it must not be made parallel, because the package's other top-level tests
// share this cluster. The caller passes the node it pinned its suspended namespace
// to. Stopping a different one leaves every row underneath green with Mode A's
// discriminating property gone.
func withHomeNodeStopped(t *testing.T, homeNode string, body func(t *testing.T)) {
	t.Helper()
	ctx := context.Background()
	// StopNode counts from 0, matching the container name, where GetWeaviateNode
	// counts from 1.
	index := nodeIndexFromName(t, homeNode) - 1

	// A failed stop or start would leave the node down for every later test in
	// the package, so put it back whatever happens here.
	t.Cleanup(func() { require.NoError(t, sharedCompose.EnsureRunning(ctx, index)) })
	require.NoError(t, sharedCompose.StopNode(ctx, index, nil))

	body(t)

	require.NoError(t, sharedCompose.StartNode(ctx, index))
}

// nodeURIs returns one cluster node's REST and gRPC addresses. A namespaced
// collection's shard lands only on its namespace's home node, so a row picks its
// target node by naming a home node rather than by querying /v1/nodes.
func nodeURIs(t *testing.T, nodeName string) (restURI, grpcURI string) {
	t.Helper()
	node := sharedCompose.GetWeaviateNode(nodeIndexFromName(t, nodeName))
	return node.URI(), node.GrpcURI()
}

// grpcTo dials one named node, where newGrpcClient is fixed to the node the shared
// client talks to. The connection closes with the test, so the caller holds only
// the client.
func grpcTo(t *testing.T, grpcURI string) pb.WeaviateClient {
	t.Helper()
	conn, err := helper.CreateGrpcConnectionClient(grpcURI)
	require.NoError(t, err)
	require.NotNil(t, conn)
	t.Cleanup(func() { conn.Close() })
	return helper.CreateGrpcWeaviateClient(conn)
}

// requestJSON sends an authenticated request to one node and returns the status
// and the decoded body. Raw HTTP rather than the generated client, because the
// status these endpoints give a namespace refusal is expected to change, and a
// typed response would bind every row to today's. Pass a nil body for the verbs
// that carry none, and any JSON-encodable value for the rest: PUT on a reference
// list takes an array where POST takes one object. A response with no body of
// its own, which HEAD always gives
// and 204 gives too, comes back as a nil map rather than failing the row.
func requestJSON(t *testing.T, method, uri, path, key string, body any) (int, map[string]any) {
	t.Helper()
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, fmt.Sprintf("http://%s%s", uri, path), payload)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+key)

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var decoded any
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &decoded))
	}
	// A batch that succeeded answers with an array rather than an object, which
	// is what a polling row sees before the suspend reaches the node it asks.
	// Returning a nil map lets that row keep polling instead of dying on the
	// decode.
	out, _ := decoded.(map[string]any)
	return resp.StatusCode, out
}

// postJSON is requestJSON for the POST rows, which are most of them.
func postJSON(t *testing.T, uri, path, key string, body map[string]any) (int, map[string]any) {
	t.Helper()
	return requestJSON(t, http.MethodPost, uri, path, key, body)
}

// restSearch runs POST /v1/search/<collection>/bm25 against one node. bm25 needs
// no vectorizer, which the shared cluster does not configure.
func restSearch(t *testing.T, uri, collection, key string) (int, map[string]any) {
	t.Helper()
	return postJSON(t, uri, "/v1/search/"+collection+"/bm25", key,
		map[string]any{"query": gateSearchTitle})
}

// restAggregateCount runs POST /v1/aggregate/<collection> asking only for the
// count, which is the shape whose replica fan-out swallows the shard guard's
// error and answers 0 inside a success.
func restAggregateCount(t *testing.T, uri, collection, key string) (int, map[string]any) {
	t.Helper()
	return postJSON(t, uri, "/v1/aggregate/"+collection, key,
		map[string]any{"returnMetrics": []string{"count"}})
}

// requireRefused retries until the call is turned away and says why.
// SuspendNamespace confirms the flip on one node and no surface reads another
// node's own copy, so a row aimed elsewhere can only wait for the answer to change.
func requireRefused(t *testing.T, call func() error) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		err := call()
		if !assert.Error(c, err) {
			return
		}
		assert.Contains(c, err.Error(), "suspended")
	}, 30*time.Second, 200*time.Millisecond, "the refusal never reached the node the request went to")
}

// requireRESTRefusedAs is requireRESTRefused with the status pinned. Use it where
// the status is what separates this gate's refusal from another layer's.
func requireRESTRefusedAs(t *testing.T, wantStatus int, call func() (int, map[string]any)) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, body := call()
		if !assert.Equal(c, wantStatus, status, "body: %v", body) {
			return
		}
		assert.Contains(c, restErrMessage(body), "suspended")
	}, 30*time.Second, 200*time.Millisecond, "the refusal never reached the node the request went to")
}

// requireRESTRefused is requireRefused for the raw-HTTP rows, which carry their
// refusal in the status and body rather than in a returned error.
func requireRESTRefused(t *testing.T, call func() (int, map[string]any)) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status, body := call()
		if !assert.NotEqual(c, http.StatusOK, status, "the request must not succeed: %v", body) {
			return
		}
		assert.Contains(c, restErrMessage(body), "suspended")
	}, 30*time.Second, 200*time.Millisecond, "the refusal never reached the node the request went to")
}

// awaitSuspendVisible blocks until the node at grpcURI turns a root search away,
// which is the only signal that its own copy of the state has caught up. A row
// whose assertion holds whatever the state is needs this, or it passes against a
// namespace the node still believes is active.
func awaitSuspendVisible(t *testing.T, grpcURI, qualifiedClass string) {
	t.Helper()
	client := grpcTo(t, grpcURI)
	requireRefused(t, func() error {
		_, err := client.Search(authCtx(adminKey), searchReq(qualifiedClass, 10))
		return err
	})
}

// restErrMessage reads the message out of either error shape the REST tier
// produces: the handler's ErrorResponse, or the swagger bind tier's flat object.
// It returns the whole body when neither shape matches, so a row that fails says
// what came back.
func restErrMessage(body map[string]any) string {
	if items, ok := body["error"].([]any); ok && len(items) > 0 {
		if item, ok := items[0].(map[string]any); ok {
			if msg, ok := item["message"].(string); ok {
				return msg
			}
		}
	}
	if msg, ok := body["message"].(string); ok {
		return msg
	}
	return fmt.Sprintf("%v", body)
}

// The home nodes each mode pins its namespaces to. Mode A puts the suspended
// namespace on the node it stops and the active one on another node the request
// does not reach. Its control row therefore takes the same remote arm the refusal
// rows do. Mode B puts both on the node it sends to and stops nothing.
const (
	modeAHomeSuspended = restartNodeName
	modeAHomeActive    = docker.Weaviate1
	modeARequestNode   = docker.Weaviate0
	modeBNode          = docker.Weaviate1
)
