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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_SuspendedNamespaceRefusesSearch runs every search transport in
// Mode A, where each row fails on transport without the gate. It must never run
// in parallel, because it stops a node the package's other tests share.
func TestNamespaces_SuspendedNamespaceRefusesSearch(t *testing.T) {
	pair := newGatePair(t, modeAHomeSuspended, modeAHomeActive)
	restURI, grpcURI := nodeURIs(t, modeARequestNode)
	alpha := 0.0

	hybrid := func(t *testing.T, collection string) error {
		t.Helper()
		var resp *search.QueryHybridResp
		return helper.CallToolOnce(t.Context(), t, mcpToolHybrid, &search.QueryHybridArgs{
			CollectionName: collection,
			Query:          gateSearchTitle,
			Alpha:          &alpha,
		}, &resp, adminKey)
	}

	withHomeNodeStopped(t, modeAHomeSuspended, func(t *testing.T) {
		// A namespace that is active but equally remote still answers here, so a
		// transport failure caused by the stopped node does not read as a refusal.
		t.Run("an active namespace still answers with a node stopped", func(t *testing.T) {
			status, body := restSearch(t, restURI, pair.activeClass, adminKey)
			require.Equal(t, http.StatusOK, status, "%v", body)

			client := grpcTo(t, grpcURI)
			_, err := client.Search(authCtx(adminKey), searchReq(pair.activeClass, 10))
			require.NoError(t, err)

			require.NoError(t, hybrid(t, pair.activeClass))
		})

		t.Run("REST search is refused", func(t *testing.T) {
			requireRESTRefused(t, func() (int, map[string]any) {
				return restSearch(t, restURI, pair.suspendedClass, adminKey)
			})
		})

		t.Run("gRPC search is refused", func(t *testing.T) {
			client := grpcTo(t, grpcURI)
			requireRefused(t, func() error {
				_, err := client.Search(authCtx(adminKey), searchReq(pair.suspendedClass, 10))
				return err
			})
		})

		t.Run("MCP hybrid search is refused", func(t *testing.T) {
			// CallToolOnce builds its client from the process-global address and
			// takes none, so this row reaches modeARequestNode only while that is
			// the node TestMain pinned.
			restURIOfGlobalClient, _ := nodeURIs(t, modeARequestNode)
			require.Contains(t, helper.GetWeaviateURL(), restURIOfGlobalClient)

			requireRefused(t, func() error { return hybrid(t, pair.suspendedClass) })
		})
	})
}

// TestNamespaces_SuspendedNamespaceRefusesAggregate drives the count aggregate
// in Mode B, on the node holding the shard. Its replica fan-out drops the shard
// guard's error and answers 0 in a success, so only the gate refuses it.
func TestNamespaces_SuspendedNamespaceRefusesAggregate(t *testing.T) {
	t.Parallel()
	pair := newGatePair(t, modeBNode, modeBNode)
	restURI, grpcURI := nodeURIs(t, modeBNode)
	client := grpcTo(t, grpcURI)

	grpcCount := func(collection string) (*pb.AggregateReply, error) {
		return client.Aggregate(authCtx(adminKey), &pb.AggregateRequest{
			Collection: collection, ObjectsCount: true,
		})
	}

	// An active namespace answers with the object that was seeded, so a refusal row
	// is not green on a count that would have come back empty anyway.
	t.Run("an active namespace counts its objects", func(t *testing.T) {
		status, body := restAggregateCount(t, restURI, pair.activeClass, adminKey)
		require.Equal(t, http.StatusOK, status, "%v", body)
		assert.Equal(t, float64(1), body["count"])

		reply, err := grpcCount(pair.activeClass)
		require.NoError(t, err)
		require.Equal(t, int64(1), reply.GetSingleResult().GetObjectsCount())
	})

	t.Run("REST aggregate is refused", func(t *testing.T) {
		requireRESTRefused(t, func() (int, map[string]any) {
			return restAggregateCount(t, restURI, pair.suspendedClass, adminKey)
		})
	})

	t.Run("gRPC aggregate is refused", func(t *testing.T) {
		requireRefused(t, func() error {
			_, err := grpcCount(pair.suspendedClass)
			return err
		})
	})

	// Last, because it puts the namespace back. The gate must lift again, or a
	// suspend would be one-way. Mode B keeps every node up, so the shard the read
	// needs is still resident, where on Mode A a resume reopens no shard a boot
	// skipped.
	t.Run("resuming the namespace serves the class again", func(t *testing.T) {
		helper.ResumeNamespace(t, pair.suspendedNS, adminKey)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			status, body := restAggregateCount(t, restURI, pair.suspendedClass, adminKey)
			assert.Equal(c, http.StatusOK, status, "%v", body)
		}, 30*time.Second, 200*time.Millisecond, "the resume never reached the node the request went to")
	})
}

// TestNamespaces_SuspendedNamespaceRefusesBatchDelete drives gRPC BatchDelete on
// the node holding the shard. Mode B. The gate answers at parameter parsing,
// before any shard is touched, where the same request without it reaches the
// closed shard.
func TestNamespaces_SuspendedNamespaceRefusesBatchDelete(t *testing.T) {
	t.Parallel()
	pair := newGatePair(t, modeBNode, modeBNode)
	_, grpcURI := nodeURIs(t, modeBNode)

	client := grpcTo(t, grpcURI)
	deleteFrom := func(collection string) error {
		_, err := client.BatchDelete(authCtx(adminKey), &pb.BatchDeleteRequest{
			Collection: collection,
			DryRun:     true,
			Filters: &pb.Filters{
				Operator:  pb.Filters_OPERATOR_EQUAL,
				Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "title"}},
				TestValue: &pb.Filters_ValueText{ValueText: gateSearchTitle},
			},
		})
		return err
	}

	// An active namespace takes the same request, so the row below is not green on
	// a filter shape that would have been rejected anyway.
	require.NoError(t, deleteFrom(pair.activeClass))

	// The shard-load guard answers this verb with the same sentinel text, and it
	// reaches the caller through "batch delete: " at service.go:207. Only the gate
	// answers at parameter parsing, so only the gate's refusal carries this wrap.
	requireRefused(t, func() error { return deleteFrom(pair.suspendedClass) }, "batch delete params: ")
}

// TestNamespaces_SuspendedNamespaceKeepsDenialsOpaque pins that the gate does not
// turn an unauthorized request into a namespace-state probe. A global operator
// holding no permission gets the same answer whether the namespace is suspended or
// active. Mode B, since authorization runs before any dispatch.
func TestNamespaces_SuspendedNamespaceKeepsDenialsOpaque(t *testing.T) {
	t.Parallel()
	pair := newGatePair(t, modeBNode, modeBNode)
	restURI, grpcURI := nodeURIs(t, modeBNode)
	awaitSuspendVisible(t, grpcURI, pair.suspendedClass)

	// gNoRole is a global static-key operator that is not a root and that no test
	// binds a role to, so every request below is denied on the resource alone.
	// gAdmin cannot stand in. A parallel test grants it the admin role at runtime,
	// and the overlap fails these rows with the signature of the leak they exist
	// to catch.
	for _, tt := range []struct{ name, collection string }{
		{"a suspended namespace", pair.suspendedClass},
		{"an active namespace", pair.activeClass},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, search := range []func(*testing.T, string, string, string) (int, map[string]any){
				restSearch, restAggregateCount,
			} {
				status, body := search(t, restURI, tt.collection, gNoRoleKey)
				assert.Equal(t, http.StatusForbidden, status, "%v", body)
				assert.NotContains(t, restErrMessage(body), "suspended")
			}

			client := grpcTo(t, grpcURI)
			_, err := client.Search(authCtx(gNoRoleKey), searchReq(tt.collection, 10))
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "suspended")

			_, err = client.Aggregate(authCtx(gNoRoleKey), &pb.AggregateRequest{
				Collection: tt.collection, ObjectsCount: true,
			})
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "suspended")
		})
	}
}
