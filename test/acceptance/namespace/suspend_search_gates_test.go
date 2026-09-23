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
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_SuspendedNamespaceRefusesSearch runs every search transport
// against a node holding none of the suspended namespace's shards, with its home
// node stopped. Mode A: the shard-load guard is out of reach there, so without the
// request-rejection gate each row answers a transport failure instead.
//
// Not parallel, and it must stay that way — it takes a node away from the cluster
// the package's parallel tests share.
func TestNamespaces_SuspendedNamespaceRefusesSearch(t *testing.T) {
	pair := newGatePairWithAliases(t, modeAHomeSuspended, modeAHomeActive)
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

		// Resolve returns the alias target, so both alias rows are decided by the
		// target's namespace and not by the one the alias is named in.
		t.Run("an alias whose target is in a suspended namespace is refused", func(t *testing.T) {
			requireRESTRefused(t, func() (int, map[string]any) {
				return restSearch(t, restURI, pair.aliasOntoSuspended, adminKey)
			})

			client := grpcTo(t, grpcURI)
			requireRefused(t, func() error {
				_, err := client.Search(authCtx(adminKey), searchReq(pair.aliasOntoSuspended, 10))
				return err
			})
		})

		t.Run("an alias named in a suspended namespace whose target is active is served", func(t *testing.T) {
			awaitSuspendVisible(t, grpcURI, pair.suspendedClass)

			status, body := restSearch(t, restURI, pair.aliasOntoActive, adminKey)
			require.Equal(t, http.StatusOK, status, "%v", body)

			client := grpcTo(t, grpcURI)
			_, err := client.Search(authCtx(adminKey), searchReq(pair.aliasOntoActive, 10))
			require.NoError(t, err)
		})
	})
}

// TestNamespaces_SuspendedNamespaceRefusesAggregate drives the count aggregate on
// the node holding the shard. Mode B, and one of its discriminating shapes. The
// replica fan-out swallows the shard guard's error and answers 0 inside a success,
// so only the request-rejection gate turns the request away.
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

// TestNamespaces_SuspendedNamespaceRefusesBatchDelete checks that gRPC
// BatchDelete is refused at parameter parsing, before it reaches the shard.
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

	// The active twin accepts the request, so the refusal below is not a bad filter.
	require.NoError(t, deleteFrom(pair.activeClass))

	// Only the gate's refusal carries this wrap. The shard-load guard's arrives
	// under "batch delete: ".
	requireRefused(t, func() error { return deleteFrom(pair.suspendedClass) }, "batch delete params: ")
}

// TestNamespaces_SuspendedNamespaceRefusesGRPCBatchWrites checks that
// BatchObjects and BatchStream refuse only the suspended class's object. The
// shard-load guard fails the same way, so grpc/v1/batch unit tests pin the gate.
func TestNamespaces_SuspendedNamespaceRefusesGRPCBatchWrites(t *testing.T) {
	t.Parallel()
	pair := newGatePair(t, modeBNode, modeBNode)
	_, grpcURI := nodeURIs(t, modeBNode)
	client := grpcTo(t, grpcURI)

	// A retry reuses these ids and overwrites the active object.
	activeID, suspendedID := uuid.NewString(), uuid.NewString()
	objects := []*pb.BatchObject{
		{Uuid: activeID, Collection: pair.activeClass, Properties: titleProps("into the active namespace")},
		{Uuid: suspendedID, Collection: pair.suspendedClass, Properties: titleProps("into the suspended namespace")},
	}

	t.Run("BatchObjects refuses the suspended class's object only", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			reply, err := client.BatchObjects(authCtx(adminKey), &pb.BatchObjectsRequest{Objects: objects})
			if !assert.NoError(c, err) {
				return
			}
			errs := map[int32]string{}
			for _, e := range reply.GetErrors() {
				errs[e.GetIndex()] = e.GetError()
			}
			assert.NotContains(c, errs, int32(0), "the active namespace's object was refused")
			assert.Contains(c, errs[1], "suspended")
		}, 30*time.Second, 200*time.Millisecond, "the refusal never reached the node the request went to")
	})

	// The ack races the workers' authorization, so read the refusal from Results frames.
	t.Run("BatchStream refuses the suspended class's object only", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			successes, errs, err := streamBatch(client, adminKey, objects)
			if !assert.NoError(c, err) {
				return
			}
			assert.Equal(c, 1, successes)
			byID := map[string]string{}
			for _, e := range errs {
				byID[e.GetUuid()] = e.GetError()
			}
			assert.NotContains(c, byID, activeID, "the active namespace's object was refused")
			assert.Contains(c, byID[suspendedID], "suspended")
		}, 30*time.Second, 200*time.Millisecond, "the refusal never reached the node the request went to")
	})
}

func titleProps(title string) *pb.BatchObject_Properties {
	return &pb.BatchObject_Properties{NonRefProperties: &structpb.Struct{
		Fields: map[string]*structpb.Value{"title": structpb.NewStringValue(title)},
	}}
}

// streamBatch sends objs in one Data message and totals the Results frames.
func streamBatch(client pb.WeaviateClient, key string, objs []*pb.BatchObject) (int, []*pb.BatchStreamReply_Results_Error, error) {
	stream, err := client.BatchStream(authCtx(key))
	if err != nil {
		return 0, nil, err
	}
	for _, msg := range []*pb.BatchStreamRequest{
		{Message: &pb.BatchStreamRequest_Start_{Start: &pb.BatchStreamRequest_Start{}}},
		{Message: &pb.BatchStreamRequest_Data_{Data: &pb.BatchStreamRequest_Data{
			Objects: &pb.BatchStreamRequest_Data_Objects{Values: objs},
		}}},
		{Message: &pb.BatchStreamRequest_Stop_{Stop: &pb.BatchStreamRequest_Stop{}}},
	} {
		if err := stream.Send(msg); err != nil {
			return 0, nil, err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return 0, nil, err
	}

	var successes int
	var errs []*pb.BatchStreamReply_Results_Error
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return successes, errs, nil
		}
		if err != nil {
			return 0, nil, err
		}
		if r := msg.GetResults(); r != nil {
			successes += len(r.GetSuccesses())
			errs = append(errs, r.GetErrors()...)
		}
	}
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
