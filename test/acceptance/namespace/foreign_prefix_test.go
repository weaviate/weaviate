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
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

// foreignAnswer is what one request naming another namespace's collection gets
// back. status holds the HTTP status or the gRPC code.
type foreignAnswer struct {
	status  string
	message string
}

// TestNamespaces_ForeignPrefixedClassIsRejected sends "<ns2>:<class>" from a
// caller confined to ns1. Every surface must refuse it as an invalid name and
// answer the same whether ns2 is missing, empty or holds the object each row names.
func TestNamespaces_ForeignPrefixedClassIsRejected(t *testing.T) {
	t.Parallel()
	ns1, ns2 := uniqueNS(), uniqueNS()
	helper.CreateNamespace(t, ns1, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns1, adminKey) })
	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns1+":u1", adminKey) })

	const class, tenant = "ForeignTarget", "t1"
	foreign := ns2 + ":" + class
	invalidName := "'" + foreign + "' is not a valid class name"
	id := strfmt.UUID("6b3f2a10-0000-4000-8000-f0e1d2c3b4a5")

	node := sharedCompose.GetWeaviate()
	grpcClient := grpcTo(t, node.GrpcURI())

	rest := func(method, path string, body any) func(t *testing.T) foreignAnswer {
		return func(t *testing.T) foreignAnswer {
			code, payload := requestJSON(t, method, node.URI(), path, user1Key, body)
			return foreignAnswer{status: strconv.Itoa(code), message: restErrMessage(payload)}
		}
	}
	viaGRPC := func(call func(ctx context.Context) error) func(t *testing.T) foreignAnswer {
		return func(t *testing.T) foreignAnswer {
			st, _ := status.FromError(call(authCtx(user1Key)))
			return foreignAnswer{status: st.Code().String(), message: st.Message()}
		}
	}

	object := map[string]any{"class": foreign, "id": id, "tenant": tenant, "properties": map[string]any{"title": "probe"}}
	objectURL := objectPath(foreign, id) + "?tenant=" + tenant
	refURL := objectPath(foreign, id) + "/references/related?tenant=" + tenant
	beacon := map[string]any{"beacon": "weaviate://localhost/" + class + "/" + string(id)}
	tenantsURL := "/v1/schema/" + foreign + "/tenants"
	restMatchAll := map[string]any{"operator": "Like", "path": []string{"title"}, "valueText": "*"}
	// gRPC hands a plain handler error back as Unknown, and the REST search
	// routes answer an invalid collection name with 400.
	grpcUnknown, searchInvalid := codes.Unknown.String(), strconv.Itoa(http.StatusBadRequest)
	matchAll := &pb.Filters{
		Operator:  pb.Filters_OPERATOR_LIKE,
		TestValue: &pb.Filters_ValueText{ValueText: "*"},
		Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "title"}},
	}

	probes := []struct {
		name   string
		call   func(t *testing.T) foreignAnswer
		want   string // the loop fills in 422
		noBody bool   // a HEAD answer carries no message
	}{
		{name: "create object", call: rest(http.MethodPost, "/v1/objects", object)},
		{name: "get object", call: rest(http.MethodGet, objectURL, nil)},
		{name: "head object", call: rest(http.MethodHead, objectURL, nil), noBody: true},
		// PUT takes the class from the body and ignores the path's, so this row pins the body.
		{name: "replace object", call: rest(http.MethodPut, objectURL, object)},
		{name: "merge object", call: rest(http.MethodPatch, objectURL, object)},
		{name: "delete object", call: rest(http.MethodDelete, objectURL, nil)},
		{name: "validate object", call: rest(http.MethodPost, "/v1/objects/validate", object)},
		{name: "list objects", call: rest(http.MethodGet, "/v1/objects?class="+foreign+"&tenant="+tenant, nil)},
		{name: "batch delete objects", call: rest(http.MethodDelete, "/v1/batch/objects?tenant="+tenant,
			map[string]any{"match": map[string]any{"class": foreign, "where": restMatchAll}})},
		{name: "batch create objects", call: rest(http.MethodPost, "/v1/batch/objects", map[string]any{"objects": []any{object}})},
		{name: "add reference", call: rest(http.MethodPost, refURL, beacon)},
		{name: "replace references", call: rest(http.MethodPut, refURL, []any{beacon})},
		{name: "delete reference", call: rest(http.MethodDelete, refURL, beacon)},
		{name: "list tenants", call: rest(http.MethodGet, tenantsURL, nil)},
		{name: "get tenant", call: rest(http.MethodGet, tenantsURL+"/"+tenant, nil)},
		{name: "tenant exists", call: rest(http.MethodHead, tenantsURL+"/"+tenant, nil), noBody: true},
		{name: "create tenant", call: rest(http.MethodPost, tenantsURL, []any{map[string]any{"name": "t2"}})},
		{name: "update tenant", call: rest(http.MethodPut, tenantsURL,
			[]any{map[string]any{"name": tenant, "activityStatus": models.TenantActivityStatusCOLD}})},
		{name: "delete tenant", call: rest(http.MethodDelete, tenantsURL, []string{tenant})},
		{name: "shard status", call: rest(http.MethodGet, "/v1/schema/"+foreign+"/shards?tenant="+tenant, nil)},
		{name: "update shard status", call: rest(http.MethodPut, "/v1/schema/"+foreign+"/shards/"+tenant,
			map[string]any{"status": "READONLY"})},
		{name: "tokenize property", call: rest(http.MethodPost, "/v1/schema/"+foreign+"/properties/title/tokenize",
			map[string]any{"text": "hello world"})},
		{name: "search", call: rest(http.MethodPost, "/v1/search/"+foreign+"/bm25",
			map[string]any{"query": "probe", "tenant": tenant}), want: searchInvalid},
		{name: "aggregate", call: rest(http.MethodPost, "/v1/aggregate/"+foreign,
			map[string]any{"returnMetrics": []string{"count"}, "tenant": tenant}), want: searchInvalid},
		{name: "gRPC Search", call: viaGRPC(func(ctx context.Context) error {
			req := searchReq(foreign, 1)
			req.Tenant = tenant
			_, err := grpcClient.Search(ctx, req)
			return err
		}), want: grpcUnknown},
		{name: "gRPC Aggregate", call: viaGRPC(func(ctx context.Context) error {
			_, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{Collection: foreign, Tenant: tenant, ObjectsCount: true})
			return err
		}), want: grpcUnknown},
		{name: "gRPC BatchDelete", call: viaGRPC(func(ctx context.Context) error {
			t1 := tenant
			_, err := grpcClient.BatchDelete(ctx, &pb.BatchDeleteRequest{Collection: foreign, Tenant: &t1, Filters: matchAll})
			return err
		}), want: grpcUnknown},
		{name: "gRPC TenantsGet", call: viaGRPC(func(ctx context.Context) error {
			_, err := grpcClient.TenantsGet(ctx, &pb.TenantsGetRequest{Collection: foreign})
			return err
		}), want: grpcUnknown},
	}

	states := []struct {
		name  string
		setup func(t *testing.T)
	}{
		{name: "the foreign namespace does not exist", setup: func(t *testing.T) {}},
		{name: "the foreign namespace has no such collection", setup: func(t *testing.T) {
			helper.CreateNamespace(t, ns2, adminKey)
			t.Cleanup(func() { helper.DeleteNamespace(t, ns2, adminKey) })
		}},
		// The control read proves the object is there, so a row that resolved the
		// foreign name would have reached it.
		{name: "the foreign collection holds the object", setup: func(t *testing.T) {
			user2Key := createNamespacedUser(t, "u2", ns2, adminKey)
			t.Cleanup(func() { helper.DeleteUser(t, ns2+":u2", adminKey) })
			foreignClass := mtClass(class)
			foreignClass.Properties = append(foreignClass.Properties,
				&models.Property{Name: "related", DataType: []string{class}})
			helper.CreateClassAuth(t, foreignClass, user2Key)
			t.Cleanup(func() { helper.DeleteClassAuth(t, foreign, adminKey) })
			require.NoError(t, addTenantsAuth(t, class, []*models.Tenant{{Name: tenant}}, user2Key))
			_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
				ID: id, Class: class, Tenant: tenant, Properties: map[string]any{"title": "foreign"},
			}, user2Key)
			require.NoError(t, err)

			code, body := requestJSON(t, http.MethodGet, node.URI(), objectPath(class, id)+"?tenant="+tenant, user2Key, nil)
			require.Equal(t, http.StatusOK, code, "%v", body)
		}},
	}

	baseline := make([]foreignAnswer, len(probes))
	for _, state := range states {
		state.setup(t)
		t.Run(state.name, func(t *testing.T) {
			for i, p := range probes {
				t.Run(p.name, func(t *testing.T) {
					want := p.want
					if want == "" {
						want = strconv.Itoa(http.StatusUnprocessableEntity)
					}
					got := p.call(t)
					assert.Equal(t, want, got.status, got.message)
					if !p.noBody {
						assert.Contains(t, got.message, invalidName)
					}
					if baseline[i] == (foreignAnswer{}) {
						baseline[i] = got
					} else {
						assert.Equal(t, baseline[i], got, "the answer depends on what %s holds", ns2)
					}
				})
			}
		})
	}
}
