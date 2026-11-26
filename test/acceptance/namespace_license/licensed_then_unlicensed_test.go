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
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestNamespaceLicense_LicensedThenUnlicensed restarts a licensed node without
// its key, then with it. The unlicensed node refuses namespace operations and
// requests naming a collection. No refused write lands, and no object is lost.
func TestNamespaceLicense_LicensedThenUnlicensed(t *testing.T) {
	compose := start(t, newCompose())
	client := grpcClient(t, compose)

	helper.CreateNamespace(t, ns, adminKey)
	userKey := helper.CreateUserWithNamespace(t, "carol", ns, adminKey)
	helper.AssignRoleToUser(t, adminKey, authorization.Admin, ns+":carol")
	createClasses(t, userKey)

	var (
		movieID       = strfmt.UUID("22222222-0000-0000-0000-000000000001")
		controlID     = strfmt.UUID("22222222-0000-0000-0000-000000000002")
		expiringID    = strfmt.UUID("22222222-0000-0000-0000-000000000003")
		refusedRESTID = strfmt.UUID("22222222-0000-0000-0000-000000000004")
		refusedGRPCID = strfmt.UUID("22222222-0000-0000-0000-000000000005")
	)
	require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
		ID: movieID, Class: "Movies", Properties: map[string]any{"title": "Heat"},
	}, userKey))
	createExpiring(t, userKey, controlID, time.Now().Add(24*time.Hour))

	// A namespaced user names its collections bare, and an operator qualifies
	// them.
	callers := []struct{ name, key, prefix string }{
		{"namespaced user", userKey, ""},
		{"operator", adminKey, ns + ":"},
	}

	t.Run("licensed", func(t *testing.T) {
		for _, caller := range callers {
			t.Run(caller.name, func(t *testing.T) {
				class := caller.prefix + "Movies"
				for _, r := range dataRequests(class, movieID) {
					code, body := send(t, caller.key, r.method, r.path, r.body)
					assert.Equal(t, http.StatusOK, code, "%s: %s", r.name, body)
					// A REST batch answers 200 with FAILED rows.
					assert.NotContains(t, body, "FAILED", r.name)
				}
				for _, c := range grpcCalls(client, class, caller.prefix+"Shelves") {
					assert.NoError(t, c.call(authCtx(t, caller.key)), c.name)
				}
				resp, err := client.BatchObjects(authCtx(t, caller.key), &pb.BatchObjectsRequest{
					Objects: []*pb.BatchObject{batchObject(class, movieID)},
				})
				require.NoError(t, err)
				assert.Empty(t, resp.Errors)
				for _, b := range referenceBatches(client, class, movieID) {
					assert.Empty(t, b.send(t, caller.key), b.name)
				}
				assert.NoError(t, mcpHybridSearch(t, caller.key, class), "MCP hybrid search")
			})
		}

		t.Run("namespace operations", func(t *testing.T) {
			assert.Equal(t, ns, helper.GetNamespace(t, ns, adminKey).Name)
			var names []string
			for _, n := range helper.ListNamespaces(t, adminKey) {
				names = append(names, n.Name)
			}
			assert.Contains(t, names, ns)
			const ns2 = "ns2"
			helper.CreateNamespace(t, ns2, adminKey)
			helper.SuspendNamespace(t, ns2, adminKey)
			helper.ResumeNamespace(t, ns2, adminKey)
			helper.DeleteNamespace(t, ns2, adminKey)
		})

		t.Run("an operator's bare class create still needs a namespace", func(t *testing.T) {
			code, body := send(t, adminKey, http.MethodPost, "/v1/schema", map[string]any{"class": "Movies"})
			assert.Equal(t, http.StatusForbidden, code, body)
			assert.Contains(t, body, "must be namespaced")
		})
	})

	// The unlicensed node must keep running object TTL. expiringID expires after
	// the licensed node has stopped, so if the count later drops from 2 to 1, the
	// unlicensed node deleted it.
	expiry := time.Now().Add(90 * time.Second)
	createExpiring(t, userKey, expiringID, expiry)
	requireObjectCount(t, ns+":Expiring", 2, 30*time.Second)

	require.NoError(t, compose.SetLicenseKeyFileAt(context.Background(), 0, ""))
	require.True(t, time.Now().Add(stopGrace).Before(expiry), "the node must stop before the object expires")
	client = restart(t, compose)
	// expiringID must survive the restart, or the TTL check below passes
	// without TTL deleting anything.
	requireObjectCount(t, ns+":Expiring", 2, time.Until(expiry))

	t.Run("unlicensed", func(t *testing.T) {
		for _, caller := range callers {
			t.Run(caller.name, func(t *testing.T) {
				class := caller.prefix + "Movies"

				t.Run("REST requests naming a collection or a namespace answer 403", func(t *testing.T) {
					requests := append(dataRequests(class, refusedRESTID),
						restRequest{"get object", http.MethodGet, "/v1/objects/" + class + "/" + movieID.String(), nil},
						restRequest{"create object", http.MethodPost, "/v1/objects", map[string]any{"class": class, "id": refusedRESTID}},
						restRequest{"add tenants", http.MethodPost, "/v1/schema/" + caller.prefix + "Shelves/tenants", []any{map[string]any{"name": "t2"}}},
						restRequest{"add property", http.MethodPost, "/v1/schema/" + class + "/properties", map[string]any{"name": "year", "dataType": []string{"int"}}},
						restRequest{"get shard status", http.MethodGet, "/v1/schema/" + class + "/shards", nil},
						restRequest{"update shard status", http.MethodPut, "/v1/schema/" + class + "/shards/s1", map[string]any{"status": "READONLY"}},
						restRequest{"list indexes", http.MethodGet, "/v1/schema/" + class + "/indexes", nil},
						// An operator's bare class create is refused for the
						// license, not for naming no namespace.
						restRequest{"create class", http.MethodPost, "/v1/schema", map[string]any{"class": "Movies"}},
						restRequest{"create namespace", http.MethodPost, "/v1/namespaces/ns3", nil},
						restRequest{"get namespace", http.MethodGet, "/v1/namespaces/" + ns, nil},
						restRequest{"list namespaces", http.MethodGet, "/v1/namespaces", nil},
						restRequest{"update namespace", http.MethodPut, "/v1/namespaces/" + ns, map[string]any{"home_node": "weaviate-0"}},
						restRequest{"suspend namespace", http.MethodPost, "/v1/namespaces/" + ns + "/suspend", nil},
						restRequest{"resume namespace", http.MethodPost, "/v1/namespaces/" + ns + "/resume", nil},
						restRequest{"delete namespace", http.MethodDelete, "/v1/namespaces/" + ns, nil},
					)
					for _, r := range requests {
						t.Run(r.name, func(t *testing.T) {
							assertLicenseRefusal(t, caller.key, r.method, r.path, r.body)
						})
					}
				})

				t.Run("gRPC calls answer PermissionDenied", func(t *testing.T) {
					for _, c := range grpcCalls(client, class, caller.prefix+"Shelves") {
						t.Run(c.name, func(t *testing.T) {
							err := c.call(authCtx(t, caller.key))
							assert.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
						})
					}
				})

				t.Run("gRPC BatchObjects refuses each object", func(t *testing.T) {
					resp, err := client.BatchObjects(authCtx(t, caller.key), &pb.BatchObjectsRequest{
						Objects: []*pb.BatchObject{batchObject(class, refusedGRPCID)},
					})
					require.NoError(t, err)
					require.Len(t, resp.Errors, 1)
					assert.Contains(t, resp.Errors[0].Error, licenseText)
				})

				t.Run("a BatchStream message with objects ends the stream", func(t *testing.T) {
					_, err := runBatchStream(t, client, caller.key, &pb.BatchStreamRequest_Data{
						Objects: &pb.BatchStreamRequest_Data_Objects{Values: []*pb.BatchObject{batchObject(class, refusedGRPCID)}},
					})
					assert.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
				})

				t.Run("batch references are refused per reference", func(t *testing.T) {
					for _, b := range referenceBatches(client, class, movieID) {
						t.Run(b.name, func(t *testing.T) {
							errs := b.send(t, caller.key)
							require.Len(t, errs, 1)
							assert.Contains(t, errs[0], licenseText)
						})
					}
				})

				t.Run("MCP hybrid search fails", func(t *testing.T) {
					err := mcpHybridSearch(t, caller.key, class)
					require.Error(t, err)
					assert.Contains(t, err.Error(), licenseText)
				})
			})
		}

		t.Run("REST listings that name no collection answer 200", func(t *testing.T) {
			for _, path := range []string{"/v1/schema", "/v1/aliases", "/v1/nodes?output=verbose", "/v1/users/db", "/v1/authz/roles"} {
				t.Run(path, func(t *testing.T) {
					code, body := send(t, adminKey, http.MethodGet, path, nil)
					assert.Equal(t, http.StatusOK, code, body)
				})
			}
		})

		t.Run("user and role writes succeed and grant no data access", func(t *testing.T) {
			aliceKey := helper.CreateUserWithNamespace(t, "alice", ns, adminKey)
			helper.CreateRole(t, adminKey, readDataRole("reader"))
			helper.AssignRoleToUser(t, adminKey, "reader", ns+":alice")
			assertLicenseRefusal(t, aliceKey, http.MethodGet, "/v1/objects/Expiring/"+controlID.String(), nil)
		})

		t.Run("object TTL deletes the expired object", func(t *testing.T) {
			requireObjectCount(t, ns+":Expiring", 1, 120*time.Second)
		})

		t.Run("the node logs the unlicensed warning once", func(t *testing.T) {
			assert.Equal(t, 1, unlicensedWarnings(t, compose))
		})
	})

	key, err := docker.LicenseKey()
	require.NoError(t, err)
	require.NoError(t, compose.SetLicenseKeyFileAt(context.Background(), 0, key))
	restart(t, compose)

	t.Run("relicensed", func(t *testing.T) {
		for _, tc := range []struct {
			name, path string
			want       int
		}{
			{"expired object", "/v1/objects/Expiring/" + expiringID.String(), http.StatusNotFound},
			{"control object", "/v1/objects/Expiring/" + controlID.String(), http.StatusOK},
			{"refused REST write", "/v1/objects/Movies/" + refusedRESTID.String(), http.StatusNotFound},
			{"refused gRPC write", "/v1/objects/Movies/" + refusedGRPCID.String(), http.StatusNotFound},
		} {
			t.Run(tc.name, func(t *testing.T) {
				code, body := send(t, userKey, http.MethodGet, tc.path, nil)
				assert.Equal(t, tc.want, code, body)
			})
		}

		t.Run("the node logs no new unlicensed warning", func(t *testing.T) {
			assert.Equal(t, 1, unlicensedWarnings(t, compose))
		})
	})
}

// createClasses creates three classes in the caller's namespace. Movies
// references itself. Shelves holds tenant t1 for TenantsGet. TTL deletes an
// Expiring object at its expiresAt.
func createClasses(t *testing.T, key string) {
	t.Helper()
	helper.CreateClassAuth(t, &models.Class{
		Class: "Movies",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "related", DataType: []string{"Movies"}},
		},
	}, key)
	helper.CreateClassAuth(t, &models.Class{
		Class:              "Shelves",
		Properties:         []*models.Property{{Name: "title", DataType: []string{"text"}}},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}, key)
	helper.CreateTenantsAuth(t, "Shelves", []*models.Tenant{{Name: "t1"}}, key)
	helper.CreateClassAuth(t, &models.Class{
		Class:           "Expiring",
		Properties:      []*models.Property{{Name: "expiresAt", DataType: []string{"date"}}},
		ObjectTTLConfig: &models.ObjectTTLConfig{Enabled: true, DeleteOn: "expiresAt"},
	}, key)
}

func createExpiring(t *testing.T, key string, id strfmt.UUID, expiresAt time.Time) {
	t.Helper()
	require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
		ID: id, Class: "Expiring", Properties: map[string]any{"expiresAt": expiresAt.UTC().Format(time.RFC3339Nano)},
	}, key))
}
