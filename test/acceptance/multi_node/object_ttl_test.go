//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package multi_node

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func TestObjectTTLMultiNode(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	// send TTL delete requests to a different node, to test cluster-wide propagation
	secondNode := compose.GetWeaviateNode2().DebugURI()

	// helper.SetupClient("127.0.0.1:8080")
	// secondNode := "127.0.0.1:6060"

	t.Run("Object TTL ST", func(t *testing.T) {
		class := &models.Class{
			Class: "TestingTTLST",
			Properties: []*models.Property{
				{
					Name:     "contents",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "expireDate",
					DataType: schema.DataTypeDate.PropString(),
				},
			},
			ObjectTTLConfig: &models.ObjectTTLConfig{
				Enabled:    true,
				DeleteOn:   "expireDate",
				DefaultTTL: 60, // 1 minute
			},
			Vectorizer: "none",
		}

		baseTime := time.Now().UTC()
		helper.DeleteClass(t, class.Class)
		defer helper.DeleteClass(t, class.Class)
		helper.CreateClass(t, class)
		for i := 0; i < 11; i++ {
			require.NoError(t, helper.CreateObject(t, &models.Object{
				ID:    helper.IntToUUID(uint64(i)),
				Class: class.Class,
				Properties: map[string]interface{}{
					"contents":   "some text",
					"expireDate": baseTime.Add(time.Minute * time.Duration(i)).Format(time.RFC3339),
				},
			}))
		}

		expirationTime := baseTime.Add(5 * time.Minute).Add(time.Second)
		deleteTTL(t, secondNode, expirationTime, false)
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			objs, err := helper.GetObjects(t, class.Class)
			require.NoError(ct, err)
			require.Len(ct, objs, 6) // 0..4 should be deleted => 11 - 5 = 6
		}, time.Second*5, 500*time.Millisecond)
	})

	t.Run("Object TTL MT", func(t *testing.T) {
		class := &models.Class{
			Class: "TestingTTLMT",
			Properties: []*models.Property{
				{
					Name:     "contents",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "tenant",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "expireDate",
					DataType: schema.DataTypeDate.PropString(),
				},
			},
			ObjectTTLConfig: &models.ObjectTTLConfig{
				Enabled:    true,
				DeleteOn:   "expireDate",
				DefaultTTL: 0,
			},
			Vectorizer:         "none",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
		helper.DeleteClass(t, class.Class)
		// defer helper.DeleteClass(t, class.Class)
		helper.CreateClass(t, class)

		// create a lot of tenants and objects
		tenants := make([]*models.Tenant, 50)
		for i := range tenants {
			tenants[i] = &models.Tenant{Name: "tenant_" + strconv.Itoa(i)}
		}
		helper.CreateTenants(t, class.Class, tenants)

		baseTime := time.Now().UTC()
		objects := make([]*models.Object, 50)
		for i := range tenants {
			for j := range objects {
				objects[j] = &models.Object{
					ID:     helper.IntToUUID(uint64(i*len(tenants) + j)),
					Class:  class.Class,
					Tenant: tenants[i].Name,
					Properties: map[string]interface{}{
						"contents":   "some text",
						"tenant":     tenants[i].Name,
						"expireDate": baseTime.Add(time.Minute * time.Duration(j)).Format(time.RFC3339),
					},
				}
			}
			helper.CreateObjectsBatch(t, objects)
		}

		// deactivate some tenants to test that only active tenants are affected
		tenantNamesDeactivated := make([]string, 0, len(tenants)/2)
		tenantsDeactivate := make([]*models.Tenant, 0, len(tenants)/2)
		for i := range tenants {
			if i%2 == 0 {
				continue
			}
			tenantNamesDeactivated = append(tenantNamesDeactivated, tenants[i].Name)
			tenantsDeactivate = append(tenantsDeactivate, &models.Tenant{
				Name:           tenants[i].Name,
				ActivityStatus: models.TenantActivityStatusINACTIVE,
			})
		}
		helper.UpdateTenants(t, class.Class, tenantsDeactivate)

		// note that custom date starts at 0minutes
		deleteTTL(t, secondNode, baseTime.Add(25*time.Minute).Add(10*time.Second), false)

		// verify that active tenants objects have been deleted
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, tenant := range tenants {
				query := fmt.Sprintf(`{Aggregate{%s(tenant: %q){meta{count}}}}`, class.Class, tenant.Name)
				if slices.Contains(tenantNamesDeactivated, tenant.Name) {
					continue
				} else {
					result, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
					require.NoError(ct, err)
					require.NotNil(ct, result)
					countStr := result.Data["Aggregate"].(map[string]interface{})[class.Class].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"]
					count, err := countStr.(json.Number).Int64()
					require.NoError(ct, err)

					require.Equal(ct, int(count), 24) // 0..24 should be deleted => 50 - 26 = 24
				}
			}
		}, time.Second*5, 500*time.Millisecond)

		// activate tenants again to be able to see their objects
		tenantsActivate := make([]*models.Tenant, 0, len(tenants)/2)
		for i := range tenants {
			if i%2 == 0 {
				continue
			}
			tenantsActivate = append(tenantsActivate, &models.Tenant{
				Name:           tenants[i].Name,
				ActivityStatus: models.TenantActivityStatusACTIVE,
			})
		}
		helper.UpdateTenants(t, class.Class, tenantsActivate)

		for _, tenant := range tenants {
			query := fmt.Sprintf(`{Aggregate{%s(tenant: %q){meta{count}}}}`, class.Class, tenant.Name)
			if !slices.Contains(tenantNamesDeactivated, tenant.Name) {
				continue
			} else {
				result, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
				require.NoError(t, err)
				require.NotNil(t, result)
				countStr := result.Data["Aggregate"].(map[string]interface{})[class.Class].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"]
				count, err := countStr.(json.Number).Int64()
				require.NoError(t, err)

				require.Equal(t, int(count), 50) // unaffected
			}
		}
	})
}

func deleteTTL(t *testing.T, node string, deletionTime time.Time, ownNode bool) {
	t.Helper()

	u := url.URL{
		Scheme: "http",
		Host:   node,
		Path:   "/debug/ttl/deleteall",
	}
	q := u.Query()
	q.Set("expiration", deletionTime.UTC().Format(time.RFC3339))
	q.Set("targetOwnNode", strconv.FormatBool(ownNode))
	u.RawQuery = q.Encode()

	client := &http.Client{Timeout: time.Minute}
	resp, err := client.Get(u.String())
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}
