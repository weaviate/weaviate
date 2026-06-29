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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	objectsCli "github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_AutoSchema exercises auto-schema and auto-tenant-creation
// for namespaced principals: a class auto-created from an object insert,
// a property auto-added on insert / PUT / PATCH, and tenants auto-created
// on single + batch inserts must all land under the caller's namespace.
// A global principal on the same cluster cannot trigger auto-schema and
// must be rejected with 403.
func TestNamespaces_AutoSchema(t *testing.T) {
	t.Parallel()
	ns1, ns2, user1Key, user2Key := twoNamespaces(t)

	t.Run("auto-create class on first object insert", func(t *testing.T) {
		const class = "AutoCreated"
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns1+":"+class, adminKey) })

		id := strfmt.UUID("11111111-aaaa-bbbb-cccc-111111111111")
		createObjectWithLagRetry(t, &models.Object{
			ID:         id,
			Class:      class,
			Properties: map[string]any{"title": "Inception"},
		}, user1Key)

		// Namespaced principal sees the short class on read-back (response stripping).
		got, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got.Class)
		assert.Equal(t, "Inception", got.Properties.(map[string]any)["title"])

		// Admin's raw schema view confirms a single-qualified class exists.
		gotClass := helper.GetClassAuth(t, ns1+":"+class, adminKey)
		assert.Equal(t, ns1+":"+class, gotClass.Class)

		// Same short name in a second namespace creates an independent class.
		createObjectWithLagRetry(t, &models.Object{
			ID:         id,
			Class:      class,
			Properties: map[string]any{"title": "Memento"},
		}, user2Key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns2+":"+class, adminKey) })

		gotClass2 := helper.GetClassAuth(t, ns2+":"+class, adminKey)
		assert.Equal(t, ns2+":"+class, gotClass2.Class)
	})

	t.Run("auto-add property on object insert", func(t *testing.T) {
		const class = "AutoProp"
		setupClassInBothNamespaces(t, ns1, ns2, class, user1Key, user2Key)

		id := strfmt.UUID("22222222-aaaa-bbbb-cccc-222222222222")
		createObjectWithLagRetry(t, &models.Object{
			ID:    id,
			Class: class,
			Properties: map[string]any{
				"title":   "Tenet",
				"runtime": int64(150),
			},
		}, user1Key)

		got, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got.Class)
		props := got.Properties.(map[string]any)
		assert.Equal(t, "Tenet", props["title"])
		assert.NotNil(t, props["runtime"])

		gotNs1 := helper.GetClassAuth(t, ns1+":"+class, adminKey)
		assert.True(t, hasProperty(gotNs1, "runtime"),
			"runtime property should have been auto-added to %s:%s", ns1, class)

		// The schema change is namespace-local: the second namespace's class with
		// the same short name must not inherit "runtime".
		gotNs2 := helper.GetClassAuth(t, ns2+":"+class, adminKey)
		assert.False(t, hasProperty(gotNs2, "runtime"),
			"runtime property must not appear on %s:%s", ns2, class)
	})

	t.Run("auto-add property via PUT", func(t *testing.T) {
		const class = "AutoPropPut"
		setupClassInNs1(t, ns1, class, user1Key)

		id := strfmt.UUID("33333333-aaaa-bbbb-cccc-333333333333")
		createObjectWithLagRetry(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "v1"},
		}, user1Key)

		_, err := helper.Client(t).Objects.ObjectsClassPut(
			objectsCli.NewObjectsClassPutParams().WithClassName(class).WithID(id).
				WithBody(&models.Object{
					ID:    id,
					Class: class,
					Properties: map[string]any{
						"title":  "v2",
						"rating": int64(9),
					},
				}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		gotClass := helper.GetClassAuth(t, ns1+":"+class, adminKey)
		assert.True(t, hasProperty(gotClass, "rating"),
			"rating property should have been auto-added via PUT")
	})

	t.Run("auto-add property via PATCH", func(t *testing.T) {
		const class = "AutoPropPatch"
		setupClassInNs1(t, ns1, class, user1Key)

		id := strfmt.UUID("44444444-aaaa-bbbb-cccc-444444444444")
		createObjectWithLagRetry(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "v1"},
		}, user1Key)

		_, err := helper.Client(t).Objects.ObjectsClassPatch(
			objectsCli.NewObjectsClassPatchParams().WithClassName(class).WithID(id).
				WithBody(&models.Object{
					Class: class,
					Properties: map[string]any{
						"director": "Christopher Nolan",
					},
				}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		gotClass := helper.GetClassAuth(t, ns1+":"+class, adminKey)
		assert.True(t, hasProperty(gotClass, "director"),
			"director property should have been auto-added via PATCH")
	})

	t.Run("auto-create tenant on single-object insert", func(t *testing.T) {
		const class = "AutoTenantSingle"
		mustCreateMTClass(t, class, user1Key, mtAutoCreate)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns1+":"+class, adminKey) })

		id := strfmt.UUID("55555555-aaaa-bbbb-cccc-555555555555")
		createObjectWithLagRetry(t, &models.Object{
			ID:         id,
			Class:      class,
			Tenant:     "tenantA",
			Properties: map[string]any{"title": "Oppenheimer"},
		}, user1Key)

		got, err := helper.GetObjectAuthWithTenant(t, class, id, "tenantA", user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got.Class)
		assert.Equal(t, "tenantA", got.Tenant)
	})

	t.Run("auto-create tenant on batch insert", func(t *testing.T) {
		const class = "AutoTenantBatch"
		mustCreateMTClass(t, class, user1Key, mtAutoCreate)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns1+":"+class, adminKey) })

		id1 := strfmt.UUID("66666666-aaaa-bbbb-cccc-666666666666")
		id2 := strfmt.UUID("66666666-aaaa-bbbb-cccc-777777777777")
		createObjectsBatchWithLagRetry(t, []*models.Object{
			{ID: id1, Class: class, Tenant: "tenantB", Properties: map[string]any{"title": "Dune"}},
			{ID: id2, Class: class, Tenant: "tenantC", Properties: map[string]any{"title": "Dune2"}},
		}, user1Key)

		got1, err := helper.GetObjectAuthWithTenant(t, class, id1, "tenantB", user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got1.Class)
		got2, err := helper.GetObjectAuthWithTenant(t, class, id2, "tenantC", user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got2.Class)
	})

	t.Run("auto-activate cold tenant on insert", func(t *testing.T) {
		const class = "AutoTenantActivate"
		mustCreateMTClass(t, class, user1Key, mtAutoActivate)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns1+":"+class, adminKey) })

		// Pre-create tenant HOT, then deactivate.
		helper.CreateTenantsAuth(t, class,
			[]*models.Tenant{{Name: "tenantD", ActivityStatus: models.TenantActivityStatusHOT}},
			user1Key,
		)
		helper.UpdateTenantsWithAuthz(t, class,
			[]*models.Tenant{{Name: "tenantD", ActivityStatus: models.TenantActivityStatusCOLD}},
			helper.CreateAuth(user1Key),
		)

		id := strfmt.UUID("77777777-aaaa-bbbb-cccc-777777777777")
		createObjectWithLagRetry(t, &models.Object{
			ID:         id,
			Class:      class,
			Tenant:     "tenantD",
			Properties: map[string]any{"title": "Interstellar"},
		}, user1Key)

		got, err := helper.GetObjectAuthWithTenant(t, class, id, "tenantD", user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got.Class)
	})

	t.Run("auto-add cross-ref property on object insert", func(t *testing.T) {
		// Auto-schema detects a beacon-shaped value in the payload and adds a
		// cross-ref property whose DataType is the SHORT target class. The
		// schema handler then namespaces that DataType via
		// QualifyPropertyDataTypes, so storage holds the qualified target class.
		// Pinned here because the path crosses three NS-aware seams:
		//   1. The validator's QualifyRefTarget call on the beacon's class.
		//   2. The schema handler qualifying the auto-added DataType.
		//   3. The class-response stripping back to short on read.
		const source, target = "AutoRefSource", "AutoRefTarget"
		setupClassInNs1(t, ns1, source, user1Key)
		setupClassInNs1(t, ns1, target, user1Key)

		targetID := strfmt.UUID("88888888-aaaa-bbbb-cccc-111111111111")
		createObjectWithLagRetry(t, &models.Object{
			ID: targetID, Class: target, Properties: map[string]any{"title": "ref-target"},
		}, user1Key)

		sourceID := strfmt.UUID("88888888-aaaa-bbbb-cccc-222222222222")
		createObjectWithLagRetry(t, &models.Object{
			ID: sourceID, Class: source,
			Properties: map[string]any{
				"title": "ref-source",
				"linkedTo": []any{
					map[string]any{"beacon": "weaviate://localhost/" + target + "/" + string(targetID)},
				},
			},
		}, user1Key)

		// Admin sees the qualified DataType in storage.
		gotAdmin := helper.GetClassAuth(t, ns1+":"+source, adminKey)
		var foundDT []string
		for _, p := range gotAdmin.Properties {
			if p.Name == "linkedTo" {
				foundDT = p.DataType
			}
		}
		require.Len(t, foundDT, 1, "linkedTo should have been auto-added with one DataType entry")
		assert.Equal(t, ns1+":"+target, foundDT[0],
			"auto-schema must qualify the cross-ref DataType under the caller's namespace")

		// Namespaced caller reads it back as the short form (response stripping).
		gotUser := helper.GetClassAuth(t, source, user1Key)
		for _, p := range gotUser.Properties {
			if p.Name == "linkedTo" {
				assert.Equal(t, []string{target}, p.DataType,
					"namespaced view of the cross-ref DataType must be short")
			}
		}

		// And the stored beacon is short (portability invariant).
		got, err := helper.GetObjectAuth(t, ns1+":"+source, sourceID, adminKey)
		require.NoError(t, err)
		refs := got.Properties.(map[string]any)["linkedTo"].([]interface{})
		require.Len(t, refs, 1)
		beaconStr, _ := refs[0].(map[string]any)["beacon"].(string)
		assert.Equal(t, "weaviate://localhost/"+target+"/"+string(targetID), beaconStr)
	})

	t.Run("global principal auto-create rejected with 403", func(t *testing.T) {
		const class = "AdminAutoCreate"
		err := helper.CreateObjectAuth(t, &models.Object{
			Class:      class,
			Properties: map[string]any{"title": "Should not exist"},
		}, adminKey)
		require.Error(t, err)
		var forbidden *objectsCli.ObjectsCreateForbidden
		require.True(t, errors.As(err, &forbidden), "expected ObjectsCreateForbidden, got %T: %v", err, err)
	})
}

// mtAutoCreate / mtAutoActivate select which auto-tenant flag the helper turns on.
type mtMode int

const (
	mtAutoCreate mtMode = iota
	mtAutoActivate
)

func hasProperty(class *models.Class, name string) bool {
	for _, p := range class.Properties {
		if p.Name == name {
			return true
		}
	}
	return false
}

func mustCreateMTClass(t *testing.T, name, key string, mode mtMode) {
	t.Helper()
	cfg := &models.MultiTenancyConfig{Enabled: true}
	switch mode {
	case mtAutoCreate:
		cfg.AutoTenantCreation = true
	case mtAutoActivate:
		cfg.AutoTenantActivation = true
	}
	helper.CreateClassAuth(t, &models.Class{
		Class: name,
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
		},
		MultiTenancyConfig: cfg,
	}, key)
}

// createObjectWithLagRetry inserts an object, retrying the brief window where
// a just-auto-created class/tenant has not yet converged on the write's target
// node. Callers use unique fixed ids, so an "already exists" rejection on retry
// means a prior attempt landed and counts as success.
func createObjectWithLagRetry(t *testing.T, obj *models.Object, key string) {
	t.Helper()
	retryOnAliasLag(t, func() error {
		_, err := helper.CreateObjectWithResponseAuth(t, obj, key)
		if objectAlreadyExists(err) {
			return nil
		}
		return err
	})
}

// objectAlreadyExists reports whether err is the duplicate-id 422. The message
// is in the typed payload, not err.Error(), so it is read off the response.
func objectAlreadyExists(err error) bool {
	var unprocessable *objectsCli.ObjectsCreateUnprocessableEntity
	if !errors.As(err, &unprocessable) || unprocessable.Payload == nil {
		return false
	}
	for _, e := range unprocessable.Payload.Error {
		if e != nil && strings.Contains(e.Message, "already exists") {
			return true
		}
	}
	return false
}

// createObjectsBatchWithLagRetry retries a batch insert through the same
// convergence window as createObjectWithLagRetry. The batch endpoint reports
// per-object failures inside a 200 payload, so item errors are retried too.
func createObjectsBatchWithLagRetry(t *testing.T, objs []*models.Object, key string) {
	t.Helper()
	retryOnAliasLag(t, func() error {
		resp, err := helper.Client(t).Batch.BatchObjectsCreate(
			batch.NewBatchObjectsCreateParams().
				WithBody(batch.BatchObjectsCreateBody{Objects: objs}),
			helper.CreateAuth(key),
		)
		if err != nil {
			return err
		}
		for _, elem := range resp.Payload {
			if elem.Result != nil && elem.Result.Errors != nil && len(elem.Result.Errors.Error) > 0 {
				return errors.New(elem.Result.Errors.Error[0].Message)
			}
		}
		return nil
	})
}
