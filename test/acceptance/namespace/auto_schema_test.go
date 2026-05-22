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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	user1Key, user2Key := twoNamespaces(t)

	t.Run("auto-create class on first object insert", func(t *testing.T) {
		const class = "AutoCreated"
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+class, adminKey) })

		id := strfmt.UUID("11111111-aaaa-bbbb-cccc-111111111111")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:         id,
			Class:      class,
			Properties: map[string]any{"title": "Inception"},
		}, user1Key)
		require.NoError(t, err)

		// Namespaced principal sees the short class on read-back (response stripping).
		got, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got.Class)
		assert.Equal(t, "Inception", got.Properties.(map[string]any)["title"])

		// Admin's raw schema view confirms a single-qualified class exists.
		gotClass := helper.GetClassAuth(t, "customer1:"+class, adminKey)
		assert.Equal(t, "customer1:"+class, gotClass.Class)

		// Same short name in a second namespace creates an independent class.
		_, err = helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:         id,
			Class:      class,
			Properties: map[string]any{"title": "Memento"},
		}, user2Key)
		require.NoError(t, err)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer2:"+class, adminKey) })

		gotClass2 := helper.GetClassAuth(t, "customer2:"+class, adminKey)
		assert.Equal(t, "customer2:"+class, gotClass2.Class)
	})

	t.Run("auto-add property on object insert", func(t *testing.T) {
		const class = "AutoProp"
		setupClassInBothNamespaces(t, class, user1Key, user2Key)

		id := strfmt.UUID("22222222-aaaa-bbbb-cccc-222222222222")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:    id,
			Class: class,
			Properties: map[string]any{
				"title":   "Tenet",
				"runtime": int64(150),
			},
		}, user1Key)
		require.NoError(t, err)

		got, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got.Class)
		props := got.Properties.(map[string]any)
		assert.Equal(t, "Tenet", props["title"])
		assert.NotNil(t, props["runtime"])

		gotNs1 := helper.GetClassAuth(t, "customer1:"+class, adminKey)
		assert.True(t, hasProperty(gotNs1, "runtime"),
			"runtime property should have been auto-added to customer1:%s", class)

		// The schema change is namespace-local: customer2's class with the
		// same short name must not inherit "runtime".
		gotNs2 := helper.GetClassAuth(t, "customer2:"+class, adminKey)
		assert.False(t, hasProperty(gotNs2, "runtime"),
			"runtime property must not appear on customer2:%s", class)
	})

	t.Run("auto-add property via PUT", func(t *testing.T) {
		const class = "AutoPropPut"
		setupClassInNs1(t, class, user1Key)

		id := strfmt.UUID("33333333-aaaa-bbbb-cccc-333333333333")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "v1"},
		}, user1Key)
		require.NoError(t, err)

		_, err = helper.Client(t).Objects.ObjectsClassPut(
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

		gotClass := helper.GetClassAuth(t, "customer1:"+class, adminKey)
		assert.True(t, hasProperty(gotClass, "rating"),
			"rating property should have been auto-added via PUT")
	})

	t.Run("auto-add property via PATCH", func(t *testing.T) {
		const class = "AutoPropPatch"
		setupClassInNs1(t, class, user1Key)

		id := strfmt.UUID("44444444-aaaa-bbbb-cccc-444444444444")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "v1"},
		}, user1Key)
		require.NoError(t, err)

		_, err = helper.Client(t).Objects.ObjectsClassPatch(
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

		gotClass := helper.GetClassAuth(t, "customer1:"+class, adminKey)
		assert.True(t, hasProperty(gotClass, "director"),
			"director property should have been auto-added via PATCH")
	})

	t.Run("auto-create tenant on single-object insert", func(t *testing.T) {
		const class = "AutoTenantSingle"
		mustCreateMTClass(t, class, user1Key, mtAutoCreate)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+class, adminKey) })

		id := strfmt.UUID("55555555-aaaa-bbbb-cccc-555555555555")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:         id,
			Class:      class,
			Tenant:     "tenantA",
			Properties: map[string]any{"title": "Oppenheimer"},
		}, user1Key)
		require.NoError(t, err)

		got, err := helper.GetObjectAuthWithTenant(t, class, id, "tenantA", user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got.Class)
		assert.Equal(t, "tenantA", got.Tenant)
	})

	t.Run("auto-create tenant on batch insert", func(t *testing.T) {
		const class = "AutoTenantBatch"
		mustCreateMTClass(t, class, user1Key, mtAutoCreate)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+class, adminKey) })

		id1 := strfmt.UUID("66666666-aaaa-bbbb-cccc-666666666666")
		id2 := strfmt.UUID("66666666-aaaa-bbbb-cccc-777777777777")
		helper.CreateObjectsBatchAuth(t, []*models.Object{
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
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+class, adminKey) })

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
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:         id,
			Class:      class,
			Tenant:     "tenantD",
			Properties: map[string]any{"title": "Interstellar"},
		}, user1Key)
		require.NoError(t, err)

		got, err := helper.GetObjectAuthWithTenant(t, class, id, "tenantD", user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got.Class)
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
