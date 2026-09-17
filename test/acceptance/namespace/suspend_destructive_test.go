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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemaCli "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// A suspended namespace must lose no collection, tenant or alias. Each refusal
// renders its own status, so the subtests assert the typed responder: an
// untyped "it errored" would stay green through a regression to 500.
//
// Everything runs as the global operator against qualified names, because the
// namespace's own key stops authenticating once the suspend applies.
func TestNamespaces_SuspendRefusesDestructiveSchemaOps(t *testing.T) {
	t.Parallel()
	ns1, _, user1Key, user2Key := twoNamespaces(t)

	const (
		mtClassName = "SuspendTenants"
		stClassName = "SuspendDocs"
		aliasName   = "SuspendAlias"
		keptTenant  = "warm"
		goneTenant  = "toasty"
		objTitle    = "survives the suspend"
		// The sentinel a global operator sees; a namespaced caller gets the
		// namespace-free wording instead, but their key does not authenticate here.
		refusalText = "namespace is suspended"
	)
	qualifiedMT := ns1 + ":" + mtClassName
	qualifiedST := ns1 + ":" + stClassName
	qualifiedAlias := ns1 + ":" + aliasName

	setupMTClassInNs1(t, ns1, mtClassName, user1Key)
	setupClassInNs1(t, ns1, stClassName, user1Key)
	require.NoError(t, addTenantsAuth(t, mtClassName, []*models.Tenant{
		{Name: keptTenant, ActivityStatus: models.TenantActivityStatusHOT},
		{Name: goneTenant, ActivityStatus: models.TenantActivityStatusHOT},
	}, user1Key))

	// Two tenants, each holding an object, so the refusal covers a batch and
	// the resumed-namespace subtest can read data back off the survivor.
	objectIDs := map[string]strfmt.UUID{}
	for _, tenant := range []string{keptTenant, goneTenant} {
		created, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class: mtClassName, Tenant: tenant, Properties: map[string]any{"title": objTitle},
		}, user1Key)
		require.NoError(t, err)
		objectIDs[tenant] = created.ID
	}
	helper.CreateAliasAuth(t, &models.Alias{Alias: aliasName, Class: stClassName}, user1Key)

	requireShardsEventually(t, qualifiedMT, keptTenant, goneTenant)

	helper.SuspendNamespace(t, ns1, adminKey)
	// Registered after the class cleanups, so LIFO resumes before they run.
	t.Cleanup(func() { helper.ResumeNamespace(t, ns1, adminKey) })

	t.Run("a tenant delete is refused", func(t *testing.T) {
		err := deleteTenantsAuth(t, qualifiedMT, []string{keptTenant, goneTenant}, adminKey)
		var refused *schemaCli.TenantsDeleteUnprocessableEntity
		require.ErrorAs(t, err, &refused, "got %T: %v", err, err)
		require.NotEmpty(t, refused.Payload.Error)
		assert.Contains(t, refused.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refused.Payload.Error[0].Message, ns1)

		// The whole list, not the two names, so a refused delete is told apart
		// from a read that failed for its own reasons.
		tenants, err := getTenantsAuth(t, qualifiedMT, adminKey)
		require.NoError(t, err)
		assert.Subset(t, tenantNames(tenants), []string{keptTenant, goneTenant})

		shards, err := shardsForClass(t, qualifiedMT)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{keptTenant, goneTenant}, shards)
	})

	t.Run("a collection delete is refused", func(t *testing.T) {
		err := helper.DeleteClassAuthWithReturn(t, qualifiedST, adminKey)
		var refused *schemaCli.SchemaObjectsDeleteBadRequest
		require.ErrorAs(t, err, &refused, "got %T: %v", err, err)
		require.NotEmpty(t, refused.Payload.Error)
		assert.Contains(t, refused.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refused.Payload.Error[0].Message, ns1)

		_, err = helper.GetClassAuthWithReturn(t, qualifiedST, adminKey)
		require.NoError(t, err, "the collection must survive the refusal")
	})

	t.Run("an alias delete and replace are refused", func(t *testing.T) {
		_, err := helper.DeleteAliasAuthWithReturn(t, qualifiedAlias, adminKey)
		var refusedDelete *schemaCli.AliasesDeleteUnprocessableEntity
		require.ErrorAs(t, err, &refusedDelete, "got %T: %v", err, err)
		require.NotEmpty(t, refusedDelete.Payload.Error)
		assert.Contains(t, refusedDelete.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refusedDelete.Payload.Error[0].Message, ns1)

		_, err = helper.UpdateAliasAuthWithReturn(t, qualifiedAlias, qualifiedMT, adminKey)
		var refusedUpdate *schemaCli.AliasesUpdateUnprocessableEntity
		require.ErrorAs(t, err, &refusedUpdate, "got %T: %v", err, err)
		require.NotEmpty(t, refusedUpdate.Payload.Error)
		assert.Contains(t, refusedUpdate.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refusedUpdate.Payload.Error[0].Message, ns1)

		// The target too, not just the alias: a replace that got through leaves
		// the alias resolving, at the wrong class.
		got, err := helper.GetAliasAuthWithReturn(t, qualifiedAlias, adminKey)
		require.NoError(t, err, "the alias must survive the refusal")
		assert.Equal(t, qualifiedST, got.Payload.Class)
	})

	// Fails if the gate refuses whenever any namespace is suspended, rather
	// than the one the command names.
	t.Run("a delete in the other namespace still succeeds", func(t *testing.T) {
		const otherClass, otherAlias = "OtherDocs", "OtherAlias"
		helper.CreateClassAuth(t, &models.Class{
			Class:      otherClass,
			Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
		}, user2Key)
		helper.CreateAliasAuth(t, &models.Alias{Alias: otherAlias, Class: otherClass}, user2Key)

		// As ns2's own user, so this also covers their key still working while a
		// different namespace is suspended. Their key qualifies the short name.
		retryOnAliasLag(t, func() error {
			_, err := helper.DeleteAliasAuthWithReturn(t, otherAlias, user2Key)
			return err
		})
		require.NoError(t, helper.DeleteClassAuthWithReturn(t, otherClass, user2Key))
	})

	// Resume flips straight to active, so this is the active state rather than
	// resuming. It is the non-regression half: it passes without the gate.
	t.Run("a resumed namespace lets the deletes through", func(t *testing.T) {
		helper.ResumeNamespace(t, ns1, adminKey)

		require.NoError(t, deleteTenantsAuth(t, qualifiedMT, []string{goneTenant}, adminKey))
		_, err := helper.DeleteAliasAuthWithReturn(t, qualifiedAlias, adminKey)
		require.NoError(t, err)
		require.NoError(t, helper.DeleteClassAuthWithReturn(t, qualifiedST, adminKey))

		tenants, err := getTenantsAuth(t, qualifiedMT, adminKey)
		require.NoError(t, err)
		assert.NotContains(t, tenantNames(tenants), goneTenant, "the delete must have applied")

		// The read is retried because ResumeNamespace polls the leader-served
		// GET, so the node owning the shard may still be refusing reads.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			obj, err := helper.GetObjectAuthWithTenant(t, qualifiedMT, objectIDs[keptTenant], keptTenant, adminKey)
			if !assert.NoError(c, err) {
				return
			}
			props, ok := obj.Properties.(map[string]any)
			if !assert.True(c, ok, "unexpected property shape %T", obj.Properties) {
				return
			}
			assert.Equal(c, objTitle, props["title"])
		}, 10*time.Second, 50*time.Millisecond,
			"the surviving tenant's object must be readable once the resume propagates")
	})
}
