//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
)

var (
	Anything = mock.Anything
	ErrAny   = errors.New("any error")
)

func newMockExecutor(m *fakeMigrator, s *fakeMetaHandler) *executor {
	logger, _ := test.NewNullLogger()
	x := NewExecutor(m, s, logger)
	x.RegisterSchemaUpdateCallback(func(updatedSchema schema.Schema) {})
	return x
}

func TestExecutor(t *testing.T) {
	ctx := context.Background()
	store := &fakeMetaHandler{}
	cls := &models.Class{
		Class:             "A",
		VectorIndexConfig: flat.NewDefaultUserConfig(),
	}
	store.On("ReadOnlySchema").Return(models.Schema{})
	store.On("ReadOnlyClass", "A").Return(cls)

	t.Run("OpenClose", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("WaitForStartup", ctx).Return(nil)
		migrator.On("Shutdown", ctx).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.Open(ctx))
		assert.Nil(t, x.Close(ctx))
	})

	t.Run("AddClass", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("AddClass", Anything, Anything, Anything).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.AddClass(cluster.AddClassRequest{}))
	})
	t.Run("AddClassWithError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("AddClass", Anything, Anything, Anything).Return(ErrAny)
		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.AddClass(cluster.AddClassRequest{}), ErrAny)
	})

	t.Run("DropClass", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("DropClass", Anything, Anything).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.DeleteClass("A"))
	})
	t.Run("DropClassWithError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("DropClass", Anything, Anything).Return(ErrAny)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.DeleteClass("A"))
	})

	t.Run("UpdateIndex", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("UpdateVectorIndexConfig", Anything, "A", Anything).Return(nil)
		migrator.On("UpdateInvertedIndexConfig", Anything, "A", Anything).Return(nil)

		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.UpdateClass(cluster.UpdateClassRequest{Class: cls}))
	})

	t.Run("UpdateVectorIndexConfig", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("UpdateVectorIndexConfig", Anything, "A", Anything).Return(ErrAny)

		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.UpdateClass(cluster.UpdateClassRequest{Class: cls}), ErrAny)
	})
	t.Run("UpdateInvertedIndexConfig", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("UpdateVectorIndexConfig", Anything, "A", Anything).Return(nil)
		migrator.On("UpdateInvertedIndexConfig", Anything, "A", Anything).Return(ErrAny)

		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.UpdateClass(cluster.UpdateClassRequest{Class: cls}), ErrAny)
	})

	t.Run("AddProperty", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := cluster.AddPropertyRequest{Property: &models.Property{}}
		migrator.On("AddProperty", Anything, "A", req.Property).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.AddProperty("A", req))
	})

	commit := func(success bool) {}
	tenants := []*cluster.Tenant{{Name: "T1"}, {Name: "T2"}}

	t.Run("DeleteTenants", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &cluster.DeleteTenantsRequest{}
		migrator.On("DeleteTenants", Anything, "A", req.Tenants).Return(commit, nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.DeleteTenants("A", req))
	})
	t.Run("DeleteTenantsWithError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &cluster.DeleteTenantsRequest{}
		migrator.On("DeleteTenants", Anything, "A", req.Tenants).Return(commit, ErrAny)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.DeleteTenants("A", req))
	})

	t.Run("UpdateTenants", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &cluster.UpdateTenantsRequest{Tenants: tenants}
		migrator.On("UpdateTenants", Anything, cls, Anything).Return(commit, nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.UpdateTenants("A", req))
	})

	t.Run("UpdateTenantsClassNotFound", func(t *testing.T) {
		store := &fakeMetaHandler{}
		store.On("ReadOnlyClass", "A").Return(nil)

		req := &cluster.UpdateTenantsRequest{Tenants: tenants}
		x := newMockExecutor(&fakeMigrator{}, store)
		assert.ErrorIs(t, x.UpdateTenants("A", req), ErrNotFound)
	})

	t.Run("UpdateTenantsError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &cluster.UpdateTenantsRequest{Tenants: tenants}
		migrator.On("UpdateTenants", Anything, cls, Anything).Return(commit, ErrAny)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.UpdateTenants("A", req))
	})

	t.Run("AddTenants", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &cluster.AddTenantsRequest{Tenants: tenants}
		migrator.On("NewTenants", Anything, cls, Anything).Return(commit, nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.AddTenants("A", req))
	})
	t.Run("AddTenantsEmpty", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &cluster.AddTenantsRequest{Tenants: nil}
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.AddTenants("A", req))
	})
	t.Run("AddTenantsError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &cluster.AddTenantsRequest{Tenants: tenants}
		migrator.On("NewTenants", Anything, cls, Anything).Return(commit, ErrAny)
		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.AddTenants("A", req), ErrAny)
	})
	t.Run("AddTenantsClassNotFound", func(t *testing.T) {
		store := &fakeMetaHandler{}
		store.On("ReadOnlyClass", "A").Return(nil)
		req := &cluster.AddTenantsRequest{Tenants: tenants}
		x := newMockExecutor(&fakeMigrator{}, store)
		assert.ErrorIs(t, x.AddTenants("A", req), ErrNotFound)
	})

	t.Run("GetShardsStatus", func(t *testing.T) {
		migrator := &fakeMigrator{}
		status := map[string]string{"A": "B"}
		migrator.On("GetShardsStatus", Anything, "A", "").Return(status, nil)
		x := newMockExecutor(migrator, store)
		_, err := x.GetShardsStatus("A")
		assert.Nil(t, err)
	})
	t.Run("GetShardsStatusError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		status := map[string]string{"A": "B"}
		migrator.On("GetShardsStatus", Anything, "A", "").Return(status, ErrAny)
		x := newMockExecutor(migrator, store)
		_, err := x.GetShardsStatus("A")
		assert.ErrorIs(t, err, ErrAny)
	})
	t.Run("UpdateShardStatus", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &cluster.UpdateShardStatusRequest{Class: "A", Shard: "S", Status: "ST"}
		migrator.On("UpdateShardStatus", Anything, "A", "S", "ST").Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.UpdateShardStatus(req))
	})
}
