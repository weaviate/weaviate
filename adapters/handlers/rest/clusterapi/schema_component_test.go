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

package clusterapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/scaler"
	schemauc "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// This is a cross-package test that tests the schema manager in a distributed
// settings including some of its dependencies, such as the REST API and
// clients. This setup pretends that replication was one-way for simplicity
// sake, but uses the same components on either side to make sure that it
// would work in both directions.
func TestComponentCluster(t *testing.T) {
	t.Run("add class", func(t *testing.T) {
		localManager, remoteManager := setupManagers(t)

		ctx := context.Background()

		err := localManager.AddClass(ctx, nil, testClass())
		require.Nil(t, err)

		localClass, err := localManager.GetClass(ctx, nil, testClass().Class)
		require.Nil(t, err)
		remoteClass, err := remoteManager.GetClass(ctx, nil, testClass().Class)
		require.Nil(t, err)

		assert.Equal(t, localClass, remoteClass)
	})

	t.Run("add class and extend property", func(t *testing.T) {
		localManager, remoteManager := setupManagers(t)

		ctx := context.Background()

		err := localManager.AddClass(ctx, nil, testClass())
		require.Nil(t, err)

		err = localManager.AddClassProperty(ctx, nil, testClass().Class, testProperty())
		require.Nil(t, err)

		localClass, err := localManager.GetClass(ctx, nil, testClass().Class)
		require.Nil(t, err)
		remoteClass, err := remoteManager.GetClass(ctx, nil, testClass().Class)
		require.Nil(t, err)

		assert.Equal(t, localClass, remoteClass)
	})

	t.Run("delete class", func(t *testing.T) {
		localManager, remoteManager := setupManagers(t)

		ctx := context.Background()

		err := localManager.AddClass(ctx, nil, testClass())
		require.Nil(t, err)

		err = localManager.DeleteClass(ctx, nil, testClass().Class)
		require.Nil(t, err)

		localSchema, err := localManager.GetSchema(nil)
		require.Nil(t, err)
		remoteSchema, err := remoteManager.GetSchema(nil)
		require.Nil(t, err)

		assert.Equal(t, localSchema, remoteSchema)
	})

	t.Run("add class update config", func(t *testing.T) {
		localManager, remoteManager := setupManagers(t)

		ctx := context.Background()

		err := localManager.AddClass(ctx, nil, testClass())
		require.Nil(t, err)

		updated := testClass()
		updated.VectorIndexConfig.(map[string]interface{})["secondKey"] = "added"

		err = localManager.UpdateClass(ctx, nil, testClass().Class, updated)
		require.Nil(t, err)

		localClass, err := localManager.GetClass(ctx, nil, testClass().Class)
		require.Nil(t, err)
		remoteClass, err := remoteManager.GetClass(ctx, nil, testClass().Class)
		require.Nil(t, err)

		assert.Equal(t, localClass, remoteClass)
	})
}

func setupManagers(t *testing.T) (*schemauc.Manager, *schemauc.Manager) {
	remoteManager := newSchemaManagerWithClusterStateAndClient(
		&fakeClusterState{hosts: []string{"node1"}}, nil)

	schemaHandlers := clusterapi.NewSchema(remoteManager.TxManager(), clusterapi.NewNoopAuthHandler())
	mux := http.NewServeMux()
	mux.Handle("/schema/transactions/", http.StripPrefix("/schema/transactions/",
		schemaHandlers.Transactions()))
	server := httptest.NewServer(mux)

	client := clients.NewClusterSchema(&http.Client{})
	parsedURL, err := url.Parse(server.URL)
	require.Nil(t, err)
	state := &fakeClusterState{hosts: []string{parsedURL.Host}}
	localManager := newSchemaManagerWithClusterStateAndClient(state, client)

	// this will also mark the tx managers as ready
	localManager.StartServing(context.Background())
	remoteManager.StartServing(context.Background())

	return localManager, remoteManager
}

func testClass() *models.Class {
	return &models.Class{
		Class: "MyClass",
		Properties: []*models.Property{
			{
				Name: "propOne", DataType: []string{"text"},
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"foo": "bar",
		},
	}
}

func testProperty() *models.Property {
	return &models.Property{
		Name: "propTwo", DataType: []string{"int"},
	}
}

// New Local Schema *Manager
func newSchemaManagerWithClusterStateAndClient(clusterState *fakeClusterState,
	client cluster.Client,
) *schemauc.Manager {
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2"},
	}
	sm, err := schemauc.NewManager(&NilMigrator{}, newFakeRepo(), logger, &fakeAuthorizer{},
		config.Config{DefaultVectorizerModule: config.VectorizerModuleNone},
		dummyParseVectorConfig, // only option for now
		vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, clusterState, client, &fakeTxPersistence{},
		&fakeScaleOutManager{},
	)
	if err != nil {
		panic(err.Error())
	}

	return sm
}

type fakeScaleOutManager struct{}

func (f *fakeScaleOutManager) Scale(ctx context.Context,
	className string, updated sharding.Config, _, _ int64,
) (*sharding.State, error) {
	return nil, nil
}

func (f *fakeScaleOutManager) SetSchemaManager(sm scaler.SchemaManager) {
}

// does nothing as this component test does not involve crashes
type fakeTxPersistence struct{}

func (f *fakeTxPersistence) StoreTx(ctx context.Context,
	tx *cluster.Transaction,
) error {
	return nil
}

func (f *fakeTxPersistence) DeleteTx(ctx context.Context,
	txID string,
) error {
	return nil
}

func (f *fakeTxPersistence) IterateAll(ctx context.Context,
	cb func(tx *cluster.Transaction),
) error {
	return nil
}
