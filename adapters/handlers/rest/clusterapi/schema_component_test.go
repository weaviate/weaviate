package clusterapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/clients"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/config"
	schemauc "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This is a cross-package test that tests the schema manager in a distributed
// settings including some of its dependencies, such as the REST API and
// clients. This setup pretends that replication was one-way for simplicity
// sake, but uses the same compoenents on either side to make sure that it
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
}

func setupManagers(t *testing.T) (*schemauc.Manager, *schemauc.Manager) {
	remoteManager := newSchemaManagerWithClusterStateAndClient(
		&fakeClusterState{hosts: []string{}}, nil)

	schemaHandlers := newSchema(remoteManager.TxManager())
	mux := http.NewServeMux()
	mux.Handle("/schema/transactions/", http.StripPrefix("/schema/transactions/",
		schemaHandlers.transactions()))
	server := httptest.NewServer(mux)

	client := clients.NewClusterSchema(&http.Client{})
	parsedURL, err := url.Parse(server.URL)
	require.Nil(t, err)
	state := &fakeClusterState{hosts: []string{parsedURL.Host}}
	localManager := newSchemaManagerWithClusterStateAndClient(state, client)

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

// New Local Schema *Manager
func newSchemaManagerWithClusterStateAndClient(clusterState *fakeClusterState,
	client cluster.Client) *schemauc.Manager {
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2"},
	}
	sm, err := schemauc.NewManager(&NilMigrator{}, newFakeRepo(), logger, &fakeAuthorizer{},
		config.Config{DefaultVectorizerModule: config.VectorizerModuleNone},
		dummyParseVectorConfig, // only option for now
		vectorizerValidator, &fakeModuleConfig{}, clusterState, client,
	)
	if err != nil {
		panic(err.Error())
	}

	return sm
}
