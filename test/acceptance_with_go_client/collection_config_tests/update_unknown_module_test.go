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

package collection_config_tests

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/modules"
	"github.com/weaviate/weaviate-go-client/v6/modules/selfprovided"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestUpdateWithUnknownModule_SingleNode(t *testing.T) {
	c := connect(t, docker.New().WithWeaviateWithGRPC().WithWeaviateEnv("ENABLE_MODULES", "reranker-dummy"))
	testUpdateWithUnknownModule(t, c)
}

func TestUpdateWithUnknownModule_Cluster(t *testing.T) {
	c := connect(t, docker.New().WithWeaviateCluster(3).WithWeaviateExposeGRPCPort().
		WithWeaviateEnv("ENABLE_MODULES", "reranker-dummy"))
	testUpdateWithUnknownModule(t, c)
}

// rerankerDummy names the reranker-dummy module, which the Go client has no type for.
type rerankerDummy struct{}

func (rerankerDummy) Name() string { return "reranker-dummy" }

// The client registry must know every module in a collection's moduleConfig:
// api.Collection.UnmarshalJSON loops until it has matched every key against the
// registry, so an unregistered key makes GetConfig spin forever.
func init() { modules.Register(rerankerDummy{}) }

// testUpdateWithUnknownModule checks that a class update naming a module the
// node does not have is rejected with 422 and leaves the module config as it
// was. The server used to accept the update and store the unknown module.
//
// The Go client v6 has no call that changes the module config of a
// collection, so the update goes through the REST client of this repository.
func testUpdateWithUnknownModule(t *testing.T, c *weaviate.Client) {
	ctx := context.Background()
	name := "UpdateWithUnknownModule"
	require.NoError(t, c.Collections.Delete(ctx, name))
	t.Cleanup(func() { require.NoError(t, c.Collections.Delete(ctx, name)) })

	_, err := c.Collections.Create(ctx, collections.Collection{
		Name:            name,
		Properties:      []collections.Property{{Name: "name", DataType: collections.DataTypeText}},
		Vectors:         map[string]collections.VectorConfig{"v": {Vectorizer: selfprovided.Vectorizer}},
		RerankerModules: []modules.Module{rerankerDummy{}},
	})
	require.NoError(t, err)

	update := helper.GetClass(t, name)
	require.NotNil(t, update)
	moduleConfig, ok := update.ModuleConfig.(map[string]any)
	require.True(t, ok, "module config of the stored class: %#v", update.ModuleConfig)
	moduleConfig["rerankr-openai"] = map[string]any{}

	_, err = helper.Client(t).Schema.SchemaObjectsUpdate(
		clschema.NewSchemaObjectsUpdateParams().WithClassName(name).WithObjectClass(update), nil)
	require.Error(t, err, "an update naming a module that does not exist must be rejected")
	var unprocessable *clschema.SchemaObjectsUpdateUnprocessableEntity
	require.ErrorAs(t, err, &unprocessable, "expected 422, got: %v", err)
	assert.Contains(t, unprocessable.Payload.Error[0].Message, "module with name rerankr-openai doesn't exist")

	got, err := c.Collections.GetConfig(ctx, name)
	require.NoError(t, err)
	var rerankers []string
	for _, m := range got.RerankerModules {
		rerankers = append(rerankers, m.Name())
	}
	assert.Equal(t, []string{"reranker-dummy"}, rerankers)
}

// connect starts the containers, points the REST client of this repository at
// Weaviate and returns a Go client v6 for the same node.
func connect(t *testing.T, compose *docker.Compose) *weaviate.Client {
	t.Helper()
	ctx := context.Background()
	env, err := compose.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Terminate(ctx)) })

	helper.SetupClient(env.GetWeaviate().URI())
	t.Cleanup(helper.ResetClient)

	host, httpPort, err := net.SplitHostPort(env.GetWeaviate().URI())
	require.NoError(t, err)
	_, grpcPort, err := net.SplitHostPort(env.GetWeaviate().GrpcURI())
	require.NoError(t, err)

	c, err := weaviate.NewLocal(ctx,
		weaviate.WithHost(host),
		weaviate.WithHTTPPort(httpPort),
		weaviate.WithGRPCPort(grpcPort),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}
