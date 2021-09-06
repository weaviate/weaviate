// +build integrationTest

package clusterintegrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/clients"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDistributedSetup uses as many real components and only mocks out
// non-essential parts. Essentially we fix the shard/cluster state and schema
// as they aren't critical to this test, but use real repos and real HTTP APIs
// between the repos.
func TestDistributedSetup(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			fmt.Println(err)
		}
	}()

	var nodes []*node
	numberOfNodes := 10
	numberOfObjects := 200

	t.Run("setup", func(t *testing.T) {
		overallShardState := multiShardState(numberOfNodes)
		shardStateSerialized, err := json.Marshal(overallShardState)
		require.Nil(t, err)

		for i := 0; i < numberOfNodes; i++ {
			node := &node{
				name: fmt.Sprintf("node-%d", i),
			}

			node.init(numberOfNodes, dirName, shardStateSerialized, &nodes)
			nodes = append(nodes, node)
		}
	})

	t.Run("apply schema", func(t *testing.T) {
		for i := range nodes {
			err := nodes[i].migrator.AddClass(context.Background(), class(),
				nodes[i].schemaGetter.shardState)
			require.Nil(t, err)
			nodes[i].schemaGetter.schema.Objects.Classes = append(nodes[i].schemaGetter.schema.Objects.Classes, class())
		}
	})

	data := exampleData(numberOfObjects)

	t.Run("import by picking a random node", func(t *testing.T) {
		for _, obj := range data {
			node := nodes[rand.Intn(len(nodes))]

			err := node.repo.PutObject(context.Background(), obj, obj.Vector)
			require.Nil(t, err)
		}
	})

	t.Run("query individually using random node", func(t *testing.T) {
		for _, obj := range data {
			node := nodes[rand.Intn(len(nodes))]

			res, err := node.repo.ObjectByID(context.Background(), obj.ID, search.SelectProperties{}, additional.Properties{})
			require.Nil(t, err)
			assert.NotNil(t, res)
			assert.Equal(t, obj.Properties, res.Object().Properties)
		}
	})
}

type node struct {
	name             string
	shardingState    *sharding.State
	repo             *db.DB
	schemaGetter     *fakeSchemaGetter
	clusterAPIServer *httptest.Server
	migrator         *db.Migrator
	hostname         string
}

func (n *node) init(numberOfNodes int, dirName string, shardStateRaw []byte,
	allNodes *[]*node) {
	localDir := path.Join(dirName, n.name)
	logger, _ := test.NewNullLogger()

	nodeResolver := &nodeResolver{
		nodes: allNodes,
		local: n.name,
	}

	shardState, err := sharding.StateFromJSON(shardStateRaw, nodeResolver)
	if err != nil {
		panic(err)
	}

	client := clients.NewRemoteIndex(&http.Client{})
	n.repo = db.New(logger, db.Config{RootPath: localDir}, client, nodeResolver)
	n.schemaGetter = &fakeSchemaGetter{
		shardState: shardState,
		schema:     schema.Schema{Objects: &models.Schema{}},
	}
	n.repo.SetSchemaGetter(n.schemaGetter)
	err = n.repo.WaitForStartup(context.Background())
	if err != nil {
		panic(err)
	}

	n.migrator = db.NewMigrator(n.repo, logger)

	indices := clusterapi.NewIndices(sharding.NewRemoteIndexIncoming(n.repo))
	mux := http.NewServeMux()
	mux.Handle("/indices/", indices.Indices())

	srv := httptest.NewServer(mux)
	u, err := url.Parse(srv.URL)
	if err != nil {
		panic(err)
	}
	n.hostname = u.Host
}

func multiShardState(nodeCount int) *sharding.State {
	config, err := sharding.ParseConfig(map[string]interface{}{
		"desiredCount": json.Number(fmt.Sprintf("%d", nodeCount)),
	})
	if err != nil {
		panic(err)
	}

	s, err := sharding.InitState("multi-shard-test-index", config,
		fakeNodes{[]string{"node-1"}})
	if err != nil {
		panic(err)
	}

	return s
}

type fakeNodes struct {
	nodes []string
}

func (f fakeNodes) AllNames() []string {
	return f.nodes
}

func (f fakeNodes) LocalName() string {
	return f.nodes[0]
}

type fakeSchemaGetter struct {
	schema     schema.Schema
	shardState *sharding.State
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaGetter) ShardingState(class string) *sharding.State {
	return f.shardState
}

type nodeResolver struct {
	nodes *[]*node
	local string
}

func (r nodeResolver) AllNames() []string {
	panic("node resolving not implemented yet")
}

func (r nodeResolver) LocalName() string {
	return r.local
}

func (r nodeResolver) NodeHostname(nodeName string) (string, bool) {
	for _, node := range *r.nodes {
		if node.name == nodeName {
			return node.hostname, true
		}
	}

	return "", false
}

func class() *models.Class {
	return &models.Class{
		Class:               "Distributed",
		VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
	}
}

func invertedConfig() *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
	}
}

func exampleData(size int) []*models.Object {
	out := make([]*models.Object, size)

	for i := range out {
		out[i] = &models.Object{
			Class: "Distributed",
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"description": fmt.Sprintf("object-%d", i),
			},
			Vector: []float32{1, float32(i) / float32(1000)},
		}
	}

	return out
}
