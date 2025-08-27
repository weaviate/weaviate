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

package test

import (
	//"context"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/config/runtime"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestAssignDynamic(t *testing.T) {
	d := runtime.NewDynamicValue("rq")
	require.Equal(t, "rq", d.Get())
}

func TestDefaultCompressionRQ8(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("DEFAULT_QUANTIZATION", "rq-8").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	cls := articles.ParagraphsClass()
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor: 1,
	}
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}

	// Create the class
	t.Log("Creating class", cls.Class)
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	// Load data
	t.Log("Loading data into tenant...")
	tenantName := "tenant"
	batch := make([]*models.Object, 0, 100000)
	start := time.Now()
	for j := 0; j < 10; j++ {
		batch = append(batch, (*models.Object)(articles.NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", j)).
			WithTenant(tenantName).
			Object()))
		if len(batch) == 50000 {
			helper.CreateObjectsBatch(t, batch)
			t.Logf("Loaded %d objects", len(batch))
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		helper.CreateObjectsBatch(t, batch)
		t.Logf("Loaded remaining %d objects", len(batch))
	}
	t.Logf("Data loading took %s", time.Since(start))

	nodes, err := helper.Client(t).Nodes.NodesGet(nil, nil)
	require.Nil(t, err)

	nodeNames := make([]string, len(nodes.GetPayload().Nodes))
	for i, node := range nodes.GetPayload().Nodes {
		nodeNames[i] = node.Name
	}

	// Get the schema

	t.Log("Getting schema")
	schema, err := helper.Client(t).Schema.SchemaDump(nil, nil)
	fmt.Printf("Schema: %+v\n", schema.GetPayload())
	require.Nil(t, err)
	require.NotNil(t, schema)
	payload := schema.GetPayload()
	require.NotNil(t, payload)
	viconfig := payload.Classes[0].VectorIndexConfig
	require.NotNil(t, viconfig)
	rq := viconfig.(map[string]interface{})["rq"]
	require.NotNil(t, rq)
	enabled := rq.(map[string]interface{})["enabled"].(bool)
	require.Equal(t, true, enabled)
	bits := rq.(map[string]interface{})["bits"].(int)
	require.Equal(t, 8, bits)
	skipDefaultQuantization := viconfig.(map[string]interface{})["skipDefaultQuantization"].(bool)
	require.Equal(t, false, skipDefaultQuantization)
	trackDefaultQuantization := viconfig.(map[string]interface{})["trackDefaultQuantization"].(bool)
	require.Equal(t, true, trackDefaultQuantization)
}

func TestDefaultCompressionRQ1(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("DEFAULT_QUANTIZATION", "rq-1").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	cls := articles.ParagraphsClass()
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor: 1,
	}
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}

	// Create the class
	t.Log("Creating class", cls.Class)
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	// Load data
	t.Log("Loading data into tenant...")
	tenantName := "tenant"
	batch := make([]*models.Object, 0, 100000)
	start := time.Now()
	for j := 0; j < 10; j++ {
		batch = append(batch, (*models.Object)(articles.NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", j)).
			WithTenant(tenantName).
			Object()))
		if len(batch) == 50000 {
			helper.CreateObjectsBatch(t, batch)
			t.Logf("Loaded %d objects", len(batch))
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		helper.CreateObjectsBatch(t, batch)
		t.Logf("Loaded remaining %d objects", len(batch))
	}
	t.Logf("Data loading took %s", time.Since(start))

	nodes, err := helper.Client(t).Nodes.NodesGet(nil, nil)
	require.Nil(t, err)

	nodeNames := make([]string, len(nodes.GetPayload().Nodes))
	for i, node := range nodes.GetPayload().Nodes {
		nodeNames[i] = node.Name
	}

	// Get the schema

	t.Log("Getting schema")
	schema, err := helper.Client(t).Schema.SchemaDump(nil, nil)
	fmt.Printf("Schema: %+v\n", schema.GetPayload())
	require.Nil(t, err)
	require.NotNil(t, schema)
	payload := schema.GetPayload()
	require.NotNil(t, payload)
	viconfig := payload.Classes[0].VectorIndexConfig
	require.NotNil(t, viconfig)
	rq := viconfig.(map[string]interface{})["rq"]
	require.NotNil(t, rq)
	enabled := rq.(map[string]interface{})["enabled"].(bool)
	require.Equal(t, true, enabled)
	bits := rq.(map[string]interface{})["bits"].(int)
	require.Equal(t, 8, bits)
	skipDefaultQuantization := viconfig.(map[string]interface{})["skipDefaultQuantization"].(bool)
	require.Equal(t, false, skipDefaultQuantization)
	trackDefaultQuantization := viconfig.(map[string]interface{})["trackDefaultQuantization"].(bool)
	require.Equal(t, true, trackDefaultQuantization)
}

func TestDefaultCompressionWithSkipDefaultQuantization(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("DEFAULT_QUANTIZATION", "rq-1").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	cls := articles.ParagraphsClass()
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor: 1,
	}
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}
	cls.VectorIndexConfig = map[string]interface{}{
		"skipDefaultQuantization": true,
	}

	// Create the class
	t.Log("Creating class", cls.Class)
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	// Load data
	t.Log("Loading data into tenant...")
	tenantName := "tenant"
	batch := make([]*models.Object, 0, 100000)
	start := time.Now()
	for j := 0; j < 10; j++ {
		batch = append(batch, (*models.Object)(articles.NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", j)).
			WithTenant(tenantName).
			Object()))
		if len(batch) == 50000 {
			helper.CreateObjectsBatch(t, batch)
			t.Logf("Loaded %d objects", len(batch))
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		helper.CreateObjectsBatch(t, batch)
		t.Logf("Loaded remaining %d objects", len(batch))
	}
	t.Logf("Data loading took %s", time.Since(start))

	nodes, err := helper.Client(t).Nodes.NodesGet(nil, nil)
	require.Nil(t, err)

	nodeNames := make([]string, len(nodes.GetPayload().Nodes))
	for i, node := range nodes.GetPayload().Nodes {
		nodeNames[i] = node.Name
	}

	// Get the schema

	t.Log("Getting schema")
	schema, err := helper.Client(t).Schema.SchemaDump(nil, nil)
	fmt.Printf("Schema: %+v\n", schema.GetPayload())
	require.Nil(t, err)
	require.NotNil(t, schema)
	payload := schema.GetPayload()
	require.NotNil(t, payload)
	viconfig := payload.Classes[0].VectorIndexConfig
	require.NotNil(t, viconfig)
	rq := viconfig.(map[string]interface{})["rq"]
	require.NotNil(t, rq)
	enabled := rq.(map[string]interface{})["enabled"].(bool)
	require.Equal(t, false, enabled)
	skipDefaultQuantization := viconfig.(map[string]interface{})["skipDefaultQuantization"].(bool)
	require.Equal(t, true, skipDefaultQuantization)
	trackDefaultQuantization := viconfig.(map[string]interface{})["trackDefaultQuantization"].(bool)
	require.Equal(t, false, trackDefaultQuantization)
}

func TestDefaultCompressionOverride(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("DEFAULT_QUANTIZATION", "rq-1").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	cls := articles.ParagraphsClass()
	cls.ReplicationConfig = &models.ReplicationConfig{
		Factor: 1,
	}
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled:              true,
		AutoTenantActivation: true,
		AutoTenantCreation:   true,
	}

	cfg := hnsw.NewDefaultUserConfig()
	cfg.BQ.Enabled = true
	cls.VectorIndexConfig = cfg

	// Create the class
	t.Log("Creating class", cls.Class)
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	// Load data
	t.Log("Loading data into tenant...")
	tenantName := "tenant"
	batch := make([]*models.Object, 0, 100000)
	start := time.Now()
	for j := 0; j < 10; j++ {
		batch = append(batch, (*models.Object)(articles.NewParagraph().
			WithContents(fmt.Sprintf("paragraph#%d", j)).
			WithTenant(tenantName).
			Object()))
		if len(batch) == 50000 {
			helper.CreateObjectsBatch(t, batch)
			t.Logf("Loaded %d objects", len(batch))
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		helper.CreateObjectsBatch(t, batch)
		t.Logf("Loaded remaining %d objects", len(batch))
	}
	t.Logf("Data loading took %s", time.Since(start))

	nodes, err := helper.Client(t).Nodes.NodesGet(nil, nil)
	require.Nil(t, err)

	nodeNames := make([]string, len(nodes.GetPayload().Nodes))
	for i, node := range nodes.GetPayload().Nodes {
		nodeNames[i] = node.Name
	}

	// Get the schema

	t.Log("Getting schema")
	schema, err := helper.Client(t).Schema.SchemaDump(nil, nil)
	fmt.Printf("Schema: %+v\n", schema.GetPayload())
	require.Nil(t, err)
	require.NotNil(t, schema)
	payload := schema.GetPayload()
	require.NotNil(t, payload)
	viconfig := payload.Classes[0].VectorIndexConfig
	require.NotNil(t, viconfig)
	rq := viconfig.(map[string]interface{})["rq"]
	require.NotNil(t, rq)
	require.False(t, rq.(map[string]interface{})["enabled"].(bool))
	bq := viconfig.(map[string]interface{})["bq"]
	require.NotNil(t, bq)
	enabled := bq.(map[string]interface{})["enabled"].(bool)
	require.Equal(t, true, enabled)
	skipDefaultQuantization := viconfig.(map[string]interface{})["skipDefaultQuantization"].(bool)
	require.Equal(t, false, skipDefaultQuantization)
	trackDefaultQuantization := viconfig.(map[string]interface{})["trackDefaultQuantization"].(bool)
	require.Equal(t, false, trackDefaultQuantization)
}
