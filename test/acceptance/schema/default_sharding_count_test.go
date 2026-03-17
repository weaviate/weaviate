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

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func getDesiredCount(t *testing.T, class *models.Class) int {
	t.Helper()
	sc, ok := class.ShardingConfig.(map[string]interface{})
	require.True(t, ok, "shardingConfig should be a map")
	dc, ok := sc["desiredCount"]
	require.True(t, ok, "desiredCount should exist")
	num, ok := dc.(json.Number)
	require.True(t, ok, "desiredCount should be json.Number, got %T", dc)
	val, err := num.Int64()
	require.NoError(t, err)
	return int(val)
}

func TestDefaultShardingCountNotSet(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		Start(mainCtx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(mainCtx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("no env var uses cluster size", func(t *testing.T) {
		className := "NoEnvVarClass"
		helper.DeleteClass(t, className)
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		})
		defer helper.DeleteClass(t, className)

		cls := helper.GetClass(t, className)
		assert.Equal(t, 3, getDesiredCount(t, cls))
	})
}

func TestDefaultShardingCountSet(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("DEFAULT_SHARDING_COUNT", "12").
		Start(mainCtx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(mainCtx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("env var overrides cluster size", func(t *testing.T) {
		className := "EnvVarClass"
		helper.DeleteClass(t, className)
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		})
		defer helper.DeleteClass(t, className)

		cls := helper.GetClass(t, className)
		assert.Equal(t, 12, getDesiredCount(t, cls))
	})

	t.Run("user explicit desiredCount wins", func(t *testing.T) {
		className := "UserOverrideClass"
		helper.DeleteClass(t, className)
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
			ShardingConfig: map[string]interface{}{
				"desiredCount": 5,
			},
		})
		defer helper.DeleteClass(t, className)

		cls := helper.GetClass(t, className)
		assert.Equal(t, 5, getDesiredCount(t, cls))
	})

	t.Run("multi-tenancy unaffected", func(t *testing.T) {
		className := "MTClass"
		helper.DeleteClass(t, className)
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
		})
		defer helper.DeleteClass(t, className)

		cls := helper.GetClass(t, className)
		assert.Equal(t, 0, getDesiredCount(t, cls))
	})
}

func TestDefaultShardingCountSetToZero(t *testing.T) {
	mainCtx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("DEFAULT_SHARDING_COUNT", "0").
		Start(mainCtx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(mainCtx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("zero means use cluster size", func(t *testing.T) {
		className := "ZeroEnvClass"
		helper.DeleteClass(t, className)
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		})
		defer helper.DeleteClass(t, className)

		cls := helper.GetClass(t, className)
		assert.Equal(t, 3, getDesiredCount(t, cls))
	})
}

func TestDefaultShardingCountRuntimeOverride(t *testing.T) {
	mainCtx := context.Background()

	const overridePath = "/etc/weaviate/runtime-overrides.yaml"

	// Pre-create an empty runtime override file so NewConfigManager can load it at startup.
	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: overridePath,
		FileMode:          0o644,
	}

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("DEFAULT_SHARDING_COUNT", "12").
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", overridePath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateFiles(emptyOverride).
		Start(mainCtx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(mainCtx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("initial default is 12", func(t *testing.T) {
		className := "InitialDefault"
		helper.DeleteClass(t, className)
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		})
		defer helper.DeleteClass(t, className)

		cls := helper.GetClass(t, className)
		assert.Equal(t, 12, getDesiredCount(t, cls))
	})

	t.Run("runtime override changes default", func(t *testing.T) {
		// Write runtime override YAML to all nodes
		for i := 1; i <= 3; i++ {
			node := compose.GetWeaviateNode(i)
			exitCode, _, err := node.Container().Exec(mainCtx, []string{
				"sh", "-c",
				fmt.Sprintf("printf 'default_sharding_count: 8\\n' > %s", overridePath),
			})
			require.NoError(t, err)
			require.Equal(t, 0, exitCode, "exec on node %d failed", i)
		}

		// Poll until the runtime override takes effect.
		// Use a manual loop because testify's Eventually runs in a goroutine
		// which doesn't support t.Fatal (used by helper functions).
		var createdClasses []string
		defer func() {
			for _, cn := range createdClasses {
				dp := schema.NewSchemaObjectsDeleteParams().WithClassName(cn)
				helper.Client(t).Schema.SchemaObjectsDelete(dp, nil) //nolint:errcheck
			}
		}()

		deadline := time.Now().Add(60 * time.Second)
		var lastDesiredCount int
		for counter := 0; time.Now().Before(deadline); counter++ {
			time.Sleep(3 * time.Second)

			className := fmt.Sprintf("OverrideCheck%d", counter)
			createdClasses = append(createdClasses, className)

			// Use client directly to avoid t.Fatal inside the polling loop.
			delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(className)
			helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil) //nolint:errcheck

			createParams := schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{"text"}},
				},
			})
			if _, err := helper.Client(t).Schema.SchemaObjectsCreate(createParams, nil); err != nil {
				t.Logf("attempt %d: create failed: %v", counter, err)
				continue
			}

			cls, err := helper.GetClassWithoutAssert(t, className)
			if err != nil || cls == nil {
				continue
			}

			sc, ok := cls.ShardingConfig.(map[string]interface{})
			if !ok {
				continue
			}
			if num, ok := sc["desiredCount"].(json.Number); ok {
				val, _ := num.Int64()
				lastDesiredCount = int(val)
				t.Logf("attempt %d: desiredCount=%d", counter, lastDesiredCount)
			}

			if lastDesiredCount == 8 {
				break
			}
		}
		assert.Equal(t, 8, lastDesiredCount, "expected desiredCount=8 after runtime override")
	})
}
