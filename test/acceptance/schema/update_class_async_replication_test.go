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
	"testing"

	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func int64Ptr(v int64) *int64 {
	return &v
}

func TestUpdateClassAsyncReplicationConfig(t *testing.T) {
	className := "AsyncReplicationClass"

	int64Ptr := func(v int64) *int64 { return &v }

	t.Run("delete class if exists", func(t *testing.T) {
		params := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		require.Nil(t, err)
	})

	t.Run("create class with async replication disabled", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			ReplicationConfig: &models.ReplicationConfig{
				Factor:       3,
				AsyncEnabled: false,
			},
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		require.Nil(t, err)
	})

	testCases := []struct {
		name   string
		config *models.ReplicationAsyncConfig
	}{
		{
			name: "workload 1",
			config: &models.ReplicationAsyncConfig{
				MaxWorkers:                  int64Ptr(20),
				HashtreeHeight:              int64Ptr(10),
				Frequency:                   int64Ptr(60000),
				FrequencyWhilePropagating:   int64Ptr(5000),
				AliveNodesCheckingFrequency: int64Ptr(7000),
				LoggingFrequency:            int64Ptr(30),
				DiffBatchSize:               int64Ptr(2000),
				DiffPerNodeTimeout:          int64Ptr(20),
				PrePropagationTimeout:       int64Ptr(600),
				PropagationTimeout:          int64Ptr(120),
				PropagationLimit:            int64Ptr(50000),
				PropagationDelay:            int64Ptr(45000),
				PropagationConcurrency:      int64Ptr(10),
				PropagationBatchSize:        int64Ptr(500),
			},
		},
		{
			name: "workload 2",
			config: &models.ReplicationAsyncConfig{
				MaxWorkers:                  int64Ptr(5),
				HashtreeHeight:              int64Ptr(6),
				Frequency:                   int64Ptr(120000),
				FrequencyWhilePropagating:   int64Ptr(15000),
				AliveNodesCheckingFrequency: int64Ptr(20000),
				LoggingFrequency:            int64Ptr(60),
				DiffBatchSize:               int64Ptr(500),
				DiffPerNodeTimeout:          int64Ptr(10),
				PrePropagationTimeout:       int64Ptr(300),
				PropagationTimeout:          int64Ptr(60),
				PropagationLimit:            int64Ptr(10000),
				PropagationDelay:            int64Ptr(10000),
				PropagationConcurrency:      int64Ptr(2),
				PropagationBatchSize:        int64Ptr(100),
			},
		},
	}

	for _, tc := range testCases {
		tc := tc // capture
		t.Run(tc.name, func(t *testing.T) {
			// fetch class
			getParams := clschema.NewSchemaObjectsGetParams().
				WithClassName(className)

			res, err := helper.Client(t).Schema.SchemaObjectsGet(getParams, nil)
			require.Nil(t, err)

			class := res.Payload
			class.ReplicationConfig.AsyncEnabled = true
			class.ReplicationConfig.AsyncConfig = tc.config

			// update
			updateParams := clschema.NewSchemaObjectsUpdateParams().
				WithClassName(className).
				WithObjectClass(class)

			_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
			require.Nil(t, err)

			// re-fetch and validate
			res, err = helper.Client(t).Schema.SchemaObjectsGet(getParams, nil)
			require.Nil(t, err)

			rc := res.Payload.ReplicationConfig
			require.NotNil(t, rc)
			require.True(t, rc.AsyncEnabled)
			require.NotNil(t, rc.AsyncConfig)

			requireAsyncConfigEquals(t, tc.config, rc.AsyncConfig)
		})
	}
}

func requireAsyncConfigEquals(
	t *testing.T,
	expected, actual *models.ReplicationAsyncConfig,
) {
	t.Helper()

	requireOptionalInt64 := func(exp, act *int64) {
		if exp == nil {
			require.Nil(t, act)
			return
		}
		require.NotNil(t, act)
		require.Equal(t, *exp, *act)
	}

	requireOptionalInt64(expected.MaxWorkers, actual.MaxWorkers)
	requireOptionalInt64(expected.HashtreeHeight, actual.HashtreeHeight)
	requireOptionalInt64(expected.Frequency, actual.Frequency)
	requireOptionalInt64(expected.FrequencyWhilePropagating, actual.FrequencyWhilePropagating)
	requireOptionalInt64(expected.AliveNodesCheckingFrequency, actual.AliveNodesCheckingFrequency)
	requireOptionalInt64(expected.LoggingFrequency, actual.LoggingFrequency)
	requireOptionalInt64(expected.DiffBatchSize, actual.DiffBatchSize)
	requireOptionalInt64(expected.DiffPerNodeTimeout, actual.DiffPerNodeTimeout)
	requireOptionalInt64(expected.PrePropagationTimeout, actual.PrePropagationTimeout)
	requireOptionalInt64(expected.PropagationTimeout, actual.PropagationTimeout)
	requireOptionalInt64(expected.PropagationLimit, actual.PropagationLimit)
	requireOptionalInt64(expected.PropagationDelay, actual.PropagationDelay)
	requireOptionalInt64(expected.PropagationConcurrency, actual.PropagationConcurrency)
	requireOptionalInt64(expected.PropagationBatchSize, actual.PropagationBatchSize)
}

func TestUpdateClassAsyncReplicationValidation(t *testing.T) {
	className := "AsyncReplicationInvalidClass"

	t.Run("create class", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			ReplicationConfig: &models.ReplicationConfig{
				Factor:       2,
				AsyncEnabled: true,
			},
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		require.Nil(t, err)
	})

	t.Run("update asyncConfig with invalid hashtreeHeight", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)

		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload
		class.ReplicationConfig.AsyncConfig = &models.ReplicationAsyncConfig{
			HashtreeHeight: int64Ptr(500),
		}

		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)

		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		require.NotNil(t, err)

		var parsed *clschema.SchemaObjectsUpdateUnprocessableEntity
		require.ErrorAs(t, err, &parsed)
		require.Contains(t, parsed.Payload.Error[0].Message, "hashtreeHeight")
	})
}
