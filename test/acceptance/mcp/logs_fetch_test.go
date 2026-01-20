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

package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// Test 1: Fetch logs with default parameters
func TestLogsFetch_DefaultParameters(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result *read.FetchLogsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-logs-fetch", &read.FetchLogsArgs{}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.NotEmpty(t, result.Logs, "should return some logs with default parameters")
}

// Test 2: Fetch logs with custom limit
func TestLogsFetch_CustomLimit(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	limit := 500
	var result *read.FetchLogsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-logs-fetch", &read.FetchLogsArgs{
		Limit: limit,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.NotEmpty(t, result.Logs, "should return logs with custom limit")
	assert.LessOrEqual(t, len(result.Logs), limit, "logs should not exceed specified limit")
}

// Test 3: Fetch logs with offset
func TestLogsFetch_WithOffset(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	offset := 1000
	var result *read.FetchLogsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-logs-fetch", &read.FetchLogsArgs{
		Offset: offset,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	// Logs may be empty if offset exceeds buffer, which is acceptable
	assert.NotNil(t, result.Logs, "logs field should exist")
}

// Test 4: Fetch logs with both limit and offset
func TestLogsFetch_LimitAndOffset(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	limit := 800
	offset := 500
	var result *read.FetchLogsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-logs-fetch", &read.FetchLogsArgs{
		Limit:  limit,
		Offset: offset,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.LessOrEqual(t, len(result.Logs), limit, "logs should not exceed specified limit")
}

// Test 5: Fetch logs after making an API call
func TestLogsFetch_AfterAPICall(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	// Create a test collection to generate log entries
	class := &models.Class{
		Class: "LogsFetchTestClass",
		Properties: []*models.Property{
			{
				Name:     "testProp",
				DataType: []string{"text"},
			},
		},
	}
	helper.CreateClassAuth(t, class, apiKey)
	defer helper.DeleteClassAuth(t, class.Class, apiKey)

	// Wait a bit for logs to be written
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fetch logs
	var result *read.FetchLogsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-logs-fetch", &read.FetchLogsArgs{
		Limit: 5000,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.NotEmpty(t, result.Logs, "should return logs after API operations")
}

// Test 6: Basic sanity check that logs are non-empty
func TestLogsFetch_LogsAreNonEmpty(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result *read.FetchLogsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-logs-fetch", &read.FetchLogsArgs{
		Limit: 3000,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Greater(t, len(result.Logs), 0, "logs should not be empty in a running server")
}
