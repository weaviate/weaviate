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

package authz

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	runtimeOverridePath = "/etc/weaviate/runtime-overrides.yaml"
	upsertToolName      = "weaviate-objects-upsert"
	pollTimeout         = 10 * time.Second
	pollInterval        = 500 * time.Millisecond
)

// writeRuntimeOverride replaces the runtime override file's contents. Using
// `sh -c` with a here-string keeps it simple and avoids shell-escaping issues.
func writeRuntimeOverride(t *testing.T, ctx context.Context, container testcontainers.Container, contents string) {
	t.Helper()
	exitCode, _, err := container.Exec(ctx, []string{
		"sh", "-c", fmt.Sprintf("cat > %s <<'EOF'\n%sEOF", runtimeOverridePath, contents),
	})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
}

// listMCPToolNames calls tools/list and returns the tool names.
func listMCPToolNames(ctx context.Context, t *testing.T, mcpURL, key string) []string {
	t.Helper()
	c, err := client.NewStreamableHttpClient(
		mcpURL,
		transport.WithHTTPHeaders(map[string]string{"Authorization": fmt.Sprintf("Bearer %s", key)}),
	)
	require.NoError(t, err)

	_, err = c.Initialize(ctx, mcp.InitializeRequest{})
	require.NoError(t, err)

	res, err := c.ListTools(ctx, mcp.ListToolsRequest{})
	require.NoError(t, err)

	names := make([]string, 0, len(res.Tools))
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
	}
	return names
}

// rawMCPInitializeStatus issues a raw HTTP POST to /v1/mcp with an initialize
// request and returns the HTTP status code. Used to verify the disabled-MCP
// 404 response without going through the MCP client (which masks status codes).
func rawMCPInitializeStatus(t *testing.T, mcpURL, key string) (int, string) {
	t.Helper()
	body := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}`)
	req, err := http.NewRequest(http.MethodPost, mcpURL, body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(bodyBytes)
}

// startMCPRuntimeToggleCluster boots a single-node cluster with MCP enabled,
// runtime overrides enabled with a 1s reload interval, and an admin user.
// Returns the compose and the MCP URL.
func startMCPRuntimeToggleCluster(t *testing.T, ctx context.Context, adminUser, adminKey string) (*docker.DockerCompose, string) {
	t.Helper()
	emptyOverride := testcontainers.ContainerFile{
		Reader:            strings.NewReader(""),
		ContainerFilePath: runtimeOverridePath,
		FileMode:          0o644,
	}

	compose, err := docker.New().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateWithGRPC().
		WithRBAC().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithRbacRoots(adminUser).
		WithMCP().
		WithWeaviateEnv("RUNTIME_OVERRIDES_ENABLED", "true").
		WithWeaviateEnv("RUNTIME_OVERRIDES_PATH", runtimeOverridePath).
		WithWeaviateEnv("RUNTIME_OVERRIDES_LOAD_INTERVAL", "1s").
		WithWeaviateFiles(emptyOverride).
		Start(ctx)
	require.NoError(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())
	mcpURL := fmt.Sprintf("http://%s", compose.GetWeaviate().McpURI())
	return compose, mcpURL
}

// toggleStep represents a single runtime-toggle scenario: a YAML override to
// apply, plus a verification function that asserts the resulting state.
type toggleStep struct {
	name     string
	override string
	verify   func(t *testing.T, mcpURL, key string)
}

// TestMCPRuntimeToggle verifies that the MCP server's Enabled and
// WriteAccessEnabled flags can be toggled at runtime via the runtime overrides
// YAML, without restarting the cluster.
func TestMCPRuntimeToggle(t *testing.T) {
	mainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminUser := "admin-user"
	adminKey := "admin-key"

	compose, mcpURL := startMCPRuntimeToggleCluster(t, mainCtx, adminUser, adminKey)
	defer func() {
		require.NoError(t, compose.Terminate(mainCtx))
	}()
	defer helper.ResetClient()

	adminAuth := helper.CreateAuth(adminKey)

	// Seed a collection so tools/call has something to operate on.
	className := "RuntimeToggleCol"
	helper.DeleteClassWithAuthz(t, className, adminAuth)
	helper.CreateClassAuth(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "content", DataType: schema.DataTypeText.PropString()},
		},
		Vectorizer: "none",
	}, adminKey)
	defer helper.DeleteClassWithAuthz(t, className, adminAuth)

	weaviateContainer := compose.GetWeaviateNode(1).Container()

	// Helpers that close over test-level state.
	expectToolVisibility := func(wantVisible bool, msg string) func(*testing.T, string, string) {
		return func(t *testing.T, mcpURL, key string) {
			t.Helper()
			ctx, c := context.WithTimeout(context.Background(), 15*time.Second)
			defer c()
			require.Eventually(t, func() bool {
				return slices.Contains(listMCPToolNames(ctx, t, mcpURL, key), upsertToolName) == wantVisible
			}, pollTimeout, pollInterval, msg)
		}
	}
	expectMCPStatus := func(wantStatus int, msg string) func(*testing.T, string, string) {
		return func(t *testing.T, mcpURL, key string) {
			t.Helper()
			require.Eventually(t, func() bool {
				status, _ := rawMCPInitializeStatus(t, mcpURL, key)
				return status == wantStatus
			}, pollTimeout, pollInterval, msg)
		}
	}

	t.Run("baseline: MCP enabled, write access enabled", func(t *testing.T) {
		ctx, c := context.WithTimeout(context.Background(), pollTimeout)
		defer c()

		names := listMCPToolNames(ctx, t, mcpURL, adminKey)
		require.Contains(t, names, upsertToolName, "write tool should be visible when write access enabled")
		require.Contains(t, names, "weaviate-query-hybrid", "read tool should be visible")
	})

	steps := []toggleStep{
		{
			name:     "disable write access → write tool hidden",
			override: "mcp_server_write_access_enabled: false\n",
			verify:   expectToolVisibility(false, "write tool should disappear from tools/list"),
		},
		{
			name:     "re-enable write access → write tool visible again",
			override: "mcp_server_write_access_enabled: true\n",
			verify:   expectToolVisibility(true, "write tool should reappear in tools/list"),
		},
		{
			name:     "disable MCP entirely → /v1/mcp returns 404",
			override: "mcp_server_enabled: false\n",
			verify:   expectMCPStatus(http.StatusNotFound, "MCP endpoint should return 404 when disabled at runtime"),
		},
		{
			name:     "re-enable MCP → /v1/mcp serves again",
			override: "mcp_server_enabled: true\n",
			verify:   expectMCPStatus(http.StatusOK, "MCP endpoint should serve 200 when re-enabled"),
		},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			writeRuntimeOverride(t, mainCtx, weaviateContainer, step.override)
			step.verify(t, mcpURL, adminKey)
		})
	}

	// Beyond the visibility check, also verify that calling the write tool
	// while disabled returns the explicit "write access is disabled" error.
	t.Run("write tool call while disabled returns explicit error", func(t *testing.T) {
		writeRuntimeOverride(t, mainCtx, weaviateContainer, "mcp_server_write_access_enabled: false\n")
		expectToolVisibility(false, "write tool should be hidden")(t, mcpURL, adminKey)

		ctx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()

		var resp create.UpsertObjectResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, upsertToolName, adminKey,
			create.UpsertObjectArgs{
				CollectionName: className,
				Objects: []create.ObjectToUpsert{
					{Properties: map[string]any{"content": "should fail"}},
				},
			}, &resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "write access is disabled")
	})
}
