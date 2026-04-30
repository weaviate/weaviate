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

const runtimeOverridePath = "/etc/weaviate/runtime-overrides.yaml"

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

// TestMCPRuntimeToggle verifies that the MCP server's Enabled and
// WriteAccessEnabled flags can be toggled at runtime via the runtime overrides
// YAML, without restarting the cluster.
func TestMCPRuntimeToggle(t *testing.T) {
	mainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminUser := "admin-user"
	adminKey := "admin-key"

	// Pre-create an empty runtime override file so NewConfigManager can load it at startup.
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
		Start(mainCtx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(mainCtx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())
	defer helper.ResetClient()

	mcpURL := fmt.Sprintf("http://%s", compose.GetWeaviate().McpURI())
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

	t.Run("baseline: MCP enabled, write access enabled", func(t *testing.T) {
		ctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()

		names := listMCPToolNames(ctx, t, mcpURL, adminKey)
		require.Contains(t, names, "weaviate-objects-upsert", "write tool should be visible when write access enabled")
		require.Contains(t, names, "weaviate-query-hybrid", "read tool should be visible")
	})

	t.Run("disable write access at runtime → write tool hidden, upsert rejected", func(t *testing.T) {
		writeRuntimeOverride(t, mainCtx, weaviateContainer, "mcp_server_write_access_enabled: false\n")

		// Wait up to ~5s for the override to be picked up (LoadInterval=1s).
		ctx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()

		require.Eventually(t, func() bool {
			names := listMCPToolNames(ctx, t, mcpURL, adminKey)
			for _, n := range names {
				if n == "weaviate-objects-upsert" {
					return false
				}
			}
			return true
		}, 10*time.Second, 500*time.Millisecond, "write tool should disappear from tools/list")

		// Calling the write tool should fail with the disabled error.
		var resp create.UpsertObjectResp
		err := callToolOnceWithAuth(ctx, t, mcpURL, "weaviate-objects-upsert", adminKey,
			create.UpsertObjectArgs{
				CollectionName: className,
				Objects: []create.ObjectToUpsert{
					{Properties: map[string]any{"content": "should fail"}},
				},
			}, &resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "write access is disabled")
	})

	t.Run("re-enable write access at runtime → write tool visible again", func(t *testing.T) {
		writeRuntimeOverride(t, mainCtx, weaviateContainer, "mcp_server_write_access_enabled: true\n")

		ctx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()

		require.Eventually(t, func() bool {
			names := listMCPToolNames(ctx, t, mcpURL, adminKey)
			for _, n := range names {
				if n == "weaviate-objects-upsert" {
					return true
				}
			}
			return false
		}, 10*time.Second, 500*time.Millisecond, "write tool should reappear in tools/list")
	})

	t.Run("disable MCP entirely at runtime → /v1/mcp returns 404", func(t *testing.T) {
		writeRuntimeOverride(t, mainCtx, weaviateContainer, "mcp_server_enabled: false\n")

		require.Eventually(t, func() bool {
			status, body := rawMCPInitializeStatus(t, mcpURL, adminKey)
			return status == http.StatusNotFound && strings.Contains(body, "MCP server is not enabled")
		}, 10*time.Second, 500*time.Millisecond, "MCP endpoint should return 404 when disabled at runtime")
	})

	t.Run("re-enable MCP at runtime → /v1/mcp serves again", func(t *testing.T) {
		writeRuntimeOverride(t, mainCtx, weaviateContainer, "mcp_server_enabled: true\n")

		require.Eventually(t, func() bool {
			status, _ := rawMCPInitializeStatus(t, mcpURL, adminKey)
			return status == http.StatusOK
		}, 10*time.Second, 500*time.Millisecond, "MCP endpoint should serve 200 when re-enabled")
	})
}
