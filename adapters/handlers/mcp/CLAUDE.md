# Weaviate MCP Server

This directory contains the implementation of the Weaviate Model Context Protocol (MCP) server, which provides LLM-friendly tools for interacting with Weaviate through MCP-compatible clients like Claude Desktop.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Available Tools](#available-tools)
- [Development](#development)
- [Building and Testing](#building-and-testing)
- [Authentication and Authorization](#authentication-and-authorization)
- [Adding New Tools](#adding-new-tools)
- [Common Issues and Solutions](#common-issues-and-solutions)

## Overview

The MCP server exposes Weaviate functionality as tools that can be called by LLMs through the Model Context Protocol. This enables natural language interactions with Weaviate databases through MCP clients.

**Key Features:**
- RESTful HTTP/SSE-based MCP server
- Full authentication and authorization support (API keys, OIDC, anonymous access)
- Multi-tenant support across all operations
- Automatic JSON schema generation for tool parameters
- Built on [mcp-go](https://mcp-go.dev/) library

**Server Configuration:**
- **Port:** 9000 (hardcoded in `server.go:58`)
- **Protocol:** HTTP with Server-Sent Events (SSE)
- **Version:** 0.1.0

## Architecture

The codebase is organized into logical packages by functionality:

```
adapters/handlers/mcp/
├── README.md           # This file
├── server.go           # Main MCP server setup and lifecycle
├── auth/               # Authentication and authorization
│   └── auth.go         # Principal extraction from requests
├── create/             # Object creation tools
│   ├── create.go       # WeaviateCreator struct and constructor
│   ├── insert_one.go   # Insert single object implementation
│   ├── requests.go     # Request argument structs
│   ├── responses.go    # Response structs
│   └── tools.go        # Tool registration
├── read/               # Schema and metadata reading tools
│   ├── reader.go       # WeaviateReader struct and constructor
│   ├── schema.go       # Get schema implementation
│   ├── tenants.go      # Get tenants implementation
│   ├── requests.go     # Request argument structs
│   ├── responses.go    # Response structs
│   └── tools.go        # Tool registration
└── search/             # Search and query tools
    ├── search.go       # WeaviateSearcher struct and constructor
    ├── hybrid.go       # Hybrid search implementation
    ├── requests.go     # Request argument structs
    └── tools.go        # Tool registration
```

### Component Responsibilities

**Server (`server.go`):**
- Creates and configures the MCP server instance
- Registers all tools from create/read/search packages
- Manages server lifecycle (start/shutdown)
- Integrates with Weaviate's state management

**Auth (`auth/`):**
- Extracts authentication credentials from MCP requests
- Validates principals using Weaviate's auth composer
- Enforces RBAC authorization for MCP operations
- Supports Bearer token authentication

**Create (`create/`):**
- Handles object creation operations
- Manages object insertion with tenant support

**Read (`read/`):**
- Provides schema introspection
- Lists and filters tenants for collections

**Search (`search/`):**
- Executes hybrid search queries
- Supports tenant-scoped searches

## Available Tools

### 1. `weaviate-collections-get-config`

Retrieves collection configuration(s) from the database. Can return all collections or a specific collection's configuration.

**Parameters:**
- `collection_name` (string, optional): Name of specific collection to get config for. If not provided, returns all collections.

**Returns:**
```json
{
  "collections": [
    {
      "class": "Article",
      "description": "...",
      "properties": [...],
      "vectorizer": "...",
      "moduleConfig": {...}
    }
  ]
}
```

When a specific `collection_name` is provided, the response contains only that collection. When omitted, all collections are returned.

**Authorization:** Requires READ permission on MCP resource.

**Example - Get all collections:**
```json
{}
```

**Example - Get specific collection:**
```json
{
  "collection_name": "Article"
}
```

---

### 2. `weaviate-tenants-list`

Lists all tenants for a specific collection.

**Parameters:**
- `collection_name` (string, **required**): Name of the collection to get tenants from

**Returns:**
```json
{
  "tenants": [
    {
      "name": "tenant1",
      "activityStatus": "HOT"
    },
    {
      "name": "tenant2",
      "activityStatus": "COLD"
    }
  ]
}
```

**Authorization:** Requires READ permission on MCP resource.

---

### 3. `weaviate-objects-upsert`

Upserts (inserts or updates) a single object into a collection.

**Parameters:**
- `collection_name` (string, **required**): Name of the collection to upsert the object into
- `tenant` (string, optional): Name of the tenant the object belongs to
- `properties` (object, optional): Key-value pairs of object properties

**Returns:**
```json
{
  "id": "uuid-of-upserted-object"
}
```

**Authorization:** Requires CREATE permission on MCP resource.

**Example:**
```json
{
  "collection_name": "Article",
  "tenant": "customer-a",
  "properties": {
    "title": "Example Article",
    "content": "Article content here"
  }
}
```

---

### 4. `weaviate-query-hybrid`

Performs hybrid search (combining vector and keyword search) on a collection.

**Parameters:**
- `query` (string, **required**): The plain-text query to search the collection on
- `collection_name` (string, **required**): Name of the collection to search
- `tenant_name` (string, optional): Name of the tenant to search within
- `alpha` (float, optional): Semantic weight (0.0 = pure keyword/BM25, 1.0 = pure vector). Default is 0.5 if not specified
- `limit` (integer, optional): Maximum number of results to return
- `target_vectors` (array of strings, optional): Target vectors to use in vector search (for named vectors)
- `target_properties` (array of strings, optional): Properties to perform BM25 keyword search on. If not specified, searches all text properties
- `return_properties` (array of strings, optional): Properties to return in the result. If not specified, all properties are returned
- `return_metadata` (array of strings, optional): Metadata to return in the result. Supported values: `id`, `vector`, `distance`, `score`, `explainScore`, `creationTimeUnix`, `lastUpdateTimeUnix`, `certainty`

**Returns:**
```json
{
  "results": [
    {
      "class": "Article",
      "properties": {
        "title": "Introduction to Machine Learning",
        "content": "..."
      },
      "id": "uuid",
      "additional": {
        "score": 0.95,
        "distance": 0.23
      }
    }
  ]
}
```

The response is a structured object containing a `results` array with all matching documents. The `additional` field contains requested metadata. This wrapper is required by MCP's response validation schema, which expects structured content to be a dictionary (object) rather than a raw array.

**Authorization:** Requires READ permission on MCP resource.

**Example - Basic search:**
```json
{
  "query": "machine learning",
  "collection_name": "Article"
}
```

**Example - Advanced search with all parameters:**
```json
{
  "query": "neural networks and deep learning",
  "collection_name": "Article",
  "tenant_name": "customer-a",
  "alpha": 0.75,
  "limit": 10,
  "target_properties": ["title", "content"],
  "return_properties": ["title", "content", "author"],
  "return_metadata": ["id", "score", "distance"]
}
```

## Development

### Prerequisites

- Go 1.23 or higher
- Running Weaviate instance
- (Optional) Claude Desktop for testing

### Dependencies

The MCP server uses:
- **mcp-go** (`github.com/mark3labs/mcp-go v0.37.1-0.20250812151906-9f16336f83e1`): Core MCP protocol implementation
- **invopop/jsonschema**: Automatic JSON schema generation from Go structs (transitive dependency via mcp-go)

### Running the Server

The MCP server is started as part of the main Weaviate server. To run it in development mode:

```bash
# Run with MCP configuration (includes RBAC auth)
./tools/dev/run_dev_server.sh local-mcp
```

This configuration:
- Enables API key authentication
- Enables RBAC authorization
- Sets up an admin user with root permissions
- Uses API key: `admin-key` for user: `admin`
- Starts Weaviate on port 8080
- Starts MCP server on port 9000

**Authentication Header Format:**
```
Authorization: Bearer admin-key
```

### Connecting from Claude Desktop

Add to your Claude Desktop MCP configuration:

```json
{
  "mcpServers": {
    "weaviate": {
      "command": "curl",
      "args": [
        "-X", "POST",
        "-H", "Authorization: Bearer admin-key",
        "http://localhost:9000/mcp"
      ]
    }
  }
}
```

## Building and Testing

### Build

Build just the MCP handlers:

```bash
go build ./adapters/handlers/mcp/...
```

Build the complete Weaviate server (includes MCP):

```bash
go build ./cmd/weaviate-server
```

### Testing

Currently, there are no dedicated unit tests for the MCP handlers. To test:

1. **Manual Testing:**
   ```bash
   # Start server
   ./tools/dev/run_dev_server.sh local-mcp

   # In another terminal, test with curl
   curl -X POST http://localhost:9000/mcp \
     -H "Authorization: Bearer admin-key" \
     -H "Content-Type: application/json" \
     -d '{
       "jsonrpc": "2.0",
       "id": 1,
       "method": "tools/call",
       "params": {
         "name": "weaviate-collections-get-config",
         "arguments": {}
       }
     }'
   ```

2. **Integration Testing:**
   - Use Claude Desktop with the MCP configuration above
   - Ask Claude to interact with your Weaviate instance
   - Verify tools are listed and callable

### Debugging

Enable debug logging:

```bash
LOG_LEVEL=debug ./tools/dev/run_dev_server.sh local-mcp
```

Common debugging approaches:
- Check server logs for authentication errors
- Verify API key is correct
- Ensure Weaviate is fully started before MCP server connects
- Use `curl` to test individual tool calls

## Authentication and Authorization

### Authentication Flow

1. MCP client sends request with `Authorization` header
2. `auth.Auth.principalFromRequest()` extracts the Bearer token
3. Token is validated using Weaviate's auth composer (supports API keys, OIDC)
4. If no token provided and anonymous access is enabled, anonymous principal is used
5. Principal is returned for authorization check

### Authorization Flow

1. `auth.Auth.Authorize()` is called with the verb (READ, CREATE, UPDATE, DELETE)
2. Weaviate's RBAC authorizer checks if principal has permission for the verb on the MCP resource
3. If authorized, the principal is returned to the tool handler
4. If not authorized, an error is returned and the tool call fails

### Supported Authentication Methods

- **API Keys:** Set via `AUTHENTICATION_APIKEY_ENABLED=true`
- **OIDC:** Set via `AUTHENTICATION_OIDC_ENABLED=true`
- **Anonymous Access:** Set via `AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true`

### Authorization Verbs

Each tool requires specific permissions:
- `weaviate-collections-get-config`: READ
- `weaviate-tenants-list`: READ
- `weaviate-objects-upsert`: CREATE
- `weaviate-query-hybrid`: READ

## Adding New Tools

### Step-by-Step Guide

Let's say you want to add a new tool called `delete-object` to delete objects.

**1. Choose the appropriate package** (create/read/search) or create a new one. For delete, we'll use `create/` or create a new `delete/` package.

**2. Define request arguments struct** in `requests.go`:

```go
type DeleteObjectArgs struct {
    Collection string `json:"collection" jsonschema:"required" jsonschema_description:"Name of collection"`
    ID         string `json:"id" jsonschema:"required" jsonschema_description:"UUID of object to delete"`
    Tenant     string `json:"tenant,omitempty" jsonschema_description:"Name of the tenant"`
}
```

**Important struct tag rules:**
- Use `json:"fieldName"` to set the JSON field name
- Use `jsonschema:"required"` to mark fields as required
- Use `jsonschema_description:"..."` to describe the field (LLMs use this!)
- Use `omitempty` for optional fields: `json:"field,omitempty"`

**3. Define response struct** in `responses.go`:

```go
type DeleteObjectResp struct {
    Success bool   `json:"success" jsonschema_description:"Whether deletion succeeded"`
    ID      string `json:"id" jsonschema_description:"UUID of deleted object"`
}
```

**4. Implement the handler** in a new file `delete_object.go`:

```go
func (c *WeaviateCreator) DeleteObject(ctx context.Context, req mcp.CallToolRequest, args DeleteObjectArgs) (*DeleteObjectResp, error) {
    // 1. Authorize the request
    principal, err := c.Authorize(ctx, req, authorization.DELETE)
    if err != nil {
        return nil, err
    }

    // 2. Perform the operation
    err = c.objectsManager.DeleteObject(ctx, principal, args.Collection, args.ID, args.Tenant)
    if err != nil {
        return nil, fmt.Errorf("failed to delete object: %w", err)
    }

    // 3. Return the response
    return &DeleteObjectResp{
        Success: true,
        ID:      args.ID,
    }, nil
}
```

**Handler signature requirements:**
- First parameter: `ctx context.Context`
- Second parameter: `req mcp.CallToolRequest` (for auth)
- Third parameter: Your args struct
- Return: `(*YourResponseStruct, error)`

**5. Register the tool** in `tools.go`:

```go
func Tools(creator *WeaviateCreator) []server.ServerTool {
    return []server.ServerTool{
        // ... existing tools ...
        {
            Tool: mcp.NewTool(
                "delete-object",
                mcp.WithDescription("Deletes a single object from a collection."),
                mcp.WithInputSchema[DeleteObjectArgs](), // CRITICAL: Include this!
            ),
            Handler: mcp.NewStructuredToolHandler(creator.DeleteObject),
        },
    }
}
```

**Critical:** You **MUST** include `mcp.WithInputSchema[YourArgsStruct]()` or the tool will not expose any parameters to LLMs!

**6. Update the server registration** (if you created a new package):

In `server.go`, add your new tools:

```go
func (s *MCPServer) registerTools() {
    s.server.AddTools(create.Tools(s.creator)...)
    s.server.AddTools(search.Tools(s.searcher)...)
    s.server.AddTools(read.Tools(s.reader)...)
    s.server.AddTools(delete.Tools(s.deleter)...) // Add this
}
```

**7. Build and test:**

```bash
go build ./adapters/handlers/mcp/...
./tools/dev/run_dev_server.sh local-mcp
```

### Best Practices

1. **Always use `jsonschema:"required"` for required fields**
   - Without this, LLMs won't know the field is mandatory

2. **Provide detailed descriptions**
   - LLMs rely on `jsonschema_description` to understand parameters
   - Be specific: "Name of the collection to delete from" vs "Collection"

3. **Keep tool names lowercase with hyphens**
   - Good: `weaviate-objects-upsert`, `weaviate-collections-get-config`, `weaviate-query-hybrid`
   - Bad: `InsertOne`, `getSchema`, `search_hybrid`

4. **Handle errors gracefully**
   - Wrap errors with context: `fmt.Errorf("failed to X: %w", err)`
   - Return user-friendly error messages

5. **Support tenants when applicable**
   - Multi-tenancy is a core Weaviate feature
   - Make tenant parameters optional with `omitempty`

6. **Follow the package structure**
   - Create operations → `create/`
   - Read operations → `read/`
   - Search operations → `search/`
   - Updates/Deletes → consider `update/` or `delete/` packages

7. **Test with real LLMs**
   - Use Claude Desktop to verify LLMs can use your tools
   - Check that parameter descriptions are clear enough

## Common Issues and Solutions

### Issue: Tool doesn't accept parameters

**Symptoms:** LLM says "the tool doesn't accept parameters" or "function expects parameters but has no way to receive them"

**Cause:** Missing `mcp.WithInputSchema[T]()` in tool registration

**Solution:** Add the input schema to your tool definition:

```go
Tool: mcp.NewTool(
    "your-tool",
    mcp.WithDescription("..."),
    mcp.WithInputSchema[YourArgsStruct](), // Add this!
),
```

---

### Issue: LLM doesn't know parameter is required

**Symptoms:** LLM tries to call tool without required parameters, gets errors

**Cause:** Missing `jsonschema:"required"` tag on struct field

**Solution:** Add the required tag:

```go
type YourArgs struct {
    Collection string `json:"collection" jsonschema:"required" jsonschema_description:"..."`
}
```

---

### Issue: Authentication failures

**Symptoms:** "failed to get principal" or "unauthorized" errors

**Cause:** Missing or incorrect Authorization header

**Solution:**
1. Check the API key in your MCP client config
2. Verify format: `Authorization: Bearer your-api-key`
3. Ensure API key authentication is enabled in Weaviate config
4. Check that the user associated with the API key has appropriate RBAC permissions

---

### Issue: Server not starting

**Symptoms:** MCP server doesn't start or crashes immediately

**Cause:** Port 9000 already in use or Weaviate state not initialized

**Solution:**
1. Check if port 9000 is available: `lsof -i :9000`
2. Ensure Weaviate server is fully started before MCP server
3. Check server logs for detailed error messages

---

### Issue: Changes not reflected after rebuild

**Symptoms:** Code changes don't appear when testing

**Cause:** Using cached binary or server not restarted

**Solution:**
1. Fully restart the dev server: `./tools/dev/run_dev_server.sh local-mcp`
2. Clear Go build cache if needed: `go clean -cache`
3. Restart Claude Desktop to refresh tool definitions

---

### Issue: Tool response validation error - expects dictionary but got array

**Symptoms:** Tool returns results but MCP client shows validation error: "expects structuredContent to be a dictionary (a single object)" when returning an array

**Cause:** MCP response validation requires tool responses to be structured objects (dictionaries), not raw arrays

**Solution:** Always wrap arrays in a response struct with a named field:

```go
// BAD - Returns raw array
func (s *WeaviateSearcher) BadTool(...) ([]any, error) {
    return results, nil  // This will fail validation!
}

// GOOD - Returns structured object with array field
type GoodToolResp struct {
    Results []any `json:"results" jsonschema_description:"The search results"`
}

func (s *WeaviateSearcher) GoodTool(...) (*GoodToolResp, error) {
    return &GoodToolResp{Results: results}, nil  // This passes validation!
}
```

**Example:** The `weaviate-query-hybrid` tool wraps its results array in a `QueryHybridResp` struct with a `Results` field.

## References

- [MCP Protocol Specification](https://spec.modelcontextprotocol.io/)
- [mcp-go Documentation](https://mcp-go.dev/)
- [mcp-go GitHub](https://github.com/mark3labs/mcp-go)
- [invopop/jsonschema](https://github.com/invopop/jsonschema) - Used for schema generation

## Future Improvements

Potential enhancements for the MCP server:

- [ ] Add comprehensive unit tests for all tools
- [ ] Support for batch operations (insert multiple objects)
- [ ] Vector search tools (near-vector, near-object)
- [ ] Object update and delete operations
- [ ] Collection/class management (create, update, delete classes)
- [ ] Advanced filtering and sorting options
- [ ] Aggregation queries
- [ ] Backup and restore operations
- [ ] Configuration options (configurable port, default collection)
- [ ] Metrics and monitoring integration
- [ ] Rate limiting for MCP requests
- [ ] Streaming responses for large result sets
