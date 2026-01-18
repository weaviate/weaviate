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
- **Enabled:** Controlled by `MCP_SERVER_ENABLED` environment variable (must be set to `true`, disabled by default)
- **Write Access:** Controlled by `MCP_SERVER_WRITE_ACCESS_DISABLED` environment variable (write access disabled by default, set to `false` to enable write tools)
- **Custom Tool Descriptions:** Controlled by `MCP_SERVER_CONFIG_PATH` environment variable (path to YAML configuration file for customizing tool descriptions)
- **Logs Tool Access:** Controlled by `MCP_SERVER_READ_LOGS_ENABLED` environment variable (set to `true` to enable the `weaviate-logs-fetch` tool, disabled by default)

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

The MCP server provides different sets of tools based on environment variables:

**Read-Only Tools (always available):**
- `weaviate-collections-get-config` - Get collection schemas
- `weaviate-tenants-list` - List tenants for a collection
- `weaviate-objects-get` - Retrieve objects by UUID or fetch paginated lists
- `weaviate-query-hybrid` - Search and query data

**Write Tools (only available when `MCP_SERVER_WRITE_ACCESS_DISABLED=false`):**
- `weaviate-collections-create` - Create new collections with schema configuration
- `weaviate-objects-delete` - Delete objects from a collection based on filters (with dry-run safety)
- `weaviate-objects-upsert` - Batch upsert (insert or update) one or more objects

**Diagnostic Tools (only available when `MCP_SERVER_READ_LOGS_ENABLED=true`):**
- `weaviate-logs-fetch` - Fetch recent server logs from memory (limited to 2000 characters)

By default, write access is **disabled** for security, and logs access is **disabled**. Set `MCP_SERVER_WRITE_ACCESS_DISABLED=false` to enable write operations, and set `MCP_SERVER_READ_LOGS_ENABLED=true` to enable log access.

---

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

### 3. `weaviate-objects-get`

Retrieves one or more objects from a collection. Can fetch specific objects by UUID or retrieve a paginated list of all objects in the collection.

**Parameters:**
- `collection_name` (string, **required**): Name of the collection to retrieve objects from
- `tenant_name` (string, optional): Name of the tenant (for multi-tenant collections)
- `uuids` (array of strings, optional): Array of object UUIDs to retrieve. If provided, fetches only these specific objects. If not provided, fetches a paginated list of objects from the collection.
- `offset` (integer, optional): Number of objects to skip (for pagination). Only used when `uuids` are not specified.
- `limit` (integer, optional): Maximum number of objects to return (default: 25). Only used when `uuids` are not specified.
- `include_vector` (boolean, optional): Whether to include the default vector in the response (default: false)
- `return_properties` (array of strings, optional): Specific properties to return. If not specified, all properties are returned.
- `return_metadata` (array of strings, optional): Metadata fields to include in the response. Supported values: `id`, `vector`, `creationTimeUnix`, `lastUpdateTimeUnix`. By default, only `id` is included.

**Returns:**
```json
{
  "objects": [
    {
      "class": "Article",
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "properties": {
        "title": "Example Article",
        "content": "Article content here"
      },
      "creationTimeUnix": 1673539200000,
      "lastUpdateTimeUnix": 1673539200000
    }
  ]
}
```

**Response Fields:**
- `objects` (array): Array of retrieved objects, each containing:
  - `class` (string): Collection name
  - `id` (string): Object UUID
  - `properties` (object): Object properties (filtered by `return_properties` if specified)
  - Additional metadata fields if requested via `return_metadata`

**Authorization:** Requires READ permission on MCP resource.

**Important Notes:**
- When `uuids` are provided, only those specific objects are fetched
- When `uuids` are not provided, a paginated list is returned
- Objects that don't exist are silently skipped (no error)
- Use `return_properties` to reduce payload size when you only need specific fields
- Vectors are excluded by default for performance; set `include_vector=true` to include them

**Examples:**

Fetch specific objects by UUID:
```json
{
  "collection_name": "Article",
  "uuids": [
    "550e8400-e29b-41d4-a716-446655440000",
    "550e8400-e29b-41d4-a716-446655440001"
  ]
}
```

Fetch paginated list of objects:
```json
{
  "collection_name": "Article",
  "limit": 10,
  "offset": 0
}
```

Fetch with specific properties and metadata:
```json
{
  "collection_name": "Article",
  "limit": 5,
  "return_properties": ["title", "author"],
  "return_metadata": ["id", "creationTimeUnix", "lastUpdateTimeUnix"]
}
```

Fetch with vectors included:
```json
{
  "collection_name": "Product",
  "uuids": ["550e8400-e29b-41d4-a716-446655440000"],
  "include_vector": true
}
```

Fetch from multi-tenant collection:
```json
{
  "collection_name": "UserData",
  "tenant_name": "customer-a",
  "limit": 20
}
```

Fetch second page of results:
```json
{
  "collection_name": "Article",
  "limit": 25,
  "offset": 25
}
```

---

### 4. `weaviate-logs-fetch`

Fetches Weaviate server logs from the in-memory buffer with pagination support. This tool is only available when `MCP_SERVER_READ_LOGS_ENABLED=true`.

**Parameters:**
- `limit` (integer, optional): Maximum number of characters to return. Default: 2000, Maximum: 50000
- `offset` (integer, optional): Number of characters to skip from the end before returning logs. Default: 0 (returns most recent logs)

**Returns:**
```json
{
  "logs": "time=\"2025-01-17T10:30:45Z\" level=info msg=\"Server started\"\ntime=\"2025-01-17T10:30:46Z\" level=debug msg=\"Processing request\""
}
```

**Response Fields:**
- `logs` (string): The fetched log content

**Authorization:** Requires READ permission on MCP resource.

**Pagination:**
The tool supports pagination to fetch logs in chunks:
- **Most recent logs:** Use `offset=0` (or omit) and set your desired `limit`
- **Older logs:** Increase `offset` to skip more recent logs. For example:
  - First page: `{"limit": 2000, "offset": 0}` - returns last 2000 characters
  - Second page: `{"limit": 2000, "offset": 2000}` - returns characters from position [end-4000] to [end-2000]
  - Third page: `{"limit": 2000, "offset": 4000}` - returns characters from position [end-6000] to [end-4000]

**Notes:**
- Logs are captured in a circular buffer with a maximum size of 100KB
- The buffer automatically truncates older logs when full
- Logs are formatted using the same formatter configured for the server (text or JSON)
- The tool is disabled by default for security reasons
- When enabled, logs from all levels (debug, info, warn, error) are captured
- If `offset` exceeds the buffer size, an empty string is returned

**Examples:**

Fetch most recent 2000 characters (default):
```json
{}
```

Fetch most recent 5000 characters:
```json
{
  "limit": 5000
}
```

Fetch logs from offset 2000 with limit 3000:
```json
{
  "limit": 3000,
  "offset": 2000
}
```

---

### 5. `weaviate-collections-create`

Creates a new collection (class) in the Weaviate database with the specified schema configuration. This tool is only available when `MCP_SERVER_WRITE_ACCESS_DISABLED=false`.

**Parameters:**
- `collection_name` (string, **required**): Name of the collection to create. Multiple words should be concatenated in CamelCase (e.g., 'ArticleAuthor')
- `description` (string, optional): Description of the collection for metadata purposes
- `properties` (array, optional): Array of property definitions. Each property should have:
  - `name` (string, **required**): Name of the property
  - `dataType` (array of strings, **required**): Data types for the property (e.g., `["text"]`, `["int"]`, `["Article"]` for cross-references)
  - `description` (string, optional): Description of the property
  - `indexFilterable` (boolean, optional): Whether the property should be indexed for filtering (default: true)
  - `indexSearchable` (boolean, optional): Whether the property should be indexed for full-text search (default: true for text)
  - `tokenization` (string, optional): Tokenization method for text properties (e.g., "word", "field")
- `invertedIndexConfig` (object, optional): Configuration for the inverted index:
  - `bm25` (object, optional): BM25 configuration with `b` and `k1` parameters
  - `stopwords` (object, optional): Stopwords configuration with `preset` (e.g., "en") or `additions`/`removals`
  - `indexTimestamps` (boolean, optional): Whether to index timestamps
  - `indexNullState` (boolean, optional): Whether to index null states
  - `indexPropertyLength` (boolean, optional): Whether to index property lengths
- `vectorConfig` (object, optional): Configuration for named vectors. Maps vector name to configuration:
  - `vectorizer` (string): Name of the vectorizer module (e.g., "text2vec-transformers", "none")
  - `vectorIndexType` (string, optional): Type of vector index (e.g., "hnsw", "flat")
  - `vectorIndexConfig` (object, optional): Configuration specific to the vector index type
- `multiTenancyConfig` (object, optional): Multi-tenancy configuration:
  - `enabled` (boolean): Whether to enable multi-tenancy for this collection
  - `autoTenantCreation` (boolean, optional): Whether to automatically create tenants
  - `autoTenantActivation` (boolean, optional): Whether to automatically activate tenants

**Returns:**
```json
{
  "collection_name": "Article"
}
```

**Response Fields:**
- `collection_name` (string): Name of the created collection

**Authorization:** Requires CREATE permission on MCP resource.

**Examples:**

Simple collection with text properties:
```json
{
  "collection_name": "Article",
  "description": "A collection for articles",
  "properties": [
    {
      "name": "title",
      "dataType": ["text"],
      "description": "Title of the article"
    },
    {
      "name": "content",
      "dataType": ["text"],
      "description": "Main content of the article"
    },
    {
      "name": "publishDate",
      "dataType": ["date"],
      "description": "Publication date"
    }
  ]
}
```

Collection with vector configuration:
```json
{
  "collection_name": "Product",
  "description": "Product catalog",
  "properties": [
    {
      "name": "name",
      "dataType": ["text"]
    },
    {
      "name": "description",
      "dataType": ["text"]
    },
    {
      "name": "price",
      "dataType": ["number"]
    }
  ],
  "vectorConfig": {
    "default": {
      "vectorizer": "text2vec-transformers",
      "vectorIndexType": "hnsw"
    }
  }
}
```

Collection with multi-tenancy:
```json
{
  "collection_name": "UserData",
  "properties": [
    {
      "name": "username",
      "dataType": ["text"]
    },
    {
      "name": "email",
      "dataType": ["text"]
    }
  ],
  "multiTenancyConfig": {
    "enabled": true,
    "autoTenantCreation": false
  }
}
```

---

### 6. `weaviate-objects-delete`

Deletes objects from a collection based on an optional filter. This tool is only available when `MCP_SERVER_WRITE_ACCESS_DISABLED=false`.

**SAFETY FEATURE:** By default, this tool runs in dry-run mode (`dry_run=true`), which returns the count of objects that would be deleted without actually deleting them. To perform actual deletion, you must explicitly set `dry_run=false`.

**Parameters:**
- `collection_name` (string, **required**): Name of the collection to delete objects from
- `tenant_name` (string, optional): Name of the tenant (for multi-tenant collections). Only objects from this tenant will be deleted.
- `where` (object, optional): Filter to specify which objects to delete using Weaviate's where filter syntax. If not provided, deletes ALL objects in the collection. See examples below for filter syntax.
- `dry_run` (boolean, optional): If `true`, returns count without deleting. If `false`, performs actual deletion. **Default: `true`**

**Returns:**
```json
{
  "deleted": 0,
  "matches": 42,
  "dry_run": true
}
```

**Response Fields:**
- `deleted` (integer): Number of objects actually deleted (0 if `dry_run=true`)
- `matches` (integer): Number of objects that matched the deletion criteria
- `dry_run` (boolean): Whether this was a dry run (`true`) or actual deletion (`false`)

**Authorization:** Requires DELETE permission on MCP resource.

**Important Notes:**
- **Default behavior is safe:** Without specifying `dry_run=false`, no data will be deleted
- **Without a filter:** Automatically creates a filter that matches ALL objects in the collection (or tenant) using `Like "*"` on the `_id` field
- **With a filter:** Only deletes objects matching the filter criteria
- Cannot be undone once executed with `dry_run=false`
- For multi-tenant collections, specify `tenant_name` to delete only that tenant's objects
- The underlying Weaviate API requires a where filter, so when you don't provide one, it's automatically created to match all objects

**Where Filter Syntax:**
The `where` parameter uses Weaviate's filter syntax with the following structure:

**Basic Filter Fields:**
- `path`: Array of property names (always an array, even for single property: `["propertyName"]`)
- `operator`: One of: `"Equal"`, `"NotEqual"`, `"GreaterThan"`, `"GreaterThanEqual"`, `"LessThan"`, `"LessThanEqual"`, `"Like"`, `"ContainsAny"`, `"ContainsAll"`, `"IsNull"`, `"And"`, `"Or"`
- **Value fields** (use ONE based on data type):
  - `valueText`: String value (e.g., `"draft"`)
  - `valueInt`: Integer value (e.g., `100`)
  - `valueNumber`: Float value (e.g., `3.14`)
  - `valueBoolean`: Boolean value (e.g., `true`)
  - `valueDate`: Date string in ISO format (e.g., `"2020-01-01T00:00:00Z"`)
  - **Array variants** for `ContainsAny`/`ContainsAll`: `valueTextArray`, `valueIntArray`, `valueNumberArray`, `valueBooleanArray`, `valueDateArray`

**Complex Filters:**
- `operands`: Array of sub-filters (required when operator is `"And"` or `"Or"`)
- Do NOT include `path` when using `And`/`Or` operators

**Examples:**

Dry run to check how many objects would be deleted (all objects):
```json
{
  "collection_name": "Article"
}
```

Delete objects matching a specific property value:
```json
{
  "collection_name": "Article",
  "where": {
    "path": ["status"],
    "operator": "Equal",
    "valueText": "draft"
  },
  "dry_run": false
}
```

Delete objects with a numeric filter:
```json
{
  "collection_name": "Product",
  "where": {
    "path": ["price"],
    "operator": "LessThan",
    "valueNumber": 10
  },
  "dry_run": false
}
```

Delete with complex AND filter:
```json
{
  "collection_name": "Article",
  "where": {
    "operator": "And",
    "operands": [
      {
        "path": ["status"],
        "operator": "Equal",
        "valueText": "archived"
      },
      {
        "path": ["publishDate"],
        "operator": "LessThan",
        "valueDate": "2020-01-01T00:00:00Z"
      }
    ]
  },
  "dry_run": false
}
```

Delete all objects (no filter):
```json
{
  "collection_name": "Article",
  "dry_run": false
}
```

Delete objects for a specific tenant:
```json
{
  "collection_name": "UserData",
  "tenant_name": "customer-a",
  "where": {
    "path": ["status"],
    "operator": "Equal",
    "valueText": "inactive"
  },
  "dry_run": false
}
```

**Common Where Filter Mistakes:**

1. **Mistake:** Using `path` as a string instead of an array
   ```json
   // WRONG
   {"path": "status", "operator": "Equal", "valueText": "draft"}

   // CORRECT
   {"path": ["status"], "operator": "Equal", "valueText": "draft"}
   ```

2. **Mistake:** Using generic `value` field instead of typed value field
   ```json
   // WRONG
   {"path": ["price"], "operator": "GreaterThan", "value": 100}

   // CORRECT
   {"path": ["price"], "operator": "GreaterThan", "valueNumber": 100}
   ```

3. **Mistake:** Including `path` field in And/Or operators
   ```json
   // WRONG
   {
     "path": ["status"],
     "operator": "And",
     "operands": [...]
   }

   // CORRECT
   {
     "operator": "And",
     "operands": [
       {"path": ["status"], "operator": "Equal", "valueText": "draft"},
       {"path": ["score"], "operator": "GreaterThan", "valueNumber": 50}
     ]
   }
   ```

4. **Mistake:** Using wrong value type for the data type
   ```json
   // WRONG - using valueText for a number property
   {"path": ["age"], "operator": "GreaterThan", "valueText": "18"}

   // CORRECT - using valueNumber for a number property
   {"path": ["age"], "operator": "GreaterThan", "valueNumber": 18}
   ```

**Value Field Reference:**
- Text properties → `valueText` (string)
- Number properties (int/float) → `valueNumber` (number) or `valueInt` (integer)
- Boolean properties → `valueBoolean` (true/false)
- Date properties → `valueDate` (ISO 8601 string: "2020-01-01T00:00:00Z")
- Array operators (ContainsAny/ContainsAll) → `valueTextArray`, `valueIntArray`, etc.

---

### 7. `weaviate-objects-upsert`

Upserts (inserts or updates) one or more objects into a collection in batch. Supports efficient bulk operations for inserting or updating multiple objects at once.

**Parameters:**
- `collection_name` (string, **required**): Name of the collection to upsert objects into
- `tenant_name` (string, optional): Name of the tenant the objects belong to (for multi-tenant collections)
- `objects` (array, **required**): Array of objects to upsert. Minimum 1 object required. Each object should have:
  - `properties` (object, **required**): Key-value pairs of object properties
  - `uuid` (string, optional): UUID of the object. If not provided, a new UUID will be generated. If provided and the object exists, it will be updated; otherwise, a new object with this UUID will be created
  - `vectors` (object, optional): Named vectors for the object (e.g., `{"default": [0.1, 0.2, ...], "image": [0.3, 0.4, ...]}`)

**Returns:**
```json
{
  "results": [
    {
      "id": "uuid-of-first-object"
    },
    {
      "id": "uuid-of-second-object"
    },
    {
      "error": "error message for failed object"
    }
  ]
}
```

**Response Fields:**
- `results` (array): Results for each object in the batch, in the same order as the input
  - `id` (string): UUID of the upserted object (only present if successful)
  - `error` (string): Error message if the upsert failed for this object (only present if failed)

**Authorization:** Requires both CREATE and UPDATE permissions on MCP resource.

**Important Notes:**
- Objects are processed in batch for better performance
- Each object in the batch can succeed or fail independently
- Results are returned in the same order as the input objects
- Check the `error` field in each result to identify failures
- If a UUID is provided and exists, the object is updated; otherwise, it's inserted

**Examples:**

Upsert a single object:
```json
{
  "collection_name": "Article",
  "objects": [
    {
      "properties": {
        "title": "Example Article",
        "content": "Article content here",
        "author": "John Doe"
      }
    }
  ]
}
```

Upsert multiple objects in batch:
```json
{
  "collection_name": "Article",
  "objects": [
    {
      "properties": {
        "title": "First Article",
        "content": "Content of first article"
      }
    },
    {
      "properties": {
        "title": "Second Article",
        "content": "Content of second article"
      }
    },
    {
      "properties": {
        "title": "Third Article",
        "content": "Content of third article"
      }
    }
  ]
}
```

Upsert with specific UUIDs (update if exists, insert if not):
```json
{
  "collection_name": "Article",
  "objects": [
    {
      "uuid": "550e8400-e29b-41d4-a716-446655440000",
      "properties": {
        "title": "Updated Article",
        "content": "This will update if UUID exists"
      }
    },
    {
      "uuid": "550e8400-e29b-41d4-a716-446655440001",
      "properties": {
        "title": "Another Article",
        "content": "This will also update if UUID exists"
      }
    }
  ]
}
```

Upsert with custom vectors:
```json
{
  "collection_name": "Product",
  "objects": [
    {
      "properties": {
        "name": "Laptop",
        "price": 1299.99
      },
      "vectors": {
        "default": [0.1, 0.2, 0.3, 0.4, 0.5]
      }
    }
  ]
}
```

Upsert for multi-tenant collection:
```json
{
  "collection_name": "UserData",
  "tenant_name": "customer-a",
  "objects": [
    {
      "properties": {
        "username": "john_doe",
        "email": "john@example.com"
      }
    },
    {
      "properties": {
        "username": "jane_smith",
        "email": "jane@example.com"
      }
    }
  ]
}
```

---

### 8. `weaviate-query-hybrid`

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

The MCP server is started as part of the main Weaviate server and is **disabled by default**. You must set the `MCP_SERVER_ENABLED` environment variable to `true` to enable it.

To run it in development mode:

```bash
# Run with MCP configuration (includes RBAC auth and MCP server enabled)
./tools/dev/run_dev_server.sh local-mcp
```

This configuration:
- Enables API key authentication
- Enables RBAC authorization
- Sets up an admin user with root permissions
- Uses API key: `admin-key` for user: `admin`
- Starts Weaviate on port 8080
- **Enables and starts MCP server on port 9000** (via `MCP_SERVER_ENABLED=true`)
- **Disables write access to MCP tools** (via `MCP_SERVER_WRITE_ACCESS_DISABLED=true` - read-only mode)
- **Enables logs tool** (via `MCP_SERVER_READ_LOGS_ENABLED=true`)
- **Loads custom tool descriptions** from `tools/dev/mcp-config.yaml` (via `MCP_SERVER_CONFIG_PATH`)

**To run MCP server with a custom configuration:**
```bash
# Enable MCP server
export MCP_SERVER_ENABLED=true

# Enable write access (optional - disabled by default for security)
export MCP_SERVER_WRITE_ACCESS_DISABLED=false

# Enable logs tool (optional - disabled by default)
export MCP_SERVER_READ_LOGS_ENABLED=true

# Optional: Load custom tool descriptions
export MCP_SERVER_CONFIG_PATH=/path/to/your/mcp-config.yaml

./tools/dev/run_dev_server.sh <your-config>
```

**Notes:**
- Without `MCP_SERVER_ENABLED=true`, the MCP server will not start and you'll see a log message: "MCP server is disabled (set MCP_SERVER_ENABLED=true to enable)"
- By default, `MCP_SERVER_WRITE_ACCESS_DISABLED=true` (write access disabled). Only read-only tools will be available unless you explicitly set it to `false`
- By default, `MCP_SERVER_READ_LOGS_ENABLED=false` (logs tool disabled). The `weaviate-logs-fetch` tool will only be available when explicitly set to `true`
- For production environments, it's recommended to keep write access and logs access disabled unless specifically needed
- `MCP_SERVER_CONFIG_PATH` is optional - if not set, tools will use their default descriptions

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

## Customizing Tool Descriptions

You can customize the descriptions of MCP tools that are shown to LLM clients by providing a YAML configuration file.

### Configuration File Format

Create a YAML file with the following structure:

```yaml
# MCP Server Tool Configuration
tools:
  weaviate-collections-get-config:
    description: "Your custom description here"

  weaviate-query-hybrid:
    description: "Another custom description"
```

### Using Custom Descriptions

1. **Create your configuration file:** See `tools/dev/mcp-config.yaml` for an example

2. **Set the environment variable:**
   ```bash
   export MCP_SERVER_CONFIG_PATH=/path/to/your/mcp-config.yaml
   ```

3. **Start the server:**
   ```bash
   ./tools/dev/run_dev_server.sh local-mcp
   ```

The `local-mcp` configuration already includes a sample config file at `tools/dev/mcp-config.yaml`.

### Example Configuration

The example configuration file (`tools/dev/mcp-config.yaml`) includes custom descriptions for all available tools:

```yaml
tools:
  weaviate-collections-get-config:
    description: "Retrieves the schema configuration for one or all collections in the Weaviate database. Use this to understand the structure, properties, and settings of your data collections before querying or inserting data."

  weaviate-tenants-list:
    description: "Lists all tenants for a specific multi-tenant collection. Use this to discover which tenants exist in a collection before performing tenant-scoped operations."

  weaviate-query-hybrid:
    description: "Performs a hybrid search combining vector similarity and keyword matching (BM25) on a collection. This is the primary search tool - use it to find relevant objects based on natural language queries. The alpha parameter controls the balance between semantic (vector) and keyword search."

  weaviate-objects-upsert:
    description: "Creates a new object or updates an existing one in a collection. If a UUID is provided and exists, the object is updated; otherwise, a new object is created. Use this to add or modify data in Weaviate collections."
```

### Why Customize Descriptions?

- **Improve LLM Understanding:** Provide more context about when and how to use each tool
- **Add Domain-Specific Guidance:** Tailor descriptions to your specific use case
- **Include Usage Examples:** Help the LLM understand the tool's purpose better
- **Highlight Important Parameters:** Draw attention to key configuration options

### Notes

- Only tools listed in the configuration file will have their descriptions overridden
- Tools not listed will use their default descriptions
- If the configuration file is invalid or cannot be read, the server will silently fall back to default descriptions
- Changes to the configuration file require restarting the server

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
- `weaviate-collections-create`: CREATE
- `weaviate-tenants-list`: READ
- `weaviate-logs-fetch`: READ
- `weaviate-objects-delete`: DELETE
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
