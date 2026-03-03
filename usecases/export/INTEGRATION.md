# Export Feature Integration Guide

This document describes how to complete the integration of the export feature into Weaviate.

## Completed Components

### Core Infrastructure ✓
- `entities/export/status.go` - Status constants (STARTED, TRANSFERRING, SUCCESS, FAILED)
- `usecases/export/types.go` - Request/response types
- `usecases/export/parquet_writer.go` - Parquet file writing with batching
- `usecases/export/coordinator.go` - Export orchestration logic
- `usecases/export/scheduler.go` - High-level API with authorization
- `usecases/export/backend.go` - S3/GCS backend integration
- `usecases/export/adapters.go` - DB adapter for accessing shards
- `go.mod` - Updated with `github.com/parquet-go/parquet-go v0.27.0`
- `vendor/` - Updated with new dependencies

### REST Handlers (Placeholder) ✓
- `adapters/handlers/rest/handlers_export.go` - Handler structure created (needs OpenAPI generation)

## Remaining Integration Steps

### Step 1: Update OpenAPI Schema

Add export endpoint definitions to `openapi-specs/schema.json`:

```json
{
  "/export/{backend}": {
    "post": {
      "tags": ["exports"],
      "summary": "Start a new export",
      "operationId": "exportsCreate",
      "parameters": [
        {
          "name": "backend",
          "in": "path",
          "required": true,
          "type": "string",
          "enum": ["s3", "gcs", "azure"]
        }
      ],
      "requestBody": {
        "required": true,
        "content": {
          "application/json": {
            "schema": {
              "$ref": "#/components/schemas/ExportRequest"
            }
          }
        }
      },
      "responses": {
        "200": {
          "description": "Export started successfully",
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/ExportStatus"
              }
            }
          }
        },
        "403": {
          "description": "Forbidden"
        },
        "500": {
          "description": "Internal server error"
        }
      }
    }
  },
  "/export/{backend}/{id}": {
    "get": {
      "tags": ["exports"],
      "summary": "Get export status",
      "operationId": "exportsStatus",
      "parameters": [
        {
          "name": "backend",
          "in": "path",
          "required": true,
          "type": "string"
        },
        {
          "name": "id",
          "in": "path",
          "required": true,
          "type": "string"
        }
      ],
      "responses": {
        "200": {
          "description": "Export status",
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/ExportStatus"
              }
            }
          }
        },
        "404": {
          "description": "Export not found"
        },
        "403": {
          "description": "Forbidden"
        }
      }
    }
  }
}
```

Add schema definitions:

```json
{
  "components": {
    "schemas": {
      "ExportRequest": {
        "type": "object",
        "required": ["id"],
        "properties": {
          "id": {
            "type": "string",
            "description": "Unique identifier for the export"
          },
          "include": {
            "type": "array",
            "items": {
              "type": "string"
            },
            "description": "Class names to include in export"
          },
          "exclude": {
            "type": "array",
            "items": {
              "type": "string"
            },
            "description": "Class names to exclude from export"
          },
          "config": {
            "type": "object",
            "properties": {
              "bucket": {
                "type": "string"
              },
              "path": {
                "type": "string"
              }
            }
          }
        }
      },
      "ExportStatus": {
        "type": "object",
        "properties": {
          "id": {
            "type": "string"
          },
          "backend": {
            "type": "string"
          },
          "path": {
            "type": "string"
          },
          "status": {
            "type": "string",
            "enum": ["STARTED", "TRANSFERRING", "SUCCESS", "FAILED"]
          },
          "startedAt": {
            "type": "string",
            "format": "date-time"
          },
          "error": {
            "type": "string"
          },
          "classes": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "progress": {
            "type": "object",
            "additionalProperties": {
              "$ref": "#/components/schemas/ClassProgress"
            }
          }
        }
      },
      "ClassProgress": {
        "type": "object",
        "properties": {
          "status": {
            "type": "string"
          },
          "objectsExported": {
            "type": "integer",
            "format": "int64"
          },
          "fileSizeBytes": {
            "type": "integer",
            "format": "int64"
          },
          "error": {
            "type": "string"
          }
        }
      }
    }
  }
}
```

### Step 2: Generate Swagger Code

Run the swagger code generator to create handler interfaces:

```bash
# From the project root
make generate-swagger
# or
./tools/swagger-codegen.sh
```

This will generate the handler interfaces in `adapters/handlers/rest/operations/exports/`.

### Step 3: Implement Handler Methods

Update `adapters/handlers/rest/handlers_export.go` with actual handler implementations:

```go
func (h *exportHandlers) createExport(params exports.ExportsCreateParams,
    principal *models.Principal) middleware.Responder {

    req := &export.ExportRequest{
        ID:      params.Body.ID,
        Backend: params.Backend,
        Include: params.Body.Include,
        Exclude: params.Body.Exclude,
        Config:  params.Body.Config,
    }

    status, err := h.scheduler.Export(params.HTTPRequest.Context(), principal, req)
    if err != nil {
        h.metricRequestsTotal.logError("", err)
        return exports.NewExportsCreateInternalServerError().
            WithPayload(errPayloadFromSingleErr(err))
    }

    h.metricRequestsTotal.logOk("")
    return exports.NewExportsCreateOK().WithPayload(convertToAPIStatus(status))
}

func (h *exportHandlers) exportStatus(params exports.ExportsStatusParams,
    principal *models.Principal) middleware.Responder {

    status, err := h.scheduler.Status(params.HTTPRequest.Context(), principal,
        params.Backend, params.ID)
    if err != nil {
        h.metricRequestsTotal.logError("", err)
        return exports.NewExportsStatusNotFound().
            WithPayload(errPayloadFromSingleErr(err))
    }

    h.metricRequestsTotal.logOk("")
    return exports.NewExportsStatusOK().WithPayload(convertToAPIStatus(status))
}
```

### Step 4: Wire Up in configure_api.go

Add export scheduler initialization and handler setup in `adapters/handlers/rest/configure_api.go`:

```go
// Add after backup scheduler initialization (around line 962)
exportScheduler := startExportScheduler(appState)
setupExportHandlers(api, exportScheduler, appState.Metrics, appState.Logger)

// Add the startExportScheduler function (around line 1100)
func startExportScheduler(appState *state.State) *export.Scheduler {
    return export.NewScheduler(
        appState.Authorizer,
        export.NewDBAdapter(appState.DB),
        appState.Modules, // BackendProvider
        appState.Logger,
    )
}

// Update setupExportHandlers to register handlers
func setupExportHandlers(api *operations.WeaviateAPI,
    scheduler *export.Scheduler,
    metrics *monitoring.PrometheusMetrics,
    logger logrus.FieldLogger) {

    h := &exportHandlers{scheduler, newExportRequestsTotal(metrics, logger), logger}
    api.ExportsExportsCreateHandler = exports.ExportsCreateHandlerFunc(h.createExport)
    api.ExportsExportsStatusHandler = exports.ExportsStatusHandlerFunc(h.exportStatus)
}
```

## Testing

### Unit Tests

Create test files:
- `usecases/export/parquet_writer_test.go`
- `usecases/export/coordinator_test.go`
- `usecases/export/scheduler_test.go`

### Integration Tests

Test the full flow:

```bash
# Start Weaviate with MinIO backend
docker-compose up -d

# Create test data
curl -X POST http://localhost:8080/v1/objects \
  -H "Content-Type: application/json" \
  -d '{
    "class": "Article",
    "properties": {
      "title": "Test Article",
      "content": "Test content"
    },
    "vector": [0.1, 0.2, 0.3]
  }'

# Start export
curl -X POST http://localhost:8080/v1/export/s3 \
  -H "Content-Type: application/json" \
  -d '{
    "id": "test-export-1",
    "include": ["Article"],
    "config": {
      "bucket": "weaviate-exports",
      "path": "exports/"
    }
  }'

# Check status
curl http://localhost:8080/v1/export/s3/test-export-1

# Verify Parquet file in MinIO
aws --endpoint-url=http://localhost:9000 s3 ls s3://weaviate-exports/exports/test-export-1/
```

### Validation

Use parquet-tools to inspect exported files:

```bash
# Download parquet-tools
pip install parquet-tools

# Inspect schema
parquet-tools schema Article.parquet

# View first 10 rows
parquet-tools head -n 10 Article.parquet

# Show metadata
parquet-tools meta Article.parquet
```

## Architecture Notes

### Single-Node Export
- Each node exports only its local shard data
- No leader coordination required
- User calls export on each node independently for distributed exports
- Simpler than backup/restore which requires cluster-wide coordination

### Parquet Format
- One .parquet file per class
- Columns: id, class_name, creation_time, update_time, vector, named_vectors, properties
- Zstd compression for efficient storage
- Batched writes (10k rows) for performance

### S3 Backend
- Reuses existing backup backend infrastructure
- Streams data directly to S3 via io.Pipe
- No temporary files on disk
- Path structure: `s3://<bucket>/<path>/<export-id>/<class>.parquet`

## Future Enhancements

1. **Import/Restore from Parquet**: Reverse operation to import exported data
2. **Incremental Exports**: Only export objects changed since last export
3. **Parallel Class Export**: Export multiple classes concurrently
4. **Export Filtering**: Support property-level filtering (only export certain properties)
5. **Compression Options**: Allow user to choose compression level
6. **Export Scheduling**: Periodic automated exports via cron
7. **Cross-Node Aggregation**: Optional coordinator mode to merge exports from all nodes
