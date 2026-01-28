# Weaviate Export Feature

## Overview

The export feature allows Weaviate users to export their data to cloud storage (S3, GCS, Azure) in Parquet format. Unlike backups which are designed for disaster recovery, exports are optimized for data portability, analytics, and integration with data processing pipelines.

## Key Features

- **Parquet Format**: Industry-standard columnar format optimized for analytics
- **Cloud Storage**: Direct streaming to S3, GCS, or Azure (reuses backup backend infrastructure)
- **Single-Node Operation**: Each node exports its local shard data independently
- **Streaming**: No temporary files on disk, data streams directly to cloud storage
- **Progress Tracking**: Real-time export status and per-class progress
- **Authorization**: Integrates with Weaviate's authorization system

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                    REST API Layer                            │
│  handlers_export.go: POST /v1/export/{backend}              │
│                      GET  /v1/export/{backend}/{id}         │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                      Scheduler                               │
│  - Request validation                                        │
│  - Authorization checks                                      │
│  - Backend initialization                                    │
│  - Status tracking                                           │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                    Coordinator                               │
│  - Class resolution                                          │
│  - Shard iteration                                           │
│  - Export orchestration                                      │
│  - Metadata generation                                       │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
┌───────▼──────────┐           ┌───────▼──────────┐
│  Parquet Writer  │           │  Backend Store   │
│  - Schema def    │           │  - S3 upload     │
│  - Row batching  │           │  - GCS upload    │
│  - Compression   │           │  - Azure upload  │
└──────────────────┘           └──────────────────┘
```

### Data Flow

1. **Request**: User sends POST to `/v1/export/s3` with export configuration
2. **Authorization**: System checks user has READ permission on selected classes
3. **Backend Init**: Creates S3/GCS/Azure backend and initializes storage path
4. **Class Iteration**: For each selected class:
   - Get all local shards for the class
   - Create Parquet writer streaming to S3
   - Iterate through objects in each shard using cursor
   - Batch write objects to Parquet (10k rows per batch)
   - Close writer and finalize upload
5. **Metadata**: Write `export_metadata.json` with export summary
6. **Status**: Return export status to user

### Parquet Schema

Each class is exported to a separate `.parquet` file with this schema:

| Column        | Type   | Description                               |
|---------------|--------|-------------------------------------------|
| id            | STRING | Object UUID                               |
| class_name    | STRING | Class name                                |
| creation_time | INT64  | Creation timestamp (Unix)                 |
| update_time   | INT64  | Last update timestamp (Unix)              |
| vector        | BINARY | Primary vector (serialized []float32)     |
| named_vectors | BINARY | Named vectors (JSON map[string][]float32) |
| multi_vectors | BINARY | Multi vectors (JSON map[string][][]float32)|
| properties    | BINARY | Object properties (JSON)                  |

## File Structure

```
usecases/export/
├── README.md                 # This file
├── INTEGRATION.md           # Integration guide for wiring up REST handlers
├── status.go                # Status constants
├── types.go                 # Request/response types
├── scheduler.go             # High-level API with authorization
├── coordinator.go           # Export orchestration logic
├── parquet_writer.go        # Parquet file writing
├── backend.go               # S3/GCS/Azure backend wrapper
└── adapters.go              # DB adapter for shard access

entities/export/
└── status.go                # Export status constants

adapters/handlers/rest/
└── handlers_export.go       # REST API handlers (needs OpenAPI generation)
```

## API Reference

### POST /v1/export/{backend}

Start a new export operation.

**Parameters:**
- `backend` (path): Backend name (`s3`, `gcs`, or `azure`)

**Request Body:**
```json
{
  "id": "export-2026-01-28",
  "include": ["Article", "Product"],
  "exclude": [],
  "config": {
    "bucket": "my-export-bucket",
    "path": "exports/"
  }
}
```

**Response (200 OK):**
```json
{
  "id": "export-2026-01-28",
  "backend": "s3",
  "path": "s3://my-export-bucket/exports/export-2026-01-28/",
  "status": "STARTED",
  "startedAt": "2026-01-28T10:00:00Z",
  "classes": ["Article", "Product"],
  "progress": {
    "Article": {
      "status": "STARTED",
      "objectsExported": 0
    },
    "Product": {
      "status": "STARTED",
      "objectsExported": 0
    }
  }
}
```

### GET /v1/export/{backend}/{id}

Get the status of an export operation.

**Parameters:**
- `backend` (path): Backend name
- `id` (path): Export ID

**Response (200 OK):**
```json
{
  "id": "export-2026-01-28",
  "backend": "s3",
  "path": "s3://my-export-bucket/exports/export-2026-01-28/",
  "status": "SUCCESS",
  "startedAt": "2026-01-28T10:00:00Z",
  "classes": ["Article", "Product"],
  "progress": {
    "Article": {
      "status": "SUCCESS",
      "objectsExported": 1000000,
      "fileSizeBytes": 524288000
    },
    "Product": {
      "status": "SUCCESS",
      "objectsExported": 500000,
      "fileSizeBytes": 262144000
    }
  }
}
```

## Export Output

### S3 Structure

```
s3://<bucket>/<path>/<export-id>/
├── Article.parquet
├── Product.parquet
└── export_metadata.json
```

### Metadata File

`export_metadata.json` contains a summary of the export:

```json
{
  "id": "export-2026-01-28",
  "backend": "s3",
  "startedAt": "2026-01-28T10:00:00Z",
  "completedAt": "2026-01-28T10:15:00Z",
  "status": "SUCCESS",
  "classes": ["Article", "Product"],
  "progress": {
    "Article": {
      "status": "SUCCESS",
      "objectsExported": 1000000,
      "fileSizeBytes": 524288000
    },
    "Product": {
      "status": "SUCCESS",
      "objectsExported": 500000,
      "fileSizeBytes": 262144000
    }
  },
  "version": "1.28.0"
}
```

## Performance Characteristics

### Throughput
- Expected: >10k objects/second on standard hardware
- Factors:
  - Object size (properties, vectors)
  - Network bandwidth to S3
  - Parquet compression (Zstd)
  - Batch size (default 10k rows)

### Memory Usage
- Parquet writer buffer: ~10k objects × object size
- Cursor-based iteration: Minimal memory overhead
- No full dataset loading required

### Storage Efficiency
- Parquet columnar format: 2-5x smaller than JSON
- Zstd compression: Additional 2-3x reduction
- Overall: 4-15x smaller than raw JSON export

## Implementation Status

### ✅ Completed

- Core types and status definitions
- Parquet writer with batching and compression
- Export coordinator with shard iteration
- Scheduler with authorization
- S3/GCS/Azure backend integration
- DB adapter for shard access
- Error handling and progress tracking
- Metadata file generation
- Single-node operation (no leader requirement)

### 🚧 Needs Integration

- OpenAPI schema definitions
- Swagger code generation
- REST handler implementation
- Wire-up in configure_api.go

See [INTEGRATION.md](./INTEGRATION.md) for complete integration steps.

### 📋 Future Enhancements

- Import/restore from Parquet
- Incremental exports (delta)
- Parallel class export
- Property-level filtering
- Configurable compression
- Scheduled exports
- Multi-node aggregation (optional coordinator mode)

## Testing

### Unit Tests

```bash
go test ./usecases/export/...
```

### Integration Test Example

```bash
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

# List files
aws s3 ls s3://weaviate-exports/exports/test-export-1/

# Inspect Parquet
parquet-tools schema Article.parquet
parquet-tools head -n 10 Article.parquet
```

## Error Handling

### Common Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `backend not available` | S3/GCS/Azure backend not configured | Configure backend in Weaviate config |
| `authorization failed` | User lacks READ permission | Grant READ permission on classes |
| `class does not exist` | Class name in `include` not found | Verify class names |
| `export already in progress` | Export with same ID already running | Wait for completion or use different ID |
| `write to backend failed` | S3/GCS/Azure write error | Check credentials and bucket permissions |

### Status Values

- `STARTED`: Export initiated, processing starting
- `TRANSFERRING`: Actively writing data to backend
- `SUCCESS`: Export completed successfully
- `FAILED`: Export failed (see `error` field for details)

## Security Considerations

- Authorization uses existing Weaviate RBAC system
- Requires READ permission on exported classes
- Backend credentials use same configuration as backups
- No sensitive data in export metadata
- S3 server-side encryption recommended

## Comparison with Backups

| Feature | Export | Backup |
|---------|--------|--------|
| **Purpose** | Data portability, analytics | Disaster recovery |
| **Format** | Parquet (columnar) | Custom binary format |
| **Node Coordination** | Single-node | Cluster-wide (2PC) |
| **Schema** | Included in Parquet | Separate JSON file |
| **Indexes** | Not included | Included |
| **Restore** | Not yet implemented | Full cluster restore |
| **Use Case** | Analytics, data lakes, ETL | Backup/restore, migration |

## Dependencies

- `github.com/parquet-go/parquet-go v0.27.0` - Parquet file format
- Existing backup backend infrastructure (S3, GCS, Azure)
- Existing authorization system

## Contributing

When contributing to this feature:

1. Ensure all tests pass: `go test ./usecases/export/...`
2. Follow existing code patterns (coordinator, scheduler, backend)
3. Update documentation for API changes
4. Add integration tests for new functionality
5. Consider performance implications (memory, throughput)

## License

Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
