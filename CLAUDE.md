# Weaviate Codebase Documentation

This document provides comprehensive information about the Weaviate codebase architecture, specifically focusing on geo coordinate filtering, indexing, and deletion mechanisms.

## Table of Contents

- [Geo Coordinate Filtering Architecture](#geo-coordinate-filtering-architecture)
- [Geo Index Implementation](#geo-index-implementation)
- [Object Deletion Flow](#object-deletion-flow)
- [Bug Fix: Geo Index Cleanup on Deletion](#bug-fix-geo-index-cleanup-on-deletion)
- [Key Files and Components](#key-files-and-components)
- [Testing and Verification](#testing-and-verification)

---

## Geo Coordinate Filtering Architecture

### Overview

Weaviate implements geo coordinate filtering using an HNSW (Hierarchical Navigable Small World) vector index as the underlying spatial index structure. Geo coordinates are treated as 2D vectors where latitude becomes element [0] and longitude becomes element [1].

### Core Components

#### 1. Geo Index Wrapper
**File:** `adapters/repos/db/vector/geo/geo.go`

The main geo index implementation that wraps HNSW for spatial queries.

Key functions:
- `NewIndex()` - Creates a new geo index with HNSW configuration
- `Add(ctx, docID, coords)` - Adds a geo coordinate to the index
- `Delete(docID)` - Removes a docID from the geo index
- `WithinRange(ctx, geoRange)` - Performs spatial range queries

```go
// WithinRange returns all docIDs within a specified distance from a point
func (i *Index) WithinRange(ctx context.Context, geoRange filters.GeoRange) ([]uint64, error) {
    query := geoCoordinatesToVector(geoRange.GeoCoordinates)
    return i.vectorIndex.KnnSearchByVectorMaxDist(ctx, query, geoRange.Distance, i.config.hnswEF(), nil)
}
```

#### 2. Coordinate Conversion
**File:** `adapters/repos/db/vector/geo/coordinates_for_id.go`

Converts latitude/longitude coordinates to 2D vectors for HNSW indexing.

```go
func geoCoordinatesToVector(coords *models.GeoCoordinates) []float32 {
    return []float32{
        float32(coords.Latitude),
        float32(coords.Longitude),
    }
}
```

#### 3. Distance Calculation
**File:** `adapters/repos/db/vector/hnsw/distancer/geo_spatial.go`

Implements the Haversine formula for accurate great-circle distances on Earth.

- Uses Earth's radius constant: `6371e3` meters
- Calculates spherical distances between lat/lon pairs
- Returns distance in meters

#### 4. Property-Specific Index Management
**File:** `adapters/repos/db/propertyspecific/index.go`

Manages all per-property indices. Currently only geo coordinates are supported as property-specific indices.

```go
type Index struct {
    Type     string          // e.g., "geoCoordinates"
    GeoIndex *geo.Index      // The actual geo index
    Name     string          // Property name
}
```

---

## Geo Index Implementation

### Index Initialization

When a class property of type `geoCoordinates` is added, Weaviate creates a separate HNSW index for that property.

**File:** `adapters/repos/db/shard_geo_props.go:28-66`

```go
func (s *Shard) initGeoProp(prop *models.Property) error {
    // Start cleanup cycles
    s.index.cycleCallbacks.geoPropsCommitLoggerCycle.Start()
    s.index.cycleCallbacks.geoPropsTombstoneCleanupCycle.Start()

    idx, err := geo.NewIndex(geo.Config{
        ID:                 geoPropID(prop.Name),  // e.g., "geo.coordinates"
        RootPath:           s.path(),
        CoordinatesForID:   s.makeCoordinatesForID(prop.Name),
        HNSWEF:             s.index.Config.HNSWGeoIndexEF,
        // ... other HNSW configuration
    }, callbacks...)

    s.propertyIndices[prop.Name] = propertyspecific.Index{
        Type:     schema.DataTypeGeoCoordinates,
        GeoIndex: idx,
        Name:     prop.Name,
    }

    return nil
}
```

### Configuration Parameters

- **HNSW EF (Exploration Factor)**: Configurable via `HNSWGeoIndexEF`
- **Max Connections**: 64 (default HNSW setting)
- **EF Construction**: 128 (default HNSW setting)
- **Snapshots**: Configurable for persistence
- **Commit Log**: Maintained for crash recovery

### Index Updates

**File:** `adapters/repos/db/shard_geo_props.go:100-151`

When objects are inserted or updated:

```go
func (s *Shard) updatePropertySpecificIndices(ctx context.Context, object *storobj.Object, status objectInsertStatus) error {
    s.propertyIndicesLock.RLock()
    defer s.propertyIndicesLock.RUnlock()

    for propName, propIndex := range s.propertyIndices {
        if err := s.updatePropertySpecificIndex(ctx, propName, propIndex, object, status); err != nil {
            return errors.Wrapf(err, "property %q", propName)
        }
    }
    return nil
}
```

**Important:** Updates that change geo properties require a new docID because HNSW tombstones would otherwise block re-insertion of the same docID.

---

## Object Deletion Flow

### Standard Deletion Path

**File:** `adapters/repos/db/shard_write_delete.go:26-112`

When `DeleteObject()` is called:

1. **Retrieve Object**: Get existing object from LSM bucket
2. **Extract docID**: Get the internal document ID
3. **Delete from Bucket**: Remove from primary storage
4. **Update Hash Tree**: Update replication hash tree
5. **Cleanup Inverted Indices**: Remove from inverted indices
6. **Cleanup Property Indices**: Remove from property-specific indices (GEO FIX)
7. **Flush WALs**: Write-ahead logs
8. **Delete from Vector Indices**: Remove from all vector queues
9. **Flush Vector WALs**: Flush vector index WALs

### Batch Deletion Path

**File:** `adapters/repos/db/shard_read.go:727-795`

The `batchDeleteObject()` function follows a similar pattern but optimized for batch operations. It also now includes property index cleanup.

---

## Bug Fix: Geo Index Cleanup on Deletion

### The Problem

**Issue #8747**: Geo filter works only on a fresh collection, fails after reuse until ~5 minutes later

#### Root Cause

When objects were deleted, the deletion cleanup process was **missing cleanup for property-specific indices** (geo indexes). This caused:

1. **Orphaned docIDs** in the geo HNSW index
2. **Stale search results** when geo filters were executed
3. **Delayed cleanup** via the tombstone cleanup cycle (~5 minutes)

The deletion functions called `cleanupInvertedIndexOnDelete()` but this only handled inverted indices, not property-specific indices like geo.

### The Solution

Added a new cleanup function that handles property-specific indices during deletion.

#### New Function: cleanupPropertyIndicesOnDelete

**File:** `adapters/repos/db/shard_geo_props.go:212-257`

```go
func (s *Shard) cleanupPropertyIndicesOnDelete(previous []byte, docID uint64) error {
    previousObject, err := storobj.FromBinary(previous)
    if err != nil {
        return fmt.Errorf("unmarshal previous object: %w", err)
    }

    if previousObject.Properties() == nil {
        return nil
    }

    s.propertyIndicesLock.RLock()
    defer s.propertyIndicesLock.RUnlock()

    for propName, propIndex := range s.propertyIndices {
        if err := s.cleanupPropertyIndexOnDelete(propName, propIndex, previousObject, docID); err != nil {
            return errors.Wrapf(err, "cleanup property index %q", propName)
        }
    }

    return nil
}

func (s *Shard) cleanupPropertyIndexOnDelete(propName string, index propertyspecific.Index,
    obj *storobj.Object, docID uint64,
) error {
    if index.Type != schema.DataTypeGeoCoordinates {
        return fmt.Errorf("unsupported per-property index type %q", index.Type)
    }

    // Check if the object has this geo property
    asMap, ok := obj.Properties().(map[string]interface{})
    if !ok {
        return nil
    }

    _, hasProp := asMap[propName]
    if !hasProp {
        // Object doesn't have this property, nothing to clean up
        return nil
    }

    // Delete from geo index
    return s.deleteFromGeoIndex(index, docID)
}
```

#### Integration into Deletion Flow

**File:** `adapters/repos/db/shard_write_delete.go:82-90`

```go
err = s.cleanupInvertedIndexOnDelete(existing, docID)
if err != nil {
    return fmt.Errorf("delete object from bucket: %w", err)
}

// Clean up property-specific indices (e.g., geo indexes)
err = s.cleanupPropertyIndicesOnDelete(existing, docID)
if err != nil {
    return fmt.Errorf("cleanup property indices: %w", err)
}
```

**File:** `adapters/repos/db/shard_read.go:779-788`

```go
err = s.cleanupInvertedIndexOnDelete(existing, docID)
if err != nil {
    return errors.Wrap(err, "delete object from bucket")
}

// Clean up property-specific indices (e.g., geo indexes)
err = s.cleanupPropertyIndicesOnDelete(existing, docID)
if err != nil {
    return errors.Wrap(err, "cleanup property indices")
}
```

### Why This Works

1. **Immediate Cleanup**: Geo index entries are now removed immediately when objects are deleted, not waiting for the tombstone cleanup cycle
2. **Consistent State**: Geo queries no longer return orphaned docIDs
3. **Extensible**: The solution is designed to support future property-specific indices beyond geo coordinates
4. **Backward Compatible**: Only cleans up properties that exist on the object

---

## Key Files and Components

### Geo Filtering Core

| File | Purpose |
|------|---------|
| `adapters/repos/db/vector/geo/geo.go` | Main geo index implementation (HNSW wrapper) |
| `adapters/repos/db/vector/geo/coordinates_for_id.go` | Coordinate-to-vector conversion |
| `adapters/repos/db/vector/hnsw/distancer/geo_spatial.go` | Haversine distance calculation |
| `adapters/repos/db/shard_geo_props.go` | Geo index lifecycle management |
| `adapters/repos/db/propertyspecific/index.go` | Property-specific index container |

### Deletion and Cleanup

| File | Purpose |
|------|---------|
| `adapters/repos/db/shard_write_delete.go` | Single object deletion handler |
| `adapters/repos/db/shard_write_batch_delete.go` | Batch deletion orchestrator |
| `adapters/repos/db/shard_read.go` | Contains batchDeleteObject implementation |

### Filter Implementation

| File | Purpose |
|------|---------|
| `entities/models/where_filter_geo_range.go` | API filter structure |
| `entities/filters/filters.go` | Filter processing logic |
| `adapters/repos/db/inverted/searcher.go` | Inverted index searcher with geo support |

### Schema and Configuration

| File | Purpose |
|------|---------|
| `entities/schema/data_types.go` | Data type definitions including geoCoordinates |
| `usecases/config/config_handler.go` | Configuration handling |

---

## Testing and Verification

### Critical Reproduction Requirements

**The bug ONLY manifests with these conditions:**

1. ✅ **Persistence enabled**: `PERSISTENCE_DATA_PATH=/var/lib/weaviate`
2. ✅ **Volume mounted**: Data persists across operations
3. ✅ **Collection reused**: Don't recreate the collection, reuse it
4. ✅ **Time for disk flush**: Wait a few seconds between operations for WAL/snapshot flush

**The bug DOES NOT reproduce if:**
- ❌ Testing against fresh in-memory Weaviate
- ❌ Recreating the collection between tests
- ❌ Testing immediately without persistence flush

This explains why simple tests pass - they use in-memory indices which don't exhibit the bug.

### Test Scenarios

1. **Fresh Collection Test**
   - Create collection
   - Insert objects with geo coordinates
   - Query with geo filter → Should return all matching objects
   - Delete objects
   - Verify cleanup

2. **Reuse Collection Test WITH PERSISTENCE** (The Bug Scenario)
   - Start Weaviate with persistence enabled
   - Create collection
   - Insert objects → Query (success)
   - Delete all objects
   - **Wait for disk flush** (3-5 seconds)
   - Insert new objects → Query (**fails on unfixed, succeeds on fixed**)

3. **Update Test**
   - Insert object with geo coordinates
   - Update geo coordinates
   - Query should return object at new location
   - Old location should return nothing

### Verification Commands

```python
# Create and test
python test_distance_filter2.py create
python test_distance_filter2.py test_reuse  # Should work

# Reuse test (previously failed, now should work)
python test_distance_filter2.py test_reuse  # Should work immediately
```

### Expected Behavior After Fix

- ✅ Geo queries return correct results immediately after deletion and reinsertion
- ✅ No orphaned docIDs in geo index
- ✅ No need to wait for tombstone cleanup cycle
- ✅ Consistent behavior whether collection is fresh or reused

---

## Architecture Insights

### DocID Management

Weaviate uses internal numeric docIDs (uint64) that are distinct from UUIDs:
- **UUIDs**: External object identifiers
- **docIDs**: Internal sequential identifiers for index storage
- **Immutability**: Once assigned, docIDs should not be reused (tombstone pattern)

### HNSW Peculiarity with Geo

The geo index uses HNSW, which has a tombstone-based deletion:
- Delete initially adds a tombstone
- Tombstone blocks re-insertion with the same docID
- This is why geo property updates require a new docID (see `shard_write_put.go:389-391`)

### Cleanup Cycles

Two background cycles for geo indices:
- **geoPropsCommitLoggerCycle**: Flushes commit logs periodically
- **geoPropsTombstoneCleanupCycle**: Cleans up HNSW tombstones (~5 minutes)

These cycles are necessary for HNSW maintenance but are NOT a substitute for proper deletion cleanup.

---

## Related Issues and Commits

### Issues
- **#8747**: Geo filter works only on a fresh collection (THIS BUG)
- **#8797**: Async replicator fails to merge GeoCoordinates properties (CLOSED)
- **#2195**: Bug: PATCH Merge does not update geoCoordinates index (CLOSED)
- **#1730**: Potential Bug around geo coordinate properties clean up (CLOSED)

### Key Commits
- **0ff60a0079**: fix(geo): merge object with geo properties
- **e4ee7a89f1**: feat(geo): add possibility to configure Geo HNSW Index settings
- **c55cd3263d**: hnsw snapshots: propagate config to dynamic index and geo props

---

## Future Considerations

### Extensibility

The fix is designed to support future property-specific indices:
- Currently only geo coordinates use property-specific indices
- The infrastructure now properly handles cleanup for any property-specific index type
- To add new index types, implement cleanup in `cleanupPropertyIndexOnDelete()`

### Performance

The cleanup adds minimal overhead:
- Only iterates through property indices that exist on the object
- Lock is read-only (RLock) since we're not modifying the index registry
- Deletion from geo index is a single HNSW tombstone operation

### Monitoring

Consider adding metrics for:
- Property index cleanup duration
- Number of geo index entries cleaned up
- Failures in property index cleanup

---

## Summary

The geo index deletion bug was caused by missing cleanup in the object deletion flow. The fix adds proper property-specific index cleanup that mirrors the existing pattern for inverted indices and vector indices. This ensures geo indexes remain consistent and accurate regardless of whether collections are freshly created or reused after deletions.

**Key Takeaway**: When adding new deletion paths or index types, always ensure all index structures are properly cleaned up, not just the primary storage and inverted indices.
