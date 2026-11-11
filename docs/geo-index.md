# Geo index

A `geoCoordinates` property with a filterable index is served by a dedicated
lsmkv bucket, `property.geo_<name>`, using the `geopoints` strategy
(`adapters/repos/db/geopoints`). It answers `withinGeoRange` filters exactly.
The bucket sits under `property.` like the nested buckets, not `property_`, so
its name cannot collide with the bucket of a property called `<name>_geo`.

## Why a bucket

Until v1.40 the geo index was an HNSW graph kept beside the shard
(`geo.<prop>.hnsw.commitlog.d`, plus a `geo.<prop>.queue.d` with async
indexing). A range filter was a max-distance kNN search capped at `ef`
(800 by default, `HNSW_GEO_INDEX_EF`), so any circle holding more than `ef`
objects silently lost results, and clusters of identical coordinates could
disconnect the graph. The graph also needed its own commit log, tombstone
cleanup, queue, backup hooks and debug rebuild.

The bucket is Lucene's model: an immutable BKD tree per segment, built at
flush and rebuilt at compaction. Everything else — write-ahead log, flush,
compaction, backup, crash recovery, lazy loading — is the lsmkv machinery every
property index already uses.

## Layout

A segment's data section is one tree; the index section is empty, like
`roaringsetrange`. See `geopoints/tree.go` for the byte layout:

- a bitmap of doc IDs this segment deletes from older ones,
- the tree nodes: a bounding box each, inner nodes pointing at two children,
  leaves at a contiguous run of at most 512 points,
- the points, stored by leaf as three columns (doc IDs, latitudes, longitudes).

The tree is built by recursive median split on the wider dimension, longitude
scaled by the cosine of the mid latitude. Coordinates stay `float32`, and the
distance is the same haversine the HNSW distancer used, so a filter's boundary
has not moved. Coordinates are not range-checked on write; the bucket stores
and queries the canonical representation of the point (latitude in [-90, 90],
longitude in [-180, 180]), so `(0, 360)` is `(0, 0)`, as the haversine already
made it for the HNSW.

## Semantics

A doc ID is added once in its lifetime and removed at most once: an object
update gets a new doc ID and the old one is removed. That makes the read exact
without ordering: every layer (memtable, flushing memtable, each segment) adds
its matches to one bitmap and its deletions to another, and the second is
subtracted at the end. A removal stands over an add of the same doc ID that
arrives after it, in the memtable and at compaction, so a stale write cannot
bring a retired doc ID back.

Compaction drops points either segment deletes, keeps deletions that may refer
to segments older than the pair, and drops all of them when the pair starts at
the oldest segment (`cleanupTombstones`). The surviving points are held in
memory once while the tree is rebuilt and streamed out; the compaction checks
that estimate against the memory available before reading the segments and is
skipped under pressure, like any other compaction.

A query descends the tree with the circle's bounding boxes — split in two
across the antimeridian, widened to every longitude when the circle reaches a
pole — collects a leaf wholesale when its box lies inside the circle, and
checks the distance of every point otherwise. The boxes carry a few meters of
slack for the float32 roundings of the distance formula, so a point the
formula accepts is never pruned; `TestCircleBoxesCoverBoundary` hammers that.
The traversal is bounded by the node count, so a corrupt tree fails instead of
looping. The memtable is scanned.

## Write path

`Shard.updatePropertySpecificIndices` (`shard_geo_props.go`) writes the
object's coordinates to the bucket of every geo property of the class, after
removing the previous doc ID from each of them. It runs inside `putObjectLSM`
under the object's doc ID lock, so a delete or update of the same object
cannot slip in between the object write and the geo write. Deletes go through
`deleteFromGeoIndexes`, under the same lock. The WAL reuses the roaringset
record format keyed by doc ID, so replay needs no new parser
(`lsmkv/memtable_geo_points.go`).

## Upgrade and downgrade

On the first start after an upgrade, a shard that still holds a geo HNSW
directory rebuilds the property's bucket from the objects bucket, flushes it
to a segment, fsyncs the bucket directory and only then removes the legacy
directory (`migrateLegacyGeoIndex`). A crash mid-way repeats the rebuild. The
rebuild scans the objects bucket with a cursor, so it costs as much as the
shard's live objects rather than its doc ID history, and runs before the
shard serves.

A node downgraded to a version before the bucket existed starts with an empty
geo HNSW for the property. Its geo filters return nothing until
`POST /debug/index/rebuild/vector?collection=C&shard=S&geo=<prop>` is run there.

The same endpoint rebuilds the bucket on the current version; it no longer
needs async indexing. The bucket is unavailable while it runs, so it is for a
node without traffic.
