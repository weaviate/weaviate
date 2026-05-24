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

package reindex

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

// ShardLike is the surface a reindex worker needs from a shard. ShardLike
// (and *LazyLoadShard) satisfy it via Go structural typing. Defined in
// the consumer (reindex) per "accept interfaces, return structs"; this
// keeps db → reindex as the only import direction.
type ShardLike interface {
	Name() string
	ID() string
	// ParentIndex returns the index the shard belongs to as a
	// [reindex.IndexLike] handle. Named differently from
	// *db.Shard.Index() (which returns the concrete *Index) to avoid
	// a return-type collision at the structural-typing boundary.
	ParentIndex() IndexLike
	Store() *lsmkv.Store
	PathLSM() string

	// Inverted-index bucket writers, promoted from unexported in this
	// refactor. Same implementation; exported name lets reindex call
	// from outside the db package.
	AddToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	DeleteFromPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	AddToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error
	AddToPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	DeleteFromPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error
	DeleteInvertedIndexItemWithFrequencyLSM(bucket *lsmkv.Bucket, item inverted.Countable, docID uint64) error
	PairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair

	// Returns a disable closure the caller invokes during teardown to
	// stop the double-write callback firing on subsequent updates.
	RegisterAddToPropertyValueIndex(cb OnAddToPropertyValueIndex) func()
	RegisterDeleteFromPropertyValueIndex(cb OnDeleteFromPropertyValueIndex) func()

	SetFallbackToSearchable(fallback bool)
	SetRangeableLocallyReady(prop string, ready bool)
	MarkSearchableBlockmaxProperties(propNames ...string)
	MakeDefaultBucketOptions(strategy string, customOptions ...lsmkv.BucketOption) []lsmkv.BucketOption

	AnalyzeObject(*storobj.Object) ([]inverted.Property, []inverted.NilProperty, []inverted.NestedProperty, error)
	AnalyzeObjectForMigrationWithOverlay(*storobj.Object, map[string]inverted.PropertyOverlay) ([]inverted.Property, []inverted.NilProperty, error)
	SetTokenizationOverlay(propName, target string)
	ClearTokenizationOverlay(propName string)
	ObjectCountAsync(ctx context.Context) (int64, error)

	// Unwrap returns the concrete shard underlying any lazy wrapper.
	// *Shard returns itself; *LazyLoadShard ensures the underlying
	// shard is loaded then returns it.
	Unwrap(ctx context.Context) (ShardLike, error)
}

// IndexLike is the surface reindex needs from an Index.
//
// Naming note: ConfigSnapshot() returns the subset of IndexConfig the
// reindex worker reads. The producer side (*db.Index) has a `Config`
// *field* of type db.IndexConfig that pre-dates this extraction; a
// method named Config() on *db.Index would collide with the field, so
// the interface uses a different name.
type IndexLike interface {
	ID() string
	ClassName() schema.ClassName
	ConfigSnapshot() IndexConfig
	Logger() logrus.FieldLogger
	GetShard(ctx context.Context, shardName string) (ShardLike, func(), error)
	GetShardOrNil(shardName string) ShardLike
	ForEachShard(fn func(name string, shard ShardLike) error) error
	ForEachLoadedShard(fn func(name string, shard ShardLike) error) error
	RefuseIfReindexInFlight(shardName string) error
	WithDropLock(fn func())
	GetSchema() SchemaGetter
	InvertedIndexConfig() schema.InvertedIndexConfig
}

// IndexConfig is the slice of *db.IndexConfig reindex consumes.
// Returned by IndexLike.ConfigSnapshot().
type IndexConfig struct {
	ClassName                 schema.ClassName
	RootPath                  string
	ReplicationFactor         int64
	MinMMapSize               int64
	MaxReuseWalSize           int64
	MemtablesFlushDirtyAfter  int
	MemtablesInitialSizeMB    int
	MemtablesMaxSizeMB        int
	MemtablesMinActiveSeconds int
	MemtablesMaxActiveSeconds int
}

// SchemaGetter is the subset of schemaUC.SchemaGetter reindex
// consumes. Re-declared here so reindex doesn't need to import
// usecases/schema. Kept minimal — extending it should be driven by
// actual call sites, not speculative methods.
type SchemaGetter interface {
	ReadOnlyClass(className string) *models.Class
}

// DBLike is the surface reindex needs from the top-level *db.DB.
type DBLike interface {
	RootPath() string
	GetIndex(className schema.ClassName) IndexLike
	WaitForStartup(ctx context.Context) error
	WithLoadedIndices(fn func(loadedByID map[string]IndexLike))
	CleanStalePartialReindexState(ctx context.Context, collection, propName, indexType string) error
	ShardReplicaOwnership(ctx context.Context, className string) (map[string][]string, error)
	ShardReplicaOwnershipForMT(ctx context.Context, className string, tenantNames []string) (map[string][]string, error)
}

// Reindex-side callbacks installed on the source shard. Defined as
// proper named types (no `=` alias) so the no-alias rule holds.
type (
	OnAddToPropertyValueIndex      func(shard ShardLike, docID uint64, property *inverted.Property) error
	OnDeleteFromPropertyValueIndex func(shard ShardLike, docID uint64, property *inverted.Property) error
)
