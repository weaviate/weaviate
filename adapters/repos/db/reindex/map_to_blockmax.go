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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/schema"
)

// NewRuntimeMapToBlockmaxTask creates a ShardReindexTaskGeneric configured for
// runtime (live) Map→Blockmax migration of the searchable properties listed in
// propNames. The migration is performed via atomic bucket pointer swaps
// without shard restart.
//
// propNames must be non-empty. The class-level UsingBlockMaxWAND flag is
// only flipped once every searchable property on every shard has been
// migrated — see MapToBlockmaxStrategy.OnMigrationComplete.
func NewRuntimeMapToBlockmaxTask(
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
	propNames []string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &MapToBlockmaxStrategy{SchemaManager: schemaManager, Generation: generation}

	selectedProps := make(map[string]struct{}, len(propNames))
	for _, p := range propNames {
		selectedProps[p] = struct{}{}
	}

	cfg := ReindexTaskConfig{
		SwapBuckets:                   true,
		TidyBuckets:                   true,
		Concurrency:                   2,
		MemtableOptFactor:             4,
		BackupMemtableOptFactor:       1,
		ProcessingDuration:            10 * time.Minute,
		PauseDuration:                 1 * time.Second,
		CheckProcessingEveryNoObjects: 1000,

		SelectionEnabled: true,
		SelectedPropsByCollection: map[string]map[string]struct{}{
			collectionName: selectedProps,
		},
		SelectedShardsByCollection: map[string]map[string]struct{}{
			collectionName: nil, // nil = all shards
		},
	}

	return NewShardReindexTaskGeneric(
		"MapToBlockmax", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}

// NewFileMapToBlockmaxReindexTracker creates a file-based reindex tracker
// for the most recent searchable map-to-blockmax migration. This is used by
// the debug handler to inspect on-disk migration state. Migration dirs carry
// a per-node generation suffix (`_<N>`); the debug handler doesn't know the
// gen, so we pick the highest existing one. Returns a tracker pointing at
// the first generation if no on-disk state exists yet — operators using the
// debug endpoint to inspect a not-yet-started migration see the same
// "not_started" output they did before.
func NewFileMapToBlockmaxReindexTracker(lsmPath string, keyParser indexKeyParser) *FileReindexTracker {
	gen := MaxMigrationGeneration(lsmPath, MigrationDirSearchableMapToBlockmax, "")
	if gen == 0 {
		gen = 1
	}
	return NewFileReindexTracker(lsmPath, MigrationDirSearchableMapToBlockmax+GenSuffix(gen), keyParser)
}
