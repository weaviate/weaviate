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
	"time"

	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/entities/models"
)

// finalizeWindowMin / finalizeWindowMax bound the FINISHED-but-flag-off
// → indexing@100% override in [MergeReindexStatus]. The window is
// normally computed as 2× the DTM scheduler tick, but is clamped at
// both ends:
//
//   - 3s lower bound covers pathological sub-second tick configs.
//   - 10s upper bound caps how long a stale FINISHED task can bleed
//     an "indexing(1)" pill after a DELETE — production tick is 60s,
//     so a naive 2× would let the bleed live for 2 minutes, the
//     user-visible face of weaviate/weaviate#10675.
const (
	finalizeWindowMin = 3 * time.Second
	finalizeWindowMax = 10 * time.Second
)

// CollectionIndexStatus is the parsed-and-merged read-side response
// the GET /v1/schema/{class}/indexes handler emits. The Service
// returns a fully-populated value; the handler maps it to the
// generated swagger model.
type CollectionIndexStatus struct {
	Properties []PropertyIndexStatus
}

// PropertyIndexStatus mirrors models.PropertyIndexStatus but stays in
// the usecases layer so the service has no swagger dependency.
type PropertyIndexStatus struct {
	Name        string
	DataType    string
	Description string
	Indexes     []*models.IndexStatus
}

// CollectionStatus returns [ErrNotFound] when the collection does not
// exist.
//
// schedulerTick is the DTM scheduler's configured tick interval; the
// finalize window is computed from it (clamped to [finalizeWindowMin,
// finalizeWindowMax]) so MergeReindexStatus can decide whether a
// FINISHED-but-flag-off task is still in its legitimate swap window.
func (s *Service) CollectionStatus(ctx context.Context, collection string, schedulerTick time.Duration) (CollectionIndexStatus, error) {
	class := s.deps.SchemaManager.ReadOnlyClass(collection)
	if class == nil {
		return CollectionIndexStatus{}, ErrNotFound
	}

	var activeTasks map[string][]*dbreindex.ReindexTaskPayload
	_ = activeTasks // placeholder so the import stays minimal
	var parsedTasks []ParsedReindexTask
	if s.deps.Cluster != nil {
		tasks, err := s.deps.Cluster.ListDistributedTasks(ctx)
		if err == nil {
			parsedTasks = ParseReindexTasks(tasks[dbreindex.ReindexNamespace])
		}
	}

	finalizeWindow := 2 * schedulerTick
	if finalizeWindow < finalizeWindowMin {
		finalizeWindow = finalizeWindowMin
	}
	if finalizeWindow > finalizeWindowMax {
		finalizeWindow = finalizeWindowMax
	}

	// BM25 algorithm currently backing searchable indexes for this
	// class. The schema-level UsingBlockMaxWAND flag flips only after
	// every searchable bucket on every shard has been migrated to
	// blockmax (MapToBlockmaxStrategy.OnMigrationComplete). While a
	// per-property repair-searchable is in flight the flag is still
	// false; the targetAlgorithm field (set by MergeReindexStatus)
	// carries the "incoming" signal in that case.
	searchableAlgorithm := models.IndexStatusAlgorithmWand
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
		searchableAlgorithm = models.IndexStatusAlgorithmBlockmax
	}

	out := make([]PropertyIndexStatus, 0, len(class.Properties))
	for _, prop := range class.Properties {
		pis := PropertyIndexStatus{
			Name:        prop.Name,
			DataType:    dataTypeString(prop),
			Description: prop.Description,
		}

		// One entry per applicable index type. carryTokenization
		// mirrors historical behavior: filterable and searchable
		// expose the property's tokenization on the flag-on entry;
		// rangeable does not. Rangeable only applies to numeric/date.
		isNumeric := IsNumericProperty(prop)
		entries := []struct {
			indexType         string
			flagOn            bool
			applicable        bool
			carryTokenization bool
		}{
			{"filterable", prop.IndexFilterable == nil || *prop.IndexFilterable, true, true},
			{"searchable", prop.IndexSearchable == nil || *prop.IndexSearchable, true, true},
			{"rangeable", prop.IndexRangeFilters != nil && *prop.IndexRangeFilters, isNumeric, false},
		}

		var indexes []*models.IndexStatus
		for _, e := range entries {
			if !e.applicable {
				continue
			}
			idx := &models.IndexStatus{Type: e.indexType, Status: "ready"}
			if e.flagOn && e.carryTokenization {
				idx.Tokenization = prop.Tokenization
			}
			if e.indexType == "searchable" && e.flagOn {
				idx.Algorithm = searchableAlgorithm
			}
			MergeReindexStatus(idx, collection, prop.Name, e.indexType, e.flagOn, parsedTasks, finalizeWindow, s.logger)
			// Flag on → always emit. Flag off → emit only when a
			// reindex task carries actionable signal (in-flight or
			// terminal failure/cancellation).
			if e.flagOn || IsSyntheticStatus(idx.Status) {
				indexes = append(indexes, idx)
			}
		}

		pis.Indexes = indexes
		out = append(out, pis)
	}

	return CollectionIndexStatus{Properties: out}, nil
}

func dataTypeString(prop *models.Property) string {
	if len(prop.DataType) > 0 {
		return prop.DataType[0]
	}
	return ""
}
