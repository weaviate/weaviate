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
	"strings"
	"time"

	dbreindex "github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// finalizeWindowMin / FinalizeWindowMax bound the FINISHED-but-flag-off
// → indexing@100% override in [MergeReindexStatus]. The window is
// normally computed as 2× the DTM scheduler tick, but is clamped at
// both ends:
//
//   - 3s lower bound covers pathological sub-second tick configs.
//   - 10s upper bound caps how long a stale FINISHED task can bleed
//     an "indexing(1)" pill after a DELETE — production tick is 60s,
//     so a naive 2× would let the bleed live for 2 minutes, the
//     user-visible face of weaviate/weaviate#10675.
//
// The post-DELETE marker TTL in adapters/handlers/rest/state is derived
// from the same ceiling so raising the window can't silently let a
// phantom outlive its marker.
const (
	finalizeWindowMin = 3 * time.Second
	FinalizeWindowMax = 10 * time.Second
)

// CollectionIndexStatus is the parsed-and-merged read-side response
// the GET /v1/schema/{class}/indexes handler emits. The Service
// returns a fully-populated value; the handler maps it to the
// generated swagger model and applies namespace stripping.
type CollectionIndexStatus struct {
	Properties []PropertyIndexStatus
}

// PropertyIndexStatus mirrors models.PropertyIndexStatus but stays in
// the usecases layer so the service has no swagger-operation
// dependency. IndexStatus entries carry the raw (unstripped) TaskID —
// the handler strips the caller's own namespace.
type PropertyIndexStatus struct {
	Name        string
	DataType    string
	Description string
	Indexes     []*models.IndexStatus
}

// CanonicalIndexType maps the internal token to the API spelling used
// in responses: "rangeable" surfaces as "rangeFilters".
func CanonicalIndexType(internalToken string) string {
	if internalToken == "rangeable" {
		return models.IndexStatusTypeRangeFilters
	}
	return internalToken
}

// CollectionStatus returns [ErrNotFound] when the collection does not
// exist.
//
// schedulerTick is the DTM scheduler's configured tick interval; the
// finalize window is computed from it (clamped to [finalizeWindowMin,
// FinalizeWindowMax]) so MergeReindexStatus can decide whether a
// FINISHED-but-flag-off task is still in its legitimate swap window.
func (s *Service) CollectionStatus(ctx context.Context, collection string, schedulerTick time.Duration) (CollectionIndexStatus, error) {
	class := s.deps.SchemaManager.ReadOnlyClass(collection)
	if class == nil {
		return CollectionIndexStatus{}, ErrNotFound
	}

	var tasks map[string][]*distributedtask.Task
	if s.deps.Cluster != nil {
		var err error
		tasks, err = s.deps.Cluster.ListDistributedTasks(ctx)
		if err != nil {
			tasks = nil // degrade gracefully
		}
	}
	// Pre-parse the reindex task payloads once per request so the
	// per-property merge below doesn't re-unmarshal each task N times.
	parsedTasks := ParseReindexTasks(tasks[dbreindex.ReindexNamespace])

	// Precompute once so per-property resolution below is O(1);
	// stamp/class-flag fast paths still take precedence in
	// SearchablePropertyIsBlockmaxParsed.
	finishedBlockmaxProps := make(map[string]struct{})
	for _, pt := range parsedTasks {
		if pt.Task.Status != distributedtask.TaskStatusFinished {
			continue
		}
		if !strings.EqualFold(pt.Payload.Collection, collection) {
			continue
		}
		if _, _, producesBlockmax, _ := dbreindex.ReindexBucketEffect(pt.Payload.MigrationType); !producesBlockmax {
			continue
		}
		for _, p := range pt.Payload.Properties {
			finishedBlockmaxProps[p] = struct{}{}
		}
	}

	finalizeWindow := 2 * schedulerTick
	if finalizeWindow < finalizeWindowMin {
		finalizeWindow = finalizeWindowMin
	}
	if finalizeWindow > FinalizeWindowMax {
		finalizeWindow = FinalizeWindowMax
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
			idx := &models.IndexStatus{Type: CanonicalIndexType(e.indexType), Status: "ready"}
			if e.flagOn && e.carryTokenization {
				idx.Tokenization = prop.Tokenization
			}
			// Only searchable indexes have a BM25 algorithm; surface the
			// property's TRUE wand/blockmax state (not just the class-wide
			// flag, which flips only once every searchable property has
			// migrated).
			if e.indexType == "searchable" && e.flagOn {
				idx.Algorithm = models.IndexStatusAlgorithmWand
				if dbreindex.SearchablePropertyIsBlockmaxParsed(class, prop.Name, finishedBlockmaxProps) {
					idx.Algorithm = models.IndexStatusAlgorithmBlockmax
				}
			}
			MergeReindexStatus(idx, collection, prop.Name, e.indexType, e.flagOn, parsedTasks, finalizeWindow, s.logger)
			// Suppress a stale "indexing@100%" phantom left after DELETE.
			if !e.flagOn && idx.Status == models.IndexStatusStatusIndexing &&
				s.isPostDeleteFinalizeBleed(collection, prop.Name, CanonicalIndexType(e.indexType), idx.TaskID, parsedTasks) {
				continue
			}
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

// isPostDeleteFinalizeBleed reports whether a synthetic "indexing@100%"
// entry is a phantom: its driving task (taskID) FINISHED but the index
// was DELETEd afterward. A STARTED task always outranks a FINISHED one,
// so a live re-enable is never suppressed.
func (s *Service) isPostDeleteFinalizeBleed(collection, property, indexType, taskID string, parsedTasks []ParsedReindexTask) bool {
	if taskID == "" || s.deps.DeleteMarkers == nil {
		return false
	}
	var finishedAt time.Time
	found := false
	for _, pt := range parsedTasks {
		if pt.Task.ID != taskID {
			continue
		}
		if pt.Task.Status != distributedtask.TaskStatusFinished {
			// A live (STARTED/PREPARING/SWAPPING) task drove this entry —
			// not the finalize-window override. Never suppress.
			return false
		}
		finishedAt = pt.Task.FinishedAt
		found = true
		break
	}
	if !found {
		return false
	}
	deletedAt := s.deps.DeleteMarkers.LastDeleted(collection, property, indexType)
	return !deletedAt.IsZero() && deletedAt.After(finishedAt)
}

func dataTypeString(prop *models.Property) string {
	if len(prop.DataType) > 0 {
		return prop.DataType[0]
	}
	return ""
}
