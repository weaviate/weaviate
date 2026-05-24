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

import "github.com/weaviate/weaviate/adapters/repos/db/reindex"

// IndexTypesFromMigrationType lists the inverted-index buckets a
// migration type touches. The boolean is `false` for future migration
// types added without being mapped here — callers that need to
// log/alert on that case can check it.
func IndexTypesFromMigrationType(mt reindex.ReindexMigrationType) ([]string, bool) {
	switch mt {
	case reindex.ReindexTypeEnableSearchable, reindex.ReindexTypeChangeAlgorithm, reindex.ReindexTypeRebuildSearchable:
		return []string{"searchable"}, true
	case reindex.ReindexTypeEnableFilterable, reindex.ReindexTypeRepairFilterable:
		return []string{"filterable"}, true
	case reindex.ReindexTypeEnableRangeable, reindex.ReindexTypeRepairRangeable:
		return []string{"rangeable"}, true
	case reindex.ReindexTypeChangeTokenization:
		// change-tokenization-both runs ONE task per inverted index
		// (searchable + filterable). Each leaves its own per-property
		// migration dir on disk. Pre-cleanup must wipe both, otherwise a
		// stale tidied.mig from a previous single-index retokenize on
		// the same prop short-circuits the sub-task and produces a
		// schema / bucket state mismatch (Sev 1 silent data loss).
		return []string{"searchable", "filterable"}, true
	case reindex.ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}, true
	}
	return nil, false
}

// MigrationTypeTargetsIndex returns:
//
//   - matches: true if the migration type writes to the named index bucket.
//   - isKnown: true if the migration type is one this function knows about.
//
// A new ReindexType added to the codebase without being mapped here
// would return (false, false).
func MigrationTypeTargetsIndex(mt reindex.ReindexMigrationType, indexType string) (matches, isKnown bool) {
	switch mt {
	case reindex.ReindexTypeEnableSearchable, reindex.ReindexTypeChangeAlgorithm, reindex.ReindexTypeRebuildSearchable:
		return indexType == "searchable", true
	case reindex.ReindexTypeEnableFilterable, reindex.ReindexTypeRepairFilterable:
		return indexType == "filterable", true
	case reindex.ReindexTypeEnableRangeable, reindex.ReindexTypeRepairRangeable:
		return indexType == "rangeable", true
	case reindex.ReindexTypeChangeTokenization:
		return indexType == "searchable" || indexType == "filterable", true
	case reindex.ReindexTypeChangeTokenizationFilterable:
		return indexType == "filterable", true
	}
	return false, false
}
