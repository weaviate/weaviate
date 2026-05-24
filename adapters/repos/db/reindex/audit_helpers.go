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

// SemanticMigrationIndexTypesForAudit returns the (property,
// indexType) fan-out the audit's CleanStalePartialReindexState loop
// iterates over. Mirrors the REST-layer's indexTypesFromMigrationType
// (handlers_indexes.go) so audit cleanup covers the same per-property
// classification as the cancel/cleanup dispatch.
//
// Class-level migrations whose tracker has no per-property indexType
// fall through to the default (nil); the audit then takes the direct
// tracker-dir removal branch.
func SemanticMigrationIndexTypesForAudit(mt ReindexMigrationType) []string {
	switch mt {
	case ReindexTypeChangeTokenization:
		return []string{"searchable", "filterable"}
	case ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}
	case ReindexTypeEnableSearchable, ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable:
		return []string{"searchable"}
	case ReindexTypeEnableFilterable, ReindexTypeRepairFilterable:
		return []string{"filterable"}
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return []string{"rangeable"}
	}
	return nil
}
