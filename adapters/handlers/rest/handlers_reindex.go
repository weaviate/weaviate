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

package rest

import (
	"context"
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// buildUnitMaps creates per-replica unit IDs and maps from shard ownership.
// ShardOwnership returns map[nodeName][]shardName (node→shards it owns).
// Unit ID format: "shardName__nodeName".
func buildUnitMaps(shardOwnership map[string][]string) (unitIDs []string, unitToShard, unitToNode map[string]string) {
	unitToShard = make(map[string]string)
	unitToNode = make(map[string]string)

	for nodeName, shards := range shardOwnership {
		for _, shardName := range shards {
			unitID := fmt.Sprintf("%s__%s", shardName, nodeName)
			unitIDs = append(unitIDs, unitID)
			unitToShard[unitID] = shardName
			unitToNode[unitID] = nodeName
		}
	}
	sort.Strings(unitIDs)
	return unitIDs, unitToShard, unitToNode
}

// validateRangeableProperties validates that the named properties are
// eligible for enable-rangeable: numeric type, not already rangeable.
// Whether the property currently has a filterable index is deliberately
// NOT checked — the migration sources from the objects bucket and can
// build a rangeable index on a numeric property regardless of whether
// it already has a filterable one. Time-series and similar use cases
// often want rangeable without filterable.
func validateRangeableProperties(class *models.Class, propNames []string) error {
	propsByName := make(map[string]*models.Property, len(class.Properties))
	for _, p := range class.Properties {
		propsByName[p.Name] = p
	}

	for _, pn := range propNames {
		prop, ok := propsByName[pn]
		if !ok {
			return fmt.Errorf("property %q not found", pn)
		}
		dt, ok := entschema.AsPrimitive(prop.DataType)
		if !ok || (dt != entschema.DataTypeInt && dt != entschema.DataTypeNumber && dt != entschema.DataTypeDate) {
			return fmt.Errorf("property %q is not a numeric type (int, number, date)", pn)
		}
		if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
			return fmt.Errorf("property %q already has indexRangeFilters enabled", pn)
		}
	}
	return nil
}

// validateEnableFilterableProperty validates that the property is a
// suitable target for enable-filterable: it must not already have a
// filterable index, and its data type must support inverted filtering
// (everything except blob, geoCoordinates, and references).
func validateEnableFilterableProperty(prop *models.Property) error {
	if prop.IndexFilterable != nil && *prop.IndexFilterable {
		return fmt.Errorf("property %q already has a filterable index", prop.Name)
	}
	dt, ok := entschema.AsPrimitive(prop.DataType)
	if !ok {
		// Non-primitive (references) — not supported for filterable enable.
		return fmt.Errorf("property %q type %v does not support a filterable index", prop.Name, prop.DataType)
	}
	// Allow every primitive type EXCEPT the three below. Using a default
	// keeps the allow-list implicit so newly-added primitive types are
	// permitted by default; only types we have positively decided cannot
	// support a filterable index need to be listed.
	switch dt { //nolint:exhaustive // intentional allow-by-default
	case entschema.DataTypeBlob, entschema.DataTypeGeoCoordinates, entschema.DataTypePhoneNumber:
		return fmt.Errorf("property %q type %q does not support a filterable index", prop.Name, dt)
	}
	return nil
}

// validateEnableSearchableProperty validates that the property is a
// suitable target for enable-searchable: text/text[] type, not already
// searchable, with a valid tokenization specified.
//
// We also reject the request if the property already has a filterable
// index AND a stored tokenization that differs from the requested one.
// EnableSearchable.OnMigrationComplete unconditionally writes
// Tokenization = s.tokenization alongside IndexSearchable = true; if the
// property has a pre-existing filterable bucket built with the old
// tokenization, that bucket's terms would silently diverge from the
// schema's tokenization and from the newly built searchable bucket. The
// only safe path in that case is to retokenize the filterable index too
// (a separate operation) — fail fast here rather than let the divergence
// happen on RAFT apply.
func validateEnableSearchableProperty(prop *models.Property, tokenization string) error {
	if prop.IndexSearchable != nil && *prop.IndexSearchable {
		return fmt.Errorf("property %q already has a searchable index", prop.Name)
	}
	dt, ok := entschema.AsPrimitive(prop.DataType)
	if !ok || (dt != entschema.DataTypeText && dt != entschema.DataTypeTextArray) {
		return fmt.Errorf("property %q is not a text type", prop.Name)
	}
	if tokenization == "" {
		return fmt.Errorf("enable-searchable requires a tokenization to be set on the request body")
	}
	if !entschema.IsValidTokenization(tokenization) {
		return fmt.Errorf("invalid tokenization %q", tokenization)
	}
	// Reject if the property already has a filterable index built with a
	// different tokenization — silently rewriting Tokenization here would
	// desynchronize the filterable bucket's terms from the schema.
	if prop.IndexFilterable != nil && *prop.IndexFilterable &&
		prop.Tokenization != "" && prop.Tokenization != tokenization {
		return fmt.Errorf("property %q has an existing filterable index built with tokenization %q; "+
			"enabling searchable with tokenization %q would silently desynchronize the filterable index. "+
			"Retokenize the filterable index first or use the matching tokenization",
			prop.Name, prop.Tokenization, tokenization)
	}
	return nil
}

func validateTokenizationChange(
	appState *state.State,
	class *models.Class,
	collection, propName, targetTokenization string,
) (bucketStrategy string, err error) {
	// Find the property.
	var targetProp *models.Property
	for _, p := range class.Properties {
		if p.Name == propName {
			targetProp = p
			break
		}
	}
	if targetProp == nil {
		return "", fmt.Errorf("property %q not found", propName)
	}

	dt, ok := entschema.AsPrimitive(targetProp.DataType)
	if !ok || (dt != entschema.DataTypeText && dt != entschema.DataTypeTextArray) {
		return "", fmt.Errorf("property %q is not a text type", propName)
	}

	// Validate target tokenization.
	if !entschema.IsValidTokenization(targetTokenization) {
		return "", fmt.Errorf("invalid tokenization %q", targetTokenization)
	}

	if targetProp.Tokenization == targetTokenization {
		return "", fmt.Errorf("property %q already uses tokenization %q", propName, targetTokenization)
	}

	// Detect bucket strategy from the first shard's searchable bucket.
	className := entschema.ClassName(collection)
	idx := appState.DB.GetIndex(className)
	if idx == nil {
		return "", fmt.Errorf("collection index not found")
	}

	idx.ForEachShard(func(_ string, shard db.ShardLike) error {
		if bucketStrategy != "" {
			return nil
		}
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard.Store().Bucket(bucketName)
		if bucket != nil {
			bucketStrategy = bucket.Strategy()
		}
		return nil
	})
	if bucketStrategy == "" {
		return "", fmt.Errorf("searchable bucket not found for property %q", propName)
	}

	return bucketStrategy, nil
}

// buildUnitSpecs creates UnitSpec entries with GroupID = shardName (= tenant
// name in MT). This enables per-tenant barrier semantics via OnGroupCompleted.
func buildUnitSpecs(shardOwnership map[string][]string) []distributedtask.UnitSpec {
	var specs []distributedtask.UnitSpec
	for nodeName, shards := range shardOwnership {
		for _, shardName := range shards {
			unitID := fmt.Sprintf("%s__%s", shardName, nodeName)
			specs = append(specs, distributedtask.UnitSpec{
				ID:      unitID,
				GroupID: shardName,
			})
		}
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].ID < specs[j].ID })
	return specs
}

// isSemanticMigration returns true for migration types that change query
// behavior and require barrier semantics. Must stay in sync with the
// provider-side isSemanticMigration in adapters/repos/db/reindex_provider.go.
func isSemanticMigration(mt db.ReindexMigrationType) bool {
	return mt == db.ReindexTypeChangeTokenization ||
		mt == db.ReindexTypeEnableFilterable ||
		mt == db.ReindexTypeEnableSearchable
}

// validateTenants checks that all specified tenants exist in the collection's
// sharding state and are in a status that has local data (HOT, ACTIVE, COLD,
// INACTIVE). Returns an error for nonexistent or OFFLOADED/FROZEN tenants.
func validateTenants(database *db.DB, ctx context.Context, collection string, tenants []string) error {
	// Get all shard ownership including all statuses.
	allOwnership, err := database.ShardReplicaOwnershipForMT(ctx, collection, nil)
	if err != nil {
		return fmt.Errorf("reading shard state: %w", err)
	}

	// Build set of all known shards (= tenants).
	knownTenants := make(map[string]struct{})
	for _, shards := range allOwnership {
		for _, shard := range shards {
			knownTenants[shard] = struct{}{}
		}
	}

	// Get the filtered ownership (only HOT/ACTIVE/COLD/INACTIVE).
	activeOwnership, err := database.ShardReplicaOwnershipForMT(ctx, collection, tenants)
	if err != nil {
		return fmt.Errorf("reading shard state: %w", err)
	}
	activeTenants := make(map[string]struct{})
	for _, shards := range activeOwnership {
		for _, shard := range shards {
			activeTenants[shard] = struct{}{}
		}
	}

	for _, tenant := range tenants {
		if _, ok := knownTenants[tenant]; !ok {
			return fmt.Errorf("tenant %q does not exist", tenant)
		}
		if _, ok := activeTenants[tenant]; !ok {
			return fmt.Errorf("tenant %q is not in an active status (OFFLOADED/FROZEN tenants cannot be reindexed)", tenant)
		}
	}
	return nil
}
