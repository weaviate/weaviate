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

// isNumericProperty reports whether the property's primitive data type is
// one of int / number / date — the three types eligible for a rangeable
// index. Used by every rangeable validator + the getIndexes emitter so a
// new numeric type added in the future only needs to be added here.
func isNumericProperty(prop *models.Property) bool {
	dt, ok := entschema.AsPrimitive(prop.DataType)
	return ok && (dt == entschema.DataTypeInt || dt == entschema.DataTypeNumber || dt == entschema.DataTypeDate)
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
		if !isNumericProperty(prop) {
			return fmt.Errorf("property %q is not a numeric type (int, number, date)", pn)
		}
		if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
			return fmt.Errorf("property %q already has indexRangeFilters enabled", pn)
		}
	}
	return nil
}

// validateRebuildRangeableProperty is the inverse-precondition counterpart
// of validateRangeableProperties: the property must already have rangeable
// indexing enabled (otherwise there's nothing to rebuild — the caller
// wants enable-rangeable instead). Type must still be numeric/date.
func validateRebuildRangeableProperty(prop *models.Property) error {
	if !isNumericProperty(prop) {
		return fmt.Errorf("property %q is not a numeric type (int, number, date)", prop.Name)
	}
	if prop.IndexRangeFilters == nil || !*prop.IndexRangeFilters {
		return fmt.Errorf("property %q does not have a rangeable index to rebuild; use enable to create one", prop.Name)
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

// validateRebuildFilterableDataType guards repair-filterable against
// property types whose schema-default flips IndexFilterable=true but
// which have no inverted bucket on disk: blob (skipped by the schema
// migrator), geoCoordinates (indexed via a dedicated geo index), and
// phoneNumber (indexed via parsed sub-fields). References (non-primitive
// data types) likewise have no inverted bucket.
//
// Without this guard the dispatcher's "is IndexFilterable enabled?"
// check passes for these types — the schema sets the flag to true even
// though no bucket exists — and the rebuild task crashes at swap time
// with `target bucket "property_p" not found in store`. That surfaces
// as a TASK FAILED rather than a clean 4xx, leaving the caller with
// no actionable signal.
//
// The error wording deliberately ends with "nothing to rebuild" to
// disambiguate from validateEnableFilterableProperty's wording
// ("does not support a filterable index"), so a caller comparing the
// two paths can tell which one rejected them.
func validateRebuildFilterableDataType(prop *models.Property) error {
	dt, ok := entschema.AsPrimitive(prop.DataType)
	if !ok {
		return fmt.Errorf("property %q type %v does not support a filterable inverted index; nothing to rebuild", prop.Name, prop.DataType)
	}
	switch dt { //nolint:exhaustive // intentional allow-by-default
	case entschema.DataTypeBlob, entschema.DataTypeGeoCoordinates, entschema.DataTypePhoneNumber:
		return fmt.Errorf("property %q type %q does not support a filterable inverted index; nothing to rebuild", prop.Name, dt)
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

// validateFilterableTokenizationChange validates the body for
// `PUT /v1/schema/{class}/indexes/{prop}` with `{filterable:{tokenization:X}}`.
// Distinct from validateTokenizationChange: does NOT require a searchable
// bucket — this is the filterable-only retokenize variant. The caller
// dispatches to ReindexTypeChangeTokenizationFilterable which runs only
// the filterable retokenize task.
func validateFilterableTokenizationChange(prop *models.Property, targetTokenization string) error {
	if prop == nil {
		return fmt.Errorf("property not found")
	}
	dt, ok := entschema.AsPrimitive(prop.DataType)
	if !ok || (dt != entschema.DataTypeText && dt != entschema.DataTypeTextArray) {
		return fmt.Errorf("property %q is not a text type; filterable.tokenization only applies to text / text[]", prop.Name)
	}
	if prop.IndexFilterable == nil || !*prop.IndexFilterable {
		return fmt.Errorf("property %q has no filterable index; nothing to retokenize. Enable filterable first via {\"filterable\":{\"enabled\":true}}", prop.Name)
	}
	if !entschema.IsValidTokenization(targetTokenization) {
		return fmt.Errorf("invalid tokenization %q", targetTokenization)
	}
	if prop.Tokenization == targetTokenization {
		return fmt.Errorf("property %q already uses tokenization %q", prop.Name, targetTokenization)
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

// validateBodyExclusivity guards against ambiguous PUT
// /v1/schema/{class}/indexes/{prop} request bodies that the switch-based
// dispatch in updateIndex would otherwise silently misroute.
//
// The dispatch is a switch on field truthiness. A request like
// `{searchable:{rebuild:true}, filterable:{rebuild:true}}` would match the
// first arm (searchable.rebuild) and silently ignore filterable.rebuild —
// the user gets a 202 but only half the requested work runs. This helper
// rejects such bodies up front with a 400 listing the offending fields.
//
// Rules:
//   - At most one group (Searchable / Filterable / Rangeable) may be set.
//   - Within a group, at most one verb may be set. Verbs:
//   - Searchable: Enabled, Rebuild, Tokenization (without Enabled)
//   - Filterable: Enabled, Rebuild
//   - Rangeable:  Enabled
//   - Searchable.Tokenization with Searchable.Enabled is allowed:
//     enable-searchable REQUIRES a tokenization, so they are one verb, not two.
//   - Zero verbs total is rejected (consistent with the default arm in
//     updateIndex), so this helper covers that case too.
func validateBodyExclusivity(body *models.IndexUpdateRequest) error {
	if body == nil {
		return fmt.Errorf("request body required")
	}

	var groupsSet []string

	// Searchable group.
	if body.Searchable != nil {
		var verbs []string
		if body.Searchable.Enabled {
			verbs = append(verbs, "searchable.enabled")
		}
		if body.Searchable.Rebuild {
			verbs = append(verbs, "searchable.rebuild")
		}
		// Tokenization is a verb on its own (change-tokenization) ONLY when
		// Enabled is not set. With Enabled it is part of the enable verb.
		if body.Searchable.Tokenization != "" && !body.Searchable.Enabled {
			verbs = append(verbs, "searchable.tokenization")
		}
		if body.Searchable.Cancel {
			verbs = append(verbs, "searchable.cancel")
		}
		if len(verbs) > 1 {
			return fmt.Errorf("conflicting fields in searchable: %v — set exactly one of enabled, rebuild, tokenization, or cancel (tokenization combined with enabled is allowed)", verbs)
		}
		if len(verbs) == 1 {
			groupsSet = append(groupsSet, "searchable")
		}
	}

	// Filterable group.
	if body.Filterable != nil {
		var verbs []string
		if body.Filterable.Enabled {
			verbs = append(verbs, "filterable.enabled")
		}
		if body.Filterable.Rebuild {
			verbs = append(verbs, "filterable.rebuild")
		}
		// Tokenization is a verb on its own ONLY when Enabled is not set.
		// Mirrors the searchable.tokenization rule above.
		if body.Filterable.Tokenization != "" && !body.Filterable.Enabled {
			verbs = append(verbs, "filterable.tokenization")
		}
		if body.Filterable.Cancel {
			verbs = append(verbs, "filterable.cancel")
		}
		if len(verbs) > 1 {
			return fmt.Errorf("conflicting fields in filterable: %v — set exactly one of enabled, rebuild, tokenization, or cancel", verbs)
		}
		if len(verbs) == 1 {
			groupsSet = append(groupsSet, "filterable")
		}
	}

	// Rangeable group.
	if body.Rangeable != nil {
		var verbs []string
		if body.Rangeable.Enabled {
			verbs = append(verbs, "rangeable.enabled")
		}
		if body.Rangeable.Rebuild {
			verbs = append(verbs, "rangeable.rebuild")
		}
		if body.Rangeable.Cancel {
			verbs = append(verbs, "rangeable.cancel")
		}
		if len(verbs) > 1 {
			return fmt.Errorf("conflicting fields in rangeable: %v — set exactly one of enabled, rebuild, or cancel", verbs)
		}
		if len(verbs) == 1 {
			groupsSet = append(groupsSet, "rangeable")
		}
	}

	if len(groupsSet) > 1 {
		return fmt.Errorf("multiple index groups set in one request (%v) — issue separate requests, one per group", groupsSet)
	}
	if len(groupsSet) == 0 {
		return fmt.Errorf("no actionable change detected; set one of: searchable.tokenization, searchable.rebuild, searchable.enabled, searchable.cancel, filterable.rebuild, filterable.enabled, filterable.cancel, rangeable.enabled, rangeable.rebuild, rangeable.cancel")
	}
	return nil
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
