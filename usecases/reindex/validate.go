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
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

// IsNumericProperty reports whether the property's data type is one of
// the three numeric primitives that support rangeable indexing.
func IsNumericProperty(prop *models.Property) bool {
	dt, ok := entschema.AsPrimitive(prop.DataType)
	return ok && (dt == entschema.DataTypeInt || dt == entschema.DataTypeNumber || dt == entschema.DataTypeDate)
}

// ValidateRangeableProperties validates that the named properties are
// eligible for enable-rangeable: numeric type, not already rangeable.
// Whether the property currently has a filterable index is deliberately
// NOT checked — the migration sources from the objects bucket and can
// build a rangeable index regardless.
func ValidateRangeableProperties(class *models.Class, propNames []string) error {
	propsByName := make(map[string]*models.Property, len(class.Properties))
	for _, p := range class.Properties {
		propsByName[p.Name] = p
	}

	for _, pn := range propNames {
		prop, ok := propsByName[pn]
		if !ok {
			return fmt.Errorf("property %q not found", pn)
		}
		if !IsNumericProperty(prop) {
			return fmt.Errorf("property %q is not a numeric type (int, number, date)", pn)
		}
		if prop.IndexRangeFilters != nil && *prop.IndexRangeFilters {
			return fmt.Errorf("property %q already has indexRangeFilters enabled", pn)
		}
	}
	return nil
}

// ValidateRebuildRangeableProperty is the inverse-precondition counterpart
// of ValidateRangeableProperties: the property must already have rangeable
// indexing enabled (otherwise there's nothing to rebuild).
func ValidateRebuildRangeableProperty(prop *models.Property) error {
	if !IsNumericProperty(prop) {
		return fmt.Errorf("property %q is not a numeric type (int, number, date)", prop.Name)
	}
	if prop.IndexRangeFilters == nil || !*prop.IndexRangeFilters {
		return fmt.Errorf("property %q does not have a rangeable index to rebuild; use enable to create one", prop.Name)
	}
	return nil
}

// ValidateEnableFilterableProperty validates that the property is a
// suitable target for enable-filterable: it must not already have a
// filterable index, and its data type must support inverted filtering
// (everything except blob, geoCoordinates, and phoneNumber).
func ValidateEnableFilterableProperty(prop *models.Property) error {
	if prop.IndexFilterable != nil && *prop.IndexFilterable {
		return fmt.Errorf("property %q already has a filterable index", prop.Name)
	}
	dt, ok := entschema.AsPrimitive(prop.DataType)
	if !ok {
		return fmt.Errorf("property %q type %v does not support a filterable index", prop.Name, prop.DataType)
	}
	switch dt { //nolint:exhaustive // intentional allow-by-default
	case entschema.DataTypeBlob, entschema.DataTypeGeoCoordinates, entschema.DataTypePhoneNumber:
		return fmt.Errorf("property %q type %q does not support a filterable index", prop.Name, dt)
	}
	return nil
}

// ValidateRebuildFilterableDataType guards repair-filterable against
// property types whose schema-default flips IndexFilterable=true but
// which have no inverted bucket on disk: blob (skipped by the schema
// migrator), geoCoordinates (indexed via a dedicated geo index), and
// phoneNumber (indexed via parsed sub-fields). References (non-primitive
// data types) likewise have no inverted bucket.
//
// Without this guard the dispatcher's "is IndexFilterable enabled?"
// check passes for these types and the rebuild task crashes at swap
// time with `target bucket "property_p" not found in store`, surfacing
// as TASK FAILED rather than a clean 4xx.
func ValidateRebuildFilterableDataType(prop *models.Property) error {
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

// ValidateEnableSearchableProperty validates that the property is a
// suitable target for enable-searchable: text/text[] type, not already
// searchable, with a valid tokenization specified.
//
// Also rejects the request if the property already has a filterable
// index AND a stored tokenization that differs from the requested one.
// EnableSearchable.OnMigrationComplete unconditionally writes
// Tokenization = s.tokenization alongside IndexSearchable = true; if
// the property has a pre-existing filterable bucket built with the old
// tokenization, that bucket's terms would silently diverge from the
// schema's tokenization and from the newly built searchable bucket.
func ValidateEnableSearchableProperty(prop *models.Property, tokenization string) error {
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
	if prop.IndexFilterable != nil && *prop.IndexFilterable &&
		prop.Tokenization != "" && prop.Tokenization != tokenization {
		return fmt.Errorf("property %q has an existing filterable index built with tokenization %q; "+
			"enabling searchable with tokenization %q would silently desynchronize the filterable index. "+
			"Retokenize the filterable index first or use the matching tokenization",
			prop.Name, prop.Tokenization, tokenization)
	}
	return nil
}

// ValidateFilterableTokenizationChange validates the body for
// `PUT /v1/schema/{class}/indexes/{prop}` with `{filterable:{tokenization:X}}`.
// Distinct from ValidateTokenizationChange: does NOT require a
// searchable bucket — this is the filterable-only retokenize variant.
func ValidateFilterableTokenizationChange(prop *models.Property, targetTokenization string) error {
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
