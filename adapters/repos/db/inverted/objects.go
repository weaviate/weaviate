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

package inverted

import (
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func (a *Analyzer) Object(input map[string]any, props []*models.Property,
	uuid strfmt.UUID,
) ([]Property, []NestedProperty, error) {
	propsMap := map[string]*models.Property{}
	for _, prop := range props {
		propsMap[prop.Name] = prop
	}

	properties, nestedProps, err := a.analyzeProps(propsMap, input)
	if err != nil {
		return nil, nil, fmt.Errorf("analyze props: %w", err)
	}

	idProp, err := a.analyzeIDProp(uuid)
	if err != nil {
		return nil, nil, fmt.Errorf("analyze uuid prop: %w", err)
	}
	properties = append(properties, *idProp)

	tsProps, err := a.analyzeTimestampProps(input)
	if err != nil {
		return nil, nil, fmt.Errorf("analyze timestamp props: %w", err)
	}
	// tsProps will be nil here if weaviate is
	// not setup to index by timestamps
	if tsProps != nil {
		properties = append(properties, tsProps...)
	}

	return properties, nestedProps, nil
}

func (a *Analyzer) analyzeProps(propsMap map[string]*models.Property,
	input map[string]any,
) ([]Property, []NestedProperty, error) {
	var out []Property
	var nestedOut []NestedProperty
	for key, prop := range propsMap {
		if len(prop.DataType) < 1 {
			return nil, nil, fmt.Errorf("prop %q has no datatype", prop.Name)
		}

		if schema.IsBlobLikeDataType(prop.DataType) {
			continue
		}

		if _, ok := schema.AsNested(prop.DataType); ok {
			// Preview gate — when off we silently skip nested analysis so no
			// entries are appended to nested filterable or meta buckets. Pairs
			// with the bucket-creation skip in shard_init_properties.go; even
			// if one leaks past the other, the absence of buckets makes the
			// writes inert.
			if !entcfg.NestedFilteringEnabled() {
				continue
			}
			// TODO aliszka:nested_filtering respect top-level indexFilterable/
			// indexSearchable/indexRangeable settings for nested properties. Currently
			// these are ignored — the nested write path bypasses HasAnyInvertedIndex
			// and always indexes. The interaction between the top-level setting and
			// per-nested-property settings needs design discussion before implementing.
			nr, err := a.analyzeNestedProp(prop, input[key])
			if err != nil {
				return nil, nil, fmt.Errorf("analyze nested prop %q: %w", key, err)
			}
			if nr != nil {
				nestedOut = append(nestedOut, *nr)
			}
			continue
		}

		// Apply the in-memory schema overlay (if any) before deciding whether
		// to skip the property. This is how runtime reindex migrations
		// (enable-filterable / enable-searchable) keep the analyzer in sync
		// with the *target* schema while the live RAFT-stored schema still
		// has the index flag off. The overlay never mutates the input prop;
		// it produces a shallow copy with the relevant flags/tokenization
		// overridden for the duration of this analysis call.
		effective := a.effectiveProperty(prop)

		// Columnar is not part of HasAnyInvertedIndex (it is a column
		// store, not an inverted index), but a columnar-indexed property
		// must still be analyzed so the write path can feed the columnar
		// bucket — including during an enable-columnar backfill where the
		// overlay forces the flag on while every inverted flag is off.
		if !HasAnyInvertedIndex(effective) && !HasColumnarIndex(effective) {
			continue
		}

		if schema.IsRefDataType(effective.DataType) {
			if err := a.extendPropertiesWithReference(&out, effective, input, key); err != nil {
				return nil, nil, err
			}
		} else if schema.IsArrayDataType(effective.DataType) {
			if err := a.extendPropertiesWithArrayType(&out, effective, input, key); err != nil {
				return nil, nil, err
			}
		} else {
			if err := a.extendPropertiesWithPrimitive(&out, effective, input, key); err != nil {
				return nil, nil, err
			}
		}

	}
	return out, nestedOut, nil
}

// effectiveProperty returns either the input property unchanged (the common
// case — overlay is nil or doesn't mention this property) or a shallow copy
// with the relevant inverted-index flags / tokenization overridden. The
// original is never modified. See PropertyOverlay for context.
func (a *Analyzer) effectiveProperty(prop *models.Property) *models.Property {
	if len(a.schemaOverlay) == 0 {
		return prop
	}
	o, ok := a.schemaOverlay[prop.Name]
	if !ok {
		return prop
	}
	// Shallow copy. Pointer fields (IndexFilterable/IndexSearchable/...) are
	// replaced with locally-owned bools so we never mutate the caller's
	// schema. DataType / NestedProperties are read-only downstream so a
	// shallow copy is safe.
	clone := *prop
	if o.ForceFilterable {
		t := true
		clone.IndexFilterable = &t
	}
	if o.ForceSearchable {
		t := true
		clone.IndexSearchable = &t
	}
	if o.ForceRangeable {
		t := true
		clone.IndexRangeFilters = &t
	}
	if o.ForceColumnar {
		t := true
		clone.IndexColumnar = &t
	}
	if o.Tokenization != "" {
		clone.Tokenization = o.Tokenization
	}
	return &clone
}

func (a *Analyzer) analyzeIDProp(id strfmt.UUID) (*Property, error) {
	value, err := id.MarshalText()
	if err != nil {
		return nil, fmt.Errorf("marshal id prop: %w", err)
	}
	return &Property{
		Name: filters.InternalPropID,
		Items: []Countable{
			{
				Data: value,
			},
		},
		HasFilterableIndex: HasFilterableIndexIdProp,
		HasSearchableIndex: HasSearchableIndexIdProp,
	}, nil
}

func (a *Analyzer) analyzeTimestampProps(input map[string]any) ([]Property, error) {
	createTime, createTimeOK := input[filters.InternalPropCreationTimeUnix]
	updateTime, updateTimeOK := input[filters.InternalPropLastUpdateTimeUnix]

	var props []Property
	if createTimeOK {
		b, err := json.Marshal(createTime)
		if err != nil {
			return nil, fmt.Errorf("analyze create timestamp prop: %w", err)
		}
		props = append(props, Property{
			Name:               filters.InternalPropCreationTimeUnix,
			Items:              []Countable{{Data: b}},
			HasFilterableIndex: HasFilterableIndexTimestampProp,
			HasSearchableIndex: HasSearchableIndexTimestampProp,
		})
	}

	if updateTimeOK {
		b, err := json.Marshal(updateTime)
		if err != nil {
			return nil, fmt.Errorf("analyze update timestamp prop: %w", err)
		}
		props = append(props, Property{
			Name:               filters.InternalPropLastUpdateTimeUnix,
			Items:              []Countable{{Data: b}},
			HasFilterableIndex: HasFilterableIndexTimestampProp,
			HasSearchableIndex: HasSearchableIndexTimestampProp,
		})
	}

	return props, nil
}

func (a *Analyzer) extendPropertiesWithArrayType(properties *[]Property,
	prop *models.Property, input map[string]any, propName string,
) error {
	value, ok := input[propName]
	if !ok {
		// skip any primitive prop that's not set
		return nil
	}

	var err error
	value, err = typedSliceToUntyped(value)
	if err != nil {
		return fmt.Errorf("extend properties with array type: %w", err)
	}

	values, ok := value.([]any)
	if !ok {
		// skip any primitive prop that's not set
		return errors.New("analyze array prop: expected array prop")
	}

	property, err := a.analyzeArrayProp(prop, values)
	if err != nil {
		return fmt.Errorf("analyze array prop: %w", err)
	}
	if property == nil {
		return nil
	}

	*properties = append(*properties, *property)
	return nil
}

// extendPropertiesWithPrimitive mutates the passed in properties, by extending
// it with an additional property - if applicable
func (a *Analyzer) extendPropertiesWithPrimitive(properties *[]Property,
	prop *models.Property, input map[string]any, propName string,
) error {
	var property *Property
	var err error

	value, ok := input[propName]
	if !ok {
		// skip any primitive prop that's not set
		return nil
	}
	property, err = a.analyzePrimitiveProp(prop, value)
	if err != nil {
		return fmt.Errorf("analyze primitive prop: %w", err)
	}
	if property == nil {
		return nil
	}

	*properties = append(*properties, *property)
	return nil
}

func (a *Analyzer) analyzeArrayProp(prop *models.Property, values []any) (*Property, error) {
	dt := schema.DataType(prop.DataType[0])

	// Text arrays need special handling: TextArray aggregates term frequencies
	// across all elements, which per-element analyzeValue calls cannot do.
	if dt == schema.DataTypeTextArray {
		in, err := stringsFromValues(prop, values)
		if err != nil {
			return nil, err
		}
		hasFilterableIndex := HasFilterableIndex(prop) && !a.isFallbackToSearchable()
		var rawValues []string
		if a.captureRawValues {
			rawValues = in
		}
		return &Property{
			Name:               prop.Name,
			Items:              a.TextArray(prop.Tokenization, in, prop.Name, prop.TextAnalyzer),
			RawValues:          rawValues,
			Length:             len(values),
			HasFilterableIndex: hasFilterableIndex,
			HasSearchableIndex: HasSearchableIndex(prop),
			HasRangeableIndex:  HasRangeableIndex(prop),
		}, nil
	}

	scalarDT := schema.ScalarFromArrayType(dt)
	if scalarDT == dt {
		// unknown array type
		return nil, nil
	}

	items := make([]Countable, 0, len(values))
	for _, value := range values {
		analyzed, err := a.analyzeValue(scalarDT, prop.Tokenization, prop.Name, prop.TextAnalyzer, value)
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
		items = append(items, analyzed...)
	}

	return &Property{
		Name:               prop.Name,
		Items:              items,
		Length:             len(values),
		HasFilterableIndex: HasFilterableIndex(prop),
		HasSearchableIndex: HasSearchableIndex(prop),
		HasRangeableIndex:  HasRangeableIndex(prop),
		HasColumnarIndex:   HasColumnarIndex(prop),
	}, nil
}

func stringsFromValues(prop *models.Property, values []any) ([]string, error) {
	in := make([]string, len(values))
	for i, value := range values {
		asString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type string, but got %T", prop.Name, value)
		}
		in[i] = asString
	}
	return in, nil
}

func (a *Analyzer) analyzePrimitiveProp(prop *models.Property, value any) (*Property, error) {
	dt := schema.DataType(prop.DataType[0])
	items, err := a.analyzeValue(dt, prop.Tokenization, prop.Name, prop.TextAnalyzer, value)
	if err != nil {
		return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
	}
	if items == nil {
		return nil, nil
	}

	propertyLength := -1
	hasFilterableIndex := HasFilterableIndex(prop)
	hasSearchableIndex := HasSearchableIndex(prop)
	var rawValues []string

	if dt == schema.DataTypeText {
		hasFilterableIndex = hasFilterableIndex && !a.isFallbackToSearchable()
		if asString, ok := value.(string); ok {
			propertyLength = utf8.RuneCountInString(asString)
			if a.captureRawValues {
				rawValues = []string{asString}
			}
		}
	}

	return &Property{
		Name:               prop.Name,
		Items:              items,
		RawValues:          rawValues,
		Length:             propertyLength,
		HasFilterableIndex: hasFilterableIndex,
		HasSearchableIndex: hasSearchableIndex,
		HasRangeableIndex:  HasRangeableIndex(prop),
		HasColumnarIndex:   HasColumnarIndex(prop),
	}, nil
}

// analyzeValue converts a raw value to analyzed Countable items based on
// data type. Shared by analyzePrimitiveProp and analyzeNestedValue.
// propName and textAnalyzer are used for custom text analyzer configuration;
// pass "" and nil for nested properties which do not support custom analyzers.
func (a *Analyzer) analyzeValue(dt schema.DataType, tokenization, propName string, textAnalyzer *models.TextAnalyzerConfig, value any) ([]Countable, error) {
	switch dt {
	case schema.DataTypeText:
		asString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", value)
		}
		return a.Text(tokenization, asString, propName, textAnalyzer), nil

	case schema.DataTypeInt:
		asInt, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		return a.Int(asInt)

	case schema.DataTypeNumber:
		asFloat, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64, got %T", value)
		}
		return a.Float(asFloat)

	case schema.DataTypeBoolean:
		asBool, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool, got %T", value)
		}
		return a.Bool(asBool)

	case schema.DataTypeDate:
		asInt, err := dateToInt64(value)
		if err != nil {
			return nil, err
		}
		return a.Int(asInt)

	case schema.DataTypeUUID:
		asUUID, err := toUUID(value)
		if err != nil {
			return nil, err
		}
		return a.UUID(asUUID)

	default:
		return nil, nil
	}
}

// extendPropertiesWithReference extends the specified properties arrays with
// either 1 or 2 entries: If the ref is not set, only the ref-count property
// will be added. If the ref is set the ref-prop itself will also be added and
// contain all references as values
func (a *Analyzer) extendPropertiesWithReference(properties *[]Property,
	prop *models.Property, input map[string]any, propName string,
) error {
	value, ok := input[propName]
	if !ok {
		// explicitly set zero-value, so we can index for "ref not set"
		value = make(models.MultipleRef, 0)
	}

	var asRefs models.MultipleRef
	asRefs, ok = value.(models.MultipleRef)
	if !ok {
		// A reference can also arrive as a generic []any of beacon
		// maps — e.g. an object decoded from JSON during async-replication
		// repair. Per PR #2320 an *empty* []any means "no refs". A *populated*
		// one carries real beacons and must be parsed and indexed: silently
		// skipping it drops the refs from the filterable bucket and makes
		// by-reference filters miss the object.
		untyped, isUntyped := value.([]any)
		if !isUntyped {
			return fmt.Errorf("expected property %q to be of type models.MultipleRef,"+
				" but got %T", prop.Name, value)
		}
		if len(untyped) == 0 {
			return nil
		}
		parsed, err := refsFromUntyped(untyped)
		if err != nil {
			return fmt.Errorf("reference property %q: %w", prop.Name, err)
		}
		asRefs = parsed
	}

	property, err := a.analyzeRefPropCount(prop, asRefs)
	if err != nil {
		return fmt.Errorf("ref count: %w", err)
	}

	*properties = append(*properties, *property)

	if len(asRefs) == 0 {
		return nil
	}

	property, err = a.analyzeRefProp(prop, asRefs)
	if err != nil {
		return fmt.Errorf("refs: %w", err)
	}

	*properties = append(*properties, *property)
	return nil
}

// refsFromUntyped converts a reference property decoded from generic JSON
// ([]any of {"beacon": ...} maps) into models.MultipleRef, so it can be
// indexed like a natively-typed reference. Used when an object reaches the
// analyzer without the typed-coercion the read path normally applies.
func refsFromUntyped(value []any) (models.MultipleRef, error) {
	refs := make(models.MultipleRef, len(value))
	for i, elem := range value {
		asMap, ok := elem.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected ref element %d to be a map, got %T", i, elem)
		}
		beacon, ok := asMap["beacon"].(string)
		if !ok {
			return nil, fmt.Errorf("expected ref element %d to have a string %q, got %T", i, "beacon", asMap["beacon"])
		}
		refs[i] = &models.SingleRef{Beacon: strfmt.URI(beacon)}
	}
	return refs, nil
}

func (a *Analyzer) analyzeRefPropCount(prop *models.Property,
	value models.MultipleRef,
) (*Property, error) {
	items, err := a.RefCount(value)
	if err != nil {
		return nil, fmt.Errorf("analyze ref-property %q: %w", prop.Name, err)
	}

	return &Property{
		Name:               helpers.MetaCountProp(prop.Name),
		Items:              items,
		Length:             len(value),
		HasFilterableIndex: HasFilterableIndex(prop),
		HasSearchableIndex: HasSearchableIndex(prop),
		HasRangeableIndex:  HasRangeableIndex(prop),
		HasColumnarIndex:   HasColumnarIndex(prop),
	}, nil
}

func (a *Analyzer) analyzeRefProp(prop *models.Property,
	value models.MultipleRef,
) (*Property, error) {
	items, err := a.Ref(value)
	if err != nil {
		return nil, fmt.Errorf("analyze ref-property %q: %w", prop.Name, err)
	}

	return &Property{
		Name:               prop.Name,
		Items:              items,
		Length:             len(value),
		HasFilterableIndex: HasFilterableIndex(prop),
		HasSearchableIndex: HasSearchableIndex(prop),
		HasRangeableIndex:  HasRangeableIndex(prop),
		HasColumnarIndex:   HasColumnarIndex(prop),
	}, nil
}

func typedSliceToUntyped(in any) ([]any, error) {
	switch typed := in.(type) {
	case []any:
		// nothing to do
		return typed, nil
	case []string:
		return convertToUntyped[string](typed), nil
	case []int:
		return convertToUntyped[int](typed), nil
	case []time.Time:
		return convertToUntyped[time.Time](typed), nil
	case []bool:
		return convertToUntyped[bool](typed), nil
	case []float64:
		return convertToUntyped[float64](typed), nil
	case []uuid.UUID:
		return convertToUntyped[uuid.UUID](typed), nil
	default:
		return nil, errors.Errorf("unsupported type %T", in)
	}
}

func convertToUntyped[T comparable](in []T) []any {
	out := make([]any, len(in))
	for i := range out {
		out[i] = in[i]
	}
	return out
}

// Indicates whether property should be indexed
// Index holds document ids with property of/containing particular value
// and number of its occurrences in that property
// (index created using bucket of StrategyMapCollection)
func HasSearchableIndex(prop *models.Property) bool {
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		// by default property has searchable index only for text/text[] props
		if prop.IndexSearchable == nil {
			return true
		}
		return *prop.IndexSearchable
	default:
		return false
	}
}

// Indicates whether property should be indexed
// Index holds document ids with property of/containing particular value
// (index created using bucket of StrategyRoaringSet)
func HasFilterableIndex(prop *models.Property) bool {
	// by default property has filterable index
	if prop.IndexFilterable == nil {
		return true
	}
	return *prop.IndexFilterable
}

func HasRangeableIndex(prop *models.Property) bool {
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeDate:
		if prop.IndexRangeFilters == nil {
			return false
		}
		return *prop.IndexRangeFilters
	default:
		return false
	}
}

func HasColumnarIndex(prop *models.Property) bool {
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeDate:
		if prop.IndexColumnar == nil {
			return false
		}
		return *prop.IndexColumnar
	default:
		return false
	}
}

func HasAnyInvertedIndex(prop *models.Property) bool {
	return HasFilterableIndex(prop) || HasSearchableIndex(prop) || HasRangeableIndex(prop)
}

const (
	// always
	HasFilterableIndexIdProp = true
	HasSearchableIndexIdProp = false
	HasRangeableIndexIdProp  = false
	HasColumnarIndexIdProp   = false

	// only if index.invertedIndexConfig.IndexTimestamps set
	HasFilterableIndexTimestampProp = true
	HasSearchableIndexTimestampProp = false
	HasRangeableIndexTimestampProp  = false
	HasColumnarIndexTimestampProp   = false

	// only if property.indexFilterable or property.indexSearchable set
	HasFilterableIndexMetaCount = true
	HasSearchableIndexMetaCount = false
	HasRangeableIndexMetaCount  = false
	HasColumnarIndexMetaCount   = false

	// only if index.invertedIndexConfig.IndexNullState set
	// and either property.indexFilterable or property.indexSearchable set
	HasFilterableIndexPropNull = true
	HasSearchableIndexPropNull = false
	HasRangeableIndexPropNull  = false
	HasColumnarIndexPropNull   = false

	// only if index.invertedIndexConfig.IndexPropertyLength set
	// and either property.indexFilterable or property.indexSearchable set
	HasFilterableIndexPropLength = true
	HasSearchableIndexPropLength = false
	HasRangeableIndexPropLength  = false
	HasColumnarIndexPropLength   = false
)

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case float64:
		return int64(val), nil
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, err
		}
		return int64(f), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func dateToInt64(v any) (int64, error) {
	switch val := v.(type) {
	case time.Time:
		return val.UnixNano(), nil
	case string:
		t, err := time.Parse(time.RFC3339Nano, val)
		if err != nil {
			return 0, fmt.Errorf("parse date string: %w", err)
		}
		return t.UnixNano(), nil
	default:
		return 0, fmt.Errorf("expected time.Time or string, got %T", v)
	}
}

func toUUID(v any) (uuid.UUID, error) {
	switch val := v.(type) {
	case uuid.UUID:
		return val, nil
	case string:
		return uuid.Parse(val)
	default:
		return uuid.UUID{}, fmt.Errorf("expected uuid.UUID or string, got %T", v)
	}
}
