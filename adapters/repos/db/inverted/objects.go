//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

func (a *Analyzer) Object(input map[string]any, props []*models.Property,
	uuid strfmt.UUID,
) ([]Property, error) {
	propsMap := map[string]*models.Property{}
	for _, prop := range props {
		propsMap[prop.Name] = prop
	}

	properties, err := a.analyzeProps(propsMap, input)
	if err != nil {
		return nil, fmt.Errorf("analyze props: %w", err)
	}

	idProp, err := a.analyzeIDProp(uuid)
	if err != nil {
		return nil, fmt.Errorf("analyze uuid prop: %w", err)
	}
	properties = append(properties, *idProp)

	tsProps, err := a.analyzeTimestampProps(input)
	if err != nil {
		return nil, fmt.Errorf("analyze timestamp props: %w", err)
	}
	// tsProps will be nil here if weaviate is
	// not setup to index by timestamps
	if tsProps != nil {
		properties = append(properties, tsProps...)
	}

	return properties, nil
}

func (a *Analyzer) analyzeProps(propsMap map[string]*models.Property,
	input map[string]any,
) ([]Property, error) {
	var out []Property
	for key, prop := range propsMap {
		if len(prop.DataType) < 1 {
			return nil, fmt.Errorf("prop %q has no datatype", prop.Name)
		}

		if !HasInvertedIndex(prop) {
			continue
		}

		if schema.IsBlobDataType(prop.DataType) {
			continue
		}

		if schema.IsRefDataType(prop.DataType) {
			if err := a.extendPropertiesWithReference(&out, prop, input, key); err != nil {
				return nil, err
			}
		} else if schema.IsArrayDataType(prop.DataType) {
			if err := a.extendPropertiesWithArrayType(&out, prop, input, key); err != nil {
				return nil, err
			}
		} else {
			if err := a.extendPropertiesWithPrimitive(&out, prop, input, key); err != nil {
				return nil, err
			}
		}

	}
	return out, nil
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
	var items []Countable
	hasFilterableIndex := HasFilterableIndex(prop)
	hasSearchableIndex := HasSearchableIndex(prop)

	switch dt := schema.DataType(prop.DataType[0]); dt {
	case schema.DataTypeTextArray:
		hasFilterableIndex = hasFilterableIndex && !a.isFallbackToSearchable()
		in, err := stringsFromValues(prop, values)
		if err != nil {
			return nil, err
		}
		items = a.TextArray(prop.Tokenization, in)
	case schema.DataTypeIntArray:
		in := make([]int64, len(values))
		for i, value := range values {
			if asJsonNumber, ok := value.(json.Number); ok {
				var err error
				value, err = asJsonNumber.Float64()
				if err != nil {
					return nil, err
				}
			}

			if asFloat, ok := value.(float64); ok {
				// unmarshaling from json into a dynamic schema will assume every number
				// is a float64
				value = int64(asFloat)
			}

			asInt, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("expected property %s to be of type int64, but got %T", prop.Name, value)
			}
			in[i] = asInt
		}

		var err error
		items, err = a.IntArray(in)
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	case schema.DataTypeNumberArray:
		in := make([]float64, len(values))
		for i, value := range values {
			if asJsonNumber, ok := value.(json.Number); ok {
				var err error
				value, err = asJsonNumber.Float64()
				if err != nil {
					return nil, err
				}
			}

			asFloat, ok := value.(float64)
			if !ok {
				return nil, fmt.Errorf("expected property %s to be of type float64, but got %T", prop.Name, value)
			}
			in[i] = asFloat
		}

		var err error
		items, err = a.FloatArray(in) // convert to int before analyzing
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	case schema.DataTypeBooleanArray:
		in := make([]bool, len(values))
		for i, value := range values {
			asBool, ok := value.(bool)
			if !ok {
				return nil, fmt.Errorf("expected property %s to be of type bool, but got %T", prop.Name, value)
			}
			in[i] = asBool
		}

		var err error
		items, err = a.BoolArray(in) // convert to int before analyzing
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	case schema.DataTypeDateArray:
		in := make([]int64, len(values))
		for i, value := range values {
			// dates can be either a date-string or directly a time object. Try to parse both
			if asTime, okTime := value.(time.Time); okTime {
				in[i] = asTime.UnixNano()
			} else if asString, okString := value.(string); okString {
				parsedTime, err := time.Parse(time.RFC3339Nano, asString)
				if err != nil {
					return nil, fmt.Errorf("parse time: %w", err)
				}
				in[i] = parsedTime.UnixNano()
			} else {
				return nil, fmt.Errorf("expected property %s to be a time-string or time object, but got %T", prop.Name, value)
			}
		}

		var err error
		items, err = a.IntArray(in)
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	case schema.DataTypeUUIDArray:
		parsed, err := validation.ParseUUIDArray(values)
		if err != nil {
			return nil, fmt.Errorf("parse uuid array: %w", err)
		}

		items, err = a.UUIDArray(parsed)
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}

	default:
		// ignore unsupported prop type
		return nil, nil
	}

	return &Property{
		Name:               prop.Name,
		Items:              items,
		Length:             len(values),
		HasFilterableIndex: hasFilterableIndex,
		HasSearchableIndex: hasSearchableIndex,
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
	var items []Countable
	propertyLength := -1 // will be overwritten for string/text, signals not to add the other types.
	hasFilterableIndex := HasFilterableIndex(prop)
	hasSearchableIndex := HasSearchableIndex(prop)

	switch dt := schema.DataType(prop.DataType[0]); dt {
	case schema.DataTypeText:
		hasFilterableIndex = hasFilterableIndex && !a.isFallbackToSearchable()
		asString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type string, but got %T", prop.Name, value)
		}
		items = a.Text(prop.Tokenization, asString)
		propertyLength = utf8.RuneCountInString(asString)
	case schema.DataTypeInt:
		if asFloat, ok := value.(float64); ok {
			// unmarshaling from json into a dynamic schema will assume every number
			// is a float64
			value = int64(asFloat)
		}

		if asInt, ok := value.(int); ok {
			// when merging an existing object we may retrieve an untyped int
			value = int64(asInt)
		}

		asInt, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type int64, but got %T", prop.Name, value)
		}

		var err error
		items, err = a.Int(asInt)
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	case schema.DataTypeNumber:
		asFloat, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type float64, but got %T", prop.Name, value)
		}

		var err error
		items, err = a.Float(asFloat) // convert to int before analyzing
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	case schema.DataTypeBoolean:
		asBool, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type bool, but got %T", prop.Name, value)
		}

		var err error
		items, err = a.Bool(asBool) // convert to int before analyzing
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	case schema.DataTypeDate:
		var err error
		if asString, ok := value.(string); ok {
			// for example when patching the date may have been loaded as a string
			value, err = time.Parse(time.RFC3339Nano, asString)
			if err != nil {
				return nil, fmt.Errorf("parse stringified timestamp: %w", err)
			}
		}
		asTime, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be time.Time, but got %T", prop.Name, value)
		}

		items, err = a.Int(asTime.UnixNano())
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	case schema.DataTypeUUID:
		var err error

		if asString, ok := value.(string); ok {
			// for example when patching the uuid may have been loaded as a string
			value, err = uuid.Parse(asString)
			if err != nil {
				return nil, fmt.Errorf("parse stringified uuid: %w", err)
			}
		}

		asUUID, ok := value.(uuid.UUID)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be uuid.UUID, but got %T", prop.Name, value)
		}

		items, err = a.UUID(asUUID)
		if err != nil {
			return nil, fmt.Errorf("analyze property %s: %w", prop.Name, err)
		}
	default:
		// ignore unsupported prop type
		return nil, nil
	}

	return &Property{
		Name:               prop.Name,
		Items:              items,
		Length:             propertyLength,
		HasFilterableIndex: hasFilterableIndex,
		HasSearchableIndex: hasSearchableIndex,
	}, nil
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
		// due to the fix introduced in https://github.com/weaviate/weaviate/pull/2320,
		// MultipleRef's can appear as empty []any when no actual refs are provided for
		// an object's reference property.
		//
		// if we encounter []any, assume it indicates an empty ref prop, and skip it.
		_, ok := value.([]any)
		if !ok {
			return fmt.Errorf("expected property %q to be of type models.MutlipleRef,"+
				" but got %T", prop.Name, value)
		}
		return nil
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
		HasFilterableIndex: HasFilterableIndex(prop),
		HasSearchableIndex: HasSearchableIndex(prop),
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

func HasInvertedIndex(prop *models.Property) bool {
	return HasFilterableIndex(prop) || HasSearchableIndex(prop)
}

const (
	// always
	HasFilterableIndexIdProp = true
	HasSearchableIndexIdProp = false

	// only if index.invertedIndexConfig.IndexTimestamps set
	HasFilterableIndexTimestampProp = true
	HasSearchableIndexTimestampProp = false

	// only if property.indexFilterable or property.indexSearchable set
	HasFilterableIndexMetaCount = true
	HasSearchableIndexMetaCount = false

	// only if index.invertedIndexConfig.IndexNullState set
	// and either property.indexFilterable or property.indexSearchable set
	HasFilterableIndexPropNull = true
	HasSearchableIndexPropNull = false

	// only if index.invertedIndexConfig.IndexPropertyLength set
	// and either property.indexFilterable or property.indexSearchable set
	HasFilterableIndexPropLength = true
	HasSearchableIndexPropLength = false
)
