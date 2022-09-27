//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (a *Analyzer) Object(input map[string]interface{}, props []*models.Property,
	uuid strfmt.UUID,
) ([]Property, error) {
	propsMap := map[string]*models.Property{}
	for _, prop := range props {
		propsMap[prop.Name] = prop
	}

	properties, err := a.analyzeProps(propsMap, input)
	if err != nil {
		return nil, errors.Wrap(err, "analyze props")
	}

	idProp, err := a.analyzeIDProp(uuid)
	if err != nil {
		return nil, errors.Wrap(err, "analyze uuid prop")
	}
	properties = append(properties, *idProp)

	tsProps, err := a.analyzeTimestampProps(input)
	if err != nil {
		return nil, errors.Wrap(err, "analyze timestamp props")
	}
	// tsProps will be nil here if weaviate is
	// not setup to index by timestamps
	if tsProps != nil {
		properties = append(properties, tsProps...)
	}

	return properties, nil
}

func (a *Analyzer) analyzeProps(propsMap map[string]*models.Property,
	input map[string]interface{},
) ([]Property, error) {
	var out []Property
	for key, prop := range propsMap {
		if len(prop.DataType) < 1 {
			return nil, fmt.Errorf("prop %q has no datatype", prop.Name)
		}

		if prop.IndexInverted != nil && !*prop.IndexInverted {
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
		return nil, errors.Wrap(err, "marshal id prop")
	}
	return &Property{
		Name:         filters.InternalPropID,
		HasFrequency: false,
		Items: []Countable{
			{
				Data: value,
			},
		},
	}, nil
}

func (a *Analyzer) analyzeTimestampProps(input map[string]interface{}) ([]Property, error) {
	createTime, createTimeOK := input[filters.InternalPropCreationTimeUnix]
	updateTime, updateTimeOK := input[filters.InternalPropLastUpdateTimeUnix]

	var props []Property
	if createTimeOK {
		b, err := json.Marshal(createTime)
		if err != nil {
			return nil, errors.Wrap(err, "analyze create timestamp prop")
		}
		props = append(props, Property{
			Name:  filters.InternalPropCreationTimeUnix,
			Items: []Countable{{Data: b}},
		})
	}

	if updateTimeOK {
		b, err := json.Marshal(updateTime)
		if err != nil {
			return nil, errors.Wrap(err, "analyze update timestamp prop")
		}
		props = append(props, Property{
			Name:  filters.InternalPropLastUpdateTimeUnix,
			Items: []Countable{{Data: b}},
		})
	}

	return props, nil
}

func (a *Analyzer) extendPropertiesWithArrayType(properties *[]Property,
	prop *models.Property, input map[string]interface{}, propName string,
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

	values, ok := value.([]interface{})
	if !ok {
		// skip any primitive prop that's not set
		return errors.New("analyze array prop: expected array prop")
	}

	property, err := a.analyzeArrayProp(prop, values)
	if err != nil {
		return errors.Wrap(err, "analyze primitive prop")
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
	prop *models.Property, input map[string]interface{}, propName string,
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
		return errors.Wrap(err, "analyze primitive prop")
	}
	if property == nil {
		return nil
	}

	*properties = append(*properties, *property)
	return nil
}

func HasFrequency(dt schema.DataType) bool {
	if dt == schema.DataTypeText || dt == schema.DataTypeString ||
		dt == schema.DataTypeStringArray || dt == schema.DataTypeTextArray {
		return true
	}

	return false
}

func (a *Analyzer) analyzeArrayProp(prop *models.Property, values []interface{}) (*Property, error) {
	var hasFrequency bool
	var items []Countable
	dt := schema.DataType(prop.DataType[0])
	switch dt {
	case schema.DataTypeTextArray:
		hasFrequency = HasFrequency(dt)
		in, err := stringsFromValues(prop, values)
		if err != nil {
			return nil, err
		}
		items = a.TextArray(prop.Tokenization, in)
	case schema.DataTypeStringArray:
		hasFrequency = HasFrequency(dt)
		in, err := stringsFromValues(prop, values)
		if err != nil {
			return nil, err
		}
		items = a.StringArray(prop.Tokenization, in)
	case schema.DataTypeIntArray:
		hasFrequency = HasFrequency(dt)
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
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	case schema.DataTypeNumberArray:
		hasFrequency = HasFrequency(dt)
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
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	case schema.DataTypeBooleanArray:
		hasFrequency = HasFrequency(dt)
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
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	case schema.DataTypeDateArray:
		hasFrequency = HasFrequency(dt)
		in := make([]int64, len(values))
		for i, value := range values {
			// dates can be either a date-string or directly a time object. Try to parse both
			if asTime, okTime := value.(time.Time); okTime {
				in[i] = asTime.UnixNano()
			} else if asString, okString := value.(string); okString {
				parsedTime, err := time.Parse(time.RFC3339Nano, asString)
				if err != nil {
					return nil, errors.Wrapf(err, "Time parsing")
				}
				in[i] = parsedTime.UnixNano()
			} else {
				return nil, fmt.Errorf("expected property %s to be a time-string or time object, but got %T", prop.Name, value)
			}
		}

		var err error
		items, err = a.IntArray(in)
		if err != nil {
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}

	default:
		// ignore unsupported prop type
		return nil, nil
	}

	return &Property{
		Name:         prop.Name,
		Items:        items,
		HasFrequency: hasFrequency,
		Length:       len(values),
	}, nil
}

func stringsFromValues(prop *models.Property, values []interface{}) ([]string, error) {
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

func (a *Analyzer) analyzePrimitiveProp(prop *models.Property, value interface{}) (*Property, error) {
	var hasFrequency bool
	var items []Countable
	PropertyLength := 1 // will be overwritten for string/text
	dt := schema.DataType(prop.DataType[0])
	switch dt {
	case schema.DataTypeText:
		hasFrequency = HasFrequency(dt)
		asString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type string, but got %T", prop.Name, value)
		}
		items = a.Text(prop.Tokenization, asString)
		PropertyLength = utf8.RuneCountInString(asString)
	case schema.DataTypeString:
		hasFrequency = HasFrequency(dt)
		asString, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type string, but got %T", prop.Name, value)
		}
		items = a.String(prop.Tokenization, asString)
		PropertyLength = utf8.RuneCountInString(asString)
	case schema.DataTypeInt:
		hasFrequency = HasFrequency(dt)
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
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	case schema.DataTypeNumber:
		hasFrequency = HasFrequency(dt)
		asFloat, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type float64, but got %T", prop.Name, value)
		}

		var err error
		items, err = a.Float(asFloat) // convert to int before analyzing
		if err != nil {
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	case schema.DataTypeBoolean:
		hasFrequency = HasFrequency(dt)
		asBool, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be of type bool, but got %T", prop.Name, value)
		}

		var err error
		items, err = a.Bool(asBool) // convert to int before analyzing
		if err != nil {
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	case schema.DataTypeDate:
		hasFrequency = HasFrequency(dt)
		var err error
		if asString, ok := value.(string); ok {
			// for example when patching the date may have been loaded as a string
			value, err = time.Parse(time.RFC3339Nano, asString)
			if err != nil {
				return nil, errors.Wrap(err, "parse stringified timestamp")
			}
		}
		asTime, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected property %s to be time.Time, but got %T", prop.Name, value)
		}

		items, err = a.Int(asTime.UnixNano())
		if err != nil {
			return nil, errors.Wrapf(err, "analyze property %s", prop.Name)
		}
	default:
		// ignore unsupported prop type
		return nil, nil
	}

	return &Property{
		Name:         prop.Name,
		Items:        items,
		HasFrequency: hasFrequency,
		Length:       PropertyLength,
	}, nil
}

// extendPropertiesWithReference extends the specified properties arrays with
// either 1 or 2 entries: If the ref is not set, only the ref-count property
// will be added. If the ref is set the ref-prop itself will also be added and
// contain all references as values
func (a *Analyzer) extendPropertiesWithReference(properties *[]Property,
	prop *models.Property, input map[string]interface{}, propName string,
) error {
	value, ok := input[propName]
	if !ok {
		// explicitly set zero-value, so we can index for "ref not set"
		value = make(models.MultipleRef, 0)
	}

	asRefs, ok := value.(models.MultipleRef)
	if !ok {
		return fmt.Errorf("expected property %q to be of type models.MutlipleRef,"+
			" but got %T", prop.Name, value)
	}

	property, err := a.analyzeRefPropCount(prop, asRefs)
	if err != nil {
		return errors.Wrap(err, "ref count")
	}

	*properties = append(*properties, *property)

	if len(asRefs) == 0 {
		return nil
	}

	property, err = a.analyzeRefProp(prop, asRefs)
	if err != nil {
		return errors.Wrap(err, "refs")
	}

	*properties = append(*properties, *property)
	return nil
}

func (a *Analyzer) analyzeRefPropCount(prop *models.Property,
	value models.MultipleRef,
) (*Property, error) {
	items, err := a.RefCount(value)
	if err != nil {
		return nil, errors.Wrapf(err, "analyze ref-property %q", prop.Name)
	}

	return &Property{
		Name:         helpers.MetaCountProp(prop.Name),
		Items:        items,
		HasFrequency: false,
		Length:       len(value),
	}, nil
}

func (a *Analyzer) analyzeRefProp(prop *models.Property,
	value models.MultipleRef,
) (*Property, error) {
	items, err := a.Ref(value)
	if err != nil {
		return nil, errors.Wrapf(err, "analyze ref-property %q", prop.Name)
	}

	return &Property{
		Name:         prop.Name,
		Items:        items,
		HasFrequency: false,
	}, nil
}

func typedSliceToUntyped(in interface{}) ([]interface{}, error) {
	switch typed := in.(type) {
	case []interface{}:
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
	default:
		return nil, errors.Errorf("unsupported type %T", in)
	}
}

func convertToUntyped[T comparable](in []T) []interface{} {
	out := make([]interface{}, len(in))
	for i := range out {
		out[i] = in[i]
	}
	return out
}
