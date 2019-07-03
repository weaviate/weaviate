/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package fetch

import (
	"fmt"
	"strings"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/state"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeNameSource struct{}

func (f *fakeNameSource) MustGetMappedPropertyName(className schema.ClassName,
	propName schema.PropertyName) state.MappedPropertyName {
	switch className {
	case "City":
		switch propName {
		case schema.PropertyName("name"):
			return "prop_1"
		case schema.PropertyName("population"):
			return "prop_2"
		case schema.PropertyName("inCountry"):
			return "prop_3"
		case schema.PropertyName("isCapital"):
			return "prop_4"
		case schema.PropertyName("inContinent"):
			return "prop_5"
		case schema.PropertyName("area"):
			return "prop_6"
		case schema.PropertyName("dateOfFirstAppearance"):
			return "prop_7"
		}
	case "Town":
		switch propName {
		case schema.PropertyName("title"):
			return "prop_11"
		case schema.PropertyName("inhabitants"):
			return "prop_12"
		case schema.PropertyName("inCountry"):
			return "prop_13"
		case schema.PropertyName("isCapital"):
			return "prop_14"
		case schema.PropertyName("inContinent"):
			return "prop_15"
		case schema.PropertyName("area"):
			return "prop_16"
		case schema.PropertyName("dateOfFirstAppearance"):
			return "prop_17"
		}
	}
	panic(fmt.Sprintf("fake name source does not contain a fake for '%s.%s'", className, propName))
}

func (f *fakeNameSource) MustGetMappedClassName(className schema.ClassName) state.MappedClassName {
	switch className {
	case schema.ClassName("City"):
		return state.MappedClassName("class_18")
	case schema.ClassName("Town"):
		return state.MappedClassName("class_19")
	default:
		panic(fmt.Sprintf("fake name source does not contain a fake for '%s'", className))
	}
}

func (f *fakeNameSource) GetClassNameFromMapped(className state.MappedClassName) schema.ClassName {
	switch className {
	case "class_18":
		return schema.ClassName("City")
	default:
		panic(fmt.Sprintf("fake name source does not contain a class for mapped id '%s'", className))
	}
}

type fakeTypeSource struct{}

func (f *fakeTypeSource) GetProperty(kind kind.Kind, className schema.ClassName,
	propName schema.PropertyName) (error, *models.Property) {

	switch className {
	case "City":
		switch propName {
		case "isCapital":
			return nil, &models.Property{DataType: []string{"bool"}}
		case "population":
			return nil, &models.Property{DataType: []string{"int"}}
		case "area":
			return nil, &models.Property{DataType: []string{"number"}}
		case "name":
			return nil, &models.Property{DataType: []string{"string"}}
		case "dateOfFirstAppearance":
			return nil, &models.Property{DataType: []string{"date"}}
		case "inCountry":
			return nil, &models.Property{DataType: []string{"Country"}}
		}
	case "Town":
		switch propName {
		case "isCapital":
			return nil, &models.Property{DataType: []string{"bool"}}
		case "inhabitants":
			return nil, &models.Property{DataType: []string{"int"}}
		case "area":
			return nil, &models.Property{DataType: []string{"number"}}
		case "title":
			return nil, &models.Property{DataType: []string{"string"}}
		case "dateOfFirstAppearance":
			return nil, &models.Property{DataType: []string{"date"}}
		case "inCountry":
			return nil, &models.Property{DataType: []string{"Country"}}
		}
	}

	return fmt.Errorf("fake type source does not have an implementation for prop '%s'", propName), nil
}

func (f *fakeTypeSource) FindPropertyDataType(dataType []string) (schema.PropertyDataType, error) {
	switch dataType[0] {
	case "bool":
		return &fakeDataType{dataType: schema.DataTypeBoolean}, nil
	case "int":
		return &fakeDataType{dataType: schema.DataTypeInt}, nil
	case "number":
		return &fakeDataType{dataType: schema.DataTypeNumber}, nil
	case "string":
		return &fakeDataType{dataType: schema.DataTypeString}, nil
	case "date":
		return &fakeDataType{dataType: schema.DataTypeDate}, nil
	case "Country":
		return &fakeDataType{dataType: schema.DataTypeCRef}, nil
	}

	return nil, fmt.Errorf("fake type source does not have an implementation for dataType '%v'", dataType)
}

type fakeDataType struct {
	dataType schema.DataType
}

func (p *fakeDataType) Kind() schema.PropertyKind {
	panic("not implemented")
}

func (p *fakeDataType) IsPrimitive() bool {
	return p.dataType != schema.DataTypeCRef
}

func (p *fakeDataType) AsPrimitive() schema.DataType {
	return p.dataType
}

func (p *fakeDataType) IsReference() bool {
	return p.dataType == schema.DataTypeCRef
}

func (p *fakeDataType) Classes() []schema.ClassName {
	return []schema.ClassName{"Country", "WeaviateB/Country"}
}

func (p *fakeDataType) ContainsClass(needle schema.ClassName) bool {
	panic("not implemented")
}

type testCase struct {
	name          string
	inputParams   traverser.FetchParams
	expectedQuery string
	expectedErr   error
}

type testCases []testCase

func (tests testCases) AssertQuery(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := NewQuery(test.inputParams, &fakeNameSource{}, &fakeTypeSource{}).String()
			require.Equal(t, test.expectedErr, err, "should match expected error")
			assert.Equal(t, breakOnDot(stripAll(test.expectedQuery)), breakOnDot(stripAll(query)), "should match the query")
		})
	}
}

func stripAll(input string) string {
	input = strings.Replace(input, " ", "", -1)
	input = strings.Replace(input, "\t", "", -1)
	input = strings.Replace(input, "\n", "", -1)
	return input
}

func breakOnDot(input string) string {
	// input = strings.Replace(input, ".", "\n.", -1)
	return input
}
