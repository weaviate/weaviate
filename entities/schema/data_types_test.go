//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectPrimitiveTypes(t *testing.T) {
	s := &Schema{}

	for _, type_ := range PrimitiveDataTypes {
		pdt, err := s.FindPropertyDataType([]string{string(type_)})
		if err != nil {
			t.Fatal(err)
		}

		if !pdt.IsPrimitive() {
			t.Fatal("not primitive")
		}

		if pdt.AsPrimitive() != type_ {
			t.Fatal("wrong value")
		}
	}
}

func TestNonExistingClassSingleRef(t *testing.T) {
	s := Empty()

	pdt, err := s.FindPropertyDataType([]string{"NonExistingClass"})

	if err == nil {
		t.Fatal("Should have error")
	}

	if pdt != nil {
		t.Fatal("Should return nil result")
	}
}

func TestExistingClassSingleRef(t *testing.T) {
	s := Empty()

	s.Actions.Classes = append(s.Actions.Classes, &models.Class{
		Class: "ExistingClass",
	})

	pdt, err := s.FindPropertyDataType([]string{"ExistingClass"})

	if err != nil {
		t.Fatal(err)
	}

	if !pdt.IsReference() {
		t.Fatal("not single ref")
	}
}

func TestGetPropertyDataType(t *testing.T) {

	class := &models.Class{Class: "TestClass"}
	dataTypes := []string{"string", "text", "int", "number", "boolean",
		"date", "geoCoordinates", "phoneNumber", "Ref", "invalid"}
	class.Properties = make([]*models.Property, len(dataTypes))
	for i, dtString := range dataTypes {

		class.Properties[i] = &models.Property{
			Name:     dtString + "Prop",
			DataType: []string{dtString},
		}
	}

	type test struct {
		propName         string
		expectedDataType *DataType
		expectedErr      error
	}

	tests := []test{
		test{
			propName:         "stringProp",
			expectedDataType: ptDataType(DataTypeString),
		},
		test{
			propName:         "textProp",
			expectedDataType: ptDataType(DataTypeText),
		},
		test{
			propName:         "numberProp",
			expectedDataType: ptDataType(DataTypeNumber),
		},
		test{
			propName:         "intProp",
			expectedDataType: ptDataType(DataTypeInt),
		},
		test{
			propName:         "booleanProp",
			expectedDataType: ptDataType(DataTypeBoolean),
		},
		test{
			propName:         "dateProp",
			expectedDataType: ptDataType(DataTypeDate),
		},
		test{
			propName:         "phoneNumberProp",
			expectedDataType: ptDataType(DataTypePhoneNumber),
		},
		test{
			propName:         "geoCoordinatesProp",
			expectedDataType: ptDataType(DataTypeGeoCoordinates),
		},
		test{
			propName:         "RefProp",
			expectedDataType: ptDataType(DataTypeCRef),
		},
		test{
			propName:         "wrongProp",
			expectedDataType: nil,
			expectedErr:      fmt.Errorf("no such prop with name 'wrongProp' found in class 'TestClass' in the schema. Check your schema files for which properties in this class are available"),
		},
		test{
			propName:         "invalidProp",
			expectedDataType: nil,
			expectedErr:      fmt.Errorf("given value-DataType does not exist."),
		},
	}

	for _, test := range tests {
		t.Run(test.propName, func(t *testing.T) {
			dt, err := GetPropertyDataType(class, test.propName)
			require.Equal(t, test.expectedErr, err)
			assert.Equal(t, test.expectedDataType, dt)
		})
	}
}

func ptDataType(dt DataType) *DataType {
	return &dt
}
