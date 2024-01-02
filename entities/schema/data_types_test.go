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

package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestDetectPrimitiveTypes(t *testing.T) {
	s := Empty()

	for _, dt := range append(PrimitiveDataTypes, DeprecatedPrimitiveDataTypes...) {
		pdt, err := s.FindPropertyDataType(dt.PropString())

		assert.Nil(t, err)
		assert.True(t, pdt.IsPrimitive())
		assert.Equal(t, dt, pdt.AsPrimitive())

		assert.False(t, pdt.IsNested())
		assert.False(t, pdt.IsReference())
	}
}

func TestDetectNestedTypes(t *testing.T) {
	s := Empty()

	for _, dt := range NestedDataTypes {
		ndt, err := s.FindPropertyDataType(dt.PropString())

		assert.Nil(t, err)
		assert.True(t, ndt.IsNested())
		assert.Equal(t, dt, ndt.AsNested())

		assert.False(t, ndt.IsPrimitive())
		assert.False(t, ndt.IsReference())
	}
}

func TestExistingClassSingleRef(t *testing.T) {
	className := "ExistingClass"
	s := Empty()
	s.Objects.Classes = []*models.Class{{Class: className}}

	pdt, err := s.FindPropertyDataType([]string{className})

	assert.Nil(t, err)
	assert.True(t, pdt.IsReference())
	assert.True(t, pdt.ContainsClass(ClassName(className)))
}

func TestNonExistingClassSingleRef(t *testing.T) {
	className := "NonExistingClass"
	s := Empty()

	pdt, err := s.FindPropertyDataType([]string{className})

	assert.EqualError(t, err, ErrRefToNonexistentClass.Error())
	assert.Nil(t, pdt)
}

func TestNonExistingClassRelaxedCrossValidation(t *testing.T) {
	className := "NonExistingClass"
	s := Empty()

	pdt, err := s.FindPropertyDataTypeWithRefs([]string{className}, true, ClassName("AnotherNonExistingClass"))

	assert.Nil(t, err)
	assert.True(t, pdt.IsReference())
	assert.True(t, pdt.ContainsClass(ClassName(className)))
}

func TestNonExistingClassPropertyBelongsTo(t *testing.T) {
	className := "NonExistingClass"
	s := Empty()

	pdt, err := s.FindPropertyDataTypeWithRefs([]string{className}, false, ClassName(className))

	assert.Nil(t, err)
	assert.True(t, pdt.IsReference())
	assert.True(t, pdt.ContainsClass(ClassName(className)))
}

func TestGetPropertyDataType(t *testing.T) {
	class := &models.Class{Class: "TestClass"}
	dataTypes := []string{
		"string", "text", "int", "number", "boolean",
		"date", "geoCoordinates", "phoneNumber", "blob", "Ref", "invalid",
		"string[]", "text[]", "int[]", "number[]", "boolean[]", "date[]",
		"uuid", "uuid[]",

		"object", "object[]",
	}
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
		{
			propName:         "stringProp",
			expectedDataType: ptDataType(DataTypeString),
		},
		{
			propName:         "textProp",
			expectedDataType: ptDataType(DataTypeText),
		},
		{
			propName:         "numberProp",
			expectedDataType: ptDataType(DataTypeNumber),
		},
		{
			propName:         "intProp",
			expectedDataType: ptDataType(DataTypeInt),
		},
		{
			propName:         "booleanProp",
			expectedDataType: ptDataType(DataTypeBoolean),
		},
		{
			propName:         "dateProp",
			expectedDataType: ptDataType(DataTypeDate),
		},
		{
			propName:         "phoneNumberProp",
			expectedDataType: ptDataType(DataTypePhoneNumber),
		},
		{
			propName:         "geoCoordinatesProp",
			expectedDataType: ptDataType(DataTypeGeoCoordinates),
		},
		{
			propName:         "blobProp",
			expectedDataType: ptDataType(DataTypeBlob),
		},
		{
			propName:         "string[]Prop",
			expectedDataType: ptDataType(DataTypeStringArray),
		},
		{
			propName:         "text[]Prop",
			expectedDataType: ptDataType(DataTypeTextArray),
		},
		{
			propName:         "int[]Prop",
			expectedDataType: ptDataType(DataTypeIntArray),
		},
		{
			propName:         "number[]Prop",
			expectedDataType: ptDataType(DataTypeNumberArray),
		},
		{
			propName:         "boolean[]Prop",
			expectedDataType: ptDataType(DataTypeBooleanArray),
		},
		{
			propName:         "date[]Prop",
			expectedDataType: ptDataType(DataTypeDateArray),
		},
		{
			propName:         "uuidProp",
			expectedDataType: ptDataType(DataTypeUUID),
		},
		{
			propName:         "uuid[]Prop",
			expectedDataType: ptDataType(DataTypeUUIDArray),
		},
		{
			propName:         "objectProp",
			expectedDataType: ptDataType(DataTypeObject),
		},
		{
			propName:         "object[]Prop",
			expectedDataType: ptDataType(DataTypeObjectArray),
		},
		{
			propName:         "RefProp",
			expectedDataType: ptDataType(DataTypeCRef),
		},
		{
			propName:         "wrongProp",
			expectedDataType: nil,
			expectedErr:      fmt.Errorf("no such prop with name 'wrongProp' found in class 'TestClass' in the schema. Check your schema files for which properties in this class are available"),
		},
		{
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

func Test_DataType_AsPrimitive(t *testing.T) {
	type testCase struct {
		name                string
		inputDataType       []string
		expectedDataType    DataType
		expectedIsPrimitive bool
	}

	runTestCases := func(t *testing.T, testCases []testCase) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dataType, ok := AsPrimitive(tc.inputDataType)
				assert.Equal(t, tc.expectedDataType, dataType)
				assert.Equal(t, tc.expectedIsPrimitive, ok)
			})
		}
	}

	t.Run("is primitive data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, dt := range append(PrimitiveDataTypes, DeprecatedPrimitiveDataTypes...) {
			inputDataType := dt.PropString()
			testCases = append(testCases, testCase{
				name:                fmt.Sprintf("%v", inputDataType),
				inputDataType:       inputDataType,
				expectedDataType:    dt,
				expectedIsPrimitive: true,
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("is empty data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, dtStr := range []string{""} {
			inputDataType := []string{dtStr}
			testCases = append(testCases, testCase{
				name:                fmt.Sprintf("%v", inputDataType),
				inputDataType:       inputDataType,
				expectedDataType:    "",
				expectedIsPrimitive: true,
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("is non existent data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, dtStr := range []string{"non-existent"} {
			inputDataType := []string{dtStr}
			testCases = append(testCases, testCase{
				name:                fmt.Sprintf("%v", inputDataType),
				inputDataType:       inputDataType,
				expectedDataType:    "",
				expectedIsPrimitive: false,
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("is nested data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, dt := range NestedDataTypes {
			inputDataType := dt.PropString()
			testCases = append(testCases, testCase{
				name:                fmt.Sprintf("%v", inputDataType),
				inputDataType:       inputDataType,
				expectedDataType:    "",
				expectedIsPrimitive: false,
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("is reference data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, inputDataType := range [][]string{
			{"SomeClass"},
			{"SomeOtherClass", "AndAnotherOne"},
		} {
			testCases = append(testCases, testCase{
				name:                fmt.Sprintf("%v", inputDataType),
				inputDataType:       inputDataType,
				expectedDataType:    "",
				expectedIsPrimitive: false,
			})
		}

		runTestCases(t, testCases)
	})
}

func Test_DataType_AsNested(t *testing.T) {
	type testCase struct {
		name             string
		inputDataType    []string
		expectedDataType DataType
		expectedIsNested bool
	}

	runTestCases := func(t *testing.T, testCases []testCase) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dataType, ok := AsNested(tc.inputDataType)
				assert.Equal(t, tc.expectedDataType, dataType)
				assert.Equal(t, tc.expectedIsNested, ok)
			})
		}
	}

	t.Run("is nested data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, dt := range NestedDataTypes {
			inputDataType := dt.PropString()
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%v", inputDataType),
				inputDataType:    inputDataType,
				expectedDataType: dt,
				expectedIsNested: true,
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("is empty data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, dtStr := range []string{""} {
			inputDataType := []string{dtStr}
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%v", inputDataType),
				inputDataType:    inputDataType,
				expectedDataType: "",
				expectedIsNested: false,
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("is non existent data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, dtStr := range []string{"non-existent"} {
			inputDataType := []string{dtStr}
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%v", inputDataType),
				inputDataType:    inputDataType,
				expectedDataType: "",
				expectedIsNested: false,
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("is primitive data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, dt := range append(PrimitiveDataTypes, DeprecatedPrimitiveDataTypes...) {
			inputDataType := dt.PropString()
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%v", inputDataType),
				inputDataType:    inputDataType,
				expectedDataType: "",
				expectedIsNested: false,
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("is reference data type", func(t *testing.T) {
		testCases := []testCase{}
		for _, inputDataType := range [][]string{
			{"SomeClass"},
			{"SomeOtherClass", "AndAnotherOne"},
		} {
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%v", inputDataType),
				inputDataType:    inputDataType,
				expectedDataType: "",
				expectedIsNested: false,
			})
		}

		runTestCases(t, testCases)
	})
}

func ptDataType(dt DataType) *DataType {
	return &dt
}
