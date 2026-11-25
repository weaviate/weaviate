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

package filters

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type fakeFinder struct {
	mock.Mock
}

func (f *fakeFinder) ReadOnlyClass(name string) (*models.Class, error) {
	args := f.Called(name)
	model := args.Get(0)
	if model == nil {
		return nil, nil
	}
	return model.(*models.Class), nil
}

func TestValidateIsNullOperator(t *testing.T) {
	tests := []struct {
		name       string
		schemaType schema.DataType
		valid      bool
	}{
		{
			name:       "Valid datatype",
			schemaType: schema.DataTypeBoolean,
			valid:      true,
		},
		{
			name:       "Invalid datatype (array)",
			schemaType: schema.DataTypeBooleanArray,
			valid:      false,
		},
		{
			name:       "Invalid datatype (text)",
			schemaType: schema.DataTypeText,
			valid:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := Clause{
				Operator: OperatorIsNull,
				Value:    &Value{Value: true, Type: tt.schemaType},
				On:       &Path{Class: "Car", Property: "horsepower"},
			}

			f := &fakeFinder{}
			f.On("ReadOnlyClass", mock.Anything).Return(
				&models.Class{
					Class: "Car",
					Properties: []*models.Property{
						{Name: "modelName", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
						{Name: "manufacturerName", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
						{Name: "horsepower", DataType: []string{"int"}},
					},
				},
			)
			err := validateClause(f.ReadOnlyClass, newClauseWrapper(&cl))
			if tt.valid {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}

func TestValidatePropertyLength(t *testing.T) {
	tests := []struct {
		name       string
		schemaType schema.DataType
		valid      bool
		operator   Operator
		value      int
	}{
		{
			name:       "Valid datatype and operator",
			schemaType: schema.DataTypeInt,
			valid:      true,
			operator:   OperatorEqual,
			value:      0,
		},
		{
			name:       "Invalid datatype (array)",
			schemaType: schema.DataTypeBooleanArray,
			valid:      false,
			operator:   OperatorEqual,
			value:      1,
		},
		{
			name:       "Invalid datatype (text)",
			schemaType: schema.DataTypeText,
			valid:      false,
			operator:   OperatorEqual,
			value:      2,
		},
		{
			name:       "Invalid operator (Or)",
			schemaType: schema.DataTypeText,
			valid:      false,
			operator:   OperatorOr,
			value:      10,
		},
		{
			name:       "Invalid value (negative)",
			schemaType: schema.DataTypeText,
			valid:      false,
			operator:   OperatorEqual,
			value:      -5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := Clause{
				Operator: OperatorEqual,
				Value:    &Value{Value: tt.value, Type: tt.schemaType},
				On:       &Path{Class: "Car", Property: "len(horsepower)"},
			}

			f := &fakeFinder{}
			f.On("ReadOnlyClass", mock.Anything).Return(
				&models.Class{
					Class: "Car",
					Properties: []*models.Property{
						{Name: "horsepower", DataType: []string{"int"}},
					},
				},
			)
			err := validateClause(f.ReadOnlyClass, newClauseWrapper(&cl))
			if tt.valid {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}

func TestValidateUUIDFilter(t *testing.T) {
	tests := []struct {
		name       string
		schemaType schema.DataType
		valid      bool
		operator   Operator
		value      int
	}{
		{
			name:       "Valid datatype and operator",
			schemaType: schema.DataTypeText,
			valid:      true,
			operator:   OperatorEqual,
			value:      0,
		},
		{
			name:       "Wrong data type (int)",
			schemaType: schema.DataTypeInt,
			valid:      false,
			operator:   OperatorEqual,
			value:      0,
		},
		{
			name:       "Wrong operator (Like)",
			schemaType: schema.DataTypeText,
			valid:      false,
			operator:   OperatorLike,
			value:      0,
		},

		{
			name:       "[deprecated string] Valid datatype and operator",
			schemaType: schema.DataTypeString,
			valid:      true,
			operator:   OperatorEqual,
			value:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, prop := range []schema.PropertyName{"my_id", "my_idz"} {
				cl := Clause{
					Operator: tt.operator,
					Value:    &Value{Value: tt.value, Type: tt.schemaType},
					On:       &Path{Class: "Car", Property: prop},
				}

				f := &fakeFinder{}
				f.On("ReadOnlyClass", mock.Anything).Return(
					&models.Class{
						Class: "Car",
						Properties: []*models.Property{
							{Name: "my_id", DataType: []string{string(schema.DataTypeUUID)}},
							{Name: "my_idz", DataType: []string{string(schema.DataTypeUUIDArray)}},
						},
					},
				)
				err := validateClause(f.ReadOnlyClass, newClauseWrapper(&cl))
				if tt.valid {
					require.Nil(t, err)
				} else {
					require.NotNil(t, err)
				}
			}
		})
	}
}

func TestClauseWrapper(t *testing.T) {
	type testCase struct {
		name         string
		valueType    schema.DataType
		requiredType schema.DataType

		expectedValid     bool
		expectedValueName string
	}

	testCases := []testCase{
		{
			name:              "string accepted where text is required",
			valueType:         schema.DataTypeString,
			requiredType:      schema.DataTypeText,
			expectedValid:     true,
			expectedValueName: "valueString",
		},
		{
			name:              "text accepted where text is required",
			valueType:         schema.DataTypeText,
			requiredType:      schema.DataTypeText,
			expectedValid:     true,
			expectedValueName: "valueText",
		},
		{
			name:              "string[] accepted where text[] is required",
			valueType:         schema.DataTypeStringArray,
			requiredType:      schema.DataTypeTextArray,
			expectedValid:     true,
			expectedValueName: "valueString[]",
		},
		{
			name:              "text[] accepted where text[] is required",
			valueType:         schema.DataTypeTextArray,
			requiredType:      schema.DataTypeTextArray,
			expectedValid:     true,
			expectedValueName: "valueText[]",
		},
		{
			name:              "text not accepted where string is required",
			valueType:         schema.DataTypeText,
			requiredType:      schema.DataTypeString,
			expectedValid:     false,
			expectedValueName: "valueText",
		},
		{
			name:              "text[] not accepted where string[] is required",
			valueType:         schema.DataTypeTextArray,
			requiredType:      schema.DataTypeStringArray,
			expectedValid:     false,
			expectedValueName: "valueText[]",
		},
		{
			name:              "int not accepted where boolean is required",
			valueType:         schema.DataTypeInt,
			requiredType:      schema.DataTypeBoolean,
			expectedValid:     false,
			expectedValueName: "valueInt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clause := Clause{
				Operator: OperatorEqual,
				Value:    &Value{Value: "someValue", Type: tc.valueType},
				On:       &Path{Class: "SomeClass", Property: "someProperty"},
			}

			cw := newClauseWrapper(&clause)

			assert.Equal(t, tc.expectedValid, cw.isType(tc.requiredType))
			assert.Equal(t, tc.expectedValueName, cw.getValueNameFromType())

			assert.Equal(t, "someValue", cw.getValue())
			assert.Equal(t, schema.PropertyName("someProperty"), cw.getPropertyName())
			assert.Equal(t, OperatorEqual, cw.getOperator())
			assert.Nil(t, cw.getOperands())

			t.Run("clause is updated to required type if valid", func(t *testing.T) {
				cw.updateClause()

				if tc.expectedValid {
					assert.Equal(t, tc.requiredType, clause.Value.Type)
				} else {
					assert.Equal(t, tc.valueType, clause.Value.Type)
				}
			})
		})
	}
}

type deniedError string

func (e deniedError) Error() string { return "denied " + string(e) }

func TestValidateFiltersAuthorizesWholePath(t *testing.T) {
	classes := map[string]*models.Class{
		"Article": {Class: "Article", Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "author", DataType: []string{"Author"}},
		}},
		"Author": {Class: "Author", Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString()},
			{Name: "employer", DataType: []string{"Company"}},
		}},
		"Company": {Class: "Company", Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString()},
		}},
	}

	// path takes the elements of a filter path with the root class prepended
	path := func(elements ...string) *Path {
		var p *Path
		for i := len(elements) - 2; i >= 0; i -= 2 {
			p = &Path{Class: schema.ClassName(elements[i]), Property: schema.PropertyName(elements[i+1]), Child: p}
		}
		return p
	}
	clause := func(dt schema.DataType, value any, elements ...string) Clause {
		return Clause{Operator: OperatorEqual, On: path(elements...), Value: &Value{Value: value, Type: dt}}
	}
	text := func(elements ...string) Clause { return clause(schema.DataTypeText, "x", elements...) }
	number := func(elements ...string) Clause { return clause(schema.DataTypeInt, 1, elements...) }
	date := func(elements ...string) Clause { return clause(schema.DataTypeDate, "x", elements...) }
	compound := func(op Operator, operands ...Clause) Clause { return Clause{Operator: op, Operands: operands} }

	twoHops := text("Article", "author", "Author", "employer", "Company", "name")
	oneHop := text("Article", "author", "Author", "name")
	root := text("Article", "title")

	tests := []struct {
		name   string
		clause Clause
		deny   string
		// absent is authorized but not in the schema
		absent      string
		expectAsked []string
		expectErr   string
	}{
		{name: "root property", clause: root, expectAsked: []string{"Article"}},
		{name: "one hop", clause: oneHop, expectAsked: []string{"Article", "Author"}},
		{name: "two hops", clause: twoHops, expectAsked: []string{"Article", "Author", "Company"}},
		{name: "_id at root", clause: text("Article", InternalPropID), expectAsked: []string{"Article"}},
		{name: "id at root", clause: text("Article", InternalPropBackwardsCompatID), expectAsked: []string{"Article"}},
		{name: "creation time at root", clause: date("Article", InternalPropCreationTimeUnix), expectAsked: []string{"Article"}},
		{name: "update time at root", clause: date("Article", InternalPropLastUpdateTimeUnix), expectAsked: []string{"Article"}},
		{name: "_id behind one hop", clause: text("Article", "author", "Author", InternalPropID), expectAsked: []string{"Article", "Author"}},
		{name: "creation time behind one hop", clause: date("Article", "author", "Author", InternalPropCreationTimeUnix), expectAsked: []string{"Article", "Author"}},
		{name: "update time behind two hops", clause: date("Article", "author", "Author", "employer", "Company", InternalPropLastUpdateTimeUnix), expectAsked: []string{"Article", "Author", "Company"}},
		{name: "len behind one hop", clause: number("Article", "author", "Author", "len(name)"), expectAsked: []string{"Article", "Author"}},
		{name: "reference count behind one hop", clause: number("Article", "author", "Author", "employer"), expectAsked: []string{"Article", "Author"}},
		{name: "And", clause: compound(OperatorAnd, root, oneHop), expectAsked: []string{"Article", "Article", "Author"}},
		{name: "Or", clause: compound(OperatorOr, oneHop, root), expectAsked: []string{"Article", "Author", "Article"}},
		{name: "Not", clause: compound(OperatorNot, twoHops), expectAsked: []string{"Article", "Author", "Company"}},

		{name: "denied at root", clause: twoHops, deny: "Article", expectAsked: []string{"Article"}, expectErr: "denied Article"},
		{name: "denied at middle", clause: twoHops, deny: "Author", expectAsked: []string{"Article", "Author"}, expectErr: "denied Author"},
		{name: "denied at leaf", clause: twoHops, deny: "Company", expectAsked: []string{"Article", "Author", "Company"}, expectErr: "denied Company"},
		{name: "denied at root for _id", clause: text("Article", InternalPropID), deny: "Article", expectAsked: []string{"Article"}, expectErr: "denied Article"},
		{name: "denied at leaf for creation time", clause: date("Article", "author", "Author", InternalPropCreationTimeUnix), deny: "Author", expectAsked: []string{"Article", "Author"}, expectErr: "denied Author"},
		{
			name:   "denied in a nested operand next to an invalid one",
			clause: compound(OperatorAnd, text("Article", "unknown"), compound(OperatorNot, twoHops)), deny: "Author",
			expectAsked: []string{"Article", "Article", "Author"}, expectErr: "denied Author",
		},

		{name: "absent at root for _id", clause: text("Article", InternalPropID), absent: "Article", expectAsked: []string{"Article"}, expectErr: `class "Article" does not exist in schema`},
		{name: "absent at leaf for _id", clause: text("Article", "author", "Author", InternalPropID), absent: "Author", expectAsked: []string{"Article", "Author"}, expectErr: `class "Author" does not exist in schema`},
		{name: "absent at middle", clause: twoHops, absent: "Author", expectAsked: []string{"Article", "Author"}, expectErr: `class "Author" does not exist in schema`},
		{name: "absent at leaf", clause: twoHops, absent: "Company", expectAsked: []string{"Article", "Author", "Company"}, expectErr: `class "Company" does not exist in schema`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var asked []string
			getClass := func(name string) (*models.Class, error) {
				asked = append(asked, name)
				if name == tt.deny {
					return nil, deniedError(name)
				}
				if name == tt.absent {
					return nil, nil
				}
				return classes[name], nil
			}

			err := ValidateFilters(getClass, &LocalFilter{Root: &tt.clause})

			assert.Equal(t, tt.expectAsked, asked)
			if tt.expectErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.expectErr)
			}
			var denied deniedError
			assert.Equal(t, tt.deny != "", errors.As(err, &denied))
		})
	}
}
