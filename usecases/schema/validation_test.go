/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package schema

import (
	"context"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Validation_ClassNames(t *testing.T) {
	type testCase struct {
		input    string
		valid    bool
		storedAs string
		name     string
	}

	// for all test cases keep in mind that the word "carrot" is not present in
	// the fake c11y, but every other word is
	//
	// all inputs represent class names (!)
	tests := []testCase{
		// valid names
		testCase{
			name:     "Single uppercase word present in the c11y",
			input:    "Car",
			valid:    true,
			storedAs: "Car",
		},
		testCase{
			name:     "Single lowercase word present in the c11y, stored as uppercase",
			input:    "car",
			valid:    true,
			storedAs: "Car",
		},
		testCase{
			name:     "combination of valid words starting with uppercase letter",
			input:    "CarGarage",
			valid:    true,
			storedAs: "CarGarage",
		},
		testCase{
			name:     "combination of valid words starting with lowercase letter, stored as uppercase",
			input:    "carGarage",
			valid:    true,
			storedAs: "CarGarage",
		},

		// inavlid names
		testCase{
			name:  "Single uppercase word NOT present in the c11y",
			input: "Carrot",
			valid: false,
		},
		testCase{
			name:  "Single lowercase word NOT present in the c11y",
			input: "carrot",
			valid: false,
		},
		testCase{
			name:  "combination of valid and invalid words, valid word first lowercased",
			input: "potatoCarrot",
			valid: false,
		},
		testCase{
			name:  "combination of valid and invalid words, valid word first uppercased",
			input: "PotatoCarrot",
			valid: false,
		},
		testCase{
			name:  "combination of valid and invalid words, invalid word first lowercased",
			input: "carrotPotato",
			valid: false,
		},
		testCase{
			name:  "combination of valid and invalid words, invalid word first uppercased",
			input: "CarrotPotato",
			valid: false,
		},
	}

	t.Run("adding a class", func(t *testing.T) {
		t.Run("different class names without keywords or properties", func(t *testing.T) {

			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: test.input,
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m, kind.Thing)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: test.input,
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m, kind.Action)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})
			}
		})

		t.Run("different class names with valid keywords", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: test.input,
						Keywords: models.SemanticSchemaKeywords{{
							Keyword: "something",
							Weight:  0.7,
						}},
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m, kind.Thing)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: test.input,
						Keywords: models.SemanticSchemaKeywords{{
							Keyword: "something",
							Weight:  0.7,
						}},
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m, kind.Action)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})
			}
		})
	})

	t.Run("updating an existing class", func(t *testing.T) {
		t.Run("different class names without keywords or properties", func(t *testing.T) {

			for _, test := range tests {
				originalName := "ValidOriginalName"
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: originalName,
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					require.Nil(t, err)

					// now try to update
					updatedClass := &models.SemanticSchemaClass{
						Class: test.input,
					}

					err = m.UpdateThing(context.Background(), originalName, updatedClass)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m, kind.Thing)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: originalName,
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					require.Nil(t, err)

					// now try to update
					updatedClass := &models.SemanticSchemaClass{
						Class: test.input,
					}

					err = m.UpdateAction(context.Background(), originalName, updatedClass)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m, kind.Action)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})

			}
		})

		t.Run("different class names with valid keywords", func(t *testing.T) {
			for _, test := range tests {
				originalName := "ValidOriginalName"

				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: originalName,
						Keywords: models.SemanticSchemaKeywords{{
							Keyword: "something",
							Weight:  0.7,
						}},
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					require.Nil(t, err)

					// now update
					updatedClass := &models.SemanticSchemaClass{
						Class:    test.input,
						Keywords: class.Keywords,
					}
					err = m.UpdateThing(context.Background(), originalName, updatedClass)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m, kind.Thing)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: originalName,
						Keywords: models.SemanticSchemaKeywords{{
							Keyword: "someaction",
							Weight:  0.7,
						}},
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					require.Nil(t, err)

					// now update
					updatedClass := &models.SemanticSchemaClass{
						Class:    test.input,
						Keywords: class.Keywords,
					}
					err = m.UpdateAction(context.Background(), originalName, updatedClass)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m, kind.Action)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})
			}
		})
	})
}

func Test_Validation_PropertyNames(t *testing.T) {
	type testCase struct {
		input    string
		valid    bool
		storedAs string
		name     string
	}

	// for all test cases keep in mind that the word "carrot" is not present in
	// the fake c11y, but every other word is
	//
	// all inputs represent property names (!)
	tests := []testCase{
		// valid names
		testCase{
			name:     "Single uppercase word present in the c11y, stored as lowercase",
			input:    "Brand",
			valid:    true,
			storedAs: "brand",
		},
		testCase{
			name:     "Single lowercase word present in the c11y",
			input:    "brand",
			valid:    true,
			storedAs: "brand",
		},
		testCase{
			name:     "combination of valid words starting with uppercase letter, stored as lowercase",
			input:    "BrandGarage",
			valid:    true,
			storedAs: "brandGarage",
		},
		testCase{
			name:     "combination of valid words starting with lowercase letter",
			input:    "brandGarage",
			valid:    true,
			storedAs: "brandGarage",
		},

		// inavlid names
		testCase{
			name:  "Single uppercase word NOT present in the c11y",
			input: "Carrot",
			valid: false,
		},
		testCase{
			name:  "Single lowercase word NOT present in the c11y",
			input: "carrot",
			valid: false,
		},
		testCase{
			name:  "combination of valid and invalid words, valid word first lowercased",
			input: "potatoCarrot",
			valid: false,
		},
		testCase{
			name:  "combination of valid and invalid words, valid word first uppercased",
			input: "PotatoCarrot",
			valid: false,
		},
		testCase{
			name:  "combination of valid and invalid words, invalid word first lowercased",
			input: "carrotPotato",
			valid: false,
		},
		testCase{
			name:  "combination of valid and invalid words, invalid word first uppercased",
			input: "CarrotPotato",
			valid: false,
		},
	}

	t.Run("when adding a new class", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {

			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{{
							DataType: []string{"string"},
							Name:     test.input,
						}},
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Things.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{{
							DataType: []string{"string"},
							Name:     test.input,
						}},
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Actions.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {

			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{{
							DataType: []string{"string"},
							Name:     test.input,
							Keywords: models.SemanticSchemaKeywords{{Keyword: "something", Weight: 0.7}},
						}},
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Things.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{{
							DataType: []string{"string"},
							Name:     test.input,
							Keywords: models.SemanticSchemaKeywords{{Keyword: "something", Weight: 0.7}},
						}},
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Actions.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})
	})

	t.Run("when updating an existing class with a new property", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {

			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					require.Nil(t, err)

					property := &models.SemanticSchemaClassProperty{
						DataType: []string{"string"},
						Name:     test.input,
					}
					err = m.AddThingProperty(context.Background(), "ValidName", property)
					t.Log(err)
					require.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Things.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					require.Nil(t, err)

					property := &models.SemanticSchemaClassProperty{
						DataType: []string{"string"},
						Name:     test.input,
					}
					err = m.AddActionProperty(context.Background(), "ValidName", property)
					t.Log(err)
					require.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Actions.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})

			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {

			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{{
							DataType: []string{"string"},
							Name:     test.input,
							Keywords: models.SemanticSchemaKeywords{{Keyword: "something", Weight: 0.7}},
						}},
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Things.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{{
							DataType: []string{"string"},
							Name:     test.input,
							Keywords: models.SemanticSchemaKeywords{{Keyword: "something", Weight: 0.7}},
						}},
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Actions.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})
	})

	t.Run("when updating an existing property with a new prop name", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {

			for _, test := range tests {
				originalName := "validPropertyName"

				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{
							&models.SemanticSchemaClassProperty{
								DataType: []string{"string"},
								Name:     originalName,
							},
						},
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					require.Nil(t, err)

					updatedProperty := &models.SemanticSchemaClassProperty{
						DataType: []string{"string"},
						Name:     test.input,
					}
					err = m.UpdateThingProperty(context.Background(), "ValidName", originalName, updatedProperty)
					t.Log(err)
					require.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Things.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{
							&models.SemanticSchemaClassProperty{
								DataType: []string{"string"},
								Name:     originalName,
							},
						},
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					require.Nil(t, err)

					updatedProperty := &models.SemanticSchemaClassProperty{
						DataType: []string{"string"},
						Name:     test.input,
					}
					err = m.UpdateActionProperty(context.Background(), "ValidName", originalName, updatedProperty)
					t.Log(err)
					require.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Actions.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})

			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {

			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{{
							DataType: []string{"string"},
							Name:     test.input,
							Keywords: models.SemanticSchemaKeywords{{Keyword: "something", Weight: 0.7}},
						}},
					}

					m := newSchemaManager()
					err := m.AddThing(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Things.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})

				t.Run(test.name+" as action class", func(t *testing.T) {
					class := &models.SemanticSchemaClass{
						Class: "ValidName",
						Properties: []*models.SemanticSchemaClassProperty{{
							DataType: []string{"string"},
							Name:     test.input,
							Keywords: models.SemanticSchemaKeywords{{Keyword: "something", Weight: 0.7}},
						}},
					}

					m := newSchemaManager()
					err := m.AddAction(context.Background(), class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					propName := m.GetSchema().Actions.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})
	})

}
