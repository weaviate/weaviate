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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_Validation_NestedProperties(t *testing.T) {
	t.Parallel()
	vFalse := false
	vTrue := true

	t.Run("does not validate wrong names", func(t *testing.T) {
		for _, name := range []string{"prop@1", "prop-2", "prop$3", "4prop"} {
			t.Run(name, func(t *testing.T) {
				nestedProperties := []*models.NestedProperty{
					{
						Name:              name,
						DataType:          schema.DataTypeInt.PropString(),
						IndexFilterable:   &vFalse,
						IndexSearchable:   &vFalse,
						IndexRangeFilters: &vFalse,
						Tokenization:      "",
					},
				}

				for _, ndt := range schema.NestedDataTypes {
					t.Run(ndt.String(), func(t *testing.T) {
						propPrimitives := &models.Property{
							Name:              "objectProp",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						}
						propLvl2Primitives := &models.Property{
							Name:              "objectPropLvl2",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties: []*models.NestedProperty{
								{
									Name:              "nested_object",
									DataType:          ndt.PropString(),
									IndexFilterable:   &vFalse,
									IndexSearchable:   &vFalse,
									IndexRangeFilters: &vFalse,
									Tokenization:      "",
									NestedProperties:  nestedProperties,
								},
							},
						}

						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
							t.Run(prop.Name, func(t *testing.T) {
								err := validateNestedProperties(prop.NestedProperties, prop.Name)
								assert.ErrorContains(t, err, prop.Name)
								assert.ErrorContains(t, err, "is not a valid nested property name")
							})
						}
					})
				}
			})
		}
	})

	t.Run("validates primitive data types", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{}
		for _, pdt := range schema.PrimitiveDataTypes {
			tokenization := ""
			switch pdt {
			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
				// skip - not supported as nested
				continue
			case schema.DataTypeText, schema.DataTypeTextArray:
				tokenization = models.PropertyTokenizationWord
			default:
				// do nothing
			}

			nestedProperties = append(nestedProperties, &models.NestedProperty{
				Name:              "nested_" + pdt.AsName(),
				DataType:          pdt.PropString(),
				IndexFilterable:   &vFalse,
				IndexSearchable:   &vFalse,
				IndexRangeFilters: &vFalse,
				Tokenization:      tokenization,
			})
		}

		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propPrimitives := &models.Property{
					Name:              "objectProp",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties:  nestedProperties,
				}
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						},
					},
				}

				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.NoError(t, err)
					})
				}
			})
		}
	})

	t.Run("does not validate deprecated primitive types", func(t *testing.T) {
		for _, pdt := range schema.DeprecatedPrimitiveDataTypes {
			t.Run(pdt.String(), func(t *testing.T) {
				nestedProperties := []*models.NestedProperty{
					{
						Name:              "nested_" + pdt.AsName(),
						DataType:          pdt.PropString(),
						IndexFilterable:   &vFalse,
						IndexSearchable:   &vFalse,
						IndexRangeFilters: &vFalse,
						Tokenization:      "",
					},
				}

				for _, ndt := range schema.NestedDataTypes {
					t.Run(ndt.String(), func(t *testing.T) {
						propPrimitives := &models.Property{
							Name:              "objectProp",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						}
						propLvl2Primitives := &models.Property{
							Name:              "objectPropLvl2",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties: []*models.NestedProperty{
								{
									Name:              "nested_object",
									DataType:          ndt.PropString(),
									IndexFilterable:   &vFalse,
									IndexSearchable:   &vFalse,
									IndexRangeFilters: &vFalse,
									Tokenization:      "",
									NestedProperties:  nestedProperties,
								},
							},
						}

						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
							t.Run(prop.Name, func(t *testing.T) {
								err := validateNestedProperties(prop.NestedProperties, prop.Name)
								assert.ErrorContains(t, err, prop.Name)
								assert.ErrorContains(t, err, fmt.Sprintf("data type '%s' is deprecated and not allowed as nested property", pdt.String()))
							})
						}
					})
				}
			})
		}
	})

	t.Run("does not validate unsupported primitive types", func(t *testing.T) {
		for _, pdt := range []schema.DataType{schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber} {
			t.Run(pdt.String(), func(t *testing.T) {
				nestedProperties := []*models.NestedProperty{
					{
						Name:              "nested_" + pdt.AsName(),
						DataType:          pdt.PropString(),
						IndexFilterable:   &vFalse,
						IndexSearchable:   &vFalse,
						IndexRangeFilters: &vFalse,
						Tokenization:      "",
					},
				}

				for _, ndt := range schema.NestedDataTypes {
					t.Run(ndt.String(), func(t *testing.T) {
						propPrimitives := &models.Property{
							Name:              "objectProp",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						}
						propLvl2Primitives := &models.Property{
							Name:              "objectPropLvl2",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties: []*models.NestedProperty{
								{
									Name:              "nested_object",
									DataType:          ndt.PropString(),
									IndexFilterable:   &vFalse,
									IndexSearchable:   &vFalse,
									IndexRangeFilters: &vFalse,
									Tokenization:      "",
									NestedProperties:  nestedProperties,
								},
							},
						}

						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
							t.Run(prop.Name, func(t *testing.T) {
								err := validateNestedProperties(prop.NestedProperties, prop.Name)
								assert.ErrorContains(t, err, prop.Name)
								assert.ErrorContains(t, err, fmt.Sprintf("data type '%s' not allowed as nested property", pdt.String()))
							})
						}
					})
				}
			})
		}
	})

	t.Run("does not validate ref types", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{
			{
				Name:              "nested_ref",
				DataType:          []string{"SomeClass"},
				IndexFilterable:   &vFalse,
				IndexSearchable:   &vFalse,
				IndexRangeFilters: &vFalse,
				Tokenization:      "",
			},
		}

		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propPrimitives := &models.Property{
					Name:              "objectProp",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties:  nestedProperties,
				}
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						},
					},
				}

				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.ErrorContains(t, err, prop.Name)
						assert.ErrorContains(t, err, "reference data type not allowed")
					})
				}
			})
		}
	})

	t.Run("does not validate empty nested properties", func(t *testing.T) {
		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propPrimitives := &models.Property{
					Name:              "objectProp",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
				}
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
						},
					},
				}

				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.ErrorContains(t, err, prop.Name)
						assert.ErrorContains(t, err, "At least one nested property is required for data type object/object[]")
					})
				}
			})
		}
	})

	t.Run("does not validate tokenization on non text/text[] primitive data types", func(t *testing.T) {
		for _, pdt := range schema.PrimitiveDataTypes {
			switch pdt {
			case schema.DataTypeText, schema.DataTypeTextArray:
				continue
			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
				// skip - not supported as nested
				continue
			default:
				// do nothing
			}

			t.Run(pdt.String(), func(t *testing.T) {
				nestedProperties := []*models.NestedProperty{
					{
						Name:              "nested_" + pdt.AsName(),
						DataType:          pdt.PropString(),
						IndexFilterable:   &vFalse,
						IndexSearchable:   &vFalse,
						IndexRangeFilters: &vFalse,
						Tokenization:      models.PropertyTokenizationWord,
					},
				}

				for _, ndt := range schema.NestedDataTypes {
					t.Run(ndt.String(), func(t *testing.T) {
						propPrimitives := &models.Property{
							Name:              "objectProp",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						}
						propLvl2Primitives := &models.Property{
							Name:              "objectPropLvl2",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties: []*models.NestedProperty{
								{
									Name:              "nested_object",
									DataType:          ndt.PropString(),
									IndexFilterable:   &vFalse,
									IndexSearchable:   &vFalse,
									IndexRangeFilters: &vFalse,
									Tokenization:      "",
									NestedProperties:  nestedProperties,
								},
							},
						}

						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
							t.Run(prop.Name, func(t *testing.T) {
								err := validateNestedProperties(prop.NestedProperties, prop.Name)
								assert.ErrorContains(t, err, prop.Name)
								assert.ErrorContains(t, err, fmt.Sprintf("Tokenization is not allowed for data type '%s'", pdt.String()))
							})
						}
					})
				}
			})
		}
	})

	t.Run("does not validate tokenization on nested data types", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{
			{
				Name:              "nested_int",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vFalse,
				IndexSearchable:   &vFalse,
				IndexRangeFilters: &vFalse,
				Tokenization:      "",
			},
		}

		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      models.PropertyTokenizationWord,
							NestedProperties:  nestedProperties,
						},
					},
				}

				for _, prop := range []*models.Property{propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.ErrorContains(t, err, prop.Name)
						assert.ErrorContains(t, err, "Tokenization is not allowed for object/object[] data types")
					})
				}
			})
		}
	})

	t.Run("validates indexFilterable on primitive data types", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{}
		for _, pdt := range schema.PrimitiveDataTypes {
			tokenization := ""
			switch pdt {
			case schema.DataTypeBlob:
				// skip - not indexable
				continue
			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
				// skip - not supported as nested
				continue
			case schema.DataTypeText, schema.DataTypeTextArray:
				tokenization = models.PropertyTokenizationWord
			default:
				// do nothing
			}

			nestedProperties = append(nestedProperties, &models.NestedProperty{
				Name:              "nested_" + pdt.AsName(),
				DataType:          pdt.PropString(),
				IndexFilterable:   &vTrue,
				IndexSearchable:   &vFalse,
				IndexRangeFilters: &vFalse,
				Tokenization:      tokenization,
			})
		}

		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propPrimitives := &models.Property{
					Name:              "objectProp",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties:  nestedProperties,
				}
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						},
					},
				}

				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.NoError(t, err)
					})
				}
			})
		}
	})

	t.Run("does not validate indexFilterable on blob data type", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{
			{
				Name:              "nested_blob",
				DataType:          schema.DataTypeBlob.PropString(),
				IndexFilterable:   &vTrue,
				IndexSearchable:   &vFalse,
				IndexRangeFilters: &vFalse,
				Tokenization:      "",
			},
		}

		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propPrimitives := &models.Property{
					Name:              "objectProp",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties:  nestedProperties,
				}
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						},
					},
				}

				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.ErrorContains(t, err, prop.Name)
						assert.ErrorContains(t, err, "indexFilterable is not allowed for blob data type")
					})
				}
			})
		}
	})

	t.Run("validates indexFilterable on nested data types", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{
			{
				Name:              "nested_int",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vFalse,
				IndexSearchable:   &vFalse,
				IndexRangeFilters: &vFalse,
				Tokenization:      "",
			},
		}

		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vTrue,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						},
					},
				}

				for _, prop := range []*models.Property{propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.NoError(t, err)
					})
				}
			})
		}
	})

	t.Run("validates indexSearchable on text/text[] data types", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{}
		for _, pdt := range []schema.DataType{schema.DataTypeText, schema.DataTypeTextArray} {
			nestedProperties = append(nestedProperties, &models.NestedProperty{
				Name:              "nested_" + pdt.AsName(),
				DataType:          pdt.PropString(),
				IndexFilterable:   &vFalse,
				IndexSearchable:   &vTrue,
				IndexRangeFilters: &vFalse,
				Tokenization:      models.PropertyTokenizationWord,
			})
		}

		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propPrimitives := &models.Property{
					Name:              "objectProp",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties:  nestedProperties,
				}
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						},
					},
				}

				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.NoError(t, err)
					})
				}
			})
		}
	})

	t.Run("does not validate indexSearchable on primitive data types", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{}
		for _, pdt := range schema.PrimitiveDataTypes {
			switch pdt {
			case schema.DataTypeText, schema.DataTypeTextArray:
				continue
			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
				// skip - not supported as nested
				continue
			default:
				// do nothing
			}

			t.Run(pdt.String(), func(t *testing.T) {
				nestedProperties = append(nestedProperties, &models.NestedProperty{
					Name:              "nested_" + pdt.AsName(),
					DataType:          pdt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vTrue,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
				})

				for _, ndt := range schema.NestedDataTypes {
					t.Run(ndt.String(), func(t *testing.T) {
						propPrimitives := &models.Property{
							Name:              "objectProp",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						}
						propLvl2Primitives := &models.Property{
							Name:              "objectPropLvl2",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vFalse,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties: []*models.NestedProperty{
								{
									Name:              "nested_object",
									DataType:          ndt.PropString(),
									IndexFilterable:   &vFalse,
									IndexSearchable:   &vFalse,
									IndexRangeFilters: &vFalse,
									Tokenization:      "",
									NestedProperties:  nestedProperties,
								},
							},
						}

						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
							t.Run(prop.Name, func(t *testing.T) {
								err := validateNestedProperties(prop.NestedProperties, prop.Name)
								assert.ErrorContains(t, err, prop.Name)
								assert.ErrorContains(t, err, "`indexSearchable` is not allowed for other than text/text[] data types")
							})
						}
					})
				}
			})
		}
	})

	t.Run("does not validate indexSearchable on nested data types", func(t *testing.T) {
		nestedProperties := []*models.NestedProperty{
			{
				Name:              "nested_int",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vFalse,
				IndexSearchable:   &vFalse,
				IndexRangeFilters: &vFalse,
				Tokenization:      "",
			},
		}

		for _, ndt := range schema.NestedDataTypes {
			t.Run(ndt.String(), func(t *testing.T) {
				propLvl2Primitives := &models.Property{
					Name:              "objectPropLvl2",
					DataType:          ndt.PropString(),
					IndexFilterable:   &vFalse,
					IndexSearchable:   &vFalse,
					IndexRangeFilters: &vFalse,
					Tokenization:      "",
					NestedProperties: []*models.NestedProperty{
						{
							Name:              "nested_object",
							DataType:          ndt.PropString(),
							IndexFilterable:   &vFalse,
							IndexSearchable:   &vTrue,
							IndexRangeFilters: &vFalse,
							Tokenization:      "",
							NestedProperties:  nestedProperties,
						},
					},
				}

				for _, prop := range []*models.Property{propLvl2Primitives} {
					t.Run(prop.Name, func(t *testing.T) {
						err := validateNestedProperties(prop.NestedProperties, prop.Name)
						assert.ErrorContains(t, err, prop.Name)
						assert.ErrorContains(t, err, "`indexSearchable` is not allowed for other than text/text[] data types")
					})
				}
			})
		}
	})
}
