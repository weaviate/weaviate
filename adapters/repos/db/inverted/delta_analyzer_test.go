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
	"testing"

	"github.com/stretchr/testify/assert"
	ent "github.com/weaviate/weaviate/entities/inverted"
)

func TestDeltaAnalyzer(t *testing.T) {
	t.Run("without previous indexing", func(t *testing.T) {
		previous := []Property(nil)
		next := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
				Length: 2,
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
				Length: 2,
			},
		}

		res := Delta(previous, next)

		assert.ElementsMatch(t, next, res.ToAdd)
		assert.Len(t, res.ToDelete, 0)
	})

	t.Run("with previous indexing and no changes", func(t *testing.T) {
		previous := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}
		next := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
			},
		}

		res := Delta(previous, next)

		assert.Len(t, res.ToDelete, 0)
		assert.Len(t, res.ToAdd, 0)
	})

	t.Run("with previous indexing - only additions", func(t *testing.T) {
		previous := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
		}
		next := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
				Length: 2,
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
				Length: 2,
			},
		}

		expectedAdd := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
				},
				Length: 2,
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
				},
				Length: 2,
			},
		}
		expectedDelete := []Property{
			{
				Name:   "prop1",
				Items:  []Countable{},
				Length: 1,
			},
			{
				Name:   "prop2",
				Items:  []Countable{},
				Length: 1,
			},
		}

		res := Delta(previous, next)

		assert.ElementsMatch(t, expectedAdd, res.ToAdd)
		assert.ElementsMatch(t, expectedDelete, res.ToDelete)
	})

	t.Run("with previous indexing - both additions and deletions", func(t *testing.T) {
		previous := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
			{
				Name: "prop3",
				Items: []Countable{
					{
						Data:          []byte("value6"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
			{
				Name: "prop4",
				Items: []Countable{
					{
						Data:          []byte("value6"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
		}
		next := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
				},
				Length: 1,
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
					{
						Data:          []byte("value4"),
						TermFrequency: 3,
					},
				},
				Length: 2,
			},
			{
				Name: "prop3",
				Items: []Countable{
					{
						Data:          []byte("value6"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
			{
				Name: "prop5",
				Items: []Countable{
					{
						Data:          []byte("value10"),
						TermFrequency: 10,
					},
				},
				Length: 1,
			},
		}

		expectedAdd := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value1"),
						TermFrequency: 7,
					},
				},
				Length: 1,
			},
			{
				Name: "prop2",
				Items: []Countable{
					{
						Data:          []byte("value3"),
						TermFrequency: 7,
					},
				},
				Length: 2,
			},
			{
				Name:   "prop4",
				Items:  []Countable{},
				Length: 0,
			},
			{
				Name: "prop5",
				Items: []Countable{
					{
						Data:          []byte("value10"),
						TermFrequency: 10,
					},
				},
				Length: 1,
			},
		}

		expectedDelete := []Property{
			{
				Name: "prop1",
				Items: []Countable{
					{
						Data:          []byte("value2"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
			{
				Name:   "prop2",
				Items:  []Countable{},
				Length: 1,
			},
			{
				Name: "prop4",
				Items: []Countable{
					{
						Data:          []byte("value6"),
						TermFrequency: 3,
					},
				},
				Length: 1,
			},
			{
				Name:   "prop5",
				Items:  []Countable{},
				Length: 0,
			},
		}

		res := Delta(previous, next)

		assert.ElementsMatch(t, expectedAdd, res.ToAdd)
		assert.ElementsMatch(t, expectedDelete, res.ToDelete)
	})
}

func TestDeltaAnalyzer_SkipSearchable(t *testing.T) {
	t.Run("without previous indexing", func(t *testing.T) {
		var allNext []Property
		var allExpectedToAdd []Property
		var allSkipSearchable []string

		prev := []Property(nil)

		t.Run("searchableInv/filterable", func(t *testing.T) {
			prop1_new_searchableInv_filterable_next := Property{
				Name: "new_sif",
				Items: []Countable{{
					Data:          []byte("new_01"),
					TermFrequency: 7,
				}, {
					Data:          []byte("new_02"),
					TermFrequency: 3,
				}},
				Length:             13,
				HasFilterableIndex: true,
				HasSearchableIndex: true,
			}

			delta := DeltaSkipSearchable(prev,
				[]Property{prop1_new_searchableInv_filterable_next},
				[]string{})

			assert.ElementsMatch(t, []Property{prop1_new_searchableInv_filterable_next}, delta.ToAdd)
			assert.Empty(t, delta.ToDelete)

			allNext = append(allNext, prop1_new_searchableInv_filterable_next)
			allExpectedToAdd = append(allExpectedToAdd, prop1_new_searchableInv_filterable_next)
			allSkipSearchable = append(allSkipSearchable, prop1_new_searchableInv_filterable_next.Name)
		})

		t.Run("searchableInv", func(t *testing.T) {
			prop2_new_searchableInv_next := Property{
				Name: "new_si",
				Items: []Countable{{
					Data:          []byte("new_01"),
					TermFrequency: 7,
				}, {
					Data:          []byte("new_02"),
					TermFrequency: 3,
				}},
				Length:             13,
				HasFilterableIndex: false,
				HasSearchableIndex: true,
			}

			delta := DeltaSkipSearchable(prev,
				[]Property{prop2_new_searchableInv_next},
				[]string{})

			assert.ElementsMatch(t, []Property{prop2_new_searchableInv_next}, delta.ToAdd)
			assert.Empty(t, delta.ToDelete)

			allNext = append(allNext, prop2_new_searchableInv_next)
			allExpectedToAdd = append(allExpectedToAdd, prop2_new_searchableInv_next)
			allSkipSearchable = append(allSkipSearchable, prop2_new_searchableInv_next.Name)
		})

		t.Run("searchableMap/filterable", func(t *testing.T) {
			prop3_new_searchableMap_filterable_next := Property{
				Name: "new_smf",
				Items: []Countable{{
					Data:          []byte("new_01"),
					TermFrequency: 7,
				}, {
					Data:          []byte("new_02"),
					TermFrequency: 3,
				}},
				Length:             13,
				HasFilterableIndex: true,
				HasSearchableIndex: true,
			}

			delta := DeltaSkipSearchable(prev,
				[]Property{prop3_new_searchableMap_filterable_next},
				[]string{})

			assert.ElementsMatch(t, []Property{prop3_new_searchableMap_filterable_next}, delta.ToAdd)
			assert.Empty(t, delta.ToDelete)

			allNext = append(allNext, prop3_new_searchableMap_filterable_next)
			allExpectedToAdd = append(allExpectedToAdd, prop3_new_searchableMap_filterable_next)
		})

		t.Run("filterable", func(t *testing.T) {
			prop4_new_filterable_next := Property{
				Name: "new_f",
				Items: []Countable{{
					Data:          []byte("new_01"),
					TermFrequency: 7,
				}, {
					Data:          []byte("new_02"),
					TermFrequency: 3,
				}},
				Length:             13,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			}

			delta := DeltaSkipSearchable(prev,
				[]Property{prop4_new_filterable_next},
				[]string{})

			assert.ElementsMatch(t, []Property{prop4_new_filterable_next}, delta.ToAdd)
			assert.Empty(t, delta.ToDelete)

			allNext = append(allNext, prop4_new_filterable_next)
			allExpectedToAdd = append(allExpectedToAdd, prop4_new_filterable_next)
		})

		t.Run("sanity check - all properties at once", func(t *testing.T) {
			delta := DeltaSkipSearchable(prev, allNext, allSkipSearchable)

			assert.ElementsMatch(t, allExpectedToAdd, delta.ToAdd)
			assert.Empty(t, delta.ToDelete)
		})
	})

	t.Run("with previous indexing", func(t *testing.T) {
		var allPrev []Property
		var allNext []Property
		var allExpectedToAdd []Property
		var allExpectedToDel []Property
		var allSkipSearchable []string

		t.Run("adding 2 values to 0 values", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop01_adding2values_searchableInv_filterable_prev := Property{
					Name:               "adding2values_sif",
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop01_adding2values_searchableInv_filterable_next := Property{
					Name: "adding2values_sif",
					Items: []Countable{{
						Data:          []byte("added_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("added_02"),
						TermFrequency: 3,
					}},
					Length:             17,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop01_adding2values_searchableInv_filterable_toAdd := []Property{
					prop01_adding2values_searchableInv_filterable_next,
				}
				expected_prop01_adding2values_searchableInv_filterable_toDel := []Property{
					prop01_adding2values_searchableInv_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop01_adding2values_searchableInv_filterable_prev},
					[]Property{prop01_adding2values_searchableInv_filterable_next},
					[]string{prop01_adding2values_searchableInv_filterable_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop01_adding2values_searchableInv_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop01_adding2values_searchableInv_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop01_adding2values_searchableInv_filterable_prev)
				allNext = append(allNext, prop01_adding2values_searchableInv_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop01_adding2values_searchableInv_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop01_adding2values_searchableInv_filterable_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop01_adding2values_searchableInv_filterable_prev.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop11_adding2values_searchableInv_prev := Property{
					Name:               "adding2values_si",
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}
				prop11_adding2values_searchableInv_next := Property{
					Name: "adding2values_si",
					Items: []Countable{{
						Data:          []byte("added_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("added_02"),
						TermFrequency: 3,
					}},
					Length:             17,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}

				expected_prop11_adding2values_searchableInv_toAdd := []Property{
					prop11_adding2values_searchableInv_next,
				}
				expected_prop11_adding2values_searchableInv_toDel := []Property{
					prop11_adding2values_searchableInv_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop11_adding2values_searchableInv_prev},
					[]Property{prop11_adding2values_searchableInv_next},
					[]string{prop11_adding2values_searchableInv_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop11_adding2values_searchableInv_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop11_adding2values_searchableInv_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop11_adding2values_searchableInv_prev)
				allNext = append(allNext, prop11_adding2values_searchableInv_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop11_adding2values_searchableInv_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop11_adding2values_searchableInv_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop11_adding2values_searchableInv_prev.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop21_adding2values_searchableMap_filterable_prev := Property{
					Name:               "adding2values_smf",
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop21_adding2values_searchableMap_filterable_next := Property{
					Name: "adding2values_smf",
					Items: []Countable{{
						Data:          []byte("added_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("added_02"),
						TermFrequency: 3,
					}},
					Length:             17,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop21_adding2values_searchableMap_filterable_toAdd := []Property{
					prop21_adding2values_searchableMap_filterable_next,
				}
				expected_prop21_adding2values_searchableMap_filterable_toDel := []Property{
					prop21_adding2values_searchableMap_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop21_adding2values_searchableMap_filterable_prev},
					[]Property{prop21_adding2values_searchableMap_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop21_adding2values_searchableMap_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop21_adding2values_searchableMap_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop21_adding2values_searchableMap_filterable_prev)
				allNext = append(allNext, prop21_adding2values_searchableMap_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop21_adding2values_searchableMap_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop21_adding2values_searchableMap_filterable_toDel...)
			})

			t.Run("filterable", func(t *testing.T) {
				prop31_adding2values_filterable_prev := Property{
					Name:               "adding2values_f",
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}
				prop31_adding2values_filterable_next := Property{
					Name: "adding2values_f",
					Items: []Countable{{
						Data:          []byte("added_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("added_02"),
						TermFrequency: 3,
					}},
					Length:             17,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}

				expected_prop31_adding2values_filterable_toAdd := []Property{
					prop31_adding2values_filterable_next,
				}
				expected_prop31_adding2values_filterable_toDel := []Property{
					prop31_adding2values_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop31_adding2values_filterable_prev},
					[]Property{prop31_adding2values_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop31_adding2values_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop31_adding2values_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop31_adding2values_filterable_prev)
				allNext = append(allNext, prop31_adding2values_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop31_adding2values_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop31_adding2values_filterable_toDel...)
			})
		})

		t.Run("adding 1 value to 1 value", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop02_adding1value_searchableInv_filterable_prev := Property{
					Name: "adding1value_sif",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}},
					Length:             12,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop02_adding1value_searchableInv_filterable_next := Property{
					Name: "adding1value_sif",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("added_02"),
						TermFrequency: 3,
					}},
					Length:             21,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop02_adding1value_searchableInv_filterable_toAdd := []Property{
					{
						Name: "adding1value_sif",
						Items: []Countable{{
							Data:          []byte("added_02"),
							TermFrequency: 3,
						}},
						Length:             21,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
					{
						Name: "adding1value_sif",
						Items: []Countable{{
							Data:          []byte("immutable_01"),
							TermFrequency: 7,
						}, {
							Data:          []byte("added_02"),
							TermFrequency: 3,
						}},
						Length:             -1,
						HasFilterableIndex: false,
						HasSearchableIndex: true,
					},
				}
				expected_prop02_adding1value_searchableInv_filterable_toDel := []Property{
					{
						Name:               "adding1value_sif",
						Items:              []Countable{},
						Length:             12,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
					{
						Name: "adding1value_sif",
						Items: []Countable{{
							Data:          []byte("immutable_01"),
							TermFrequency: 7,
						}},
						Length:             -1,
						HasFilterableIndex: false,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop02_adding1value_searchableInv_filterable_prev},
					[]Property{prop02_adding1value_searchableInv_filterable_next},
					[]string{prop02_adding1value_searchableInv_filterable_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop02_adding1value_searchableInv_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop02_adding1value_searchableInv_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop02_adding1value_searchableInv_filterable_prev)
				allNext = append(allNext, prop02_adding1value_searchableInv_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop02_adding1value_searchableInv_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop02_adding1value_searchableInv_filterable_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop02_adding1value_searchableInv_filterable_prev.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop12_adding1value_searchableInv_prev := Property{
					Name: "adding1value_si",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}},
					Length:             12,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}
				prop12_adding1value_searchableInv_next := Property{
					Name: "adding1value_si",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("added_02"),
						TermFrequency: 3,
					}},
					Length:             21,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}

				expected_prop12_adding1value_searchableInv_toAdd := []Property{
					prop12_adding1value_searchableInv_next,
				}
				expected_prop12_adding1value_searchableInv_toDel := []Property{
					prop12_adding1value_searchableInv_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop12_adding1value_searchableInv_prev},
					[]Property{prop12_adding1value_searchableInv_next},
					[]string{prop12_adding1value_searchableInv_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop12_adding1value_searchableInv_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop12_adding1value_searchableInv_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop12_adding1value_searchableInv_prev)
				allNext = append(allNext, prop12_adding1value_searchableInv_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop12_adding1value_searchableInv_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop12_adding1value_searchableInv_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop12_adding1value_searchableInv_prev.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop22_adding1value_searchableMap_filterable_prev := Property{
					Name: "adding1value_smf",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}},
					Length:             12,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop22_adding1value_searchableMap_filterable_next := Property{
					Name: "adding1value_smf",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("added_02"),
						TermFrequency: 3,
					}},
					Length:             21,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop22_adding1value_searchableMap_filterable_toAdd := []Property{
					{
						Name: "adding1value_smf",
						Items: []Countable{{
							Data:          []byte("added_02"),
							TermFrequency: 3,
						}},
						Length:             21,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}
				expected_prop22_adding1value_searchableMap_filterable_toDel := []Property{
					{
						Name:               "adding1value_smf",
						Items:              []Countable{},
						Length:             12,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop22_adding1value_searchableMap_filterable_prev},
					[]Property{prop22_adding1value_searchableMap_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop22_adding1value_searchableMap_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop22_adding1value_searchableMap_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop22_adding1value_searchableMap_filterable_prev)
				allNext = append(allNext, prop22_adding1value_searchableMap_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop22_adding1value_searchableMap_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop22_adding1value_searchableMap_filterable_toDel...)
			})

			t.Run("filterable", func(t *testing.T) {
				prop32_adding1value_filterable_prev := Property{
					Name: "adding1value_f",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}},
					Length:             12,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}
				prop32_adding1value_filterable_next := Property{
					Name: "adding1value_f",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("added_02"),
						TermFrequency: 3,
					}},
					Length:             21,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}

				expected_prop32_adding1value_filterable_toAdd := []Property{
					{
						Name: "adding1value_f",
						Items: []Countable{{
							Data:          []byte("added_02"),
							TermFrequency: 3,
						}},
						Length:             21,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
				}
				expected_prop32_adding1value_filterable_toDel := []Property{
					{
						Name:               "adding1value_f",
						Items:              []Countable{},
						Length:             12,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop32_adding1value_filterable_prev},
					[]Property{prop32_adding1value_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop32_adding1value_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop32_adding1value_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop32_adding1value_filterable_prev)
				allNext = append(allNext, prop32_adding1value_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop32_adding1value_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop32_adding1value_filterable_toDel...)
			})
		})

		t.Run("deleting 2 values from 2 values", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop03_deleting2values_searchableInv_filterable_prev := Property{
					Name: "deleting2values_sif",
					Items: []Countable{{
						Data:          []byte("toBeDeleted_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDeleted_02"),
						TermFrequency: 3,
					}},
					Length:             29,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop03_deleting2values_searchableInv_filterable_next := Property{
					Name:               "deleting2values_sif",
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop03_deleting2values_searchableInv_filterable_toAdd := []Property{
					prop03_deleting2values_searchableInv_filterable_next,
				}
				expected_prop03_deleting2values_searchableInv_filterable_toDel := []Property{
					prop03_deleting2values_searchableInv_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop03_deleting2values_searchableInv_filterable_prev},
					[]Property{prop03_deleting2values_searchableInv_filterable_next},
					[]string{prop03_deleting2values_searchableInv_filterable_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop03_deleting2values_searchableInv_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop03_deleting2values_searchableInv_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop03_deleting2values_searchableInv_filterable_prev)
				allNext = append(allNext, prop03_deleting2values_searchableInv_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop03_deleting2values_searchableInv_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop03_deleting2values_searchableInv_filterable_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop03_deleting2values_searchableInv_filterable_prev.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop13_deleting2values_searchableInv_prev := Property{
					Name: "deleting2values_si",
					Items: []Countable{{
						Data:          []byte("toBeDeleted_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDeleted_02"),
						TermFrequency: 3,
					}},
					Length:             29,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}
				prop13_deleting2values_searchableInv_next := Property{
					Name:               "deleting2values_si",
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}

				expected_prop13_deleting2values_searchableInv_toAdd := []Property{
					prop13_deleting2values_searchableInv_next,
				}
				expected_prop13_deleting2values_searchableInv_toDel := []Property{
					prop13_deleting2values_searchableInv_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop13_deleting2values_searchableInv_prev},
					[]Property{prop13_deleting2values_searchableInv_next},
					[]string{prop13_deleting2values_searchableInv_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop13_deleting2values_searchableInv_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop13_deleting2values_searchableInv_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop13_deleting2values_searchableInv_prev)
				allNext = append(allNext, prop13_deleting2values_searchableInv_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop13_deleting2values_searchableInv_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop13_deleting2values_searchableInv_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop13_deleting2values_searchableInv_prev.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop23_deleting2values_searchableMap_filterable_prev := Property{
					Name: "deleting2values_smf",
					Items: []Countable{{
						Data:          []byte("toBeDeleted_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDeleted_02"),
						TermFrequency: 3,
					}},
					Length:             29,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop23_deleting2values_searchableMap_filterable_next := Property{
					Name:               "deleting2values_smf",
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop23_deleting2values_searchableMap_filterable_toAdd := []Property{
					prop23_deleting2values_searchableMap_filterable_next,
				}
				expected_prop23_deleting2values_searchableMap_filterable_toDel := []Property{
					prop23_deleting2values_searchableMap_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop23_deleting2values_searchableMap_filterable_prev},
					[]Property{prop23_deleting2values_searchableMap_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop23_deleting2values_searchableMap_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop23_deleting2values_searchableMap_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop23_deleting2values_searchableMap_filterable_prev)
				allNext = append(allNext, prop23_deleting2values_searchableMap_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop23_deleting2values_searchableMap_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop23_deleting2values_searchableMap_filterable_toDel...)
			})

			t.Run("filterable", func(t *testing.T) {
				prop33_deleting2values_filterable_prev := Property{
					Name: "deleting2values_f",
					Items: []Countable{{
						Data:          []byte("toBeDeleted_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDeleted_02"),
						TermFrequency: 3,
					}},
					Length:             29,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}
				prop33_deleting2values_filterable_next := Property{
					Name:               "deleting2values_f",
					Items:              []Countable{},
					Length:             0,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}

				expected_prop33_deleting2values_filterable_toAdd := []Property{
					prop33_deleting2values_filterable_next,
				}
				expected_prop33_deleting2values_filterable_toDel := []Property{
					prop33_deleting2values_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop33_deleting2values_filterable_prev},
					[]Property{prop33_deleting2values_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop33_deleting2values_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop33_deleting2values_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop33_deleting2values_filterable_prev)
				allNext = append(allNext, prop33_deleting2values_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop33_deleting2values_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop33_deleting2values_filterable_toDel...)
			})
		})

		t.Run("deleting 1 value from 2 values", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop04_deleting1value_searchableInv_filterable_prev := Property{
					Name: "deleting1value_sif",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDeleted_02"),
						TermFrequency: 3,
					}},
					Length:             27,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop04_deleting1value_searchableInv_filterable_next := Property{
					Name: "deleting1value_sif",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}},
					Length:             12,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop04_deleting1value_searchableInv_filterable_toAdd := []Property{
					{
						Name:               "deleting1value_sif",
						Items:              []Countable{},
						Length:             12,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
					{
						Name: "deleting1value_sif",
						Items: []Countable{{
							Data:          []byte("immutable_01"),
							TermFrequency: 7,
						}},
						Length:             -1,
						HasFilterableIndex: false,
						HasSearchableIndex: true,
					},
				}
				expected_prop04_deleting1value_searchableInv_filterable_toDel := []Property{
					{
						Name: "deleting1value_sif",
						Items: []Countable{{
							Data:          []byte("toBeDeleted_02"),
							TermFrequency: 3,
						}},
						Length:             27,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
					{
						Name: "deleting1value_sif",
						Items: []Countable{{
							Data:          []byte("immutable_01"),
							TermFrequency: 7,
						}, {
							Data:          []byte("toBeDeleted_02"),
							TermFrequency: 3,
						}},
						Length:             -1,
						HasFilterableIndex: false,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop04_deleting1value_searchableInv_filterable_prev},
					[]Property{prop04_deleting1value_searchableInv_filterable_next},
					[]string{prop04_deleting1value_searchableInv_filterable_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop04_deleting1value_searchableInv_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop04_deleting1value_searchableInv_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop04_deleting1value_searchableInv_filterable_prev)
				allNext = append(allNext, prop04_deleting1value_searchableInv_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop04_deleting1value_searchableInv_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop04_deleting1value_searchableInv_filterable_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop04_deleting1value_searchableInv_filterable_prev.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop14_deleting1value_searchableInv_prev := Property{
					Name: "deleting1value_si",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDeleted_02"),
						TermFrequency: 3,
					}},
					Length:             27,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}
				prop14_deleting1value_searchableInv_next := Property{
					Name: "deleting1value_si",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}},
					Length:             12,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}

				expected_prop14_deleting1value_searchableInv_toAdd := []Property{
					prop14_deleting1value_searchableInv_next,
				}
				expected_prop14_deleting1value_searchableInv_toDel := []Property{
					prop14_deleting1value_searchableInv_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop14_deleting1value_searchableInv_prev},
					[]Property{prop14_deleting1value_searchableInv_next},
					[]string{prop14_deleting1value_searchableInv_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop14_deleting1value_searchableInv_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop14_deleting1value_searchableInv_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop14_deleting1value_searchableInv_prev)
				allNext = append(allNext, prop14_deleting1value_searchableInv_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop14_deleting1value_searchableInv_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop14_deleting1value_searchableInv_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop14_deleting1value_searchableInv_prev.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop24_deleting1value_searchableMap_filterable_prev := Property{
					Name: "deleting1value_smf",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDeleted_02"),
						TermFrequency: 3,
					}},
					Length:             27,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop24_deleting1value_searchableMap_filterable_next := Property{
					Name: "deleting1value_smf",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}},
					Length:             12,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop24_deleting1value_searchableMap_filterable_toAdd := []Property{
					{
						Name:               "deleting1value_smf",
						Items:              []Countable{},
						Length:             12,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}
				expected_prop24_deleting1value_searchableMap_filterable_toDel := []Property{
					{
						Name: "deleting1value_smf",
						Items: []Countable{{
							Data:          []byte("toBeDeleted_02"),
							TermFrequency: 3,
						}},
						Length:             27,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop24_deleting1value_searchableMap_filterable_prev},
					[]Property{prop24_deleting1value_searchableMap_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop24_deleting1value_searchableMap_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop24_deleting1value_searchableMap_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop24_deleting1value_searchableMap_filterable_prev)
				allNext = append(allNext, prop24_deleting1value_searchableMap_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop24_deleting1value_searchableMap_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop24_deleting1value_searchableMap_filterable_toDel...)
			})

			t.Run("filterable", func(t *testing.T) {
				prop34_deleting1value_filterable_prev := Property{
					Name: "deleting1value_f",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDeleted_02"),
						TermFrequency: 3,
					}},
					Length:             27,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}
				prop34_deleting1value_filterable_next := Property{
					Name: "deleting1value_f",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}},
					Length:             12,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}

				expected_prop34_deleting1value_filterable_toAdd := []Property{
					{
						Name:               "deleting1value_f",
						Items:              []Countable{},
						Length:             12,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
				}
				expected_prop34_deleting1value_filterable_toDel := []Property{
					{
						Name: "deleting1value_f",
						Items: []Countable{{
							Data:          []byte("toBeDeleted_02"),
							TermFrequency: 3,
						}},
						Length:             27,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop34_deleting1value_filterable_prev},
					[]Property{prop34_deleting1value_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop34_deleting1value_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop34_deleting1value_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop34_deleting1value_filterable_prev)
				allNext = append(allNext, prop34_deleting1value_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop34_deleting1value_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop34_deleting1value_filterable_toDel...)
			})
		})

		t.Run("replacing 2 values of 2 values", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop05_replacing2values_searchableInv_filterable_prev := Property{
					Name: "replacing2values_sif",
					Items: []Countable{{
						Data:          []byte("toBeReplaced_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeReplaced_02"),
						TermFrequency: 3,
					}},
					Length:             31,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop05_replacing2values_searchableInv_filterable_next := Property{
					Name: "replacing2values_sif",
					Items: []Countable{{
						Data:          []byte("replaced_03"),
						TermFrequency: 7,
					}, {
						Data:          []byte("replaced_04"),
						TermFrequency: 3,
					}},
					Length:             23,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop05_replacing2values_searchableInv_filterable_toAdd := []Property{
					prop05_replacing2values_searchableInv_filterable_next,
				}
				expected_prop05_replacing2values_searchableInv_filterable_toDel := []Property{
					prop05_replacing2values_searchableInv_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop05_replacing2values_searchableInv_filterable_prev},
					[]Property{prop05_replacing2values_searchableInv_filterable_next},
					[]string{prop05_replacing2values_searchableInv_filterable_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop05_replacing2values_searchableInv_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop05_replacing2values_searchableInv_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop05_replacing2values_searchableInv_filterable_prev)
				allNext = append(allNext, prop05_replacing2values_searchableInv_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop05_replacing2values_searchableInv_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop05_replacing2values_searchableInv_filterable_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop05_replacing2values_searchableInv_filterable_prev.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop15_replacing2values_searchableInv_prev := Property{
					Name: "replacing2values_si",
					Items: []Countable{{
						Data:          []byte("toBeReplaced_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeReplaced_02"),
						TermFrequency: 3,
					}},
					Length:             31,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}
				prop15_replacing2values_searchableInv_next := Property{
					Name: "replacing2values_si",
					Items: []Countable{{
						Data:          []byte("replaced_03"),
						TermFrequency: 7,
					}, {
						Data:          []byte("replaced_04"),
						TermFrequency: 3,
					}},
					Length:             23,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}

				expected_prop15_replacing2values_searchableInv_toAdd := []Property{
					prop15_replacing2values_searchableInv_next,
				}
				expected_prop15_replacing2values_searchableInv_toDel := []Property{
					prop15_replacing2values_searchableInv_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop15_replacing2values_searchableInv_prev},
					[]Property{prop15_replacing2values_searchableInv_next},
					[]string{prop15_replacing2values_searchableInv_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop15_replacing2values_searchableInv_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop15_replacing2values_searchableInv_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop15_replacing2values_searchableInv_prev)
				allNext = append(allNext, prop15_replacing2values_searchableInv_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop15_replacing2values_searchableInv_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop15_replacing2values_searchableInv_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop15_replacing2values_searchableInv_prev.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop25_replacing2values_searchableMap_filterable_prev := Property{
					Name: "replacing2values_smf",
					Items: []Countable{{
						Data:          []byte("toBeReplaced_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeReplaced_02"),
						TermFrequency: 3,
					}},
					Length:             31,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop25_replacing2values_searchableMap_filterable_next := Property{
					Name: "replacing2values_smf",
					Items: []Countable{{
						Data:          []byte("replaced_03"),
						TermFrequency: 7,
					}, {
						Data:          []byte("replaced_04"),
						TermFrequency: 3,
					}},
					Length:             23,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop25_replacing2values_searchableMap_filterable_toAdd := []Property{
					prop25_replacing2values_searchableMap_filterable_next,
				}
				expected_prop25_replacing2values_searchableMap_filterable_toDel := []Property{
					prop25_replacing2values_searchableMap_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop25_replacing2values_searchableMap_filterable_prev},
					[]Property{prop25_replacing2values_searchableMap_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop25_replacing2values_searchableMap_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop25_replacing2values_searchableMap_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop25_replacing2values_searchableMap_filterable_prev)
				allNext = append(allNext, prop25_replacing2values_searchableMap_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop25_replacing2values_searchableMap_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop25_replacing2values_searchableMap_filterable_toDel...)
			})

			t.Run("filterable", func(t *testing.T) {
				prop35_replacing2values_filterable_prev := Property{
					Name: "replacing2values_f",
					Items: []Countable{{
						Data:          []byte("toBeReplaced_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeReplaced_02"),
						TermFrequency: 3,
					}},
					Length:             31,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}
				prop35_replacing2values_filterable_next := Property{
					Name: "replacing2values_f",
					Items: []Countable{{
						Data:          []byte("replaced_03"),
						TermFrequency: 7,
					}, {
						Data:          []byte("replaced_04"),
						TermFrequency: 3,
					}},
					Length:             23,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}

				expected_prop35_replacing2values_filterable_toAdd := []Property{
					prop35_replacing2values_filterable_next,
				}
				expected_prop35_replacing2values_filterable_toDel := []Property{
					prop35_replacing2values_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop35_replacing2values_filterable_prev},
					[]Property{prop35_replacing2values_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop35_replacing2values_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop35_replacing2values_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop35_replacing2values_filterable_prev)
				allNext = append(allNext, prop35_replacing2values_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop35_replacing2values_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop35_replacing2values_filterable_toDel...)
			})
		})

		t.Run("replacing 1 value of 2 values", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop06_replacing1value_searchableInv_filterable_prev := Property{
					Name: "replacing1value_sif",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeReplaced_02"),
						TermFrequency: 3,
					}},
					Length:             28,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop06_replacing1value_searchableInv_filterable_next := Property{
					Name: "replacing1value_sif",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("replaced_03"),
						TermFrequency: 3,
					}},
					Length:             24,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop06_replacing1value_searchableInv_filterable_toAdd := []Property{
					{
						Name: "replacing1value_sif",
						Items: []Countable{{
							Data:          []byte("replaced_03"),
							TermFrequency: 3,
						}},
						Length:             24,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
					{
						Name: "replacing1value_sif",
						Items: []Countable{{
							Data:          []byte("immutable_01"),
							TermFrequency: 7,
						}, {
							Data:          []byte("replaced_03"),
							TermFrequency: 3,
						}},
						Length:             -1,
						HasFilterableIndex: false,
						HasSearchableIndex: true,
					},
				}
				expected_prop06_replacing1value_searchableInv_filterable_toDel := []Property{
					{
						Name: "replacing1value_sif",
						Items: []Countable{{
							Data:          []byte("toBeReplaced_02"),
							TermFrequency: 3,
						}},
						Length:             28,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
					{
						Name: "replacing1value_sif",
						Items: []Countable{{
							Data:          []byte("immutable_01"),
							TermFrequency: 7,
						}, {
							Data:          []byte("toBeReplaced_02"),
							TermFrequency: 3,
						}},
						Length:             -1,
						HasFilterableIndex: false,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop06_replacing1value_searchableInv_filterable_prev},
					[]Property{prop06_replacing1value_searchableInv_filterable_next},
					[]string{prop06_replacing1value_searchableInv_filterable_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop06_replacing1value_searchableInv_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop06_replacing1value_searchableInv_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop06_replacing1value_searchableInv_filterable_prev)
				allNext = append(allNext, prop06_replacing1value_searchableInv_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop06_replacing1value_searchableInv_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop06_replacing1value_searchableInv_filterable_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop06_replacing1value_searchableInv_filterable_prev.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop16_replacing1value_searchableInv_prev := Property{
					Name: "replacing1value_si",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeReplaced_02"),
						TermFrequency: 3,
					}},
					Length:             28,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}
				prop16_replacing1value_searchableInv_next := Property{
					Name: "replacing1value_si",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("replaced_03"),
						TermFrequency: 3,
					}},
					Length:             24,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}

				expected_prop16_replacing1value_searchableInv_toAdd := []Property{
					prop16_replacing1value_searchableInv_next,
				}
				expected_prop16_replacing1value_searchableInv_toDel := []Property{
					prop16_replacing1value_searchableInv_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop16_replacing1value_searchableInv_prev},
					[]Property{prop16_replacing1value_searchableInv_next},
					[]string{prop16_replacing1value_searchableInv_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop16_replacing1value_searchableInv_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop16_replacing1value_searchableInv_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop16_replacing1value_searchableInv_prev)
				allNext = append(allNext, prop16_replacing1value_searchableInv_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop16_replacing1value_searchableInv_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop16_replacing1value_searchableInv_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop16_replacing1value_searchableInv_prev.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop26_replacing1value_searchableMap_filterable_prev := Property{
					Name: "replacing1value_smf",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeReplaced_02"),
						TermFrequency: 3,
					}},
					Length:             28,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop26_replacing1value_searchableMap_filterable_next := Property{
					Name: "replacing1value_smf",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("replaced_03"),
						TermFrequency: 3,
					}},
					Length:             24,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop26_replacing1value_searchableMap_filterable_toAdd := []Property{
					{
						Name: "replacing1value_smf",
						Items: []Countable{{
							Data:          []byte("replaced_03"),
							TermFrequency: 3,
						}},
						Length:             24,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}
				expected_prop26_replacing1value_searchableMap_filterable_toDel := []Property{
					{
						Name: "replacing1value_smf",
						Items: []Countable{{
							Data:          []byte("toBeReplaced_02"),
							TermFrequency: 3,
						}},
						Length:             28,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop26_replacing1value_searchableMap_filterable_prev},
					[]Property{prop26_replacing1value_searchableMap_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop26_replacing1value_searchableMap_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop26_replacing1value_searchableMap_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop26_replacing1value_searchableMap_filterable_prev)
				allNext = append(allNext, prop26_replacing1value_searchableMap_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop26_replacing1value_searchableMap_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop26_replacing1value_searchableMap_filterable_toDel...)
			})

			t.Run("filterable", func(t *testing.T) {
				prop36_replacing1value_filterable_prev := Property{
					Name: "replacing1value_f",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeReplaced_02"),
						TermFrequency: 3,
					}},
					Length:             28,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}
				prop36_replacing1value_filterable_next := Property{
					Name: "replacing1value_f",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("replaced_03"),
						TermFrequency: 3,
					}},
					Length:             24,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}

				expected_prop36_replacing1value_filterable_toAdd := []Property{
					{
						Name: "replacing1value_f",
						Items: []Countable{{
							Data:          []byte("replaced_03"),
							TermFrequency: 3,
						}},
						Length:             24,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
				}
				expected_prop36_replacing1value_filterable_toDel := []Property{
					{
						Name: "replacing1value_f",
						Items: []Countable{{
							Data:          []byte("toBeReplaced_02"),
							TermFrequency: 3,
						}},
						Length:             28,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{prop36_replacing1value_filterable_prev},
					[]Property{prop36_replacing1value_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop36_replacing1value_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop36_replacing1value_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop36_replacing1value_filterable_prev)
				allNext = append(allNext, prop36_replacing1value_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop36_replacing1value_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop36_replacing1value_filterable_toDel...)
			})
		})

		t.Run("creating 2 values", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop07_creating2values_searchableInv_filterable_next := Property{
					Name: "creating2values_sif",
					Items: []Countable{{
						Data:          []byte("created_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("created_02"),
						TermFrequency: 3,
					}},
					Length:             21,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop07_creating2values_searchableInv_filterable_toAdd := []Property{
					prop07_creating2values_searchableInv_filterable_next,
				}
				expected_prop07_creating2values_searchableInv_filterable_toDel := []Property{
					{
						Name:               "creating2values_sif",
						Items:              []Countable{},
						Length:             0,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{},
					[]Property{prop07_creating2values_searchableInv_filterable_next},
					[]string{prop07_creating2values_searchableInv_filterable_next.Name},
				)

				assert.ElementsMatch(t, expected_prop07_creating2values_searchableInv_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop07_creating2values_searchableInv_filterable_toDel, delta.ToDelete)

				allNext = append(allNext, prop07_creating2values_searchableInv_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop07_creating2values_searchableInv_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop07_creating2values_searchableInv_filterable_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop07_creating2values_searchableInv_filterable_next.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop17_creating2values_searchableInv_next := Property{
					Name: "creating2values_si",
					Items: []Countable{{
						Data:          []byte("created_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("created_02"),
						TermFrequency: 3,
					}},
					Length:             21,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}

				expected_prop17_creating2values_searchableInv_toAdd := []Property{
					prop17_creating2values_searchableInv_next,
				}
				expected_prop17_creating2values_searchableInv_toDel := []Property{
					{
						Name:               "creating2values_si",
						Items:              []Countable{},
						Length:             0,
						HasFilterableIndex: false,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{},
					[]Property{prop17_creating2values_searchableInv_next},
					[]string{prop17_creating2values_searchableInv_next.Name},
				)

				assert.ElementsMatch(t, expected_prop17_creating2values_searchableInv_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop17_creating2values_searchableInv_toDel, delta.ToDelete)

				allNext = append(allNext, prop17_creating2values_searchableInv_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop17_creating2values_searchableInv_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop17_creating2values_searchableInv_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop17_creating2values_searchableInv_next.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop27_creating2values_searchableMap_filterable_next := Property{
					Name: "creating2values_smf",
					Items: []Countable{{
						Data:          []byte("created_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("created_02"),
						TermFrequency: 3,
					}},
					Length:             21,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop27_creating2values_searchableMap_filterable_toAdd := []Property{
					prop27_creating2values_searchableMap_filterable_next,
				}
				expected_prop27_creating2values_searchableMap_filterable_toDel := []Property{
					{
						Name:               "creating2values_smf",
						Items:              []Countable{},
						Length:             0,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{},
					[]Property{prop27_creating2values_searchableMap_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop27_creating2values_searchableMap_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop27_creating2values_searchableMap_filterable_toDel, delta.ToDelete)

				allNext = append(allNext, prop27_creating2values_searchableMap_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop27_creating2values_searchableMap_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop27_creating2values_searchableMap_filterable_toDel...)
			})

			t.Run("filterable", func(t *testing.T) {
				prop37_creating2values_filterable_next := Property{
					Name: "creating2values_f",
					Items: []Countable{{
						Data:          []byte("created_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("created_02"),
						TermFrequency: 3,
					}},
					Length:             21,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}

				expected_prop37_creating2values_filterable_toAdd := []Property{
					prop37_creating2values_filterable_next,
				}
				expected_prop37_creating2values_filterable_toDel := []Property{
					{
						Name:               "creating2values_f",
						Items:              []Countable{},
						Length:             0,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
				}

				delta := DeltaSkipSearchable(
					[]Property{},
					[]Property{prop37_creating2values_filterable_next},
					nil,
				)

				assert.ElementsMatch(t, expected_prop37_creating2values_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop37_creating2values_filterable_toDel, delta.ToDelete)

				allNext = append(allNext, prop37_creating2values_filterable_next)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop37_creating2values_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop37_creating2values_filterable_toDel...)
			})
		})

		t.Run("dropping 2 values", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop08_dropping2values_searchableInv_filterable_prev := Property{
					Name: "dropping2values_sif",
					Items: []Countable{{
						Data:          []byte("toBeDropped_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDropped_02"),
						TermFrequency: 3,
					}},
					Length:             29,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop08_dropping2values_searchableInv_filterable_toAdd := []Property{
					{
						Name:               "dropping2values_sif",
						Items:              []Countable{},
						Length:             0,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}
				expected_prop08_dropping2values_searchableInv_filterable_toDel := []Property{
					prop08_dropping2values_searchableInv_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop08_dropping2values_searchableInv_filterable_prev},
					[]Property{},
					[]string{prop08_dropping2values_searchableInv_filterable_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop08_dropping2values_searchableInv_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop08_dropping2values_searchableInv_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop08_dropping2values_searchableInv_filterable_prev)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop08_dropping2values_searchableInv_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop08_dropping2values_searchableInv_filterable_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop08_dropping2values_searchableInv_filterable_prev.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop18_dropping2values_searchableInv_prev := Property{
					Name: "dropping2values_si",
					Items: []Countable{{
						Data:          []byte("toBeDropped_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDropped_02"),
						TermFrequency: 3,
					}},
					Length:             29,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}

				expected_prop18_dropping2values_searchableInv_toAdd := []Property{
					{
						Name:               "dropping2values_si",
						Items:              []Countable{},
						Length:             0,
						HasFilterableIndex: false,
						HasSearchableIndex: true,
					},
				}
				expected_prop18_dropping2values_searchableInv_toDel := []Property{
					prop18_dropping2values_searchableInv_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop18_dropping2values_searchableInv_prev},
					[]Property{},
					[]string{prop18_dropping2values_searchableInv_prev.Name},
				)

				assert.ElementsMatch(t, expected_prop18_dropping2values_searchableInv_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop18_dropping2values_searchableInv_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop18_dropping2values_searchableInv_prev)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop18_dropping2values_searchableInv_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop18_dropping2values_searchableInv_toDel...)
				allSkipSearchable = append(allSkipSearchable, prop18_dropping2values_searchableInv_prev.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop28_dropping2values_searchableMap_filterable_prev := Property{
					Name: "dropping2values_smf",
					Items: []Countable{{
						Data:          []byte("toBeDropped_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDropped_02"),
						TermFrequency: 3,
					}},
					Length:             29,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}

				expected_prop28_dropping2values_searchableMap_filterable_toAdd := []Property{
					{
						Name:               "dropping2values_smf",
						Items:              []Countable{},
						Length:             0,
						HasFilterableIndex: true,
						HasSearchableIndex: true,
					},
				}
				expected_prop28_dropping2values_searchableMap_filterable_toDel := []Property{
					prop28_dropping2values_searchableMap_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop28_dropping2values_searchableMap_filterable_prev},
					[]Property{},
					nil,
				)

				assert.ElementsMatch(t, expected_prop28_dropping2values_searchableMap_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop28_dropping2values_searchableMap_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop28_dropping2values_searchableMap_filterable_prev)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop28_dropping2values_searchableMap_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop28_dropping2values_searchableMap_filterable_toDel...)
			})

			t.Run("filterable", func(t *testing.T) {
				prop38_dropping2values_filterable_prev := Property{
					Name: "dropping2values_smf",
					Items: []Countable{{
						Data:          []byte("toBeDropped_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("toBeDropped_02"),
						TermFrequency: 3,
					}},
					Length:             29,
					HasFilterableIndex: true,
					HasSearchableIndex: false,
				}

				expected_prop38_dropping2values_filterable_toAdd := []Property{
					{
						Name:               "dropping2values_smf",
						Items:              []Countable{},
						Length:             0,
						HasFilterableIndex: true,
						HasSearchableIndex: false,
					},
				}
				expected_prop38_dropping2values_filterable_toDel := []Property{
					prop38_dropping2values_filterable_prev,
				}

				delta := DeltaSkipSearchable(
					[]Property{prop38_dropping2values_filterable_prev},
					[]Property{},
					nil,
				)

				assert.ElementsMatch(t, expected_prop38_dropping2values_filterable_toAdd, delta.ToAdd)
				assert.ElementsMatch(t, expected_prop38_dropping2values_filterable_toDel, delta.ToDelete)

				allPrev = append(allPrev, prop38_dropping2values_filterable_prev)
				allExpectedToAdd = append(allExpectedToAdd, expected_prop38_dropping2values_filterable_toAdd...)
				allExpectedToDel = append(allExpectedToDel, expected_prop38_dropping2values_filterable_toDel...)
			})
		})

		t.Run("no changes to 2 values", func(t *testing.T) {
			t.Run("searchableInv/filterable", func(t *testing.T) {
				prop09_noChanges_searchableInv_filterable_prev := Property{
					Name: "noChanges_sif",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("immutable_02"),
						TermFrequency: 3,
					}},
					Length:             25,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop09_noChanges_searchableInv_filterable_next := prop09_noChanges_searchableInv_filterable_prev

				delta := DeltaSkipSearchable(
					[]Property{prop09_noChanges_searchableInv_filterable_prev},
					[]Property{prop09_noChanges_searchableInv_filterable_next},
					[]string{prop09_noChanges_searchableInv_filterable_prev.Name},
				)

				assert.Empty(t, delta.ToAdd)
				assert.Empty(t, delta.ToDelete)

				allPrev = append(allPrev, prop09_noChanges_searchableInv_filterable_prev)
				allNext = append(allNext, prop09_noChanges_searchableInv_filterable_next)
				allSkipSearchable = append(allSkipSearchable, prop09_noChanges_searchableInv_filterable_prev.Name)
			})

			t.Run("searchableInv", func(t *testing.T) {
				prop19_noChanges_searchableInv_prev := Property{
					Name: "noChanges_si",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("immutable_02"),
						TermFrequency: 3,
					}},
					Length:             25,
					HasFilterableIndex: false,
					HasSearchableIndex: true,
				}
				prop19_noChanges_searchableInv_next := prop19_noChanges_searchableInv_prev

				delta := DeltaSkipSearchable(
					[]Property{prop19_noChanges_searchableInv_prev},
					[]Property{prop19_noChanges_searchableInv_next},
					[]string{prop19_noChanges_searchableInv_prev.Name},
				)

				assert.Empty(t, delta.ToAdd)
				assert.Empty(t, delta.ToDelete)

				allPrev = append(allPrev, prop19_noChanges_searchableInv_prev)
				allNext = append(allNext, prop19_noChanges_searchableInv_next)
				allSkipSearchable = append(allSkipSearchable, prop19_noChanges_searchableInv_prev.Name)
			})

			t.Run("searchableMap/filterable", func(t *testing.T) {
				prop29_noChanges_searchableMap_filterable_prev := Property{
					Name: "noChanges_smf",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("immutable_02"),
						TermFrequency: 3,
					}},
					Length:             25,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop29_noChanges_searchableMap_filterable_next := prop29_noChanges_searchableMap_filterable_prev

				delta := DeltaSkipSearchable(
					[]Property{prop29_noChanges_searchableMap_filterable_prev},
					[]Property{prop29_noChanges_searchableMap_filterable_next},
					nil,
				)

				assert.Empty(t, delta.ToAdd)
				assert.Empty(t, delta.ToDelete)

				allPrev = append(allPrev, prop29_noChanges_searchableMap_filterable_prev)
				allNext = append(allNext, prop29_noChanges_searchableMap_filterable_next)
			})

			t.Run("filterable", func(t *testing.T) {
				prop39_noChanges_filterable_prev := Property{
					Name: "noChanges_f",
					Items: []Countable{{
						Data:          []byte("immutable_01"),
						TermFrequency: 7,
					}, {
						Data:          []byte("immutable_02"),
						TermFrequency: 3,
					}},
					Length:             25,
					HasFilterableIndex: true,
					HasSearchableIndex: true,
				}
				prop39_noChanges_filterable_next := prop39_noChanges_filterable_prev

				delta := DeltaSkipSearchable(
					[]Property{prop39_noChanges_filterable_prev},
					[]Property{prop39_noChanges_filterable_next},
					nil,
				)

				assert.Empty(t, delta.ToAdd)
				assert.Empty(t, delta.ToDelete)

				allPrev = append(allPrev, prop39_noChanges_filterable_prev)
				allNext = append(allNext, prop39_noChanges_filterable_next)
			})
		})

		t.Run("sanity check - all properties at once", func(t *testing.T) {
			delta := DeltaSkipSearchable(allPrev, allNext, allSkipSearchable)

			assert.ElementsMatch(t, allExpectedToAdd, delta.ToAdd)
			assert.ElementsMatch(t, allExpectedToDel, delta.ToDelete)
		})
	})
}

func TestDeltaAnalyzer_Arrays(t *testing.T) {
	lexInt64 := func(val int64) []byte {
		bytes, _ := ent.LexicographicallySortableInt64(val)
		return bytes
	}
	lexBool := func(val bool) []byte {
		if val {
			return []uint8{1}
		}
		return []uint8{0}
	}

	t.Run("with previous indexing - both additions and deletions", func(t *testing.T) {
		previous := []Property{
			{
				Name: "ints",
				Items: []Countable{
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(102)},
					{Data: lexInt64(103)},
					{Data: lexInt64(104)},
				},
				Length:             9,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "booleans",
				Items: []Countable{
					{Data: lexBool(true)},
					{Data: lexBool(true)},
					{Data: lexBool(true)},
					{Data: lexBool(false)},
				},
				Length:             4,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "numbers",
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "texts",
				Items: []Countable{
					{Data: []byte("aaa")},
					{Data: []byte("bbb")},
					{Data: []byte("ccc")},
				},
				Length:             3,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "dates",
				Items: []Countable{
					{Data: []byte("2021-06-01T22:18:59.640162Z")},
					{Data: []byte("2022-06-01T22:18:59.640162Z")},
				},
				Length:             2,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_creationTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778000000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_lastUpdateTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778000000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}
		next := []Property{
			{
				Name: "ints",
				Items: []Countable{
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(101)},
					{Data: lexInt64(103)},
					{Data: lexInt64(104)},
					{Data: lexInt64(105)},
				},
				Length:             7,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "booleans",
				Items: []Countable{
					{Data: lexBool(true)},
					{Data: lexBool(true)},
					{Data: lexBool(true)},
					{Data: lexBool(false)},
				},
				Length:             4,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "texts",
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_creationTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778000000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_lastUpdateTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778500000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}

		expectedAdd := []Property{
			{
				Name: "ints",
				Items: []Countable{
					{Data: lexInt64(105)},
				},
				Length:             7,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "texts",
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "dates",
				Items:              []Countable{},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_lastUpdateTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778500000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}
		expectedDelete := []Property{
			{
				Name: "ints",
				Items: []Countable{
					{Data: lexInt64(102)},
				},
				Length:             9,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "texts",
				Items: []Countable{
					{Data: []byte("aaa")},
					{Data: []byte("bbb")},
					{Data: []byte("ccc")},
				},
				Length:             3,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "_lastUpdateTimeUnix",
				Items: []Countable{
					{Data: []byte("1703778000000")},
				},
				Length:             0,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name: "dates",
				Items: []Countable{
					{Data: []byte("2021-06-01T22:18:59.640162Z")},
					{Data: []byte("2022-06-01T22:18:59.640162Z")},
				},
				Length:             2,
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}

		delta := Delta(previous, next)

		assert.ElementsMatch(t, expectedAdd, delta.ToAdd)
		assert.ElementsMatch(t, expectedDelete, delta.ToDelete)
	})
}

func TestDeltaNilAnalyzer(t *testing.T) {
	previous := []NilProperty{
		{
			Name:                "ints",
			AddToPropertyLength: false,
		},
		{
			Name:                "booleans",
			AddToPropertyLength: true,
		},
		{
			Name:                "numbers",
			AddToPropertyLength: true,
		},
	}
	next := []NilProperty{
		{
			Name:                "booleans",
			AddToPropertyLength: true,
		},
		{
			Name:                "texts",
			AddToPropertyLength: true,
		},
		{
			Name:                "dates",
			AddToPropertyLength: false,
		},
	}

	expectedAdd := []NilProperty{
		{
			Name:                "texts",
			AddToPropertyLength: true,
		},
		{
			Name:                "dates",
			AddToPropertyLength: false,
		},
	}
	expectedDelete := []NilProperty{
		{
			Name:                "ints",
			AddToPropertyLength: false,
		},
		{
			Name:                "numbers",
			AddToPropertyLength: true,
		},
	}

	deltaNil := DeltaNil(previous, next)
	assert.Equal(t, expectedAdd, deltaNil.ToAdd)
	assert.Equal(t, expectedDelete, deltaNil.ToDelete)
}
