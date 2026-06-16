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

package nested

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// fast_path_python_port_test.go ports filter scenarios from the Python e2e
// suite `test_nested_props_array_intermediates.py` into the fast-path test
// harness. The goal is to verify that the fast-path implementation produces
// the same wantDocs the Python suite already asserts at the wire level —
// without spinning up a real DB.
//
// Schema mapping. Python defines three variants of the path to cars[]:
//
//   at_root_cars         — cars[] at the property root
//     → reuses the existing L0 schema (l0Schema)
//
//   under_country_object — country.garages[].cars[]
//                          (single OBJECT at the outer level, OBJECT_ARRAYs
//                          below)
//     → SKIPPED in this first cut. AssignPositions encodes a single OBJECT
//       as a 1-element array internally, so semantically this is a degenerate
//       countries_array with one country per doc. Adding it back as a third
//       schema is a small follow-up if a regression ever shows up specifically
//       on the OBJECT-outer shape.
//
//   under_countries_array — countries[].garages[].cars[]
//     → reuses the existing L2 schema (l2Schema)
//
// Coverage scope. Of the 79 Python tests, 36 directly port to the existing
// L0+L2 helpers, 3 partially port (the all_datatypes subset our fixtures
// already cover), and 22 don't port without harness extensions:
//
//   - Tokenization (multi_token_equal, contains_any_multi_token,
//     like_word_tokenized): AssignPositions emits a single PositionedValue
//     per scalar field with the raw value. Tokenization is the analyzer's
//     job, done by the production write path. The harness's addDoc stores
//     idx.values[path][rawValue] without splitting. Porting these needs
//     either a tokenizer step in addDoc or pre-tokenized fixtures.
//
//   - Range operators (<, >, <=, >=, comparison_operators): no leaf builder
//     for ranges yet.
//
//   - NotEqual: distinct from owner-level NOT (Equal negation). Needs a
//     dedicated leaf builder for the per-element-existential reading.
//
//   - Flat doc-level property combined with nested (flat_and_nested_in_and,
//     flat_or_nested): the L0/L2 schemas have no flat property; would need
//     a schema extension.
//
//   - Wire-protocol error (invalid_filter_returns_server_error): not a
//     filter-result test.
//
// Structure. Two test functions — TestFastPathL0_PythonPort and TestFastPathL2_PythonPort — each
// with one t.Run subtest per ported scenario. Both share a fixture built
// at the top of the test (buildPythonPortL0 / buildPythonPortL2). Each fixture grows as
// new scenarios are ported; docs are designed so each contributes to as
// many scenarios' discriminators as possible.
//
// Field mapping. Python uses `make` (text, FIELD) + `color` (text, FIELD)
// as its two scalar filter fields. The L0/L2 schemas now carry `make` +
// `model` (model added for the ports). Cars in the fixtures keep
// brand-consistent make+model pairs (Toyota+Camry, Honda+Civic,
// Kia+Sportage, Ford+F150/Mustang) so a human reader sees realistic
// values. The same_element_and discriminator filters on `make + year`
// instead of `make + model` — year is brand-independent, so the split
// fixture (`Toyota+Camry+2019` AND `Honda+Civic+2020`) needs no
// semantically-wrong cross-brand pair like Honda+Camry to discriminate.

// buildPythonPortL0 builds the shared fixture for the L0 (cars[] at root) port
// tests. Each doc's role across the ported scenarios is annotated; roles use
// the Python test's vocabulary (e.g. "cars1_present_no_make") so the port
// maps cleanly back.
func buildPythonPortL0(t *testing.T) *fastPathIndex {
	t.Helper()
	prop := l0Schema()
	idx := newFastPathIndex("cars")

	// doc 100: single Toyota Camry, 2020.
	//   - same_element_and (make=Toyota AND year=2020): match.
	//   - arr_n_pin (cars[0].make=Toyota): match.
	//   - pinned_is_null (cars[1].make): cars1_missing.
	idx.addDoc(t, prop, 100, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
	})
	// doc 200: Toyota Camry 2019 + Honda Civic 2020 — both cars have
	// brand-consistent make+model. make=Toyota fires at cars[0]; year=
	// 2020 fires at cars[1]; no same-car satisfies both.
	//   - same_element_and: split-within-cars, no match.
	//   - arr_n_pin: cars[0]=Toyota → match.
	//   - pinned_is_null: cars1_present_with_make.
	idx.addDoc(t, prop, 200, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "year": 2019},
		map[string]any{"make": "Honda", "model": "Civic", "year": 2020},
	})
	// doc 300: single Honda Civic.
	//   - arr_n_pin: cars[0]=Honda → miss.
	//   - pinned_is_null: cars1_missing.
	idx.addDoc(t, prop, 300, []any{
		map[string]any{"make": "Honda", "model": "Civic", "year": 2018},
	})
	// doc 400: Toyota Camry 2020 (cars[0]) + a car with only year set
	// (cars[1] — no make, no model).
	//   - same_element_and: cars[0] matches (Toyota+2020 same-car).
	//   - is_null_on_leaf: some_missing_make.
	//   - pinned_is_null: cars1_present_no_make.
	idx.addDoc(t, prop, 400, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
		map[string]any{"year": 2018}, // no make, no model
	})
	// doc 500: single car with only year set (no make, no model).
	//   - is_null_on_leaf: none_have_make.
	//   - pinned_is_null: cars1_missing.
	idx.addDoc(t, prop, 500, []any{
		map[string]any{"year": 2017},
	})
	// doc 600: empty cars[]. Vacuous case for every scenario.
	idx.addDoc(t, prop, 600, []any{})
	// doc 700: Toyota Camry with colors=[red, blue].
	//   - contains_all [red, blue]: one_car_has_both → match.
	//   - contains_any [red, blue]: match.
	idx.addDoc(t, prop, 700, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red", "blue"}},
	})
	// doc 800: Toyota Camry with colors=[red] only.
	//   - contains_all: one_car_has_one → no match (missing blue).
	//   - contains_any: match (has red).
	idx.addDoc(t, prop, 800, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
	})
	// doc 900: Toyota Camry red + Honda Civic blue.
	//   - contains_all: split across cars, no match.
	//   - contains_any: match.
	//   - pinned_is_null: cars1_present_with_make.
	idx.addDoc(t, prop, 900, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
		map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"blue"}},
	})
	// doc 1000: Ford F150 with colors=[green] (outside {red, blue}).
	//   - contains_any: no match (all values outside list).
	//   - contains_none: has_outside_value → match.
	idx.addDoc(t, prop, 1000, []any{
		map[string]any{"make": "Ford", "model": "F150", "colors": []any{"green"}},
	})
	// doc 1100: two Fords, both outside {red, blue}.
	//   - contains_any: all_values_outside_list → no match.
	//   - pinned_is_null: cars1_present_with_make.
	idx.addDoc(t, prop, 1100, []any{
		map[string]any{"make": "Ford", "model": "F150", "colors": []any{"green"}},
		map[string]any{"make": "Ford", "model": "Mustang", "colors": []any{"yellow"}},
	})
	// doc 1200: Toyota Camry red (in list) + Ford F150 green (outside).
	//   - contains_none: mixed_inside_and_outside → match (∃ outside value).
	//   - pinned_is_null: cars1_present_with_make.
	idx.addDoc(t, prop, 1200, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
		map[string]any{"make": "Ford", "model": "F150", "colors": []any{"green"}},
	})

	return idx
}

// buildPythonPortL2 builds the shared fixture for the L2 (countries[].garages[].
// cars[]) port tests. Same role-annotation convention as buildPythonPortL0.
// Multi-garage and multi-country discriminators (Python's split_across_garages,
// split_across_countries, match_via_one_garage, match_via_one_country, etc.)
// motivate most L2-only docs beyond the L0 set.
func buildPythonPortL2(t *testing.T) *fastPathIndex {
	t.Helper()
	prop := l2Schema()
	idx := newFastPathIndex("countries")

	// --- same_element_and discriminators -------------------------------
	// Filter: make=Toyota AND year=2020 (year is brand-independent, so
	// the split fixture can keep brand-consistent make+model pairs).
	//
	// doc 100: one country, one Amsterdam garage, one Toyota Camry 2020.
	// Same-car match.
	idx.addDoc(t, prop, 100, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
			}},
		}},
	})
	// doc 200: split within one garage — Toyota Camry 2019 and Honda
	// Civic 2020 live in different cars. make=Toyota fires at cars[0];
	// year=2020 fires at cars[1]; no same-car match.
	idx.addDoc(t, prop, 200, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "year": 2019},
				map[string]any{"make": "Honda", "model": "Civic", "year": 2020},
			}},
		}},
	})
	// doc 300: split across garages — Toyota Camry 2019 in Amsterdam,
	// Honda Civic 2020 in Rotterdam. No same-car match anywhere.
	idx.addDoc(t, prop, 300, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "year": 2019},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic", "year": 2020},
			}},
		}},
	})
	// doc 400: match via one garage — Amsterdam has Toyota Camry 2020;
	// Rotterdam has an unrelated Kia Sportage 2018. Same-car match in
	// Amsterdam.
	idx.addDoc(t, prop, 400, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"make": "Kia", "model": "Sportage", "year": 2018},
			}},
		}},
	})

	// --- is_null_on_leaf discriminators --------------------------------
	// doc 500: cars[0]=Toyota+make, cars[1]={year only}. Some_missing_make;
	// pinned_is_null cars1_present_no_make.
	idx.addDoc(t, prop, 500, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
				map[string]any{"year": 2018}, // no make, no model
			}},
		}},
	})
	// doc 600: single car with only year set (no make, no model).
	// none_have_make; pinned_is_null cars1_missing.
	idx.addDoc(t, prop, 600, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"year": 2017},
			}},
		}},
	})
	// doc 700: single garage with empty cars[]. empty_cars vacuous case.
	idx.addDoc(t, prop, 700, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{}},
		}},
	})
	// doc 800: split across garages — Amsterdam has Toyota Camry,
	// Rotterdam has a year-only car (no make). some_missing_across_garages.
	idx.addDoc(t, prop, 800, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry"},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"year": 2018}, // no make, no model
			}},
		}},
	})
	// doc 900: split across countries — Netherlands has Toyota Camry,
	// Germany has a year-only car. some_missing_across_countries.
	idx.addDoc(t, prop, 900, []any{
		map[string]any{"name": "Netherlands", "garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry"},
			}},
		}},
		map[string]any{"name": "Germany", "garages": []any{
			map[string]any{"city": "Berlin", "cars": []any{
				map[string]any{"year": 2018}, // no make, no model
			}},
		}},
	})

	// --- arr_n_pin discriminators --------------------------------------
	// doc 1000: Honda Civic + Toyota Camry in one garage. cars[0]=Honda
	// → arr_n_pin miss; cars[1]=Toyota has make → pinned_is_null
	// cars1_present_with_make.
	idx.addDoc(t, prop, 1000, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic"},
				map[string]any{"make": "Toyota", "model": "Camry"},
			}},
		}},
	})
	// doc 1100: arr_n_pin miss_across_garages — both garages cars[0]
	// non-Toyota. Amsterdam=[Honda Civic, Toyota Camry];
	// Rotterdam=[Ford F150, Toyota Camry].
	idx.addDoc(t, prop, 1100, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic"},
				map[string]any{"make": "Toyota", "model": "Camry"},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"make": "Ford", "model": "F150"},
				map[string]any{"make": "Toyota", "model": "Camry"},
			}},
		}},
	})
	// doc 1200: arr_n_pin miss_across_countries — both countries cars[0]
	// non-Toyota. NL=[Honda Civic, Toyota Camry]; DE=[Ford F150, Toyota
	// Camry].
	idx.addDoc(t, prop, 1200, []any{
		map[string]any{"name": "Netherlands", "garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic"},
				map[string]any{"make": "Toyota", "model": "Camry"},
			}},
		}},
		map[string]any{"name": "Germany", "garages": []any{
			map[string]any{"city": "Berlin", "cars": []any{
				map[string]any{"make": "Ford", "model": "F150"},
				map[string]any{"make": "Toyota", "model": "Camry"},
			}},
		}},
	})

	// --- contains_X discriminators -------------------------------------
	// doc 1300: Toyota Camry with colors=[red, blue]. contains_all
	// one_car_has_both.
	idx.addDoc(t, prop, 1300, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red", "blue"}},
			}},
		}},
	})
	// doc 1400: Toyota Camry with colors=[red]. contains_all one_car_has_one.
	idx.addDoc(t, prop, 1400, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
			}},
		}},
	})
	// doc 1500: split colors within one garage — Toyota Camry red +
	// Honda Civic blue. contains_all split_across_cars.
	idx.addDoc(t, prop, 1500, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
				map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"blue"}},
			}},
		}},
	})
	// doc 1600: split colors across garages — Amsterdam Toyota Camry red,
	// Rotterdam Honda Civic blue. contains_all split_across_garages.
	idx.addDoc(t, prop, 1600, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"blue"}},
			}},
		}},
	})
	// doc 1700: split colors across countries — NL Toyota Camry red,
	// DE Honda Civic blue. contains_all split_across_countries.
	idx.addDoc(t, prop, 1700, []any{
		map[string]any{"name": "Netherlands", "garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
			}},
		}},
		map[string]any{"name": "Germany", "garages": []any{
			map[string]any{"city": "Berlin", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"blue"}},
			}},
		}},
	})
	// doc 1800: Amsterdam Toyota Camry [red,blue]; Rotterdam Ford F150
	// green (unrelated). contains_all match_via_one_garage.
	idx.addDoc(t, prop, 1800, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red", "blue"}},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"make": "Ford", "model": "F150", "colors": []any{"green"}},
			}},
		}},
	})
	// doc 1900: NL Toyota Camry [red,blue]; DE Ford F150 green (unrelated).
	// contains_all match_via_one_country.
	idx.addDoc(t, prop, 1900, []any{
		map[string]any{"name": "Netherlands", "garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red", "blue"}},
			}},
		}},
		map[string]any{"name": "Germany", "garages": []any{
			map[string]any{"city": "Berlin", "cars": []any{
				map[string]any{"make": "Ford", "model": "F150", "colors": []any{"green"}},
			}},
		}},
	})
	// doc 2000: Ford F150 with colors=[green] outside the {red, blue} list.
	// contains_none has_outside_value; contains_any all_outside.
	idx.addDoc(t, prop, 2000, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Ford", "model": "F150", "colors": []any{"green"}},
			}},
		}},
	})

	return idx
}

// TestFastPathL0_PythonPort runs the L0 (cars[] at root) ports as subtests against the
// shared buildPythonPortL0 fixture. Subtest names mirror the Python test function
// names (without the _at_root_cars suffix).
func TestFastPathL0_PythonPort(t *testing.T) {
	idx := buildPythonPortL0(t)

	t.Run("same_element_and", func(t *testing.T) {
		// Python: test_same_element_and_at_root_cars.
		// Same-Scope AND at cars: Witnesses survive only at cars-self
		// bits where both leaves fire same-car. Doc 200's split (Toyota
		// 2019 at cars[0], Honda 2020 at cars[1]) loses the cars-self
		// intersection. Docs 100 and 400 each have a single Toyota 2020
		// car that matches.
		r := andLeaves(idx,
			leafPositive(idx, "cars.make", "Toyota"),
			leafPositive(idx, "cars.year", 2020))

		assert.ElementsMatch(t, []uint64{100, 400}, idx.docIDs(r),
			"docs with a Toyota 2020 car (single or alongside others) "+
				"match; cross-car split (doc 200) does not")
	})

	t.Run("is_null_on_leaf", func(t *testing.T) {
		// Python: test_is_null_on_leaf_at_root_cars.
		// Both polarities of IS NULL on cars.make.
		//
		// IS NULL true matches docs with ∃ car missing make.
		//   doc 400 (cars[1] missing make), doc 500 (single car no make).
		//   Docs where all cars have make (100, 200, 300, 700, 800, 900,
		//   1000, 1100, 1200) and empty cars[] (600) do not match.
		//
		// IS NULL false matches docs with ∃ car having make.
		//   docs 100, 200, 300, 400, 700, 800, 900, 1000, 1100, 1200.
		//   Doc 500 (no make on any car) and doc 600 (empty cars[]) do
		//   not match.
		rTrue := leafIsNullTrue(idx, "cars.make")
		assert.ElementsMatch(t, []uint64{400, 500}, idx.docIDs(rTrue),
			"IS NULL true: docs with ∃ car missing make")

		rFalse := leafIsNullFalse(idx, "cars.make")
		assert.ElementsMatch(t,
			[]uint64{100, 200, 300, 400, 700, 800, 900, 1000, 1100, 1200},
			idx.docIDs(rFalse),
			"IS NULL false: docs with ∃ car having make")
	})

	t.Run("arr_n_pin", func(t *testing.T) {
		// Python: test_arr_n_pin_at_root_cars.
		// cars[0].make=Toyota — only docs whose first car is Toyota.
		// Match: docs where cars[0]=Toyota → {100, 200, 400, 700, 800,
		// 900, 1200}. Miss: cars[0]=Honda (300), Ford (1000, 1100), no
		// make (500), empty (600).
		r := leafPinnedPositive(idx, "cars.make", "Toyota",
			[]pinSpec{{"cars", 0}})

		assert.ElementsMatch(t,
			[]uint64{100, 200, 400, 700, 800, 900, 1200}, idx.docIDs(r),
			"docs where cars[0].make=Toyota match; cars[0]≠Toyota miss")
	})

	t.Run("pinned_is_null", func(t *testing.T) {
		// Python: test_pinned_is_null_at_root_cars.
		// cars[1].make IS NULL — owner-level negation (no pin gap; routes
		// through negate(leafPinnedIsNullFalse)).
		//
		// pos.Witnesses = docs where cars[1] exists and has make set:
		// {200 (Honda), 900 (Honda), 1100 (Ford), 1200 (Ford)}.
		// negate at pathRoot: docUniverse AndNot pos.Witnesses = all
		// ingested docs except those four.
		//
		// DIVERGENCE FROM PYTHON: Python (with TODOs) currently asserts
		// that docs lacking cars[1] (cars1_missing) do NOT match and that
		// empty cars[] (empty_cars) does NOT match. The fast-path includes
		// both via docUniverse-based negation. Python's TODO flags that
		// cars1_missing should flip to match once "pinned-IsNull recovery"
		// lands; empty_cars stays out per Python's intent. The fast-path
		// already implements the recovered semantic for cars1_missing AND
		// extends it to empty_cars. Asserts the fast-path's actual output.
		r := leafPinnedIsNullTrue(idx, "cars.make",
			[]pinSpec{{"cars", 1}})

		assert.ElementsMatch(t,
			[]uint64{100, 300, 400, 500, 600, 700, 800, 1000}, idx.docIDs(r),
			"fast-path: cars[1] missing or no-make → match; "+
				"includes empty_cars (600), which Python excludes")
	})

	t.Run("contains_any", func(t *testing.T) {
		// Python: test_contains_any_at_root_cars.
		// cars.colors ContainsAny [red, blue] — ∃ car owns any listed value.
		// Match: {700, 800, 900, 1200} (each has red or blue somewhere).
		// Miss: docs without colors (100-500), empty cars (600), all-
		// outside (1000, 1100).
		r := leafContainsAny(idx, "cars.colors", "red", "blue")

		assert.ElementsMatch(t, []uint64{700, 800, 900, 1200}, idx.docIDs(r),
			"docs where ∃ car owns red or blue in colors match")
	})

	t.Run("contains_all", func(t *testing.T) {
		// Python: test_contains_all_at_root_cars.
		// cars.colors ContainsAll [red, blue] — ∃ car owns BOTH values in
		// the same colors array. Only doc 700 (colors=[red, blue] in one
		// car) qualifies. Doc 800 (red only) missing blue; doc 900 split
		// across cars.
		r := leafContainsAll(idx, "cars.colors", "red", "blue")

		assert.ElementsMatch(t, []uint64{700}, idx.docIDs(r),
			"only the single car with both red and blue colors matches")
	})

	t.Run("contains_none", func(t *testing.T) {
		// Python: test_contains_none_at_root_cars.
		// cars.colors ContainsNone [red, blue].
		//
		// Fast-path is OWNER-LEVEL: Witnesses = anchor(cars) AndNot
		// values[red] AndNot values[blue]. Any cars-self bit not in
		// either value bucket survives — which includes cars with no
		// colors at all (no bits in the value buckets).
		//
		// DIVERGENCE FROM PYTHON: Python is per-tag-element existential
		// — requires ∃ a colors value outside the listed set. Docs with
		// no colors at all (100-500) drop in Python (vacuous, nothing to
		// be outside the list). Fast-path matches them because cars-self
		// bits survive the AndNot trivially.
		//
		// Asserts the fast-path's actual output.
		r := leafContainsNone(idx, "cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{100, 200, 300, 400, 500, 1000, 1100, 1200}, idx.docIDs(r),
			"fast-path: docs with at least one car whose colors don't "+
				"intersect the listed values match (including no-colors-"+
				"at-all docs which Python excludes as vacuous)")
	})
}

// TestFastPathL2_PythonPort runs the L2 (countries[].garages[].cars[]) ports as subtests
// against the shared buildPythonPortL2 fixture. Subtest names mirror the Python
// test function names (without the _under_countries_array suffix).
func TestFastPathL2_PythonPort(t *testing.T) {
	idx := buildPythonPortL2(t)

	t.Run("same_element_and", func(t *testing.T) {
		// Python: test_same_element_and_under_countries_array.
		// Filter: make=Toyota AND year=2020. Same-Scope AND at
		// countries.garages.cars; Witnesses survive only at cars-self
		// bits where both leaves fire same-car.
		//   doc 100: single Toyota Camry 2020 → match.
		//   doc 400: Amsterdam Toyota Camry 2020 → match.
		//   doc 500: cars[0] is Toyota Camry 2020 → match.
		//   docs 200, 300: splits within / across garages → no match.
		r := andLeaves(idx,
			leafPositive(idx, "countries.garages.cars.make", "Toyota"),
			leafPositive(idx, "countries.garages.cars.year", 2020))

		assert.ElementsMatch(t, []uint64{100, 400, 500}, idx.docIDs(r),
			"docs with a Toyota 2020 car somewhere match (100, 400, 500); "+
				"splits across cars/garages (200, 300) do not")
	})

	t.Run("is_null_on_leaf", func(t *testing.T) {
		// Python: test_is_null_on_leaf_under_countries_array.
		//
		// IS NULL true (fast-path): Witnesses_not = anchor(cars) AndNot
		// pos.Witnesses (= cars-self bits where make is set). Docs
		// with ∃ car missing make match:
		//   doc 500 (cars[1] missing make), doc 600 (single car no make),
		//   doc 800 (Rotterdam car missing make), doc 900 (Germany car
		//   missing make).
		// Docs where all cars have make (100, 200, 300, 400, 1000, 1100,
		// 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000) and
		// empty cars[] (700) do not match.
		rTrue := leafIsNullTrue(idx, "countries.garages.cars.make")
		assert.ElementsMatch(t,
			[]uint64{500, 600, 800, 900}, idx.docIDs(rTrue),
			"IS NULL true: docs with ∃ car missing make")

		// IS NULL false: docs with ∃ car having make.
		rFalse := leafIsNullFalse(idx, "countries.garages.cars.make")
		assert.ElementsMatch(t,
			[]uint64{100, 200, 300, 400, 500, 800, 900, 1000, 1100, 1200,
				1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000},
			idx.docIDs(rFalse),
			"IS NULL false: docs with ∃ car having make")
	})

	t.Run("arr_n_pin", func(t *testing.T) {
		// Python: test_arr_n_pin_under_countries_array.
		// countries.garages.cars[0].make=Toyota — ∃ garage whose first car
		// is Toyota. Match: docs where ∃ garage's cars[0]=Toyota.
		// Miss: pinned_miss (1000), miss_across_garages (1100),
		// miss_across_countries (1200), empty (700), no-Toyota docs
		// (300 Amsterdam Toyota+Civic, but its cars[0]=Toyota — actually
		// matches; let me recount).
		//
		// Per-doc trace:
		//   100 cars[0]=Toyota → match.
		//   200 cars[0]=Toyota → match.
		//   300 Amsterdam cars[0]=Toyota → match.
		//   400 Amsterdam cars[0]=Toyota → match.
		//   500 cars[0]=Toyota → match.
		//   600 cars[0]=no-make → miss.
		//   700 empty cars[] → miss.
		//   800 Amsterdam cars[0]=Toyota → match.
		//   900 NL cars[0]=Toyota → match.
		//   1000 cars[0]=Honda → miss.
		//   1100 Amsterdam cars[0]=Honda, Rotterdam cars[0]=Ford → miss.
		//   1200 NL cars[0]=Honda, DE cars[0]=Ford → miss.
		//   1300 cars[0]=Toyota → match.
		//   1400 cars[0]=Toyota → match.
		//   1500 cars[0]=Toyota → match.
		//   1600 Amsterdam cars[0]=Toyota → match.
		//   1700 NL cars[0]=Toyota → match.
		//   1800 Amsterdam cars[0]=Toyota → match.
		//   1900 NL cars[0]=Toyota → match.
		//   2000 cars[0]=Ford → miss.
		r := leafPinnedPositive(idx, "countries.garages.cars.make", "Toyota",
			[]pinSpec{{"countries.garages.cars", 0}})

		assert.ElementsMatch(t,
			[]uint64{100, 200, 300, 400, 500, 800, 900,
				1300, 1400, 1500, 1600, 1700, 1800, 1900}, idx.docIDs(r),
			"docs with ∃ garage whose cars[0]=Toyota match")
	})

	t.Run("pinned_is_null", func(t *testing.T) {
		// Python: test_pinned_is_null_under_countries_array.
		// cars[1].make IS NULL — owner-level negation via negate(IsNullFalse).
		//
		// pos.Witnesses = garage-self bits where cars[1] exists and has
		// make set (lifted from cars[1]-self to countries.garages).
		// Per-doc trace at the garage level:
		//   100 single car: cars[1] missing.
		//   200 cars[1]=Honda (has make): garage in pos.W.
		//   300 Amsterdam cars[1] missing, Rotterdam cars[1] missing.
		//   400 same: both garages cars[1] missing.
		//   500 cars[1]=no-make: not in pos.W.
		//   600 single no-make: cars[1] missing.
		//   700 empty cars: cars[1] missing.
		//   800 Amsterdam single car, Rotterdam single car: both cars[1] missing.
		//   900 NL/DE single cars: cars[1] missing.
		//   1000 cars[1]=Toyota: in pos.W.
		//   1100 both garages cars[1]=Toyota: both in pos.W.
		//   1200 both countries cars[1]=Toyota: both in pos.W.
		//   1300, 1400 single car: cars[1] missing.
		//   1500 cars[1]=Honda: in pos.W.
		//   1600 split: both garages single car, cars[1] missing in both.
		//   1700 split countries: NL+DE single car, cars[1] missing.
		//   1800 Amsterdam single car, Rotterdam single car: cars[1] missing.
		//   1900 NL+DE single car: cars[1] missing.
		//   2000 single car: cars[1] missing.
		//
		// Witnesses_not = anchor(garages) AndNot pos.W. Docs with ∃ garage
		// not in pos.W match.
		//
		// DIVERGENCE FROM PYTHON: empty_cars (doc 700) is included by
		// fast-path because countries.garages-self bit exists even when
		// the garage's cars[] is empty. Python excludes empty_cars per
		// vacuous-drop rule. Same divergence shape as L0.
		r := leafPinnedIsNullTrue(idx, "countries.garages.cars.make",
			[]pinSpec{{"countries.garages.cars", 1}})

		assert.ElementsMatch(t,
			[]uint64{100, 300, 400, 500, 600, 700, 800, 900,
				1300, 1400, 1600, 1700, 1800, 1900, 2000}, idx.docIDs(r),
			"fast-path: docs with ∃ garage whose cars[1] missing or "+
				"no-make match; includes empty_cars (700)")
	})

	t.Run("contains_any", func(t *testing.T) {
		// Python: test_contains_any_under_countries_array.
		// cars.colors ContainsAny [red, blue] — ∃ car owns any listed
		// value. Match docs with red or blue somewhere:
		//   1300 colors=[red,blue], 1400 colors=[red], 1500 split,
		//   1600 split_across_garages, 1700 split_across_countries,
		//   1800 match_via_one_garage, 1900 match_via_one_country.
		// All other docs lack red/blue colors.
		r := leafContainsAny(idx, "countries.garages.cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{1300, 1400, 1500, 1600, 1700, 1800, 1900}, idx.docIDs(r),
			"docs where ∃ car owns red or blue in colors match")
	})

	t.Run("contains_all", func(t *testing.T) {
		// Python: test_contains_all_under_countries_array.
		// cars.colors ContainsAll [red, blue] — ∃ car owns BOTH values in
		// the same colors array. Match: doc 1300 (single car has both),
		// doc 1800 (Amsterdam has both), doc 1900 (NL has both).
		// Miss: split-across-cars (1500), split-across-garages (1600),
		// split-across-countries (1700), and every doc without both
		// colors on the same car.
		r := leafContainsAll(idx, "countries.garages.cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{1300, 1800, 1900}, idx.docIDs(r),
			"docs with ∃ single car holding both red and blue match")
	})

	t.Run("contains_none", func(t *testing.T) {
		// Python: test_contains_none_under_countries_array.
		// cars.colors ContainsNone [red, blue].
		//
		// Fast-path is OWNER-LEVEL: Witnesses = anchor(cars) AndNot
		// values[red] AndNot values[blue]. Docs with ∃ cars-self bit
		// not in either value bucket match — INCLUDING docs with no
		// colors at all (cars-self bits trivially survive AndNot).
		//
		// DIVERGENCE FROM PYTHON: Python is per-tag-element existential
		// (requires ∃ a value outside the list). Docs without any
		// colors values (100-1200) drop in Python as vacuous; fast-path
		// matches them.
		r := leafContainsNone(idx, "countries.garages.cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{
				// All docs with no colors set at all (100-900 single-
				// country/garage shapes, 1000-1200 multi-garage/country
				// arr_n_pin shapes) — cars-self bits trivially survive.
				100, 200, 300, 400, 500, 600, 800, 900,
				1000, 1100, 1200,
				// Docs with mixed inside/outside: 1800 Rotterdam=green,
				// 1900 DE=green — those cars-self survive AndNot.
				1800, 1900,
				// Doc 2000: colors=[green] outside the list.
				2000,
				// NOT: 700 (empty cars, no cars-self), 1300 (all red+blue),
				// 1400 (all red), 1500 (red + blue split — every car-self
				// in values), 1600 (split garages, same), 1700 (split
				// countries, same).
			}, idx.docIDs(r),
			"fast-path: docs with ∃ car-self not in any value bucket "+
				"(includes no-colors docs that Python excludes as vacuous)")
	})
}
