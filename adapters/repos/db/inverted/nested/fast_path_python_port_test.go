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
	"github.com/stretchr/testify/require"
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
//     → uses the l2ObjectSchema helper. AssignPositions encodes a single
//       OBJECT as a 1-element array internally, so dispatch is the same as
//       L2 with one-country-per-doc fixtures. The port mirrors L2 but
//       drops the multi-country discriminators (no analog under a single
//       OBJECT root).
//
//   under_countries_array — countries[].garages[].cars[]
//     → reuses the existing L2 schema (l2Schema)
//
// Coverage scope. Of the 79 Python tests, 60 port across the three
// schemas (20 scenarios × 3 variants), broken down as:
//
//   - Groups A+B+C (18 scenarios × 3 = 54): the existing L0+L2+L2_object
//     helpers cover them directly.
//   - Group D (2 scenarios × 3 = 6): tokenization-aware scenarios
//     enabled by running tokenizer.Tokenize between AssignPositions and
//     the value-bucket emit. Filters: multi_token_equal,
//     contains_any_multi_token.
//   - NotEqual (1 scenario × 3 = 3): leafNotEqual as a thin alias for
//     negate(leafPositive(...)) — owner-level / include-missing
//     semantics matching the harness's negate convention.
//
// 3 scenarios partially port (the all_datatypes subset our fixtures
// already cover — int/text/text[]). 19 wire-tests are deferred (see below).
//
// On top of the Python ports, the file carries harness-gap coverage —
// scenarios not in Python but where the harness's behaviour benefits
// from explicit lock-down tests. Each adds 1–3 t.Run cases × 3 schemas:
//
//   - Tokenized Contains (5 scenarios): contains_none_mixed_...,
//     contains_all_mixed_..., contains_none_two_multi_token,
//     contains_any_two_multi_token, contains_all_two_multi_token.
//   - Pinned Contains tokenization (3 scenarios):
//     pinned_contains_any_multi_token, pinned_contains_all_mixed_...,
//     pinned_contains_none_mixed_....
//   - Pinned NotEqual (1 scenario): pinned_not_equal — single pin, no
//     gap, plus a missing-make discriminator.
//   - Multi-pin tokenized Contains + intermediate-pin NotEqual (2
//     scenarios × L2 + L2_object): multi_pin_contains_any_multi_token,
//     intermediate_pin_not_equal — verify dispatch under longer pin
//     chains and the gap branch.
//
// Group D wantDocs follow the harness's lenient same-element rule for
// tokenized text[]: tokens may come from DIFFERENT tag entries of the
// same parent element. Concretely a doc with tags=["family sedan",
// "hybrid model"] matches `tags ContainsAny ["family hybrid"]` because
// the cars-self chain bit is in both token buckets. This mirrors the
// flat-index behaviour (a flat tokenized text[] inverted index has no
// per-array-entry identity at all, so same-doc co-occurrence is the
// only available semantic). It diverges from the Python wire-test
// assertions for `contains_any_multi_token`, which expect a strict
// same-tag-entry rule unique to the nested index — that strict mode
// was prototyped in this file's earlier revisions but not adopted, on
// the grounds that nested-default-strict would silently change the
// semantic of flat→nested migrations.
//
// Deferred — these don't port without further harness extensions:
//
//   - LIKE pattern matching (like_word_tokenized × 3 variants = 3): no
//     pattern-vs-bucket lookup in the harness.
//
//   - Range operators (comparison_operators × 3 = 3): no leaf builder
//     for ranges yet.
//
//   - Flat doc-level property combined with nested
//     (flat_and_nested_in_and, flat_or_nested, or_of_mixed_correlated_ands
//     × 3 = 9): the L0/L2 schemas have no flat property; would need
//     a schema extension.
//
//   - all_datatypes (3): the bool/float/date/uuid subset; fixtures
//     would need extra type coverage. The int/text/text[] subset is
//     already implicit in other tests.
//
//   - Wire-protocol error (invalid_filter_returns_server_error × 1):
//     not a filter-result test; exercises the validator path.
//
// Deferred follow-up (Python-parity, not a correctness fix): adding
// `AndNot ¬exists[cars]` after pinned negation would emulate Python's
// vacuous-drop layer at empty arrays. The distinction it would add has
// no semantic basis from the user's standpoint — the dispatch can't
// distinguish "no cars" from "no pin slot" — and the harness applies a
// uniform "missing pinned slot → match" rule. See the pinned_is_null,
// not_of_pinned_correlated_and, and pinned_not_equal tests for the
// framing.
//
// Structure. Three test functions — TestFastPathL0_PythonPort,
// TestFastPathL2_PythonPort, and TestFastPathL2Object_PythonPort — each
// with one t.Run subtest per ported scenario. Each shares a fixture built
// at the top of the test (buildPythonPortL0 / buildPythonPortL2 /
// buildPythonPortL2Object). Each fixture grows as new scenarios are
// ported; docs are designed so each contributes to as many scenarios'
// discriminators as possible. The L2_object variant skips Python's
// multi-country discriminators (split_across_countries, miss_across_
// countries, match_via_one_country) since they have no analog under a
// single OBJECT root.
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

	// --- Group B discriminators ---------------------------------------

	// doc 1300: Honda Civic 2018 at cars[0], Toyota Camry 2020 at cars[1].
	// Toyota+2020 lives at the WRONG slot for pin=cars[0].
	//   - arr_n_pin_with_and: match_at_wrong_index — must not match.
	//   - or_of_correlated_ands: group1 (Toyota+2020) fires via cars[1].
	//   - not_of_correlated_and: cars[0] is a violator → IN.
	idx.addDoc(t, prop, 1300, []any{
		map[string]any{"make": "Honda", "model": "Civic", "year": 2018},
		map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
	})
	// doc 1400: Honda Civic 2020.
	//   - arr_n_pin_with_or (cars[0].make=Toyota OR cars[0].year=2020):
	//     cars[0].year=2020 → match via year leaf only.
	//   - or_of_correlated_ands ((Toyota+2020) OR (Honda+2019)): neither.
	idx.addDoc(t, prop, 1400, []any{
		map[string]any{"make": "Honda", "model": "Civic", "year": 2020},
	})
	// doc 1500: Honda Civic 2019.
	//   - or_of_correlated_ands group2 match.
	idx.addDoc(t, prop, 1500, []any{
		map[string]any{"make": "Honda", "model": "Civic", "year": 2019},
	})
	// doc 1600: Toyota Camry 2020 + Honda Civic 2019.
	//   - or_of_correlated_ands: both groups match (cars[0] for group1,
	//     cars[1] for group2).
	idx.addDoc(t, prop, 1600, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
		map[string]any{"make": "Honda", "model": "Civic", "year": 2019},
	})
	// doc 1700: single car with only year=2020 set (no make).
	//   - is_null_in_correlated_and (year=2020 AND make IS NULL):
	//     match_same_car (the only car satisfies both leaves).
	idx.addDoc(t, prop, 1700, []any{
		map[string]any{"year": 2020},
	})
	// doc 1800: Toyota Camry with tires=[{width:215}].
	//   - is_null_on_object_array_subprop (cars.tires IS NULL):
	//     car_with_tires — must NOT match (single car has tires).
	idx.addDoc(t, prop, 1800, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
			map[string]any{"width": 215},
		}},
	})
	// doc 1900: cars[0] has tires, cars[1] has no tires. Mixed.
	//   - is_null_on_object_array_subprop: mixed_cars — match (∃ car
	//     with no tires).
	idx.addDoc(t, prop, 1900, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
			map[string]any{"width": 215},
		}},
		map[string]any{"make": "Honda", "model": "Civic"}, // no tires
	})
	// doc 2000: single car with TWO tires.
	//   - is_null_on_object_array_subprop: car_with_multiple_tires —
	//     must NOT match.
	idx.addDoc(t, prop, 2000, []any{
		map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
			map[string]any{"width": 215},
			map[string]any{"width": 215},
		}},
	})
	// doc 2100: Honda Civic with colors=[red, blue] (brand-mismatch for
	// the Equal-make leaf).
	//   - contains_all_with_equal_in_and (colors ContainsAll [red,blue]
	//     AND make=Toyota): contains satisfied but make=Honda — no match.
	//   - contains_any_with_equal_in_and: same — Honda fails the Equal.
	//   - contains_none_with_equal_in_and (colors ContainsNone [outside]
	//     AND make=Toyota): contains satisfied, but make=Honda — no match.
	idx.addDoc(t, prop, 2100, []any{
		map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"red", "blue"}},
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

	// --- Group B discriminators (single-garage focused) ---------------

	// doc 2200: Honda Civic 2018 at cars[0], Toyota Camry 2020 at cars[1]
	// — Toyota+2020 lives at wrong pinned slot.
	idx.addDoc(t, prop, 2200, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic", "year": 2018},
				map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
			}},
		}},
	})
	// doc 2300: single Honda Civic 2020. arr_n_pin_with_or
	// pinned_only_year discriminator.
	idx.addDoc(t, prop, 2300, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic", "year": 2020},
			}},
		}},
	})
	// doc 2400: single Honda Civic 2019. or_of_correlated_ands group2.
	idx.addDoc(t, prop, 2400, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic", "year": 2019},
			}},
		}},
	})
	// doc 2500: Toyota Camry 2020 + Honda Civic 2019 in one garage.
	// or_of_correlated_ands both_groups_match.
	idx.addDoc(t, prop, 2500, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
				map[string]any{"make": "Honda", "model": "Civic", "year": 2019},
			}},
		}},
	})
	// doc 2600: single car with only year=2020 set (no make).
	// is_null_in_correlated_and match_same_car.
	idx.addDoc(t, prop, 2600, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"year": 2020},
			}},
		}},
	})
	// doc 2700: Toyota Camry with tires=[{width:215}].
	// is_null_on_object_array_subprop car_with_tires.
	idx.addDoc(t, prop, 2700, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
					map[string]any{"width": 215},
				}},
			}},
		}},
	})
	// doc 2800: cars[0] has tires, cars[1] has no tires.
	// is_null_on_object_array_subprop mixed_cars.
	idx.addDoc(t, prop, 2800, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
					map[string]any{"width": 215},
				}},
				map[string]any{"make": "Honda", "model": "Civic"},
			}},
		}},
	})
	// doc 2900: single car with TWO tires.
	// is_null_on_object_array_subprop car_with_multiple_tires.
	idx.addDoc(t, prop, 2900, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
					map[string]any{"width": 215},
					map[string]any{"width": 215},
				}},
			}},
		}},
	})
	// doc 3000: Honda Civic with colors=[red, blue] (brand-mismatch).
	// contains_*_with_equal_in_and discriminator.
	idx.addDoc(t, prop, 3000, []any{
		map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"red", "blue"}},
			}},
		}},
	})

	return idx
}

// buildPythonPortL2Object builds the shared fixture for the L2_object
// (country.garages[].cars[]) port tests — same as L2 but with a single
// OBJECT root instead of OBJECT_ARRAY. AssignPositions treats a single
// OBJECT as a 1-element array internally, so dispatch is the same as L2
// with one-country-per-doc fixtures. Multi-country discriminators (Python's
// split_across_countries, miss_across_countries, match_via_one_country)
// have no analog under a single OBJECT root and are omitted.
func buildPythonPortL2Object(t *testing.T) *fastPathIndex {
	t.Helper()
	prop := l2ObjectSchema()
	idx := newFastPathIndex("country")

	// --- same_element_and discriminators -------------------------------
	// Filter: make=Toyota AND year=2020 (year is brand-independent, so
	// the split fixture can keep brand-consistent make+model pairs).
	//
	// doc 100: one country, one Amsterdam garage, one Toyota Camry 2020.
	// Same-car match.
	idx.addDoc(t, prop, 100, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
		}},
	}})
	// doc 200: split within one garage — Toyota Camry 2019 and Honda
	// Civic 2020 live in different cars. make=Toyota fires at cars[0];
	// year=2020 fires at cars[1]; no same-car match.
	idx.addDoc(t, prop, 200, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "year": 2019},
			map[string]any{"make": "Honda", "model": "Civic", "year": 2020},
		}},
	}})
	// doc 300: split across garages — Toyota Camry 2019 in Amsterdam,
	// Honda Civic 2020 in Rotterdam. No same-car match anywhere.
	idx.addDoc(t, prop, 300, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "year": 2019},
		}},
		map[string]any{"city": "Rotterdam", "cars": []any{
			map[string]any{"make": "Honda", "model": "Civic", "year": 2020},
		}},
	}})
	// doc 400: match via one garage — Amsterdam has Toyota Camry 2020;
	// Rotterdam has an unrelated Kia Sportage 2018. Same-car match in
	// Amsterdam.
	idx.addDoc(t, prop, 400, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
		}},
		map[string]any{"city": "Rotterdam", "cars": []any{
			map[string]any{"make": "Kia", "model": "Sportage", "year": 2018},
		}},
	}})

	// --- is_null_on_leaf discriminators --------------------------------
	// doc 500: cars[0]=Toyota+make, cars[1]={year only}. Some_missing_make;
	// pinned_is_null cars1_present_no_make.
	idx.addDoc(t, prop, 500, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
			map[string]any{"year": 2018}, // no make, no model
		}},
	}})
	// doc 600: single car with only year set (no make, no model).
	// none_have_make; pinned_is_null cars1_missing.
	idx.addDoc(t, prop, 600, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"year": 2017},
		}},
	}})
	// doc 700: single garage with empty cars[]. empty_cars vacuous case.
	idx.addDoc(t, prop, 700, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{}},
	}})
	// doc 800: split across garages — Amsterdam has Toyota Camry,
	// Rotterdam has a year-only car (no make). some_missing_across_garages.
	idx.addDoc(t, prop, 800, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry"},
		}},
		map[string]any{"city": "Rotterdam", "cars": []any{
			map[string]any{"year": 2018}, // no make, no model
		}},
	}})
	// doc 900 [skipped — multi-country, no L2_object analog]

	// --- arr_n_pin discriminators --------------------------------------
	// doc 1000: Honda Civic + Toyota Camry in one garage. cars[0]=Honda
	// → arr_n_pin miss; cars[1]=Toyota has make → pinned_is_null
	// cars1_present_with_make.
	idx.addDoc(t, prop, 1000, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Honda", "model": "Civic"},
			map[string]any{"make": "Toyota", "model": "Camry"},
		}},
	}})
	// doc 1100: arr_n_pin miss_across_garages — both garages cars[0]
	// non-Toyota. Amsterdam=[Honda Civic, Toyota Camry];
	// Rotterdam=[Ford F150, Toyota Camry].
	idx.addDoc(t, prop, 1100, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Honda", "model": "Civic"},
			map[string]any{"make": "Toyota", "model": "Camry"},
		}},
		map[string]any{"city": "Rotterdam", "cars": []any{
			map[string]any{"make": "Ford", "model": "F150"},
			map[string]any{"make": "Toyota", "model": "Camry"},
		}},
	}})
	// doc 1200 [skipped — multi-country, no L2_object analog]

	// --- contains_X discriminators -------------------------------------
	// doc 1300: Toyota Camry with colors=[red, blue]. contains_all
	// one_car_has_both.
	idx.addDoc(t, prop, 1300, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red", "blue"}},
		}},
	}})
	// doc 1400: Toyota Camry with colors=[red]. contains_all one_car_has_one.
	idx.addDoc(t, prop, 1400, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
		}},
	}})
	// doc 1500: split colors within one garage — Toyota Camry red +
	// Honda Civic blue. contains_all split_across_cars.
	idx.addDoc(t, prop, 1500, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
			map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"blue"}},
		}},
	}})
	// doc 1600: split colors across garages — Amsterdam Toyota Camry red,
	// Rotterdam Honda Civic blue. contains_all split_across_garages.
	idx.addDoc(t, prop, 1600, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red"}},
		}},
		map[string]any{"city": "Rotterdam", "cars": []any{
			map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"blue"}},
		}},
	}})
	// doc 1700 [skipped — multi-country, no L2_object analog]
	// doc 1800: Amsterdam Toyota Camry [red,blue]; Rotterdam Ford F150
	// green (unrelated). contains_all match_via_one_garage.
	idx.addDoc(t, prop, 1800, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "colors": []any{"red", "blue"}},
		}},
		map[string]any{"city": "Rotterdam", "cars": []any{
			map[string]any{"make": "Ford", "model": "F150", "colors": []any{"green"}},
		}},
	}})
	// doc 1900 [skipped — multi-country, no L2_object analog]
	// doc 2000: Ford F150 with colors=[green] outside the {red, blue} list.
	// contains_none has_outside_value; contains_any all_outside.
	idx.addDoc(t, prop, 2000, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Ford", "model": "F150", "colors": []any{"green"}},
		}},
	}})

	// --- Group B discriminators (single-garage focused) ---------------

	// doc 2200: Honda Civic 2018 at cars[0], Toyota Camry 2020 at cars[1]
	// — Toyota+2020 lives at wrong pinned slot.
	idx.addDoc(t, prop, 2200, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Honda", "model": "Civic", "year": 2018},
			map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
		}},
	}})
	// doc 2300: single Honda Civic 2020. arr_n_pin_with_or
	// pinned_only_year discriminator.
	idx.addDoc(t, prop, 2300, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Honda", "model": "Civic", "year": 2020},
		}},
	}})
	// doc 2400: single Honda Civic 2019. or_of_correlated_ands group2.
	idx.addDoc(t, prop, 2400, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Honda", "model": "Civic", "year": 2019},
		}},
	}})
	// doc 2500: Toyota Camry 2020 + Honda Civic 2019 in one garage.
	// or_of_correlated_ands both_groups_match.
	idx.addDoc(t, prop, 2500, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "year": 2020},
			map[string]any{"make": "Honda", "model": "Civic", "year": 2019},
		}},
	}})
	// doc 2600: single car with only year=2020 set (no make).
	// is_null_in_correlated_and match_same_car.
	idx.addDoc(t, prop, 2600, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"year": 2020},
		}},
	}})
	// doc 2700: Toyota Camry with tires=[{width:215}].
	// is_null_on_object_array_subprop car_with_tires.
	idx.addDoc(t, prop, 2700, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
				map[string]any{"width": 215},
			}},
		}},
	}})
	// doc 2800: cars[0] has tires, cars[1] has no tires.
	// is_null_on_object_array_subprop mixed_cars.
	idx.addDoc(t, prop, 2800, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
				map[string]any{"width": 215},
			}},
			map[string]any{"make": "Honda", "model": "Civic"},
		}},
	}})
	// doc 2900: single car with TWO tires.
	// is_null_on_object_array_subprop car_with_multiple_tires.
	idx.addDoc(t, prop, 2900, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Toyota", "model": "Camry", "tires": []any{
				map[string]any{"width": 215},
				map[string]any{"width": 215},
			}},
		}},
	}})
	// doc 3000: Honda Civic with colors=[red, blue] (brand-mismatch).
	// contains_*_with_equal_in_and discriminator.
	idx.addDoc(t, prop, 3000, map[string]any{"garages": []any{
		map[string]any{"city": "Amsterdam", "cars": []any{
			map[string]any{"make": "Honda", "model": "Civic", "colors": []any{"red", "blue"}},
		}},
	}})

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

		assert.ElementsMatch(t, []uint64{100, 400, 1300, 1600}, idx.docIDs(r),
			"docs with a Toyota 2020 car (single or alongside others) "+
				"match; cross-car split (doc 200) does not")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("is_null_on_leaf", func(t *testing.T) {
		// Python: test_is_null_on_leaf_at_root_cars.
		// Both polarities of IS NULL on cars.make.
		rTrue := leafIsNullTrue(idx, "cars.make")
		assert.ElementsMatch(t, []uint64{400, 500, 1700}, idx.docIDs(rTrue),
			"IS NULL true: docs with ∃ car missing make")
		require.Equal(t, "cars", rTrue.Scope, "Scope")
		require.Equal(t, "cars", rTrue.Ceiling, "Ceiling")

		rFalse := leafIsNullFalse(idx, "cars.make")
		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 700, 800, 900, 1000, 1100, 1200,
				1300, 1400, 1500, 1600, 1800, 1900, 2000, 2100,
			},
			idx.docIDs(rFalse),
			"IS NULL false: docs with ∃ car having make")
		require.Equal(t, "cars", rFalse.Scope, "Scope")
		require.Equal(t, pathRoot, rFalse.Ceiling, "Ceiling")
	})

	t.Run("arr_n_pin", func(t *testing.T) {
		// Python: test_arr_n_pin_at_root_cars.
		// cars[0].make=Toyota — only docs whose first car is Toyota.
		r := leafPinnedPositive(idx, "cars.make", "Toyota",
			[]pinSpec{{"cars", 0}})

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 400, 700, 800, 900, 1200,
				1600, 1800, 1900, 2000,
			}, idx.docIDs(r),
			"docs where cars[0].make=Toyota match; cars[0]≠Toyota miss")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("pinned_is_null", func(t *testing.T) {
		// Python: test_pinned_is_null_at_root_cars.
		// cars[1].make IS NULL — owner-level negation (no pin gap; routes
		// through negate(leafPinnedIsNullFalse)).
		//
		// Two divergences from Python, both rooted in the harness's
		// uniform "missing pinned slot → match" rule (negate at the
		// pinned scope = docUniverse AndNot pos.Witnesses, with no
		// separate "this doc has any cars" filter):
		//
		//   1) cars1_missing (doc has cars[] but cars[1] slot is absent —
		//      e.g. only one car at cars[0]): fast-path matches, Python
		//      currently doesn't but has a TODO to flip after the pinned-
		//      IsNull recovery lands. IMPROVEMENT — fast-path already
		//      implements the intended future semantic.
		//
		//   2) empty_cars (cars=[] entirely): fast-path matches under
		//      the same uniform rule (there's no encoding difference
		//      between "no cars" and "no cars[1] slot in a one-car
		//      array" — both look like "no slot" to the pinned
		//      dispatch). Python applies an additional vacuous-drop
		//      layer on top — when the array itself is empty, nested
		//      filters on it short-circuit to no-match. Not a
		//      divergence in per-element semantics; it's the absence
		//      of a separate vacuous-drop layer.
		//
		// Optional Python-parity follow-up (deferred): add
		// `AndNot ¬exists[cars]` after the negate to emulate Python's
		// vacuous-drop layer. Not a correctness fix — the distinction
		// it adds has no semantic basis from the user's standpoint
		// (both "no cars" and "cars[1] missing in a one-car array"
		// look like "no slot"). Both divergences are pinned by the
		// wantDocs below.
		r := leafPinnedIsNullTrue(idx, "cars.make",
			[]pinSpec{{"cars", 1}})

		assert.ElementsMatch(t,
			[]uint64{
				100, 300, 400, 500, 600, 700, 800, 1000,
				1400, 1500, 1700, 1800, 2000, 2100,
			}, idx.docIDs(r),
			"fast-path: cars[1] missing or no-make → match; "+
				"includes empty_cars (600), which Python excludes")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_any", func(t *testing.T) {
		// Python: test_contains_any_at_root_cars.
		// cars.colors ContainsAny [red, blue] — ∃ car owns any listed value.
		r := leafContainsAny(idx, "cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{700, 800, 900, 1200, 2100}, idx.docIDs(r),
			"docs where ∃ car owns red or blue in colors match")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_all", func(t *testing.T) {
		// Python: test_contains_all_at_root_cars.
		// cars.colors ContainsAll [red, blue] — ∃ car owns BOTH values in
		// the same colors array.
		r := leafContainsAll(idx, "cars.colors", "red", "blue")

		assert.ElementsMatch(t, []uint64{700, 2100}, idx.docIDs(r),
			"docs with ∃ single car holding both red and blue match")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_none", func(t *testing.T) {
		// Python: test_contains_none_at_root_cars.
		// cars.colors ContainsNone [red, blue].
		//
		// DIVERGENCE FROM PYTHON: fast-path is owner-level
		// (anchor(cars) AndNot value-buckets); Python is per-tag-element
		// existential. Docs without any colors values drop in Python as
		// vacuous; fast-path matches them.
		r := leafContainsNone(idx, "cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 1000, 1100, 1200,
				1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000,
			},
			idx.docIDs(r),
			"fast-path: docs with at least one car whose colors don't "+
				"intersect the listed values match (including no-colors-"+
				"at-all docs which Python excludes as vacuous)")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("arr_n_pin_with_and", func(t *testing.T) {
		// Python: test_arr_n_pin_with_and_at_root_cars.
		// cars[0].make=Toyota AND cars[0].year=2020 — both leaves at the
		// SAME pinned slot.
		//   pinned_match (doc 100): cars[0]=Toyota+2020.
		//   doc 400: cars[0]=Toyota+2020.
		//   doc 1600: cars[0]=Toyota+2020.
		//   split_within_pinned_car (doc 200): cars[0]=Toyota+2019 — pin
		//     slot has Toyota but year=2019 → miss.
		//   match_at_wrong_index (doc 1300): cars[1]=Toyota+2020 but pin
		//     is to cars[0]=Honda+2018 → miss.
		r := andLeaves(idx,
			leafPinnedPositive(idx, "cars.make", "Toyota",
				[]pinSpec{{"cars", 0}}),
			leafPinnedPositive(idx, "cars.year", 2020,
				[]pinSpec{{"cars", 0}}))

		assert.ElementsMatch(t, []uint64{100, 400, 1600}, idx.docIDs(r),
			"docs whose cars[0] satisfies both make=Toyota AND year=2020")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("arr_n_pin_with_or", func(t *testing.T) {
		// Python: test_arr_n_pin_with_or_at_root_cars.
		// cars[0].make=Toyota OR cars[0].year=2020.
		//   pinned_both_leaves: cars[0]=Toyota+2020 → 100, 400, 1600.
		//   pinned_only_make: cars[0]=Toyota+(other year) → 200, 700,
		//     800, 900, 1200, 1800, 1900, 2000.
		//   pinned_only_year: cars[0]=Honda+2020 → 1400.
		//   pinned_satisfies_neither: cars[0]=Honda/Ford+(other year) →
		//     300, 1000, 1100, 1300, 1500, 2100.
		r := orLeaves(idx,
			leafPinnedPositive(idx, "cars.make", "Toyota",
				[]pinSpec{{"cars", 0}}),
			leafPinnedPositive(idx, "cars.year", 2020,
				[]pinSpec{{"cars", 0}}))

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 400, 700, 800, 900, 1200,
				1400, 1600, 1700, 1800, 1900, 2000,
			}, idx.docIDs(r),
			"docs whose cars[0] satisfies make=Toyota OR year=2020")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("or_of_correlated_ands", func(t *testing.T) {
		// Python: test_or_of_correlated_ands_at_root_cars.
		// (make=Toyota AND year=2020) OR (make=Honda AND year=2019)
		//   group1 match (Toyota+2020 same car): 100, 400, 1300 (cars[1]),
		//     1600 (cars[0]).
		//   group2 match (Honda+2019 same car): 1500, 1600 (cars[1]).
		//   cross_group_split (doc 200): cars[0]=Toyota+2019,
		//     cars[1]=Honda+2020 — neither AND fires same-car → miss.
		r := orLeaves(idx,
			andLeaves(idx,
				leafPositive(idx, "cars.make", "Toyota"),
				leafPositive(idx, "cars.year", 2020)),
			andLeaves(idx,
				leafPositive(idx, "cars.make", "Honda"),
				leafPositive(idx, "cars.year", 2019)))

		assert.ElementsMatch(t,
			[]uint64{100, 400, 1300, 1500, 1600}, idx.docIDs(r),
			"docs whose ∃ car satisfies (Toyota+2020) OR (Honda+2019) same-car")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("not_of_correlated_and", func(t *testing.T) {
		// Python: test_not_of_correlated_and_at_root_cars.
		// NOT (make=Toyota AND year=2020). Per-car existential negation:
		// match when ∃ car violates the AND.
		//   only_satisfying_car: doc 100 single Toyota+2020 → no violator
		//     → NO match.
		//   mixed (doc 400 cars[1]={year:2018}, doc 1300 cars[0]=Honda,
		//     doc 1600 cars[1]=Honda+2019) → has violator → match.
		//   empty_cars (doc 600) → vacuous, NO match.
		r := negate(idx, andLeaves(idx,
			leafPositive(idx, "cars.make", "Toyota"),
			leafPositive(idx, "cars.year", 2020)))

		assert.ElementsMatch(t,
			[]uint64{
				200, 300, 400, 500, 700, 800, 900, 1000, 1100, 1200,
				1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 2100,
			},
			idx.docIDs(r),
			"docs with ∃ car not satisfying Toyota+2020 same-car match; "+
				"empty (600) and all-satisfying (100) do not")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("is_null_in_correlated_and", func(t *testing.T) {
		// Python: test_is_null_in_correlated_and_at_root_cars.
		// year=2020 AND make IS NULL — both leaves correlated per car.
		// Need ∃ car with year=2020 AND no make field.
		//   match_same_car: doc 1700 (single car {year:2020}, no make).
		//   split_across_cars: doc 400 cars[0]=Toyota+2020 (make set);
		//     cars[1]={year:2018} no make but year doesn't fire — no
		//     same-car match.
		//   doc 600 empty cars → no match.
		r := andLeaves(idx,
			leafPositive(idx, "cars.year", 2020),
			leafIsNullTrue(idx, "cars.make"))

		assert.ElementsMatch(t, []uint64{1700}, idx.docIDs(r),
			"only doc 1700 has a single car with year=2020 AND make missing")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("is_null_on_object_array_subprop", func(t *testing.T) {
		// Python: test_is_null_on_object_array_subprop_at_root_cars.
		// cars.tires IS NULL — ∃ car with no tires entry.
		//
		// Fast-path: leafIsNullTrue("cars.tires") via negate(
		// leafIsNullFalse("cars.tires")). pos.Witnesses = anchor(cars) ∩
		// exists[cars.tires] = cars-self where ∃ tires entry below.
		// Witnesses_not = anchor(cars) AndNot pos.Witnesses = cars-self
		// where NO tires entry.
		//
		// IMPROVEMENT vs PYTHON (current strict): Python's TODOs flag
		// that the production impl over-includes docs whose only car has
		// tires (encoding-gap over-inclusion at sub-root LCAs).
		// Fast-path correctly EXCLUDES car_with_tires (doc 1800) and
		// car_with_multiple_tires (doc 2000). Once Python's gap is
		// closed, the two suites agree.
		r := leafIsNullTrue(idx, "cars.tires")

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 700, 800, 900, 1000, 1100,
				1200, 1300, 1400, 1500, 1600, 1700, 1900, 2100,
			},
			idx.docIDs(r),
			"docs with ∃ car missing tires match (everything except docs "+
				"600 empty, 1800 all-have-tires, 2000 all-have-tires)")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_all_with_equal_in_and_at_root_cars.
		// cars.colors ContainsAll [red, blue] AND cars.make=Toyota.
		// Per-car: ∃ a car holding both colors AND make=Toyota.
		//   match: doc 700 (Toyota+colors=[red,blue] single car).
		//   contains-only (doc 2100): Honda has both colors but make
		//     wrong → no match.
		r := andLeaves(idx,
			leafContainsAll(idx, "cars.colors", "red", "blue"),
			leafPositive(idx, "cars.make", "Toyota"))

		assert.ElementsMatch(t, []uint64{700}, idx.docIDs(r),
			"only single Toyota car with both red and blue colors matches")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_any_with_equal_in_and_at_root_cars.
		// cars.colors ContainsAny [red, blue] AND cars.make=Toyota.
		// Per-car: ∃ a car owning any listed color AND make=Toyota.
		//   700 Toyota+[red,blue], 800 Toyota+[red], 900 cars[0]=Toyota+
		//     [red] (cars[1]=Honda+blue not Toyota same-car), 1200 cars[0]=
		//     Toyota+[red] → match.
		//   2100 Honda+[red,blue] → make wrong → no match.
		r := andLeaves(idx,
			leafContainsAny(idx, "cars.colors", "red", "blue"),
			leafPositive(idx, "cars.make", "Toyota"))

		assert.ElementsMatch(t,
			[]uint64{700, 800, 900, 1200}, idx.docIDs(r),
			"docs with ∃ Toyota car owning red or blue colors")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_none_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_none_with_equal_in_and_at_root_cars.
		// cars.colors ContainsNone [red, blue] AND cars.make=Toyota.
		// Fast-path: contains_none is owner-level; AND with make=Toyota
		// further requires the doc to have ∃ Toyota car.
		//
		// DIVERGENCE FROM PYTHON: same root cause as contains_none —
		// fast-path is owner-level, Python is per-tag-element existential.
		// Docs with Toyota cars that have no listed-color values match
		// in fast-path even when those docs have no colors at all.
		r := andLeaves(idx,
			leafContainsNone(idx, "cars.colors", "red", "blue"),
			leafPositive(idx, "cars.make", "Toyota"))

		// Toyota docs: 100, 200, 400, 700, 800, 900, 1200, 1300, 1600,
		// 1800, 1900, 2000. Intersect with contains_none result above:
		// excludes 700 (Toyota with red+blue all in list), 900 cars[0]
		// (Toyota+red — but the contains_none survives via cars-self...
		// trace this).
		assert.ElementsMatch(t,
			[]uint64{100, 200, 400, 1300, 1600, 1800, 1900, 2000},
			idx.docIDs(r),
			"fast-path: Toyota docs with at least one Toyota car whose "+
				"colors don't intersect the listed values")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("not_of_pinned_correlated_and", func(t *testing.T) {
		// Python: test_not_of_pinned_correlated_and_at_root_cars.
		// NOT (cars[0].make=Toyota AND cars[0].year=2020). The inner AND
		// is pinned correlated at cars[0]; negate over docUniverse gives
		// "doc does NOT have cars[0]=Toyota+2020".
		//
		// L0 Scope is pathRoot (parent of the root cars[] array), so
		// `negate` here is docUniverse AndNot Witnesses — owner-level
		// (per-doc), not per-element.
		//
		// Per-doc trace:
		//   100 cars[0]=Toyota+Camry+2020 (pinned_satisfies_and):
		//     inner AND fires → Witness → NOT match.
		//   200 cars[0]=Toyota+Camry+2019 (pinned_only_make): year≠2020
		//     → no Witness → match.
		//   300 cars[0]=Honda+Civic+2018 (pinned_neither): no Witness → match.
		//   400 cars[0]=Toyota+Camry+2020 (pinned_satisfies_with_distractor
		//     via cars[1]={year:2018}): inner AND fires → NOT match.
		//   500 cars[0]={year:2017} (pinned_only_year-ish, no make):
		//     no Witness → match.
		//   600 empty cars (empty_cars): no cars-self bit, no Witness.
		//     Match under uniform "missing pinned slot → match" rule —
		//     docUniverse-self survives the AndNot.
		//   700-1200, 1300, 1500, 1800-2100: cars[0] doesn't fire make=Toyota
		//     AND year=2020 same-slot → match.
		//   1300 cars[0]=Honda+2018, cars[1]=Toyota+Camry+2020
		//     (match_at_wrong_index): pinned at slot 0, Toyota+2020 lives at
		//     slot 1 → no Witness at slot 0 → match. Agrees with Python.
		//   1400 cars[0]=Honda+Civic+2020 (pinned_only_year): year fires, make
		//     not → no Witness → match.
		//   1600 cars[0]=Toyota+Camry+2020 (another pinned_satisfies_and):
		//     Witness → NOT match.
		//   1700 cars[0]={year:2020} (year fires, no make): no Witness → match.
		//
		// Two divergences from Python:
		//   1) pinned_satisfies_with_distractor (1600): fast-path correctly
		//      EXCLUDES because the pinned slot satisfies the inner AND.
		//      Python's TODO acknowledges the production impl ignores the pin
		//      and flips this in — fast-path already implements the intended
		//      post-fix semantic. IMPROVEMENT.
		//   2) empty_cars (600): fast-path INCLUDES under the uniform
		//      "missing pinned slot → match" rule (no encoding
		//      difference between "no cars" and "no cars[0] slot in a
		//      one-car array"). Python applies a separate vacuous-drop
		//      layer when the array itself is empty — not a divergence
		//      in per-element semantics, just the absence of that layer.
		//
		// Optional Python-parity follow-up (deferred): add
		// `AndNot ¬exists[cars]` after the negate to emulate Python's
		// vacuous-drop layer. Not a correctness fix — same rationale as
		// the pinned_is_null sibling.
		r := negate(idx, andLeaves(idx,
			leafPinnedPositive(idx, "cars.make", "Toyota",
				[]pinSpec{{"cars", 0}}),
			leafPinnedPositive(idx, "cars.year", 2020,
				[]pinSpec{{"cars", 0}})))

		assert.ElementsMatch(t,
			[]uint64{
				200, 300, 500, 600, 700, 800, 900, 1000, 1100, 1200,
				1300, 1400, 1500, 1700, 1800, 1900, 2000, 2100,
			},
			idx.docIDs(r),
			"fast-path: docs whose cars[0] does NOT satisfy Toyota+2020 "+
				"same-slot; excludes 100/400/1600 (cars[0] satisfies the "+
				"inner AND); includes 600 (empty cars — uniform "+
				"\"missing pinned slot → match\" rule)")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	// or_of_mixed_correlated_ands NOT PORTED. The Python scenario combines
	// a flat doc-level property (`category` text scalar at root) with a
	// nested same-element AND inside cars[]. The L0/L2 fixture schemas
	// don't carry a flat property; porting would need a schema extension.
	// Listed in the file-level header alongside the other unsupported
	// scenarios. Skipped — re-port once a flat property exists in the
	// harness.

	// Group D — tokenization-aware scenarios. Each subtest builds its own
	// minimal fixture so the multi-token Equal / ContainsAny discriminators
	// don't pollute the shared `is_null_on_leaf` / `arr_n_pin` / etc.
	// wantDocs of the earlier groups. The harness applies
	// tokenizer.Tokenize between AssignPositions and the value-bucket
	// emit (mirroring the production analyzer); on the query side
	// leafPositive tokenizes the query value and fans multi-token values
	// out through andN of per-token raw leaves at the same path. Single-
	// token query values fall through to the original single-bucket path
	// unchanged.

	t.Run("multi_token_equal", func(t *testing.T) {
		// Python: test_multi_token_equal_at_root_cars.
		// Filter cars.description = "Camry Hybrid" against a word-tokenized
		// text field. After tokenization the query value becomes
		// ["camry", "hybrid"]; both tokens must live at the same cars
		// element. Fast-path expresses this as andN of per-token raw
		// leafPositive calls, which the public leafPositive wraps
		// internally — so the test calls it the natural way.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// doc 3100 both_tokens_one_car: description="Camry Hybrid".
		local.addDoc(t, prop, 3100, []any{
			map[string]any{"description": "Camry Hybrid"},
		})
		// doc 3200 single_token_one_car: description="Camry" only.
		local.addDoc(t, prop, 3200, []any{
			map[string]any{"description": "Camry"},
		})
		// doc 3300 tokens_split_across_cars: cars[0]=Camry, cars[1]=Hybrid.
		local.addDoc(t, prop, 3300, []any{
			map[string]any{"description": "Camry"},
			map[string]any{"description": "Hybrid"},
		})
		// doc 3400 unrelated: description="Civic LX" — neither token.
		local.addDoc(t, prop, 3400, []any{
			map[string]any{"description": "Civic LX"},
		})

		r := leafPositive(local, "cars.description", "Camry Hybrid")

		assert.ElementsMatch(t, []uint64{3100}, local.docIDs(r),
			"only doc 3100 has one car owning both tokens; doc 3300 split "+
				"across two cars must NOT match (same-element on tokens)")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_multi_token", func(t *testing.T) {
		// Python: test_contains_any_multi_token_at_root_cars.
		// Filter cars.tags ContainsAny ["family hybrid"] against a
		// word-tokenized text[] field. The single query list element
		// tokenizes to ["family", "hybrid"]; with one list value the
		// outer OR is a no-op so the result is just the per-value
		// match bitmap M = bucket["family"] ∩ bucket["hybrid"].
		//
		// Lenient same-element rule (parent-Scope AND): doc 3600
		// (tokens split across two tag entries of the SAME car)
		// matches because the cars-self chain bit survives the
		// intersection. See the file-level header for why we chose
		// lenient over Python's strict same-tag-entry rule.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// doc 3500 match_via_one_tag: tags=["family hybrid car"] —
		// one tag entry contains both tokens.
		local.addDoc(t, prop, 3500, []any{
			map[string]any{"tags": []any{"family hybrid car"}},
		})
		// doc 3600 tokens_split_across_tags: tags=["family sedan",
		// "hybrid model"] — tokens in DIFFERENT tag entries of the
		// same car. Matches under lenient.
		local.addDoc(t, prop, 3600, []any{
			map[string]any{"tags": []any{"family sedan", "hybrid model"}},
		})
		// doc 3700 single_token_missing: tags=["family car"] — only
		// "family" present, "hybrid" bucket has no bit for this doc.
		local.addDoc(t, prop, 3700, []any{
			map[string]any{"tags": []any{"family car"}},
		})
		// doc 3800 split_across_cars: cars[0].tags=["family car"],
		// cars[1].tags=["hybrid model"] — tokens in DIFFERENT cars.
		// Excluded — cars-self bits differ.
		local.addDoc(t, prop, 3800, []any{
			map[string]any{"tags": []any{"family car"}},
			map[string]any{"tags": []any{"hybrid model"}},
		})
		// doc 3900 empty_tags: a car with tags=[].
		local.addDoc(t, prop, 3900, []any{
			map[string]any{"tags": []any{}},
		})
		// doc 4000 empty_cars: no cars at all.
		local.addDoc(t, prop, 4000, []any{})

		r := leafContainsAny(local, "cars.tags", "family hybrid")

		assert.ElementsMatch(t, []uint64{3500, 3600}, local.docIDs(r),
			"docs whose single car has both tokens somewhere in its tags "+
				"array match (3500 in one tag, 3600 split across two tags "+
				"of the same car); splits across separate cars (3800), "+
				"empty tags (3900), and empty cars (4000) excluded")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	// ---- Harness gap coverage (not in Python suite) ----
	// Python's array-intermediates suite has no multi-token list-value
	// tests for ContainsAll / ContainsNone. Adding them here pins the
	// per-value match composition the new tokenizedMatchBitmap helper
	// drives — especially the "materialize per-value AND before chained
	// AndNot" rule that leafContainsNone documents and that's easy to
	// regress under a future refactor.

	t.Run("contains_none_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter cars.tags ContainsNone ["family hybrid", "luxury"].
		// Discriminator doc 6300 is the load-bearing one: it has the
		// single token "family" but neither the full "family hybrid"
		// pair nor "luxury". A flatten-tokens implementation would
		// compute anchor AndNot (family ∪ hybrid ∪ luxury) and wrongly
		// exclude 6300; the correct anchor AndNot (M_fh ∪ M_lux) leaves
		// it in.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 6100: tags=["family hybrid car"] — full "family hybrid"
		// satisfaction → excluded.
		local.addDoc(t, prop, 6100, []any{
			map[string]any{"tags": []any{"family hybrid car"}},
		})
		// 6200: tags=["luxury sedan"] — has luxury → excluded.
		local.addDoc(t, prop, 6200, []any{
			map[string]any{"tags": []any{"luxury sedan"}},
		})
		// 6300: tags=["family car"] — has family alone, no hybrid, no
		// luxury → MATCH. Load-bearing.
		local.addDoc(t, prop, 6300, []any{
			map[string]any{"tags": []any{"family car"}},
		})
		// 6400: tags=["sedan"] — no relevant tokens → match.
		local.addDoc(t, prop, 6400, []any{
			map[string]any{"tags": []any{"sedan"}},
		})
		// 6500: tags=["family sedan", "hybrid model"] — tokens split
		// across tag entries of the same car. Lenient: same as having
		// the full pair → excluded.
		local.addDoc(t, prop, 6500, []any{
			map[string]any{"tags": []any{"family sedan", "hybrid model"}},
		})

		r := leafContainsNone(local, "cars.tags", "family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{6300, 6400}, local.docIDs(r),
			"docs without (family AND hybrid) AND without luxury match; "+
				"6300 (family alone) is the load-bearing case — a "+
				"flattened implementation would wrongly exclude it")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter cars.tags ContainsAll ["family hybrid", "luxury"].
		// Requires (family AND hybrid) AND luxury all present on the
		// same car. Single-token "luxury" and multi-token "family
		// hybrid" mix in the outer AND-fold over per-value Mᵢ.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 6600: all four tokens present → match.
		local.addDoc(t, prop, 6600, []any{
			map[string]any{"tags": []any{"family hybrid", "luxury car"}},
		})
		// 6700: missing hybrid → no match.
		local.addDoc(t, prop, 6700, []any{
			map[string]any{"tags": []any{"family", "luxury"}},
		})
		// 6800: missing luxury → no match.
		local.addDoc(t, prop, 6800, []any{
			map[string]any{"tags": []any{"family hybrid"}},
		})
		// 6900: all tokens present, split across two tag entries of
		// the same car. Lenient parent-Scope AND → match.
		local.addDoc(t, prop, 6900, []any{
			map[string]any{"tags": []any{"luxury hybrid", "family bag"}},
		})
		// 7000: no relevant tokens → no match.
		local.addDoc(t, prop, 7000, []any{
			map[string]any{"tags": []any{"sedan"}},
		})

		r := leafContainsAll(local, "cars.tags", "family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{6600, 6900}, local.docIDs(r),
			"docs where all four tokens are present on the same car "+
				"(lenient: tokens may live in different tag entries — "+
				"doc 6900 splits them across two entries)")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_none_two_multi_token", func(t *testing.T) {
		// Filter cars.tags ContainsNone ["family hybrid", "luxury car"].
		// Both list values multi-token — chained AndNot over consecutive
		// multi-token Mᵢ. Docs 7100 and 7400 are load-bearing: each has
		// one token from each list value but neither full pair.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 7100: has family + luxury individually, neither full pair →
		// MATCH. Load-bearing.
		local.addDoc(t, prop, 7100, []any{
			map[string]any{"tags": []any{"family", "luxury"}},
		})
		// 7200: full "family hybrid" → excluded.
		local.addDoc(t, prop, 7200, []any{
			map[string]any{"tags": []any{"family hybrid"}},
		})
		// 7300: full "luxury car" → excluded.
		local.addDoc(t, prop, 7300, []any{
			map[string]any{"tags": []any{"luxury car"}},
		})
		// 7400: has hybrid + car individually, neither full pair →
		// match. Load-bearing.
		local.addDoc(t, prop, 7400, []any{
			map[string]any{"tags": []any{"hybrid", "car"}},
		})
		// 7500: full "family hybrid" + "car" extra. First value
		// satisfied → excluded (second value's "car" alone isn't
		// enough on its own).
		local.addDoc(t, prop, 7500, []any{
			map[string]any{"tags": []any{"family hybrid car"}},
		})
		// 7600: no relevant tokens → match.
		local.addDoc(t, prop, 7600, []any{
			map[string]any{"tags": []any{"sedan", "wagon"}},
		})

		r := leafContainsNone(local, "cars.tags", "family hybrid", "luxury car")

		assert.ElementsMatch(t, []uint64{7100, 7400, 7600}, local.docIDs(r),
			"docs where neither (family AND hybrid) nor (luxury AND "+
				"car) is fully present; 7100 and 7400 are the "+
				"load-bearing cases — a flatten-tokens implementation "+
				"would wrongly exclude them")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_two_multi_token", func(t *testing.T) {
		// Filter cars.tags ContainsAny ["family hybrid", "luxury car"].
		// Both list values multi-token — exercises the OR-fold over
		// consecutive multi-token Mᵢ. Doc 8000 is load-bearing: it has
		// "family" and "luxury" individually but neither full pair, and
		// a flatten-tokens implementation (token-level OR instead of
		// per-value AND then OR) would wrongly include it.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 7700: tags=["family hybrid"] — first full pair → match.
		local.addDoc(t, prop, 7700, []any{
			map[string]any{"tags": []any{"family hybrid"}},
		})
		// 7800: tags=["luxury car"] — second full pair → match.
		local.addDoc(t, prop, 7800, []any{
			map[string]any{"tags": []any{"luxury car"}},
		})
		// 7900: tags=["family hybrid car"] — both pairs in one tag →
		// match (first satisfies; second satisfies via lenient).
		local.addDoc(t, prop, 7900, []any{
			map[string]any{"tags": []any{"family hybrid car"}},
		})
		// 8000: tags=["family", "luxury"] — individual tokens from each
		// pair but NO full pair → NO match. Load-bearing.
		local.addDoc(t, prop, 8000, []any{
			map[string]any{"tags": []any{"family", "luxury"}},
		})
		// 8100: tags=["family"] — only one token → no match.
		local.addDoc(t, prop, 8100, []any{
			map[string]any{"tags": []any{"family"}},
		})
		// 8200: tags=["sedan"] — no relevant tokens → no match.
		local.addDoc(t, prop, 8200, []any{
			map[string]any{"tags": []any{"sedan"}},
		})

		r := leafContainsAny(local, "cars.tags", "family hybrid", "luxury car")

		assert.ElementsMatch(t, []uint64{7700, 7800, 7900}, local.docIDs(r),
			"docs where (family AND hybrid) OR (luxury AND car) is fully "+
				"present match; 8000 (individual tokens, no full pair) is "+
				"the load-bearing case — a flatten-tokens implementation "+
				"would wrongly include it")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_two_multi_token", func(t *testing.T) {
		// Filter cars.tags ContainsAll ["family hybrid", "luxury car"].
		// Both list values multi-token — exercises the AND-fold over
		// consecutive multi-token Mᵢ. Doc 8700 confirms lenient parent-
		// Scope AND: all four tokens spread across four tag entries of
		// the same car still match because every M_i survives via the
		// shared cars-self chain bit.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 8300: tags=["family hybrid", "luxury car"] — each pair in its
		// own tag entry → match.
		local.addDoc(t, prop, 8300, []any{
			map[string]any{"tags": []any{"family hybrid", "luxury car"}},
		})
		// 8400: tags=["family hybrid car luxury"] — all four tokens in
		// one tag entry → match.
		local.addDoc(t, prop, 8400, []any{
			map[string]any{"tags": []any{"family hybrid car luxury"}},
		})
		// 8500: tags=["family hybrid"] — only first pair → no match.
		local.addDoc(t, prop, 8500, []any{
			map[string]any{"tags": []any{"family hybrid"}},
		})
		// 8600: tags=["luxury car"] — only second pair → no match.
		local.addDoc(t, prop, 8600, []any{
			map[string]any{"tags": []any{"luxury car"}},
		})
		// 8700: tags=["family", "hybrid", "luxury", "car"] — all four
		// tokens, each in its own tag entry. Lenient parent-Scope AND
		// preserves the cars-self bit across each Mᵢ → match.
		local.addDoc(t, prop, 8700, []any{
			map[string]any{"tags": []any{"family", "hybrid", "luxury", "car"}},
		})
		// 8800: tags=["sedan"] — no relevant tokens → no match.
		local.addDoc(t, prop, 8800, []any{
			map[string]any{"tags": []any{"sedan"}},
		})

		r := leafContainsAll(local, "cars.tags", "family hybrid", "luxury car")

		assert.ElementsMatch(t, []uint64{8300, 8400, 8700}, local.docIDs(r),
			"docs where (family AND hybrid) AND (luxury AND car) is fully "+
				"present match; 8700 (each token in its own tag entry) "+
				"matches under lenient parent-Scope AND")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_any_multi_token", func(t *testing.T) {
		// Filter cars[0].tags ContainsAny ["family hybrid"] — pinned
		// to cars[0] with a multi-token list value. Verifies that the
		// pinned dispatch correctly tokenizes through the per-value
		// match step (M = bucket["family"] ∩ bucket["hybrid"]) and
		// that the pin-narrow keeps only positions where cars-self is
		// at array index 0.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 8900: cars[0]=tags["family hybrid car"] → match (full pair
		// at pinned slot).
		local.addDoc(t, prop, 8900, []any{
			map[string]any{"tags": []any{"family hybrid car"}},
		})
		// 9000: cars[0]=tags["family sedan", "hybrid model"] → match
		// (lenient: tokens in different tag entries but at the same
		// pinned car).
		local.addDoc(t, prop, 9000, []any{
			map[string]any{"tags": []any{"family sedan", "hybrid model"}},
		})
		// 9100: cars[0]=tags["sedan"], cars[1]=tags["family hybrid"]
		// → NO match (full pair at WRONG pinned slot).
		local.addDoc(t, prop, 9100, []any{
			map[string]any{"tags": []any{"sedan"}},
			map[string]any{"tags": []any{"family hybrid"}},
		})
		// 9200: cars[0]=tags["family"] (only one token) → no match.
		local.addDoc(t, prop, 9200, []any{
			map[string]any{"tags": []any{"family"}},
		})
		// 9300: no cars[0] (empty cars[]) → no match.
		local.addDoc(t, prop, 9300, []any{})

		r := leafPinnedContainsAny(local, "cars.tags",
			[]pinSpec{{"cars", 0}}, "family hybrid")

		assert.ElementsMatch(t, []uint64{8900, 9000}, local.docIDs(r),
			"docs where cars[0] has both tokens of \"family hybrid\" match; "+
				"9100 (full pair at wrong slot) and 9200 (single token) "+
				"excluded; verifies tokenization fires through the "+
				"pinned dispatch")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_none_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter cars[0].tags ContainsNone ["family hybrid", "luxury"].
		// Mirrors the unpinned correctness test for ContainsNone with
		// the pin restricting evaluation to cars[0]. Load-bearing doc
		// 9600 has "family" alone at cars[0] — no full "family hybrid"
		// pair, no "luxury" — and must match. A flatten-tokens
		// implementation under pin would wrongly exclude it.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 9400: cars[0]=tags["family hybrid car"] → excluded (first
		// value satisfied at pinned slot).
		local.addDoc(t, prop, 9400, []any{
			map[string]any{"tags": []any{"family hybrid car"}},
		})
		// 9500: cars[0]=tags["luxury sedan"] → excluded (second value
		// satisfied).
		local.addDoc(t, prop, 9500, []any{
			map[string]any{"tags": []any{"luxury sedan"}},
		})
		// 9600: cars[0]=tags["family car"] → MATCH. Load-bearing —
		// has "family" alone at pinned slot, no full pair, no luxury.
		local.addDoc(t, prop, 9600, []any{
			map[string]any{"tags": []any{"family car"}},
		})
		// 9700: cars[0]=tags["sedan"], cars[1]=tags["family hybrid"]
		// → MATCH (full pair at WRONG slot; pinned slot has neither
		// value).
		local.addDoc(t, prop, 9700, []any{
			map[string]any{"tags": []any{"sedan"}},
			map[string]any{"tags": []any{"family hybrid"}},
		})

		r := leafPinnedContainsNone(local, "cars.tags",
			[]pinSpec{{"cars", 0}}, "family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{9600, 9700}, local.docIDs(r),
			"docs where cars[0] satisfies neither (family AND hybrid) "+
				"nor luxury; 9600 (family alone at pinned slot) is the "+
				"load-bearing case for the per-value AND correctness "+
				"rule under pin narrowing")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_all_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter cars[0].tags ContainsAll ["family hybrid", "luxury"].
		// Verifies the AND-fold over per-value Mᵢ survives pin
		// narrowing and that lenient parent-Scope AND inside each Mᵢ
		// still permits all-tokens-different-tag-entries (doc 10100).
		// Doc 10200 is the "wrong slot" discriminator — all tokens at
		// cars[1] don't match a pin on cars[0].
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 9800: cars[0]=tags["family hybrid", "luxury car"] — first
		// pair in one tag, second value in another → match.
		local.addDoc(t, prop, 9800, []any{
			map[string]any{"tags": []any{"family hybrid", "luxury car"}},
		})
		// 9900: cars[0]=tags["family hybrid"] — missing luxury →
		// no match.
		local.addDoc(t, prop, 9900, []any{
			map[string]any{"tags": []any{"family hybrid"}},
		})
		// 10000: cars[0]=tags["luxury sedan"] — missing family/hybrid
		// pair → no match.
		local.addDoc(t, prop, 10000, []any{
			map[string]any{"tags": []any{"luxury sedan"}},
		})
		// 10100: cars[0]=tags["family", "hybrid", "luxury"] — every
		// token in its own tag entry. Lenient parent-Scope AND →
		// match (cars-self chain bit survives every Mᵢ).
		local.addDoc(t, prop, 10100, []any{
			map[string]any{"tags": []any{"family", "hybrid", "luxury"}},
		})
		// 10200: cars[0]=tags["sedan"], cars[1]=tags["family hybrid",
		// "luxury"] — all values fully satisfied at WRONG slot → NO
		// match. Pin discriminator.
		local.addDoc(t, prop, 10200, []any{
			map[string]any{"tags": []any{"sedan"}},
			map[string]any{"tags": []any{"family hybrid", "luxury"}},
		})
		// 10300: cars[0]=tags["sedan"] — no relevant tokens → no match.
		local.addDoc(t, prop, 10300, []any{
			map[string]any{"tags": []any{"sedan"}},
		})

		r := leafPinnedContainsAll(local, "cars.tags",
			[]pinSpec{{"cars", 0}}, "family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{9800, 10100}, local.docIDs(r),
			"docs where cars[0] has BOTH (family AND hybrid) AND luxury; "+
				"10100 (each token in its own tag entry) matches under "+
				"lenient parent-Scope AND; 10200 (all values at wrong "+
				"slot) is the pin discriminator")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("not_equal", func(t *testing.T) {
		// Python: test_not_equal_at_root_cars.
		// Filter cars.make != Toyota. Per-Scope-element existential
		// owner-level: ∃ a car whose cars-self bit isn't in the Toyota
		// bucket. Includes cars where make isn't set at all (the no-make
		// car's cars-self bit is in anchor(cars) but not in
		// bucket["Toyota"] → survives the AndNot).
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 10400: single_toyota → no match.
		local.addDoc(t, prop, 10400, []any{
			map[string]any{"make": "Toyota"},
		})
		// 10500: single_honda → match.
		local.addDoc(t, prop, 10500, []any{
			map[string]any{"make": "Honda"},
		})
		// 10600: mixed_toyota_and_honda → match (∃ Honda car).
		local.addDoc(t, prop, 10600, []any{
			map[string]any{"make": "Toyota"},
			map[string]any{"make": "Honda"},
		})
		// 10700: multiple_non_toyota → match.
		local.addDoc(t, prop, 10700, []any{
			map[string]any{"make": "Honda"},
			map[string]any{"make": "Ford"},
		})
		// 10800: empty_cars → no match (vacuous, anchor empty).
		local.addDoc(t, prop, 10800, []any{})
		// 10900: missing_make_only — single car with no make at all.
		// Locks down the owner-level / include-missing semantic: the
		// car's cars-self bit is in anchor(cars), not in bucket["Toyota"]
		// → survives. Doc matches. If anyone refactors leafNotEqual to
		// use _exists(path) as the universe ("must exist" semantic),
		// this assertion fails.
		local.addDoc(t, prop, 10900, []any{
			map[string]any{"year": 2020},
		})

		r := leafNotEqual(local, "cars.make", "Toyota")

		assert.ElementsMatch(t,
			[]uint64{10500, 10600, 10700, 10900}, local.docIDs(r),
			"docs with ∃ car whose make != Toyota (Python set) plus 10900 "+
				"(no-make car) — locks down owner-level / include-missing "+
				"semantic")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_not_equal", func(t *testing.T) {
		// Harness gap coverage — not in Python suite. Filter
		// cars[0].make != Toyota. Single pin at cars (root array),
		// no pin gap → leafPinnedNot routes through
		// negate(leafPinnedPositive(...)). At L0 the result Scope is
		// pathRoot so the negate universe is docUniverse — every doc
		// that doesn't have cars[0]=Toyota survives.
		//
		// Doc 11400 (missing make at cars[0]) and doc 11500 (empty
		// cars) both match under the uniform "missing pinned slot →
		// match" rule. From the dispatch's perspective there's no
		// encoding difference between "no cars at all" and "cars
		// exist but the pinned slot has no leaf" — both look like
		// "no slot". See pinned_is_null for the divergence-from-
		// Python framing (Python adds a separate vacuous-drop layer
		// for empty arrays).
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 11000: cars[0]=Toyota → satisfies Equal → no match.
		local.addDoc(t, prop, 11000, []any{
			map[string]any{"make": "Toyota"},
		})
		// 11100: cars[0]=Honda → match.
		local.addDoc(t, prop, 11100, []any{
			map[string]any{"make": "Honda"},
		})
		// 11200: cars[0]=Toyota, cars[1]=Honda — pin sees Toyota → no match.
		local.addDoc(t, prop, 11200, []any{
			map[string]any{"make": "Toyota"},
			map[string]any{"make": "Honda"},
		})
		// 11300: cars[0]=Honda, cars[1]=Toyota — pin sees Honda → match.
		local.addDoc(t, prop, 11300, []any{
			map[string]any{"make": "Honda"},
			map[string]any{"make": "Toyota"},
		})
		// 11400: missing_make_at_pinned_slot — cars[0] has no make.
		// Match: include-missing under pin (anchor keeps the doc,
		// no Toyota at slot 0 → no positive witness → docUniverse AndNot
		// empty survives).
		local.addDoc(t, prop, 11400, []any{
			map[string]any{"year": 2020},
		})
		// 11500: empty_cars — no cars at all. Match under the uniform
		// "missing pinned slot → match" rule (same as 11400 — both
		// look like "no slot" to the dispatch).
		local.addDoc(t, prop, 11500, []any{})

		r := leafPinnedNotEqual(local, "cars.make", "Toyota",
			[]pinSpec{{"cars", 0}})

		assert.ElementsMatch(t,
			[]uint64{11100, 11300, 11400, 11500}, local.docIDs(r),
			"docs where cars[0].make != Toyota (or cars[0] is absent / "+
				"missing make); 11400 and 11500 lock down the uniform "+
				"missing-pin-slot → match rule")
		require.Equal(t, pathRoot, r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("mixed_ceiling_or", func(t *testing.T) {
		// Harness gap coverage — verifies orLeaves at same Scope with
		// MIXED operand Ceilings (one pathRoot, one scope). The
		// shared L0/L2 fixtures don't construct this shape — every
		// other orLeaves call has operands with matching Ceilings.
		//
		// Filter: cars.make = Toyota OR cars.make IS NULL true.
		//   leafPositive("cars.make", Toyota): Scope=cars, Ceiling=pathRoot.
		//   leafIsNullTrue("cars.make"):       Scope=cars, Ceiling=cars.
		// orLeaves uses deepestPath(l.Ceiling, r.Ceiling) → cars.
		local := newFastPathIndex("cars")
		prop := l0Schema()
		// 12000: Toyota Camry → matches Toyota leg.
		local.addDoc(t, prop, 12000, []any{
			map[string]any{"make": "Toyota", "model": "Camry"},
		})
		// 12100: Honda Civic → all cars have make and none is
		// Toyota → no match.
		local.addDoc(t, prop, 12100, []any{
			map[string]any{"make": "Honda", "model": "Civic"},
		})
		// 12200: car with no make → matches IS NULL true leg.
		local.addDoc(t, prop, 12200, []any{
			map[string]any{"year": 2020},
		})
		// 12300: Toyota + no-make car → matches via both legs.
		local.addDoc(t, prop, 12300, []any{
			map[string]any{"make": "Toyota", "model": "Camry"},
			map[string]any{"year": 2020},
		})
		// 12400: empty cars → neither leg fires.
		local.addDoc(t, prop, 12400, []any{})

		r := orLeaves(local,
			leafPositive(local, "cars.make", "Toyota"),
			leafIsNullTrue(local, "cars.make"))

		assert.ElementsMatch(t, []uint64{12000, 12200, 12300}, local.docIDs(r),
			"docs where ∃ car has make=Toyota OR ∃ car has no make")
		require.Equal(t, "cars", r.Scope, "Scope")
		require.Equal(t, "cars", r.Ceiling, "Ceiling")
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

		assert.ElementsMatch(t,
			[]uint64{100, 400, 500, 2200, 2500}, idx.docIDs(r),
			"docs with a Toyota 2020 car somewhere match; "+
				"splits across cars/garages do not")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
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
			[]uint64{500, 600, 800, 900, 2600}, idx.docIDs(rTrue),
			"IS NULL true: docs with ∃ car missing make")
		require.Equal(t, "countries.garages.cars", rTrue.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", rTrue.Ceiling, "Ceiling")

		// IS NULL false: docs with ∃ car having make.
		rFalse := leafIsNullFalse(idx, "countries.garages.cars.make")
		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 800, 900, 1000, 1100, 1200,
				1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000,
				2200, 2300, 2400, 2500, 2700, 2800, 2900, 3000,
			},
			idx.docIDs(rFalse),
			"IS NULL false: docs with ∃ car having make")
		require.Equal(t, "countries.garages.cars", rFalse.Scope, "Scope")
		require.Equal(t, pathRoot, rFalse.Ceiling, "Ceiling")
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
			[]uint64{
				100, 200, 300, 400, 500, 800, 900,
				1300, 1400, 1500, 1600, 1700, 1800, 1900,
				2500, 2700, 2800, 2900,
			}, idx.docIDs(r),
			"docs with ∃ garage whose cars[0]=Toyota match")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
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
		// Two divergences from Python, both rooted in the harness's
		// uniform "missing pinned slot → match" rule (negate at the
		// pinned scope = anchor[countries.garages] AndNot
		// pos.Witnesses, with no separate "this garage has any cars"
		// filter):
		//
		//   1) cars1_missing per garage (garage has cars[] but cars[1]
		//      slot is absent): fast-path matches, Python currently
		//      doesn't but has a TODO to flip after the pinned-IsNull
		//      recovery lands. IMPROVEMENT — fast-path already implements
		//      the intended future semantic.
		//
		//   2) empty_cars (doc 700: a garage with cars=[]): fast-path
		//      matches under the same uniform rule (no encoding
		//      difference between "garage has no cars" and "garage has
		//      cars but no cars[1] slot" — both look like "no slot"
		//      to the dispatch). Python applies an additional
		//      vacuous-drop layer at empty cars[] — not a divergence
		//      in per-element semantics, just the absence of that
		//      layer.
		//
		// Optional Python-parity follow-up (deferred): add
		// `AndNot ¬exists[cars]` at the truthScope level after the
		// negate to emulate Python's vacuous-drop layer (drop garages
		// whose cars[] is empty). Not a correctness fix — the
		// distinction it adds has no semantic basis from the user's
		// standpoint. Both divergences are pinned by the wantDocs below.
		r := leafPinnedIsNullTrue(idx, "countries.garages.cars.make",
			[]pinSpec{{"countries.garages.cars", 1}})

		assert.ElementsMatch(t,
			[]uint64{
				100, 300, 400, 500, 600, 700, 800, 900,
				1300, 1400, 1600, 1700, 1800, 1900, 2000,
				2300, 2400, 2600, 2700, 2900, 3000,
			}, idx.docIDs(r),
			"fast-path: docs with ∃ garage whose cars[1] missing or "+
				"no-make match; includes empty_cars (700)")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
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
			[]uint64{1300, 1400, 1500, 1600, 1700, 1800, 1900, 3000},
			idx.docIDs(r),
			"docs where ∃ car owns red or blue in colors match")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_all", func(t *testing.T) {
		// Python: test_contains_all_under_countries_array.
		// cars.colors ContainsAll [red, blue] — ∃ car owns BOTH values in
		// the same colors array.
		r := leafContainsAll(idx, "countries.garages.cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{1300, 1800, 1900, 3000}, idx.docIDs(r),
			"docs with ∃ single car holding both red and blue match")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
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
				100, 200, 300, 400, 500, 600, 800, 900,
				1000, 1100, 1200,
				1800, 1900,
				2000,
				// Group B additions: docs without listed colors or with
				// mixed/outside cars all survive owner-level AndNot.
				2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900,
				// NOT: 700 (empty cars, no cars-self), 1300/3000 (all
				// red+blue), 1400 (all red), 1500/1600/1700 (split — every
				// car-self in values).
			}, idx.docIDs(r),
			"fast-path: docs with ∃ car-self not in any value bucket "+
				"(includes no-colors docs that Python excludes as vacuous)")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("arr_n_pin_with_and", func(t *testing.T) {
		// Python: test_arr_n_pin_with_and_under_countries_array.
		// countries.garages.cars[0].make=Toyota AND
		// countries.garages.cars[0].year=2020 — both leaves at the same
		// pinned slot inside a garage.
		//   pinned_match (100): cars[0]=Toyota+2020 in single garage.
		//   doc 400 (Amsterdam cars[0]=Toyota+2020), 500 (cars[0]=Toyota+
		//     2020), 2500 (cars[0]=Toyota+2020).
		//   match_at_wrong_index (2200): cars[1]=Toyota+2020 but pin to
		//     cars[0]=Honda → miss.
		r := andLeaves(idx,
			leafPinnedPositive(idx, "countries.garages.cars.make", "Toyota",
				[]pinSpec{{"countries.garages.cars", 0}}),
			leafPinnedPositive(idx, "countries.garages.cars.year", 2020,
				[]pinSpec{{"countries.garages.cars", 0}}))

		assert.ElementsMatch(t, []uint64{100, 400, 500, 2500}, idx.docIDs(r),
			"docs whose ∃ garage's cars[0] satisfies both make=Toyota AND year=2020")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
	})

	t.Run("arr_n_pin_with_or", func(t *testing.T) {
		// Python: test_arr_n_pin_with_or_under_countries_array.
		// countries.garages.cars[0].make=Toyota OR cars[0].year=2020.
		r := orLeaves(idx,
			leafPinnedPositive(idx, "countries.garages.cars.make", "Toyota",
				[]pinSpec{{"countries.garages.cars", 0}}),
			leafPinnedPositive(idx, "countries.garages.cars.year", 2020,
				[]pinSpec{{"countries.garages.cars", 0}}))

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 800, 900,
				1300, 1400, 1500, 1600, 1700, 1800, 1900,
				2300, 2500, 2600, 2700, 2800, 2900,
			}, idx.docIDs(r),
			"docs whose ∃ garage's cars[0] satisfies make=Toyota OR year=2020")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
	})

	t.Run("or_of_correlated_ands", func(t *testing.T) {
		// Python: test_or_of_correlated_ands_under_countries_array.
		// (make=Toyota AND year=2020) OR (make=Honda AND year=2019) —
		// per-car same-element across all cars in any garage / country.
		//   group1 match (Toyota+2020 same car): 100, 400, 500, 2200
		//     (via cars[1]), 2500 (via cars[0]).
		//   group2 match (Honda+2019 same car): 2400, 2500 (via cars[1]).
		//   cross_group splits (200, 300): no same-car for either group.
		r := orLeaves(idx,
			andLeaves(idx,
				leafPositive(idx, "countries.garages.cars.make", "Toyota"),
				leafPositive(idx, "countries.garages.cars.year", 2020)),
			andLeaves(idx,
				leafPositive(idx, "countries.garages.cars.make", "Honda"),
				leafPositive(idx, "countries.garages.cars.year", 2019)))

		assert.ElementsMatch(t,
			[]uint64{100, 400, 500, 2200, 2400, 2500}, idx.docIDs(r),
			"docs whose ∃ car satisfies (Toyota+2020) OR (Honda+2019) same-car")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("not_of_correlated_and", func(t *testing.T) {
		// Python: test_not_of_correlated_and_under_countries_array.
		// NOT (make=Toyota AND year=2020). Match docs with ∃ car
		// violating the AND.
		//   doc 100 (single Toyota+2020): no violator → no match.
		//   docs with ∃ non-(Toyota+2020) car → match.
		//   empty cars (700) → vacuous, no match.
		r := negate(idx, andLeaves(idx,
			leafPositive(idx, "countries.garages.cars.make", "Toyota"),
			leafPositive(idx, "countries.garages.cars.year", 2020)))

		assert.ElementsMatch(t,
			[]uint64{
				200, 300, 400, 500, 600, 800, 900,
				1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700,
				1800, 1900, 2000, 2200, 2300, 2400, 2500, 2600,
				2700, 2800, 2900, 3000,
			}, idx.docIDs(r),
			"docs with ∃ car not satisfying Toyota+2020 same-car match; "+
				"empty (700) and pure-Toyota+2020 (100) do not")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("is_null_in_correlated_and", func(t *testing.T) {
		// Python: test_is_null_in_correlated_and_under_countries_array.
		// year=2020 AND make IS NULL — both leaves correlated per car.
		//   match_same_car (2600): single car {year:2020} no make.
		r := andLeaves(idx,
			leafPositive(idx, "countries.garages.cars.year", 2020),
			leafIsNullTrue(idx, "countries.garages.cars.make"))

		assert.ElementsMatch(t, []uint64{2600}, idx.docIDs(r),
			"only doc 2600 has a car with year=2020 AND make missing same-car")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("is_null_on_object_array_subprop", func(t *testing.T) {
		// Python: test_is_null_on_object_array_subprop_under_countries_array.
		// cars.tires IS NULL — ∃ car with no tires.
		//
		// IMPROVEMENT vs PYTHON: same as L0 — fast-path correctly EXCLUDES
		// car_with_tires (2700) and car_with_multiple_tires (2900). The
		// L2 mixed_cars (2800) cars[1] has no tires → match.
		r := leafIsNullTrue(idx, "countries.garages.cars.tires")

		// Every doc that has at least one car missing tires, excluding:
		//   doc 700 (empty cars, no cars-self),
		//   doc 2700 (single car has tires),
		//   doc 2900 (single car has tires).
		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 600, 800, 900,
				1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700,
				1800, 1900, 2000, 2200, 2300, 2400, 2500, 2600,
				2800, 3000,
			}, idx.docIDs(r),
			"docs with ∃ car missing tires match")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_all_with_equal_in_and_under_countries_array.
		// colors ContainsAll [red, blue] AND make=Toyota same-car.
		//   match: doc 1300 (Toyota+[red,blue]), 1800 (Amsterdam Toyota+
		//     [red,blue]), 1900 (NL Toyota+[red,blue]).
		//   doc 3000 (Honda+[red,blue]) — make wrong → no match.
		r := andLeaves(idx,
			leafContainsAll(idx, "countries.garages.cars.colors", "red", "blue"),
			leafPositive(idx, "countries.garages.cars.make", "Toyota"))

		assert.ElementsMatch(t, []uint64{1300, 1800, 1900}, idx.docIDs(r),
			"docs with ∃ Toyota car holding both red and blue colors")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_any_with_equal_in_and_under_countries_array.
		// colors ContainsAny [red, blue] AND make=Toyota same-car.
		r := andLeaves(idx,
			leafContainsAny(idx, "countries.garages.cars.colors", "red", "blue"),
			leafPositive(idx, "countries.garages.cars.make", "Toyota"))

		assert.ElementsMatch(t,
			[]uint64{1300, 1400, 1500, 1600, 1700, 1800, 1900}, idx.docIDs(r),
			"docs with ∃ Toyota car owning red or blue (3000 Honda excluded)")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_none_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_none_with_equal_in_and_under_countries_array.
		// colors ContainsNone [red, blue] AND make=Toyota.
		//
		// DIVERGENCE FROM PYTHON: same root cause — fast-path's
		// contains_none is owner-level so docs with Toyota cars and no
		// listed colors match even when the doc has no colors at all.
		r := andLeaves(idx,
			leafContainsNone(idx, "countries.garages.cars.colors", "red", "blue"),
			leafPositive(idx, "countries.garages.cars.make", "Toyota"))

		// Toyota-having docs that also have at least one car-self surviving
		// the AndNot (i.e. that car has no red/blue color).
		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 800, 900,
				1000, 1100, 1200,
				2200, 2500, 2700, 2800, 2900,
			}, idx.docIDs(r),
			"fast-path: Toyota docs with at least one Toyota car whose "+
				"colors don't intersect the listed values")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("not_of_pinned_correlated_and", func(t *testing.T) {
		// Python: test_not_of_pinned_correlated_and_under_countries_array.
		// NOT (countries.garages.cars[0].make=Toyota AND
		//      countries.garages.cars[0].year=2020).
		//
		// L2 Scope is countries.garages (parent of the pinned cars array),
		// so `negate` is anchor(countries.garages) AndNot Witnesses — per-
		// element existential at the garages level: a doc matches when ∃
		// garage whose cars[0] does NOT fully satisfy the inner AND. Multi-
		// country docs lift through countries because any surviving garage-
		// self is enough.
		//
		// Per-doc trace at garage granularity:
		//   100 single garage cars[0]=Toyota+Camry+2020 (all_garages_satisfy):
		//     garage-self in Witnesses → no surviving → NOT match.
		//   200 single garage cars[0]=Toyota+Camry+2019: year fires, make
		//     not at slot 0 → no Witness → garage-self survives → match.
		//   300 Amsterdam cars[0]=Toyota+2019, Rotterdam cars[0]=Honda+2020:
		//     neither garage's cars[0] satisfies the inner AND → both
		//     garage-selves survive → match.
		//   400 Amsterdam cars[0]=Toyota+Camry+2020 (Witness), Rotterdam
		//     cars[0]=Kia+Sportage+2018 (no Witness): one_garage_violates —
		//     Rotterdam's garage-self survives AndNot → match. PER-ELEMENT
		//     SEMANTICS at garages. Agrees with Python.
		//   500 single garage cars[0]=Toyota+Camry+2020 (cars[1] year-only):
		//     pinned_satisfies_with_distractor at slot 0 → Witness →
		//     NOT match. IMPROVEMENT vs Python (TODO would flip after fix).
		//   600 single garage cars[0]={year:2017}: no Witness → match.
		//   700 single garage with empty cars[]: garage-self exists but no
		//     cars-self/Witness. Match under uniform "missing pinned
		//     slot → match" rule (no encoding distinction between "no
		//     cars" and "no slot").
		//   800/900 etc cars[0]=Toyota with no year: no Witness → match.
		//   1000-1200 cars[0]=Honda/Ford → no Witness → match.
		//   1300-2000 (colors/tires) cars[0]=Toyota no year → no Witness → match.
		//   2200 cars[0]=Honda+Civic+2018, cars[1]=Toyota+Camry+2020
		//     (match_at_wrong_index): no Witness at slot 0 → match.
		//   2300 single Honda+Civic+2020 (year only): no Witness → match.
		//   2400 single Honda+Civic+2019: no Witness → match.
		//   2500 single garage cars[0]=Toyota+Camry+2020, cars[1]=Honda+
		//     Civic+2019 (pinned_satisfies_with_distractor): Witness at
		//     slot 0 → NOT match. IMPROVEMENT vs Python TODO.
		//   2600 single car {year:2020} no make: no Witness → match.
		//   2700-2900 Toyota+tires no year: no Witness → match.
		//   3000 Honda+Civic+colors no year: no Witness → match.
		//
		// Same two divergences from Python as the L0 port:
		//   1) pinned_satisfies_with_distractor (500, 2500): IMPROVEMENT —
		//      fast-path correctly excludes (Python's TODO acknowledges
		//      production impl leaks; fast-path implements the intended
		//      post-fix semantic).
		//   2) empty_cars (700): fast-path matches under the uniform
		//      "missing pinned slot → match" rule. Python applies a
		//      separate vacuous-drop layer on empty arrays — not a
		//      divergence in per-element semantics, just the absence
		//      of that layer.
		//
		// Optional Python-parity follow-up (deferred): see L0 sibling
		// for the `AndNot ¬exists[cars]` shape. The per-element
		// behaviour at L2 is already correct (doc 400 matches because
		// Rotterdam violates).
		r := negate(idx, andLeaves(idx,
			leafPinnedPositive(idx, "countries.garages.cars.make", "Toyota",
				[]pinSpec{{"countries.garages.cars", 0}}),
			leafPinnedPositive(idx, "countries.garages.cars.year", 2020,
				[]pinSpec{{"countries.garages.cars", 0}})))

		assert.ElementsMatch(t,
			[]uint64{
				200, 300, 400, 600, 700, 800, 900,
				1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700,
				1800, 1900, 2000, 2200, 2300, 2400, 2600,
				2700, 2800, 2900, 3000,
			}, idx.docIDs(r),
			"fast-path: docs with ∃ garage whose cars[0] does NOT satisfy "+
				"Toyota+2020 same-slot; excludes 100/500/2500 (all garages "+
				"in those docs have cars[0]=Toyota+2020); includes 700 "+
				"(empty cars — uniform \"missing pinned slot → match\" "+
				"rule) and 400 (per-element at garages — Rotterdam "+
				"violates so doc matches)")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
	})

	// or_of_mixed_correlated_ands NOT PORTED. Same reason as the L0 sibling:
	// the Python scenario relies on a flat doc-level property (`category`)
	// which the L2 schema doesn't carry. Listed in the file-level header
	// alongside the other unsupported scenarios.

	// Group D — tokenization-aware scenarios. Same convention as the L0
	// siblings: each subtest builds a minimal local fixture so the new
	// multi-token discriminators don't pollute the shared L2 fixture's
	// wantDocs across the earlier groups. L2 adds multi-garage and multi-
	// country split discriminators that exercise per-element AND across
	// the deeper path (countries.garages.cars).

	t.Run("multi_token_equal", func(t *testing.T) {
		// Python: test_multi_token_equal_under_countries_array.
		// Filter countries.garages.cars.description = "Camry Hybrid"
		// against a word-tokenized text field. Both tokens must live at
		// the same cars element regardless of which country / garage
		// that element sits under.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// doc 4100 both_tokens_one_car: single Camry Hybrid car.
		local.addDoc(t, prop, 4100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"description": "Camry Hybrid"},
				}},
			}},
		})
		// doc 4200 single_token_one_car: description="Camry" only.
		local.addDoc(t, prop, 4200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"description": "Camry"},
				}},
			}},
		})
		// doc 4300 tokens_split_across_cars: cars[0]=Camry, cars[1]=Hybrid
		// inside one garage.
		local.addDoc(t, prop, 4300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"description": "Camry"},
					map[string]any{"description": "Hybrid"},
				}},
			}},
		})
		// doc 4400 tokens_split_across_garages: Amsterdam=Camry,
		// Rotterdam=Hybrid in one country.
		local.addDoc(t, prop, 4400, []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "Amsterdam", "cars": []any{
					map[string]any{"description": "Camry"},
				}},
				map[string]any{"city": "Rotterdam", "cars": []any{
					map[string]any{"description": "Hybrid"},
				}},
			}},
		})
		// doc 4500 tokens_split_across_countries: NL=Camry, DE=Hybrid.
		local.addDoc(t, prop, 4500, []any{
			map[string]any{"name": "Netherlands", "garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"description": "Camry"},
				}},
			}},
			map[string]any{"name": "Germany", "garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"description": "Hybrid"},
				}},
			}},
		})
		// doc 4600 match_via_one_garage: Amsterdam=Camry Hybrid, Rotterdam
		// has a Civic LX.
		local.addDoc(t, prop, 4600, []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "Amsterdam", "cars": []any{
					map[string]any{"description": "Camry Hybrid"},
				}},
				map[string]any{"city": "Rotterdam", "cars": []any{
					map[string]any{"description": "Civic LX"},
				}},
			}},
		})
		// doc 4700 match_via_one_country: NL Camry Hybrid, DE Civic LX.
		local.addDoc(t, prop, 4700, []any{
			map[string]any{"name": "Netherlands", "garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"description": "Camry Hybrid"},
				}},
			}},
			map[string]any{"name": "Germany", "garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"description": "Civic LX"},
				}},
			}},
		})
		// doc 4800 unrelated: Civic LX.
		local.addDoc(t, prop, 4800, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"description": "Civic LX"},
				}},
			}},
		})

		r := leafPositive(local, "countries.garages.cars.description", "Camry Hybrid")

		assert.ElementsMatch(t, []uint64{4100, 4600, 4700}, local.docIDs(r),
			"docs where ∃ a car has both tokens (4100 directly, 4600/4700 "+
				"via one matching garage/country); splits across cars, "+
				"garages, or countries must NOT match — same-element on tokens")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_multi_token", func(t *testing.T) {
		// Python: test_contains_any_multi_token_under_countries_array.
		// Filter countries.garages.cars.tags ContainsAny ["family hybrid"]
		// against a word-tokenized text[]. The single query list element
		// tokenizes to ["family", "hybrid"]; with one list value the
		// outer OR is a no-op so the result is the per-value match
		// bitmap.
		//
		// Lenient same-element rule (parent-Scope AND): doc 5200
		// (tokens split across two tag entries of the SAME car)
		// matches because the cars-self chain bit survives the
		// intersection. Splits across separate cars (5400), garages
		// (5500), or countries (5600) stay excluded because the chain
		// bits at those higher scopes differ.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// doc 5100 match: single car tags=["family hybrid car"] —
		// one tag entry contains both tokens.
		local.addDoc(t, prop, 5100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// doc 5200 tokens_split_across_tags: tags=["family sedan",
		// "hybrid model"] — tokens in DIFFERENT tag entries of the
		// same car. Matches under lenient.
		local.addDoc(t, prop, 5200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family sedan", "hybrid model"}},
				}},
			}},
		})
		// doc 5300 single_token_missing: tags=["family car"].
		local.addDoc(t, prop, 5300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family car"}},
				}},
			}},
		})
		// doc 5400 split_across_cars: cars[0].tags=["family car"],
		// cars[1].tags=["hybrid model"] in one garage. Excluded —
		// cars-self bits differ.
		local.addDoc(t, prop, 5400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family car"}},
					map[string]any{"tags": []any{"hybrid model"}},
				}},
			}},
		})
		// doc 5500 split_across_garages: Amsterdam tags=["family car"],
		// Rotterdam tags=["hybrid model"]. Excluded — garages-self
		// bits differ.
		local.addDoc(t, prop, 5500, []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "Amsterdam", "cars": []any{
					map[string]any{"tags": []any{"family car"}},
				}},
				map[string]any{"city": "Rotterdam", "cars": []any{
					map[string]any{"tags": []any{"hybrid model"}},
				}},
			}},
		})
		// doc 5600 split_across_countries: NL tags=["family car"],
		// DE tags=["hybrid model"]. Excluded — countries-self bits
		// differ.
		local.addDoc(t, prop, 5600, []any{
			map[string]any{"name": "Netherlands", "garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family car"}},
				}},
			}},
			map[string]any{"name": "Germany", "garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"hybrid model"}},
				}},
			}},
		})
		// doc 5700 match_via_one_garage: Amsterdam tags=["family
		// hybrid car"]; Rotterdam tags=["other"].
		local.addDoc(t, prop, 5700, []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "Amsterdam", "cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
				map[string]any{"city": "Rotterdam", "cars": []any{
					map[string]any{"tags": []any{"other"}},
				}},
			}},
		})
		// doc 5800 match_via_one_country: NL tags=["family hybrid
		// car"], DE tags=["other"].
		local.addDoc(t, prop, 5800, []any{
			map[string]any{"name": "Netherlands", "garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
			map[string]any{"name": "Germany", "garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"other"}},
				}},
			}},
		})

		r := leafContainsAny(local, "countries.garages.cars.tags", "family hybrid")

		assert.ElementsMatch(t,
			[]uint64{5100, 5200, 5700, 5800}, local.docIDs(r),
			"docs where ∃ a car has both tokens somewhere in its tags "+
				"array (5100 in one tag entry; 5200 across two tag entries "+
				"of the same car; 5700 via Amsterdam; 5800 via Netherlands); "+
				"splits across separate cars (5400), garages (5500), or "+
				"countries (5600) excluded")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	// ---- Harness gap coverage (not in Python suite) ----
	// L2 mirrors of the new L0 ContainsAll / ContainsNone tokenization
	// tests. Fixtures wrap each scenario in countries.garages.cars
	// with one car per garage, one garage per country, one country
	// per doc — same code path as the L0 versions, exercised via the
	// deeper path.

	t.Run("contains_none_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter countries.garages.cars.tags ContainsNone
		// ["family hybrid", "luxury"]. Same discriminator as L0:
		// doc 8300 has "family" alone and must match (a flatten-
		// tokens implementation would wrongly exclude it).
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 8100: tags=["family hybrid car"] → excluded.
		local.addDoc(t, prop, 8100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// 8200: tags=["luxury sedan"] → excluded.
		local.addDoc(t, prop, 8200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"luxury sedan"}},
				}},
			}},
		})
		// 8300: tags=["family car"] → MATCH. Load-bearing.
		local.addDoc(t, prop, 8300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family car"}},
				}},
			}},
		})
		// 8400: tags=["sedan"] → match.
		local.addDoc(t, prop, 8400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan"}},
				}},
			}},
		})
		// 8500: tokens split across tag entries of same car → excluded.
		local.addDoc(t, prop, 8500, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family sedan", "hybrid model"}},
				}},
			}},
		})

		r := leafContainsNone(local, "countries.garages.cars.tags",
			"family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{8300, 8400}, local.docIDs(r),
			"docs without (family AND hybrid) AND without luxury match; "+
				"8300 (family alone) is the load-bearing case — a "+
				"flattened implementation would wrongly exclude it")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter countries.garages.cars.tags ContainsAll
		// ["family hybrid", "luxury"].
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 8600: all four tokens present → match.
		local.addDoc(t, prop, 8600, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid", "luxury car"}},
				}},
			}},
		})
		// 8700: missing hybrid → no match.
		local.addDoc(t, prop, 8700, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family", "luxury"}},
				}},
			}},
		})
		// 8800: missing luxury → no match.
		local.addDoc(t, prop, 8800, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid"}},
				}},
			}},
		})
		// 8900: tokens split across two tag entries of the same car
		// → match (lenient).
		local.addDoc(t, prop, 8900, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"luxury hybrid", "family bag"}},
				}},
			}},
		})
		// 9000: no relevant tokens → no match.
		local.addDoc(t, prop, 9000, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan"}},
				}},
			}},
		})

		r := leafContainsAll(local, "countries.garages.cars.tags",
			"family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{8600, 8900}, local.docIDs(r),
			"docs where all four tokens are present on the same car "+
				"(lenient: doc 8900 splits them across two tag entries)")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_none_two_multi_token", func(t *testing.T) {
		// Filter countries.garages.cars.tags ContainsNone
		// ["family hybrid", "luxury car"].
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 9100: family + luxury individually, neither full pair →
		// MATCH. Load-bearing.
		local.addDoc(t, prop, 9100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family", "luxury"}},
				}},
			}},
		})
		// 9200: full "family hybrid" → excluded.
		local.addDoc(t, prop, 9200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid"}},
				}},
			}},
		})
		// 9300: full "luxury car" → excluded.
		local.addDoc(t, prop, 9300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"luxury car"}},
				}},
			}},
		})
		// 9400: hybrid + car individually, neither full pair → match.
		local.addDoc(t, prop, 9400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"hybrid", "car"}},
				}},
			}},
		})
		// 9500: full "family hybrid" + "car" extra → excluded.
		local.addDoc(t, prop, 9500, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// 9600: no relevant tokens → match.
		local.addDoc(t, prop, 9600, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan", "wagon"}},
				}},
			}},
		})

		r := leafContainsNone(local, "countries.garages.cars.tags",
			"family hybrid", "luxury car")

		assert.ElementsMatch(t,
			[]uint64{9100, 9400, 9600}, local.docIDs(r),
			"docs where neither (family AND hybrid) nor (luxury AND "+
				"car) is fully present; 9100 and 9400 are the "+
				"load-bearing cases — a flatten-tokens implementation "+
				"would wrongly exclude them")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_two_multi_token", func(t *testing.T) {
		// Filter countries.garages.cars.tags ContainsAny ["family
		// hybrid", "luxury car"]. Same shape as the L0 sibling; doc
		// 10000 is load-bearing for the OR-of-multi-token-Mᵢ path.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 9700: tags=["family hybrid"] → match.
		local.addDoc(t, prop, 9700, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid"}},
				}},
			}},
		})
		// 9800: tags=["luxury car"] → match.
		local.addDoc(t, prop, 9800, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"luxury car"}},
				}},
			}},
		})
		// 9900: tags=["family hybrid car"] → match.
		local.addDoc(t, prop, 9900, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// 10000: tags=["family", "luxury"] — individual tokens, no full
		// pair → NO match. Load-bearing.
		local.addDoc(t, prop, 10000, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family", "luxury"}},
				}},
			}},
		})
		// 10100: tags=["family"] → no match.
		local.addDoc(t, prop, 10100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family"}},
				}},
			}},
		})
		// 10200: tags=["sedan"] → no match.
		local.addDoc(t, prop, 10200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan"}},
				}},
			}},
		})

		r := leafContainsAny(local, "countries.garages.cars.tags",
			"family hybrid", "luxury car")

		assert.ElementsMatch(t,
			[]uint64{9700, 9800, 9900}, local.docIDs(r),
			"docs where (family AND hybrid) OR (luxury AND car) is fully "+
				"present match; 10000 (individual tokens, no full pair) "+
				"is the load-bearing case — a flatten-tokens "+
				"implementation would wrongly include it")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_two_multi_token", func(t *testing.T) {
		// Filter countries.garages.cars.tags ContainsAll ["family
		// hybrid", "luxury car"]. Same shape as L0; doc 10700 confirms
		// lenient parent-Scope AND of two multi-token Mᵢ across four
		// separate tag entries of the same car.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 10300: tags=["family hybrid", "luxury car"] → match.
		local.addDoc(t, prop, 10300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid", "luxury car"}},
				}},
			}},
		})
		// 10400: tags=["family hybrid car luxury"] — all in one tag →
		// match.
		local.addDoc(t, prop, 10400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car luxury"}},
				}},
			}},
		})
		// 10500: tags=["family hybrid"] — only first pair → no match.
		local.addDoc(t, prop, 10500, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid"}},
				}},
			}},
		})
		// 10600: tags=["luxury car"] — only second pair → no match.
		local.addDoc(t, prop, 10600, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"luxury car"}},
				}},
			}},
		})
		// 10700: tags=["family", "hybrid", "luxury", "car"] — each
		// token in its own tag entry. Lenient parent-Scope AND → match.
		local.addDoc(t, prop, 10700, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family", "hybrid", "luxury", "car"}},
				}},
			}},
		})
		// 10800: tags=["sedan"] → no match.
		local.addDoc(t, prop, 10800, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan"}},
				}},
			}},
		})

		r := leafContainsAll(local, "countries.garages.cars.tags",
			"family hybrid", "luxury car")

		assert.ElementsMatch(t,
			[]uint64{10300, 10400, 10700}, local.docIDs(r),
			"docs where (family AND hybrid) AND (luxury AND car) is "+
				"fully present match; 10700 (each token in its own tag "+
				"entry) matches under lenient parent-Scope AND")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_any_multi_token", func(t *testing.T) {
		// Filter countries.garages.cars[0].tags ContainsAny ["family
		// hybrid"]. Mirror of the L0 sibling — pin restricts to
		// cars[0] inside any garage of the country.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 10900: cars[0]=tags["family hybrid car"] → match.
		local.addDoc(t, prop, 10900, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// 11000: cars[0]=tags["family sedan", "hybrid model"] → match
		// (lenient, tokens in different tag entries of pinned car).
		local.addDoc(t, prop, 11000, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family sedan", "hybrid model"}},
				}},
			}},
		})
		// 11100: cars[0]=tags["sedan"], cars[1]=tags["family hybrid"]
		// → NO match (full pair at WRONG slot).
		local.addDoc(t, prop, 11100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan"}},
					map[string]any{"tags": []any{"family hybrid"}},
				}},
			}},
		})
		// 11200: cars[0]=tags["family"] only → no match.
		local.addDoc(t, prop, 11200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family"}},
				}},
			}},
		})
		// 11300: empty cars[] → no match.
		local.addDoc(t, prop, 11300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{}},
			}},
		})

		r := leafPinnedContainsAny(local, "countries.garages.cars.tags",
			[]pinSpec{{"countries.garages.cars", 0}}, "family hybrid")

		assert.ElementsMatch(t, []uint64{10900, 11000}, local.docIDs(r),
			"docs where cars[0] has both tokens of \"family hybrid\" match")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_none_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter countries.garages.cars[0].tags ContainsNone
		// ["family hybrid", "luxury"]. Doc 11600 is load-bearing —
		// cars[0] has "family" alone, no full pair, no luxury.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 11400: cars[0]=tags["family hybrid car"] → excluded.
		local.addDoc(t, prop, 11400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// 11500: cars[0]=tags["luxury sedan"] → excluded.
		local.addDoc(t, prop, 11500, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"luxury sedan"}},
				}},
			}},
		})
		// 11600: cars[0]=tags["family car"] → MATCH. Load-bearing.
		local.addDoc(t, prop, 11600, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family car"}},
				}},
			}},
		})
		// 11700: cars[0]=tags["sedan"], cars[1]=tags["family hybrid"]
		// → MATCH (full pair at wrong slot; pinned slot has neither
		// value).
		local.addDoc(t, prop, 11700, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan"}},
					map[string]any{"tags": []any{"family hybrid"}},
				}},
			}},
		})

		r := leafPinnedContainsNone(local, "countries.garages.cars.tags",
			[]pinSpec{{"countries.garages.cars", 0}}, "family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{11600, 11700}, local.docIDs(r),
			"docs where cars[0] satisfies neither (family AND hybrid) "+
				"nor luxury; 11600 (family alone at pinned slot) locks "+
				"down the per-value AND correctness rule under pin")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_all_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter countries.garages.cars[0].tags ContainsAll ["family
		// hybrid", "luxury"]. Mirror of the L0 sibling.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 11800: cars[0]=tags["family hybrid", "luxury car"] → match.
		local.addDoc(t, prop, 11800, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid", "luxury car"}},
				}},
			}},
		})
		// 11900: cars[0]=tags["family hybrid"] → missing luxury, no match.
		local.addDoc(t, prop, 11900, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid"}},
				}},
			}},
		})
		// 12000: cars[0]=tags["luxury sedan"] → missing pair, no match.
		local.addDoc(t, prop, 12000, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"luxury sedan"}},
				}},
			}},
		})
		// 12100: cars[0]=tags["family", "hybrid", "luxury"] — each
		// token in its own tag entry → match (lenient).
		local.addDoc(t, prop, 12100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family", "hybrid", "luxury"}},
				}},
			}},
		})
		// 12200: cars[0]=tags["sedan"], cars[1]=tags["family hybrid",
		// "luxury"] → all values at WRONG slot, no match.
		local.addDoc(t, prop, 12200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan"}},
					map[string]any{"tags": []any{"family hybrid", "luxury"}},
				}},
			}},
		})
		// 12300: cars[0]=tags["sedan"] → no relevant tokens, no match.
		local.addDoc(t, prop, 12300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"sedan"}},
				}},
			}},
		})

		r := leafPinnedContainsAll(local, "countries.garages.cars.tags",
			[]pinSpec{{"countries.garages.cars", 0}}, "family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{11800, 12100}, local.docIDs(r),
			"docs where cars[0] has BOTH (family AND hybrid) AND luxury; "+
				"12100 lenient discriminator, 12200 wrong-slot discriminator")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
	})

	t.Run("not_equal", func(t *testing.T) {
		// Python: test_not_equal_under_countries_array.
		// Filter countries.garages.cars.make != Toyota. Owner-level at
		// scope=countries.garages.cars — Python set + missing-make
		// discriminator (12900) + cross-garage existential (12800).
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 12400: single_toyota → no match.
		local.addDoc(t, prop, 12400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Toyota"},
				}},
			}},
		})
		// 12500: single_honda → match.
		local.addDoc(t, prop, 12500, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Honda"},
				}},
			}},
		})
		// 12600: mixed_toyota_and_honda → match.
		local.addDoc(t, prop, 12600, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Toyota"},
					map[string]any{"make": "Honda"},
				}},
			}},
		})
		// 12700: multiple_non_toyota → match.
		local.addDoc(t, prop, 12700, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Honda"},
					map[string]any{"make": "Ford"},
				}},
			}},
		})
		// 12800: mixed_across_garages — Amsterdam=Toyota, Rotterdam=Honda.
		// Existential per-element across garages → match (Rotterdam's
		// Honda car satisfies).
		local.addDoc(t, prop, 12800, []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "Amsterdam", "cars": []any{
					map[string]any{"make": "Toyota"},
				}},
				map[string]any{"city": "Rotterdam", "cars": []any{
					map[string]any{"make": "Honda"},
				}},
			}},
		})
		// 12900: missing_make_only — locks down the owner-level /
		// include-missing semantic at L2.
		local.addDoc(t, prop, 12900, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2020},
				}},
			}},
		})
		// 13000: empty_cars → no match.
		local.addDoc(t, prop, 13000, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{}},
			}},
		})

		r := leafNotEqual(local, "countries.garages.cars.make", "Toyota")

		assert.ElementsMatch(t,
			[]uint64{12500, 12600, 12700, 12800, 12900}, local.docIDs(r),
			"docs with ∃ car whose make != Toyota across any garage/country; "+
				"12900 (missing-make) locks down owner-level / include-missing")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_not_equal", func(t *testing.T) {
		// Harness gap coverage — not in Python suite. Filter
		// countries.garages.cars[0].make != Toyota. No pin gap (single
		// pin at cars). Scope = countries.garages, so negate is
		// per-garage existential. The cross-garage discriminator
		// (13700) verifies the per-element-at-garages behavior.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 13100: cars[0]=Toyota → no match.
		local.addDoc(t, prop, 13100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Toyota"},
				}},
			}},
		})
		// 13200: cars[0]=Honda → match.
		local.addDoc(t, prop, 13200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Honda"},
				}},
			}},
		})
		// 13300: cars[0]=Toyota, cars[1]=Honda → no match.
		local.addDoc(t, prop, 13300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Toyota"},
					map[string]any{"make": "Honda"},
				}},
			}},
		})
		// 13400: cars[0]=Honda, cars[1]=Toyota → match.
		local.addDoc(t, prop, 13400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Honda"},
					map[string]any{"make": "Toyota"},
				}},
			}},
		})
		// 13500: missing_make_at_pinned_slot → match (include-missing).
		local.addDoc(t, prop, 13500, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2020},
				}},
			}},
		})
		// 13600: empty_cars in a garage → match under uniform
		// "missing pinned slot → match" rule (no encoding distinction
		// between "no cars in garage" and "no cars[0] slot").
		local.addDoc(t, prop, 13600, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{}},
			}},
		})
		// 13700: cross-garage existential — Amsterdam cars[0]=Toyota
		// (excluded from its garage), Rotterdam cars[0]=Honda (included).
		// Per-element negate at garages keeps Rotterdam → doc matches.
		local.addDoc(t, prop, 13700, []any{
			map[string]any{"garages": []any{
				map[string]any{"city": "Amsterdam", "cars": []any{
					map[string]any{"make": "Toyota"},
				}},
				map[string]any{"city": "Rotterdam", "cars": []any{
					map[string]any{"make": "Honda"},
				}},
			}},
		})

		r := leafPinnedNotEqual(local, "countries.garages.cars.make", "Toyota",
			[]pinSpec{{"countries.garages.cars", 0}})

		assert.ElementsMatch(t,
			[]uint64{13200, 13400, 13500, 13600, 13700}, local.docIDs(r),
			"docs with ∃ garage whose cars[0].make != Toyota (or pin slot "+
				"missing); 13700 the per-garage discriminator")
		require.Equal(t, "countries.garages", r.Scope, "Scope")
		require.Equal(t, "countries.garages", r.Ceiling, "Ceiling")
	})

	t.Run("multi_pin_contains_any_multi_token", func(t *testing.T) {
		// Harness gap coverage. Filter
		// countries.garages[0].cars[0].tags ContainsAny ["family hybrid"].
		// Two pins (garages[0] + cars[0]), no gap — verifies that
		// tokenization wires through a multi-pin dispatch and that
		// every pin's _idx narrow correctly restricts the per-value
		// match bitmap. ContainsAny is positive — missing pin slot →
		// no match (different from NotEqual).
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 13800: pin slot has both tokens → match.
		local.addDoc(t, prop, 13800, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// 13900: pin slot has tokens split across tag entries → match
		// (lenient parent-Scope AND on tokenized text[]).
		local.addDoc(t, prop, 13900, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family sedan", "hybrid model"}},
				}},
			}},
		})
		// 14000: tokens at WRONG car index — garages[0].cars[1] has
		// the tokens, pin is to cars[0] → no match.
		local.addDoc(t, prop, 14000, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"other"}},
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// 14100: tokens at WRONG garage index — garages[1].cars[0]
		// has the tokens, pin is to garages[0] → no match.
		local.addDoc(t, prop, 14100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"other"}},
				}},
				map[string]any{"cars": []any{
					map[string]any{"tags": []any{"family hybrid car"}},
				}},
			}},
		})
		// 14200: missing cars[0] pin slot (garage with no cars) →
		// no match (positive operator, missing-pin → no value bucket
		// can fire).
		local.addDoc(t, prop, 14200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{}},
			}},
		})

		r := leafPinnedContainsAny(local, "countries.garages.cars.tags",
			[]pinSpec{
				{"countries.garages", 0},
				{"countries.garages.cars", 0},
			}, "family hybrid")

		assert.ElementsMatch(t, []uint64{13800, 13900}, local.docIDs(r),
			"docs where garages[0].cars[0].tags has both tokens (in one "+
				"tag entry or lenient split across two); wrong slot at "+
				"any pin level excludes; missing pin slot excludes "+
				"(positive operator)")
		require.Equal(t, "countries", r.Scope, "Scope")
		require.Equal(t, "countries", r.Ceiling, "Ceiling")
	})

	t.Run("intermediate_pin_not_equal", func(t *testing.T) {
		// Harness gap coverage. Filter
		// countries.garages[1].cars.make != Toyota. Pin only at
		// garages, cars unpinned → leafPinnedNot routes through the
		// GAP branch (perElementNotValue).
		//
		// The gap branch's per-element formula handles vacuous
		// descendants differently from the no-gap dispatch: empty
		// cars under the PINNED garage gives NO match here (no
		// per-element witness), while at no-gap empty cars under the
		// pinned slot gives MATCH (uniform "missing pinned slot →
		// match" rule). Doc 14700 below pins this divergence.
		//
		// TODO aliszka:nested_filtering — gap vs no-gap inconsistency
		// on vacuous descendants under the pin. From the user's
		// perspective, "no cars in pinned garage" and "no pin slot
		// itself" both look like "no descendant to evaluate" — the
		// two dispatches should agree. Loose semantic at gap would
		// flip 14700 to match, consistent with no-gap. Cost: one
		// additional LiftToAncestor in perElementNotFromSubtractands
		// (lift `elementUniverse` to anchor(Scope) to detect "pin
		// slot exists but vacuous descendants"). Total lifts at the
		// gap branch goes 1 → 2. Deferred — the precision was
		// originally chosen by the per-element design; flipping is a
		// product call.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 14300: no garages[1] (single garage at index 0). Pin slot
		// missing → match via scopeMissingPin.
		local.addDoc(t, prop, 14300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Ford"},
				}},
			}},
		})
		// 14400: garages[1] has [Toyota] only → no match (no car
		// satisfies != Toyota).
		local.addDoc(t, prop, 14400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Ford"},
				}},
				map[string]any{"cars": []any{
					map[string]any{"make": "Toyota"},
				}},
			}},
		})
		// 14500: garages[1] has [Honda] → match.
		local.addDoc(t, prop, 14500, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Ford"},
				}},
				map[string]any{"cars": []any{
					map[string]any{"make": "Honda"},
				}},
			}},
		})
		// 14600: garages[1] has [Toyota, Honda] → match via Honda.
		local.addDoc(t, prop, 14600, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Ford"},
				}},
				map[string]any{"cars": []any{
					map[string]any{"make": "Toyota"},
					map[string]any{"make": "Honda"},
				}},
			}},
		})
		// 14700: garages[1] has empty cars → NO match under current
		// gap-branch strict semantic. WOULD MATCH under loose
		// semantic consistent with no-gap (see t.Run-level TODO).
		// This is the only doc whose verdict depends on the
		// gap-vs-no-gap policy choice — if loose ever lands,
		// adding 14700 to wantDocs is the only fixture change.
		local.addDoc(t, prop, 14700, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Ford"},
				}},
				map[string]any{"cars": []any{}},
			}},
		})
		// 14800: garages[1] has [no-make car] → match (per-element
		// include-missing — the no-make car's cars-self bit is in
		// anchor, not in bucket[Toyota] → survives AndNot).
		local.addDoc(t, prop, 14800, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Ford"},
				}},
				map[string]any{"cars": []any{
					map[string]any{"year": 2020},
				}},
			}},
		})

		r := leafPinnedNotEqual(local, "countries.garages.cars.make", "Toyota",
			[]pinSpec{{"countries.garages", 1}})

		assert.ElementsMatch(t,
			[]uint64{14300, 14500, 14600, 14800}, local.docIDs(r),
			"docs where garages[1] doesn't exist (14300 — scopeMissingPin) "+
				"or ∃ a car under garages[1] with make != Toyota (14500, "+
				"14600, 14800); 14400 excluded (only Toyota under "+
				"garages[1]); 14700 excluded under current strict gap "+
				"semantic — would flip to match under loose (see TODO)")
		require.Equal(t, "countries", r.Scope, "Scope")
		require.Equal(t, "countries", r.Ceiling, "Ceiling")
	})

	t.Run("mixed_ceiling_or", func(t *testing.T) {
		// Harness gap coverage. Filter
		// countries.garages.cars.make = Toyota OR
		// countries.garages.cars.make IS NULL true. Mirror of L0
		// sibling — orLeaves at same Scope with mixed Ceilings.
		//
		//   leafPositive:    Scope=countries.garages.cars, Ceiling=pathRoot.
		//   leafIsNullTrue:  Scope=countries.garages.cars, Ceiling=cars.
		// orLeaves → Scope=countries.garages.cars,
		//            Ceiling=deepestPath(pathRoot, cars) =
		//            countries.garages.cars.
		local := newFastPathIndex("countries")
		prop := l2Schema()
		// 15000: Toyota Camry → matches.
		local.addDoc(t, prop, 15000, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Toyota", "model": "Camry"},
				}},
			}},
		})
		// 15100: Honda only → no match.
		local.addDoc(t, prop, 15100, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Honda", "model": "Civic"},
				}},
			}},
		})
		// 15200: no-make car → matches IS NULL true.
		local.addDoc(t, prop, 15200, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"year": 2020},
				}},
			}},
		})
		// 15300: Toyota + no-make car → both legs.
		local.addDoc(t, prop, 15300, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{
					map[string]any{"make": "Toyota", "model": "Camry"},
					map[string]any{"year": 2020},
				}},
			}},
		})
		// 15400: empty cars → no match.
		local.addDoc(t, prop, 15400, []any{
			map[string]any{"garages": []any{
				map[string]any{"cars": []any{}},
			}},
		})

		r := orLeaves(local,
			leafPositive(local, "countries.garages.cars.make", "Toyota"),
			leafIsNullTrue(local, "countries.garages.cars.make"))

		assert.ElementsMatch(t, []uint64{15000, 15200, 15300}, local.docIDs(r),
			"docs where ∃ car has make=Toyota OR ∃ car has no make")
		require.Equal(t, "countries.garages.cars", r.Scope, "Scope")
		require.Equal(t, "countries.garages.cars", r.Ceiling, "Ceiling")
	})
}

// TestFastPathL2Object_PythonPort runs the L2_object (country.garages[].
// cars[]) ports as subtests against the shared buildPythonPortL2Object
// fixture. Subtest names mirror the Python test function names (without
// the _under_country_object suffix). Multi-country discriminators have
// no analog under a single OBJECT root and are omitted from both the
// fixture and the wantDocs.
func TestFastPathL2Object_PythonPort(t *testing.T) {
	idx := buildPythonPortL2Object(t)

	t.Run("same_element_and", func(t *testing.T) {
		// Python: test_same_element_and_under_country_object.
		// Filter: make=Toyota AND year=2020. Same-Scope AND at
		// country.garages.cars; Witnesses survive only at cars-self
		// bits where both leaves fire same-car.
		//   doc 100: single Toyota Camry 2020 → match.
		//   doc 400: Amsterdam Toyota Camry 2020 → match.
		//   doc 500: cars[0] is Toyota Camry 2020 → match.
		//   docs 200, 300: splits within / across garages → no match.
		r := andLeaves(idx,
			leafPositive(idx, "country.garages.cars.make", "Toyota"),
			leafPositive(idx, "country.garages.cars.year", 2020))

		assert.ElementsMatch(t,
			[]uint64{100, 400, 500, 2200, 2500}, idx.docIDs(r),
			"docs with a Toyota 2020 car somewhere match; "+
				"splits across cars/garages do not")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("is_null_on_leaf", func(t *testing.T) {
		// Python: test_is_null_on_leaf_under_country_object.
		//
		// IS NULL true (fast-path): Witnesses_not = anchor(cars) AndNot
		// pos.Witnesses (= cars-self bits where make is set). Docs
		// with ∃ car missing make match:
		//   doc 500 (cars[1] missing make), doc 600 (single car no make),
		//   doc 800 (Rotterdam car missing make).
		rTrue := leafIsNullTrue(idx, "country.garages.cars.make")
		assert.ElementsMatch(t,
			[]uint64{500, 600, 800, 2600}, idx.docIDs(rTrue),
			"IS NULL true: docs with ∃ car missing make")
		require.Equal(t, "country.garages.cars", rTrue.Scope, "Scope")
		require.Equal(t, "country.garages.cars", rTrue.Ceiling, "Ceiling")

		// IS NULL false: docs with ∃ car having make.
		rFalse := leafIsNullFalse(idx, "country.garages.cars.make")
		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 800, 1000, 1100,
				1300, 1400, 1500, 1600, 1800, 2000,
				2200, 2300, 2400, 2500, 2700, 2800, 2900, 3000,
			},
			idx.docIDs(rFalse),
			"IS NULL false: docs with ∃ car having make")
		require.Equal(t, "country.garages.cars", rFalse.Scope, "Scope")
		require.Equal(t, pathRoot, rFalse.Ceiling, "Ceiling")
	})

	t.Run("arr_n_pin", func(t *testing.T) {
		// Python: test_arr_n_pin_under_country_object.
		// country.garages.cars[0].make=Toyota — ∃ garage whose first car
		// is Toyota.
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
		//   1000 cars[0]=Honda → miss.
		//   1100 Amsterdam cars[0]=Honda, Rotterdam cars[0]=Ford → miss.
		//   1300 cars[0]=Toyota → match.
		//   1400 cars[0]=Toyota → match.
		//   1500 cars[0]=Toyota → match.
		//   1600 Amsterdam cars[0]=Toyota → match.
		//   1800 Amsterdam cars[0]=Toyota → match.
		//   2000 cars[0]=Ford → miss.
		r := leafPinnedPositive(idx, "country.garages.cars.make", "Toyota",
			[]pinSpec{{"country.garages.cars", 0}})

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 800,
				1300, 1400, 1500, 1600, 1800,
				2500, 2700, 2800, 2900,
			}, idx.docIDs(r),
			"docs with ∃ garage whose cars[0]=Toyota match")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_is_null", func(t *testing.T) {
		// Python: test_pinned_is_null_under_country_object.
		// cars[1].make IS NULL — owner-level negation via negate(IsNullFalse).
		// Same sub-divergences from Python as the L2 sibling (see
		// L2 test for the full TODO note).
		r := leafPinnedIsNullTrue(idx, "country.garages.cars.make",
			[]pinSpec{{"country.garages.cars", 1}})

		assert.ElementsMatch(t,
			[]uint64{
				100, 300, 400, 500, 600, 700, 800,
				1300, 1400, 1600, 1800, 2000,
				2300, 2400, 2600, 2700, 2900, 3000,
			}, idx.docIDs(r),
			"fast-path: docs with ∃ garage whose cars[1] missing or "+
				"no-make match; includes empty_cars (700)")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	t.Run("contains_any", func(t *testing.T) {
		// Python: test_contains_any_under_country_object.
		// cars.colors ContainsAny [red, blue] — ∃ car owns any listed
		// value.
		r := leafContainsAny(idx, "country.garages.cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{1300, 1400, 1500, 1600, 1800, 3000},
			idx.docIDs(r),
			"docs where ∃ car owns red or blue in colors match")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_all", func(t *testing.T) {
		// Python: test_contains_all_under_country_object.
		// cars.colors ContainsAll [red, blue] — ∃ car owns BOTH values in
		// the same colors array.
		r := leafContainsAll(idx, "country.garages.cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{1300, 1800, 3000}, idx.docIDs(r),
			"docs with ∃ single car holding both red and blue match")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_none", func(t *testing.T) {
		// Python: test_contains_none_under_country_object.
		// cars.colors ContainsNone [red, blue].
		//
		// Fast-path is OWNER-LEVEL: Witnesses = anchor(cars) AndNot
		// values[red] AndNot values[blue]. Docs with ∃ cars-self bit
		// not in either value bucket match — INCLUDING docs with no
		// colors at all (cars-self bits trivially survive AndNot).
		//
		// DIVERGENCE FROM PYTHON: Python is per-tag-element existential
		// (requires ∃ a value outside the list). Docs without any
		// colors values drop in Python as vacuous; fast-path matches
		// them.
		r := leafContainsNone(idx, "country.garages.cars.colors", "red", "blue")

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 600, 800,
				1000, 1100,
				1800,
				2000,
				// Group B additions: docs without listed colors or with
				// mixed/outside cars all survive owner-level AndNot.
				2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900,
				// NOT: 700 (empty cars, no cars-self), 1300/3000 (all
				// red+blue), 1400 (all red), 1500/1600 (split — every
				// car-self in values).
			}, idx.docIDs(r),
			"fast-path: docs with ∃ car-self not in any value bucket "+
				"(includes no-colors docs that Python excludes as vacuous)")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("arr_n_pin_with_and", func(t *testing.T) {
		// Python: test_arr_n_pin_with_and_under_country_object.
		// country.garages.cars[0].make=Toyota AND
		// country.garages.cars[0].year=2020 — both leaves at the same
		// pinned slot inside a garage.
		r := andLeaves(idx,
			leafPinnedPositive(idx, "country.garages.cars.make", "Toyota",
				[]pinSpec{{"country.garages.cars", 0}}),
			leafPinnedPositive(idx, "country.garages.cars.year", 2020,
				[]pinSpec{{"country.garages.cars", 0}}))

		assert.ElementsMatch(t, []uint64{100, 400, 500, 2500}, idx.docIDs(r),
			"docs whose ∃ garage's cars[0] satisfies both make=Toyota AND year=2020")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	t.Run("arr_n_pin_with_or", func(t *testing.T) {
		// Python: test_arr_n_pin_with_or_under_country_object.
		// country.garages.cars[0].make=Toyota OR cars[0].year=2020.
		r := orLeaves(idx,
			leafPinnedPositive(idx, "country.garages.cars.make", "Toyota",
				[]pinSpec{{"country.garages.cars", 0}}),
			leafPinnedPositive(idx, "country.garages.cars.year", 2020,
				[]pinSpec{{"country.garages.cars", 0}}))

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 800,
				1300, 1400, 1500, 1600, 1800,
				2300, 2500, 2600, 2700, 2800, 2900,
			}, idx.docIDs(r),
			"docs whose ∃ garage's cars[0] satisfies make=Toyota OR year=2020")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	t.Run("or_of_correlated_ands", func(t *testing.T) {
		// Python: test_or_of_correlated_ands_under_country_object.
		// (make=Toyota AND year=2020) OR (make=Honda AND year=2019).
		r := orLeaves(idx,
			andLeaves(idx,
				leafPositive(idx, "country.garages.cars.make", "Toyota"),
				leafPositive(idx, "country.garages.cars.year", 2020)),
			andLeaves(idx,
				leafPositive(idx, "country.garages.cars.make", "Honda"),
				leafPositive(idx, "country.garages.cars.year", 2019)))

		assert.ElementsMatch(t,
			[]uint64{100, 400, 500, 2200, 2400, 2500}, idx.docIDs(r),
			"docs whose ∃ car satisfies (Toyota+2020) OR (Honda+2019) same-car")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("not_of_correlated_and", func(t *testing.T) {
		// Python: test_not_of_correlated_and_under_country_object.
		// NOT (make=Toyota AND year=2020). Match docs with ∃ car
		// violating the AND.
		r := negate(idx, andLeaves(idx,
			leafPositive(idx, "country.garages.cars.make", "Toyota"),
			leafPositive(idx, "country.garages.cars.year", 2020)))

		assert.ElementsMatch(t,
			[]uint64{
				200, 300, 400, 500, 600, 800,
				1000, 1100, 1300, 1400, 1500, 1600,
				1800, 2000, 2200, 2300, 2400, 2500, 2600,
				2700, 2800, 2900, 3000,
			}, idx.docIDs(r),
			"docs with ∃ car not satisfying Toyota+2020 same-car match; "+
				"empty (700) and pure-Toyota+2020 (100) do not")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("is_null_in_correlated_and", func(t *testing.T) {
		// Python: test_is_null_in_correlated_and_under_country_object.
		// year=2020 AND make IS NULL — both leaves correlated per car.
		r := andLeaves(idx,
			leafPositive(idx, "country.garages.cars.year", 2020),
			leafIsNullTrue(idx, "country.garages.cars.make"))

		assert.ElementsMatch(t, []uint64{2600}, idx.docIDs(r),
			"only doc 2600 has a car with year=2020 AND make missing same-car")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("is_null_on_object_array_subprop", func(t *testing.T) {
		// Python: test_is_null_on_object_array_subprop_under_country_object.
		// cars.tires IS NULL — ∃ car with no tires.
		r := leafIsNullTrue(idx, "country.garages.cars.tires")

		// Every doc that has at least one car missing tires, excluding:
		//   doc 700 (empty cars, no cars-self),
		//   doc 2700 (single car has tires),
		//   doc 2900 (single car has tires).
		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 600, 800,
				1000, 1100, 1300, 1400, 1500, 1600,
				1800, 2000, 2200, 2300, 2400, 2500, 2600,
				2800, 3000,
			}, idx.docIDs(r),
			"docs with ∃ car missing tires match")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_all_with_equal_in_and_under_country_object.
		// colors ContainsAll [red, blue] AND make=Toyota same-car.
		r := andLeaves(idx,
			leafContainsAll(idx, "country.garages.cars.colors", "red", "blue"),
			leafPositive(idx, "country.garages.cars.make", "Toyota"))

		assert.ElementsMatch(t, []uint64{1300, 1800}, idx.docIDs(r),
			"docs with ∃ Toyota car holding both red and blue colors")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_any_with_equal_in_and_under_country_object.
		// colors ContainsAny [red, blue] AND make=Toyota same-car.
		r := andLeaves(idx,
			leafContainsAny(idx, "country.garages.cars.colors", "red", "blue"),
			leafPositive(idx, "country.garages.cars.make", "Toyota"))

		assert.ElementsMatch(t,
			[]uint64{1300, 1400, 1500, 1600, 1800}, idx.docIDs(r),
			"docs with ∃ Toyota car owning red or blue (3000 Honda excluded)")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_none_with_equal_in_and", func(t *testing.T) {
		// Python: test_contains_none_with_equal_in_and_under_country_object.
		// colors ContainsNone [red, blue] AND make=Toyota.
		//
		// DIVERGENCE FROM PYTHON: same root cause as L2 — fast-path's
		// contains_none is owner-level so docs with Toyota cars and no
		// listed colors match even when the doc has no colors at all.
		r := andLeaves(idx,
			leafContainsNone(idx, "country.garages.cars.colors", "red", "blue"),
			leafPositive(idx, "country.garages.cars.make", "Toyota"))

		assert.ElementsMatch(t,
			[]uint64{
				100, 200, 300, 400, 500, 800,
				1000, 1100,
				2200, 2500, 2700, 2800, 2900,
			}, idx.docIDs(r),
			"fast-path: Toyota docs with at least one Toyota car whose "+
				"colors don't intersect the listed values")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("not_of_pinned_correlated_and", func(t *testing.T) {
		// Python: test_not_of_pinned_correlated_and_under_country_object.
		// NOT (country.garages.cars[0].make=Toyota AND
		//      country.garages.cars[0].year=2020).
		//
		// Same per-element-at-garages semantics as L2 — multi-garage
		// docs lift through country.garages because any surviving
		// garage-self is enough. Same two divergences from Python:
		// IMPROVEMENT vs Python on pinned_satisfies_with_distractor;
		// empty_cars matches under the uniform "missing pinned slot
		// → match" rule (Python applies a separate vacuous-drop
		// layer the harness doesn't model — see L0/L2 siblings).
		r := negate(idx, andLeaves(idx,
			leafPinnedPositive(idx, "country.garages.cars.make", "Toyota",
				[]pinSpec{{"country.garages.cars", 0}}),
			leafPinnedPositive(idx, "country.garages.cars.year", 2020,
				[]pinSpec{{"country.garages.cars", 0}})))

		assert.ElementsMatch(t,
			[]uint64{
				200, 300, 400, 600, 700, 800,
				1000, 1100, 1300, 1400, 1500, 1600,
				1800, 2000, 2200, 2300, 2400, 2600,
				2700, 2800, 2900, 3000,
			}, idx.docIDs(r),
			"fast-path: docs with ∃ garage whose cars[0] does NOT satisfy "+
				"Toyota+2020 same-slot; excludes 100/500/2500 (all garages "+
				"in those docs have cars[0]=Toyota+2020); includes 700 "+
				"(empty cars — uniform \"missing pinned slot → match\" "+
				"rule) and 400 (per-element at garages — Rotterdam "+
				"violates so doc matches)")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	// or_of_mixed_correlated_ands NOT PORTED. Same reason as the L0/L2
	// siblings: the Python scenario relies on a flat doc-level property
	// (`category`) which the L2_object schema doesn't carry.

	// Group D — tokenization-aware scenarios. Same convention as the
	// L0/L2 siblings: each subtest builds a minimal local fixture so the
	// new multi-token discriminators don't pollute the shared L2_object
	// fixture's wantDocs across the earlier groups. Multi-country
	// discriminators are omitted; doc IDs are bumped +10000 from the L2
	// siblings to make the local fixtures visually distinct.

	t.Run("multi_token_equal", func(t *testing.T) {
		// Python: test_multi_token_equal_under_country_object.
		// Filter country.garages.cars.description = "Camry Hybrid"
		// against a word-tokenized text field. Both tokens must live at
		// the same cars element.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// doc 14100 both_tokens_one_car: single Camry Hybrid car.
		local.addDoc(t, prop, 14100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"description": "Camry Hybrid"},
			}},
		}})
		// doc 14200 single_token_one_car: description="Camry" only.
		local.addDoc(t, prop, 14200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"description": "Camry"},
			}},
		}})
		// doc 14300 tokens_split_across_cars: cars[0]=Camry, cars[1]=Hybrid
		// inside one garage.
		local.addDoc(t, prop, 14300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"description": "Camry"},
				map[string]any{"description": "Hybrid"},
			}},
		}})
		// doc 14400 tokens_split_across_garages: Amsterdam=Camry,
		// Rotterdam=Hybrid in one country.
		local.addDoc(t, prop, 14400, map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"description": "Camry"},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"description": "Hybrid"},
			}},
		}})
		// doc 14500 [skipped — multi-country, no L2_object analog]
		// doc 14600 match_via_one_garage: Amsterdam=Camry Hybrid, Rotterdam
		// has a Civic LX.
		local.addDoc(t, prop, 14600, map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"description": "Camry Hybrid"},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"description": "Civic LX"},
			}},
		}})
		// doc 14700 [skipped — multi-country, no L2_object analog]
		// doc 14800 unrelated: Civic LX.
		local.addDoc(t, prop, 14800, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"description": "Civic LX"},
			}},
		}})

		r := leafPositive(local, "country.garages.cars.description", "Camry Hybrid")

		assert.ElementsMatch(t, []uint64{14100, 14600}, local.docIDs(r),
			"docs where ∃ a car has both tokens (14100 directly, 14600 "+
				"via one matching garage); splits across cars or "+
				"garages must NOT match — same-element on tokens")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_multi_token", func(t *testing.T) {
		// Python: test_contains_any_multi_token_under_country_object.
		// Filter country.garages.cars.tags ContainsAny ["family hybrid"]
		// against a word-tokenized text[].
		//
		// Lenient same-element rule (parent-Scope AND): doc 15200
		// (tokens split across two tag entries of the SAME car)
		// matches. Splits across separate cars (15400) or garages
		// (15500) stay excluded.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// doc 15100 match: single car tags=["family hybrid car"] —
		// one tag entry contains both tokens.
		local.addDoc(t, prop, 15100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// doc 15200 tokens_split_across_tags: tags=["family sedan",
		// "hybrid model"] — tokens in DIFFERENT tag entries of the
		// same car. Matches under lenient.
		local.addDoc(t, prop, 15200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family sedan", "hybrid model"}},
			}},
		}})
		// doc 15300 single_token_missing: tags=["family car"].
		local.addDoc(t, prop, 15300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family car"}},
			}},
		}})
		// doc 15400 split_across_cars: cars[0].tags=["family car"],
		// cars[1].tags=["hybrid model"] in one garage. Excluded.
		local.addDoc(t, prop, 15400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family car"}},
				map[string]any{"tags": []any{"hybrid model"}},
			}},
		}})
		// doc 15500 split_across_garages: Amsterdam tags=["family car"],
		// Rotterdam tags=["hybrid model"]. Excluded.
		local.addDoc(t, prop, 15500, map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"tags": []any{"family car"}},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"tags": []any{"hybrid model"}},
			}},
		}})
		// doc 15600 [skipped — multi-country, no L2_object analog]
		// doc 15700 match_via_one_garage: Amsterdam tags=["family
		// hybrid car"]; Rotterdam tags=["other"].
		local.addDoc(t, prop, 15700, map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"tags": []any{"other"}},
			}},
		}})
		// doc 15800 [skipped — multi-country, no L2_object analog]

		r := leafContainsAny(local, "country.garages.cars.tags", "family hybrid")

		assert.ElementsMatch(t,
			[]uint64{15100, 15200, 15700}, local.docIDs(r),
			"docs where ∃ a car has both tokens somewhere in its tags "+
				"array (15100 in one tag entry; 15200 across two tag entries "+
				"of the same car; 15700 via Amsterdam); "+
				"splits across separate cars (15400) or garages (15500) excluded")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	// ---- Harness gap coverage (not in Python suite) ----
	// L2_object mirrors of the L0/L2 ContainsAll / ContainsNone
	// tokenization tests. Fixtures wrap each scenario in
	// country.garages.cars with one car per garage, one garage per
	// country, one country per doc — same code path as the L2
	// versions, exercised via the single-OBJECT root.

	t.Run("contains_none_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter country.garages.cars.tags ContainsNone
		// ["family hybrid", "luxury"]. Same discriminator as L0/L2:
		// doc 18300 has "family" alone and must match (a flatten-
		// tokens implementation would wrongly exclude it).
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 18100: tags=["family hybrid car"] → excluded.
		local.addDoc(t, prop, 18100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// 18200: tags=["luxury sedan"] → excluded.
		local.addDoc(t, prop, 18200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"luxury sedan"}},
			}},
		}})
		// 18300: tags=["family car"] → MATCH. Load-bearing.
		local.addDoc(t, prop, 18300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family car"}},
			}},
		}})
		// 18400: tags=["sedan"] → match.
		local.addDoc(t, prop, 18400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan"}},
			}},
		}})
		// 18500: tokens split across tag entries of same car → excluded.
		local.addDoc(t, prop, 18500, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family sedan", "hybrid model"}},
			}},
		}})

		r := leafContainsNone(local, "country.garages.cars.tags",
			"family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{18300, 18400}, local.docIDs(r),
			"docs without (family AND hybrid) AND without luxury match; "+
				"18300 (family alone) is the load-bearing case — a "+
				"flattened implementation would wrongly exclude it")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter country.garages.cars.tags ContainsAll
		// ["family hybrid", "luxury"].
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 18600: all four tokens present → match.
		local.addDoc(t, prop, 18600, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid", "luxury car"}},
			}},
		}})
		// 18700: missing hybrid → no match.
		local.addDoc(t, prop, 18700, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family", "luxury"}},
			}},
		}})
		// 18800: missing luxury → no match.
		local.addDoc(t, prop, 18800, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid"}},
			}},
		}})
		// 18900: tokens split across two tag entries of the same car
		// → match (lenient).
		local.addDoc(t, prop, 18900, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"luxury hybrid", "family bag"}},
			}},
		}})
		// 19000: no relevant tokens → no match.
		local.addDoc(t, prop, 19000, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan"}},
			}},
		}})

		r := leafContainsAll(local, "country.garages.cars.tags",
			"family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{18600, 18900}, local.docIDs(r),
			"docs where all four tokens are present on the same car "+
				"(lenient: doc 18900 splits them across two tag entries)")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_none_two_multi_token", func(t *testing.T) {
		// Filter country.garages.cars.tags ContainsNone
		// ["family hybrid", "luxury car"].
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 19100: family + luxury individually, neither full pair →
		// MATCH. Load-bearing.
		local.addDoc(t, prop, 19100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family", "luxury"}},
			}},
		}})
		// 19200: full "family hybrid" → excluded.
		local.addDoc(t, prop, 19200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid"}},
			}},
		}})
		// 19300: full "luxury car" → excluded.
		local.addDoc(t, prop, 19300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"luxury car"}},
			}},
		}})
		// 19400: hybrid + car individually, neither full pair → match.
		local.addDoc(t, prop, 19400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"hybrid", "car"}},
			}},
		}})
		// 19500: full "family hybrid" + "car" extra → excluded.
		local.addDoc(t, prop, 19500, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// 19600: no relevant tokens → match.
		local.addDoc(t, prop, 19600, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan", "wagon"}},
			}},
		}})

		r := leafContainsNone(local, "country.garages.cars.tags",
			"family hybrid", "luxury car")

		assert.ElementsMatch(t,
			[]uint64{19100, 19400, 19600}, local.docIDs(r),
			"docs where neither (family AND hybrid) nor (luxury AND "+
				"car) is fully present; 19100 and 19400 are the "+
				"load-bearing cases — a flatten-tokens implementation "+
				"would wrongly exclude them")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("contains_any_two_multi_token", func(t *testing.T) {
		// Filter country.garages.cars.tags ContainsAny ["family
		// hybrid", "luxury car"]. Same shape as L0/L2; doc 20000 is
		// load-bearing for the OR-of-multi-token-Mᵢ path.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 19700: tags=["family hybrid"] → match.
		local.addDoc(t, prop, 19700, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid"}},
			}},
		}})
		// 19800: tags=["luxury car"] → match.
		local.addDoc(t, prop, 19800, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"luxury car"}},
			}},
		}})
		// 19900: tags=["family hybrid car"] → match.
		local.addDoc(t, prop, 19900, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// 20000: tags=["family", "luxury"] — individual tokens, no full
		// pair → NO match. Load-bearing.
		local.addDoc(t, prop, 20000, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family", "luxury"}},
			}},
		}})
		// 20100: tags=["family"] → no match.
		local.addDoc(t, prop, 20100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family"}},
			}},
		}})
		// 20200: tags=["sedan"] → no match.
		local.addDoc(t, prop, 20200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan"}},
			}},
		}})

		r := leafContainsAny(local, "country.garages.cars.tags",
			"family hybrid", "luxury car")

		assert.ElementsMatch(t,
			[]uint64{19700, 19800, 19900}, local.docIDs(r),
			"docs where (family AND hybrid) OR (luxury AND car) is fully "+
				"present match; 20000 (individual tokens, no full pair) "+
				"is the load-bearing case — a flatten-tokens "+
				"implementation would wrongly include it")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, pathRoot, r.Ceiling, "Ceiling")
	})

	t.Run("contains_all_two_multi_token", func(t *testing.T) {
		// Filter country.garages.cars.tags ContainsAll ["family
		// hybrid", "luxury car"]. Same shape as L0/L2; doc 20700
		// confirms lenient parent-Scope AND of two multi-token Mᵢ
		// across four separate tag entries of the same car.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 20300: tags=["family hybrid", "luxury car"] → match.
		local.addDoc(t, prop, 20300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid", "luxury car"}},
			}},
		}})
		// 20400: tags=["family hybrid car luxury"] → match.
		local.addDoc(t, prop, 20400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car luxury"}},
			}},
		}})
		// 20500: tags=["family hybrid"] — only first pair → no match.
		local.addDoc(t, prop, 20500, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid"}},
			}},
		}})
		// 20600: tags=["luxury car"] — only second pair → no match.
		local.addDoc(t, prop, 20600, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"luxury car"}},
			}},
		}})
		// 20700: tags=["family", "hybrid", "luxury", "car"] — each
		// token in its own tag entry. Lenient parent-Scope AND → match.
		local.addDoc(t, prop, 20700, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family", "hybrid", "luxury", "car"}},
			}},
		}})
		// 20800: tags=["sedan"] → no match.
		local.addDoc(t, prop, 20800, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan"}},
			}},
		}})

		r := leafContainsAll(local, "country.garages.cars.tags",
			"family hybrid", "luxury car")

		assert.ElementsMatch(t,
			[]uint64{20300, 20400, 20700}, local.docIDs(r),
			"docs where (family AND hybrid) AND (luxury AND car) is "+
				"fully present match; 20700 (each token in its own tag "+
				"entry) matches under lenient parent-Scope AND")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_any_multi_token", func(t *testing.T) {
		// Filter country.garages.cars[0].tags ContainsAny ["family
		// hybrid"]. Mirror of L0/L2 — pin restricts to cars[0] under
		// the single-OBJECT country root.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 20900: cars[0]=tags["family hybrid car"] → match.
		local.addDoc(t, prop, 20900, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// 21000: cars[0]=tags["family sedan", "hybrid model"] → match
		// (lenient).
		local.addDoc(t, prop, 21000, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family sedan", "hybrid model"}},
			}},
		}})
		// 21100: cars[0]=tags["sedan"], cars[1]=tags["family hybrid"]
		// → NO match (full pair at wrong slot).
		local.addDoc(t, prop, 21100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan"}},
				map[string]any{"tags": []any{"family hybrid"}},
			}},
		}})
		// 21200: cars[0]=tags["family"] only → no match.
		local.addDoc(t, prop, 21200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family"}},
			}},
		}})
		// 21300: empty cars[] → no match.
		local.addDoc(t, prop, 21300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{}},
		}})

		r := leafPinnedContainsAny(local, "country.garages.cars.tags",
			[]pinSpec{{"country.garages.cars", 0}}, "family hybrid")

		assert.ElementsMatch(t, []uint64{20900, 21000}, local.docIDs(r),
			"docs where cars[0] has both tokens of \"family hybrid\" match")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_none_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter country.garages.cars[0].tags ContainsNone
		// ["family hybrid", "luxury"]. Doc 21600 is load-bearing.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 21400: cars[0]=tags["family hybrid car"] → excluded.
		local.addDoc(t, prop, 21400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// 21500: cars[0]=tags["luxury sedan"] → excluded.
		local.addDoc(t, prop, 21500, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"luxury sedan"}},
			}},
		}})
		// 21600: cars[0]=tags["family car"] → MATCH. Load-bearing.
		local.addDoc(t, prop, 21600, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family car"}},
			}},
		}})
		// 21700: cars[0]=tags["sedan"], cars[1]=tags["family hybrid"]
		// → MATCH (wrong slot has the full pair, pinned slot doesn't).
		local.addDoc(t, prop, 21700, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan"}},
				map[string]any{"tags": []any{"family hybrid"}},
			}},
		}})

		r := leafPinnedContainsNone(local, "country.garages.cars.tags",
			[]pinSpec{{"country.garages.cars", 0}}, "family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{21600, 21700}, local.docIDs(r),
			"docs where cars[0] satisfies neither (family AND hybrid) "+
				"nor luxury; 21600 (family alone at pinned slot) locks "+
				"down the per-value AND correctness rule under pin")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_contains_all_mixed_single_and_multi_token", func(t *testing.T) {
		// Filter country.garages.cars[0].tags ContainsAll ["family
		// hybrid", "luxury"]. Mirror of L0/L2.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 21800: cars[0]=tags["family hybrid", "luxury car"] → match.
		local.addDoc(t, prop, 21800, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid", "luxury car"}},
			}},
		}})
		// 21900: cars[0]=tags["family hybrid"] → no match.
		local.addDoc(t, prop, 21900, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid"}},
			}},
		}})
		// 22000: cars[0]=tags["luxury sedan"] → no match.
		local.addDoc(t, prop, 22000, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"luxury sedan"}},
			}},
		}})
		// 22100: cars[0]=tags["family", "hybrid", "luxury"] → match
		// (lenient).
		local.addDoc(t, prop, 22100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family", "hybrid", "luxury"}},
			}},
		}})
		// 22200: cars[0]=tags["sedan"], cars[1]=tags["family hybrid",
		// "luxury"] → wrong slot, no match.
		local.addDoc(t, prop, 22200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan"}},
				map[string]any{"tags": []any{"family hybrid", "luxury"}},
			}},
		}})
		// 22300: cars[0]=tags["sedan"] → no match.
		local.addDoc(t, prop, 22300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"sedan"}},
			}},
		}})

		r := leafPinnedContainsAll(local, "country.garages.cars.tags",
			[]pinSpec{{"country.garages.cars", 0}}, "family hybrid", "luxury")

		assert.ElementsMatch(t, []uint64{21800, 22100}, local.docIDs(r),
			"docs where cars[0] has BOTH (family AND hybrid) AND luxury; "+
				"22100 lenient discriminator, 22200 wrong-slot discriminator")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	t.Run("not_equal", func(t *testing.T) {
		// Python: test_not_equal_under_country_object.
		// Filter country.garages.cars.make != Toyota. Mirror of L0/L2 —
		// Python set + cross-garage existential + missing-make discriminator.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 22400: single_toyota → no match.
		local.addDoc(t, prop, 22400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Toyota"},
			}},
		}})
		// 22500: single_honda → match.
		local.addDoc(t, prop, 22500, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Honda"},
			}},
		}})
		// 22600: mixed_toyota_and_honda → match.
		local.addDoc(t, prop, 22600, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Toyota"},
				map[string]any{"make": "Honda"},
			}},
		}})
		// 22700: multiple_non_toyota → match.
		local.addDoc(t, prop, 22700, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Honda"},
				map[string]any{"make": "Ford"},
			}},
		}})
		// 22800: mixed_across_garages → match.
		local.addDoc(t, prop, 22800, map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota"},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"make": "Honda"},
			}},
		}})
		// 22900: missing_make_only — locks down owner-level /
		// include-missing semantic.
		local.addDoc(t, prop, 22900, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"year": 2020},
			}},
		}})
		// 23000: empty_cars → no match.
		local.addDoc(t, prop, 23000, map[string]any{"garages": []any{
			map[string]any{"cars": []any{}},
		}})

		r := leafNotEqual(local, "country.garages.cars.make", "Toyota")

		assert.ElementsMatch(t,
			[]uint64{22500, 22600, 22700, 22800, 22900}, local.docIDs(r),
			"docs with ∃ car whose make != Toyota; 22900 (missing-make) "+
				"locks down owner-level / include-missing")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})

	t.Run("pinned_not_equal", func(t *testing.T) {
		// Harness gap coverage — not in Python suite. Filter
		// country.garages.cars[0].make != Toyota. Mirror of L0/L2 —
		// single-OBJECT country root, otherwise identical dispatch
		// (no pin gap, per-garage negate at country.garages scope).
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 23100: cars[0]=Toyota → no match.
		local.addDoc(t, prop, 23100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Toyota"},
			}},
		}})
		// 23200: cars[0]=Honda → match.
		local.addDoc(t, prop, 23200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Honda"},
			}},
		}})
		// 23300: cars[0]=Toyota, cars[1]=Honda → no match.
		local.addDoc(t, prop, 23300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Toyota"},
				map[string]any{"make": "Honda"},
			}},
		}})
		// 23400: cars[0]=Honda, cars[1]=Toyota → match.
		local.addDoc(t, prop, 23400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Honda"},
				map[string]any{"make": "Toyota"},
			}},
		}})
		// 23500: missing_make_at_pinned_slot → match.
		local.addDoc(t, prop, 23500, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"year": 2020},
			}},
		}})
		// 23600: empty_cars in a garage → match (structural).
		local.addDoc(t, prop, 23600, map[string]any{"garages": []any{
			map[string]any{"cars": []any{}},
		}})
		// 23700: cross-garage existential.
		local.addDoc(t, prop, 23700, map[string]any{"garages": []any{
			map[string]any{"city": "Amsterdam", "cars": []any{
				map[string]any{"make": "Toyota"},
			}},
			map[string]any{"city": "Rotterdam", "cars": []any{
				map[string]any{"make": "Honda"},
			}},
		}})

		r := leafPinnedNotEqual(local, "country.garages.cars.make", "Toyota",
			[]pinSpec{{"country.garages.cars", 0}})

		assert.ElementsMatch(t,
			[]uint64{23200, 23400, 23500, 23600, 23700}, local.docIDs(r),
			"docs with ∃ garage whose cars[0].make != Toyota (or pin slot "+
				"missing); 23700 the per-garage discriminator")
		require.Equal(t, "country.garages", r.Scope, "Scope")
		require.Equal(t, "country.garages", r.Ceiling, "Ceiling")
	})

	t.Run("multi_pin_contains_any_multi_token", func(t *testing.T) {
		// Harness gap coverage. Filter
		// country.garages[0].cars[0].tags ContainsAny ["family hybrid"].
		// Mirror of the L2 test under the single-OBJECT country root —
		// same two-pin no-gap dispatch through tokenizedMatchBitmap.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 23800: pin slot has both tokens → match.
		local.addDoc(t, prop, 23800, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// 23900: pin slot has tokens split across tag entries → match.
		local.addDoc(t, prop, 23900, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family sedan", "hybrid model"}},
			}},
		}})
		// 24000: tokens at WRONG car index → no match.
		local.addDoc(t, prop, 24000, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"other"}},
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// 24100: tokens at WRONG garage index → no match.
		local.addDoc(t, prop, 24100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"other"}},
			}},
			map[string]any{"cars": []any{
				map[string]any{"tags": []any{"family hybrid car"}},
			}},
		}})
		// 24200: missing cars[0] pin slot → no match.
		local.addDoc(t, prop, 24200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{}},
		}})

		r := leafPinnedContainsAny(local, "country.garages.cars.tags",
			[]pinSpec{
				{"country.garages", 0},
				{"country.garages.cars", 0},
			}, "family hybrid")

		assert.ElementsMatch(t, []uint64{23800, 23900}, local.docIDs(r),
			"docs where garages[0].cars[0].tags has both tokens; wrong "+
				"slot at any pin level or missing pin slot excludes")
		require.Equal(t, "country", r.Scope, "Scope")
		require.Equal(t, "country", r.Ceiling, "Ceiling")
	})

	t.Run("intermediate_pin_not_equal", func(t *testing.T) {
		// Harness gap coverage. Filter
		// country.garages[1].cars.make != Toyota. Pin only at
		// garages → gap branch (perElementNotValue). Same shape as
		// the L2 sibling. Doc 24700 pins the gap-vs-no-gap
		// divergence on vacuous descendants.
		//
		// TODO aliszka:nested_filtering — same as L2 sibling. The
		// gap branch's strict handling of empty descendants under
		// pin (24700 → no match) differs from the no-gap branch's
		// uniform "missing pinned slot → match" rule. Flipping to
		// loose costs one additional LiftToAncestor at the gap
		// branch (1 → 2 total). Deferred — product call.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 24300: no garages[1] → match via scopeMissingPin.
		local.addDoc(t, prop, 24300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Ford"},
			}},
		}})
		// 24400: garages[1] = [Toyota only] → no match.
		local.addDoc(t, prop, 24400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Ford"},
			}},
			map[string]any{"cars": []any{
				map[string]any{"make": "Toyota"},
			}},
		}})
		// 24500: garages[1] = [Honda] → match.
		local.addDoc(t, prop, 24500, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Ford"},
			}},
			map[string]any{"cars": []any{
				map[string]any{"make": "Honda"},
			}},
		}})
		// 24600: garages[1] = [Toyota, Honda] → match via Honda.
		local.addDoc(t, prop, 24600, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Ford"},
			}},
			map[string]any{"cars": []any{
				map[string]any{"make": "Toyota"},
				map[string]any{"make": "Honda"},
			}},
		}})
		// 24700: garages[1] has empty cars → NO match under current
		// gap-branch strict semantic. WOULD MATCH under loose
		// semantic consistent with no-gap (see t.Run-level TODO).
		// Only doc whose verdict depends on the policy choice.
		local.addDoc(t, prop, 24700, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Ford"},
			}},
			map[string]any{"cars": []any{}},
		}})
		// 24800: garages[1] = [no-make car] → match (include-missing
		// at intermediate pin).
		local.addDoc(t, prop, 24800, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Ford"},
			}},
			map[string]any{"cars": []any{
				map[string]any{"year": 2020},
			}},
		}})

		r := leafPinnedNotEqual(local, "country.garages.cars.make", "Toyota",
			[]pinSpec{{"country.garages", 1}})

		assert.ElementsMatch(t,
			[]uint64{24300, 24500, 24600, 24800}, local.docIDs(r),
			"docs where garages[1] doesn't exist (24300 — scopeMissingPin) "+
				"or ∃ a car under garages[1] with make != Toyota; 24700 "+
				"excluded under current strict gap semantic — would flip "+
				"to match under loose (see TODO)")
		require.Equal(t, "country", r.Scope, "Scope")
		require.Equal(t, "country", r.Ceiling, "Ceiling")
	})

	t.Run("mixed_ceiling_or", func(t *testing.T) {
		// Harness gap coverage — mirror of L0/L2 mixed-Ceiling OR
		// test under the single-OBJECT country root.
		local := newFastPathIndex("country")
		prop := l2ObjectSchema()
		// 25000: Toyota Camry → matches.
		local.addDoc(t, prop, 25000, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry"},
			}},
		}})
		// 25100: Honda only → no match.
		local.addDoc(t, prop, 25100, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Honda", "model": "Civic"},
			}},
		}})
		// 25200: no-make car → matches IS NULL true.
		local.addDoc(t, prop, 25200, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"year": 2020},
			}},
		}})
		// 25300: Toyota + no-make car → both legs.
		local.addDoc(t, prop, 25300, map[string]any{"garages": []any{
			map[string]any{"cars": []any{
				map[string]any{"make": "Toyota", "model": "Camry"},
				map[string]any{"year": 2020},
			}},
		}})
		// 25400: empty cars → no match.
		local.addDoc(t, prop, 25400, map[string]any{"garages": []any{
			map[string]any{"cars": []any{}},
		}})

		r := orLeaves(local,
			leafPositive(local, "country.garages.cars.make", "Toyota"),
			leafIsNullTrue(local, "country.garages.cars.make"))

		assert.ElementsMatch(t, []uint64{25000, 25200, 25300}, local.docIDs(r),
			"docs where ∃ car has make=Toyota OR ∃ car has no make")
		require.Equal(t, "country.garages.cars", r.Scope, "Scope")
		require.Equal(t, "country.garages.cars", r.Ceiling, "Ceiling")
	})
}
