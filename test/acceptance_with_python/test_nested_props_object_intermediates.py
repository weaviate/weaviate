"""End-to-end tests for nested object property filtering — single-OBJECT intermediates.

Complements test_nested_props_array_intermediates.py by exercising three new variants whose paths
contain single-OBJECT (non-array) intermediates above (or in place of) the
deepest array level:

    Shape A: country.garage.cars[]   — OBJECT → OBJECT → OBJECT_ARRAY
    Shape B: country.garage.car       — OBJECT → OBJECT → OBJECT
    Shape C: car                      — OBJECT (at root)

The existing variants in test_nested_props_array_intermediates.py cover paths whose intermediates
are OBJECT_ARRAY at every level (or at most one OBJECT at L0). These tests
confirm the filter pipeline — LCA computation, position encoding, dispatch —
remains correct when single-OBJECT levels appear above or instead of arrays.

Helpers and the per-datatype assertion sweep are imported from
test_nested_props_array_intermediates.py so the leaf schema and expectations stay identical.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest
from weaviate.classes.config import Configure, DataType, Property, Tokenization
from weaviate.collections.classes.filters import Filter
from weaviate.exceptions import WeaviateQueryError

from .conftest import CollectionFactory
from .test_nested_props_array_intermediates import (
    _CARS_PROPS,
    _assert_comparison_operator_sweep_selects,
    _assert_each_datatype_filter_selects,
    _honda_2015,
    _honda_civic_for_datatypes_test,
    make_car,
    omit_fields,
)


# ---------------------------------------------------------------------------
# Schema — one new collection with all three new nested shapes.
#
#   country (object)
#     └── garage (object)
#           ├── cars (object[])   ← Shape A target
#           └── car  (object)     ← Shape B target
#   car (object)                  ← Shape C target
#
# Each shape filters on the same leaves as the existing tests (make, color,
# year, tags, …), just under a different parent path.
# ---------------------------------------------------------------------------

_NEW_GARAGE_PROPS = [
    Property(name="city", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
    Property(name="cars", data_type=DataType.OBJECT_ARRAY, nested_properties=_CARS_PROPS),
    Property(name="car", data_type=DataType.OBJECT, nested_properties=_CARS_PROPS),
]

_NEW_COUNTRY_INNER = [
    Property(name="name", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
    Property(name="garage", data_type=DataType.OBJECT, nested_properties=_NEW_GARAGE_PROPS),
]

NEW_COLLECTION_PROPS: List[Property] = [
    Property(name="category", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
    Property(name="country", data_type=DataType.OBJECT, nested_properties=_NEW_COUNTRY_INNER),
    Property(name="car", data_type=DataType.OBJECT, nested_properties=_CARS_PROPS),
]


# ---------------------------------------------------------------------------
# Sub-tree builders for the new shapes. The car/leaf builder is imported from
# test_nested_props_array_intermediates (same leaf schema).
# ---------------------------------------------------------------------------


def make_new_garage(
    *,
    city: str = "Amsterdam",
    cars: List[Dict[str, Any]] = None,
    car: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Build a garage holding either `cars` (object[]) or `car` (single object),
    or both. Tests typically populate only the field that matches the variant
    under test; the other stays unset."""
    out: Dict[str, Any] = {"city": city}
    if cars is not None:
        out["cars"] = cars
    if car is not None:
        out["car"] = car
    return out


def make_new_country(
    *,
    name: str = "Netherlands",
    garage: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Build a country with a single garage (single OBJECT, not array)."""
    return {
        "name": name,
        "garage": garage if garage is not None else make_new_garage(),
    }


def make_new_collection(collection_factory: CollectionFactory):
    """Create the object-intermediates collection used by every test in this file."""
    return collection_factory(
        properties=NEW_COLLECTION_PROPS,
        vector_config=Configure.Vectors.self_provided(),
    )


# ===========================================================================
# Scenario: every leaf datatype is filterable
# ===========================================================================
#
# Mirror of the all-datatypes scenario in test_nested_props_array_intermediates.py. Inserts a
# Toyota and a Honda under each variant's path, then sweeps every leaf
# datatype filter to confirm only the Toyota doc is selected.


def test_all_datatypes_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars[] (OBJECT → OBJECT → OBJECT_ARRAY)."""
    collection = make_new_collection(collection_factory)
    toyota = make_car()
    honda = _honda_civic_for_datatypes_test()

    toyota_doc_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[toyota])),
    })
    honda_doc_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[honda])),
    })

    _assert_each_datatype_filter_selects(
        collection,
        source_car=toyota,
        cars_path="country.garage.cars",
        expected_match_ids=[toyota_doc_id],
        expected_miss_ids=[honda_doc_id],
    )


def test_all_datatypes_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car (OBJECT → OBJECT → OBJECT)."""
    collection = make_new_collection(collection_factory)
    toyota = make_car()
    honda = _honda_civic_for_datatypes_test()

    toyota_doc_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=toyota)),
    })
    honda_doc_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=honda)),
    })

    _assert_each_datatype_filter_selects(
        collection,
        source_car=toyota,
        cars_path="country.garage.car",
        expected_match_ids=[toyota_doc_id],
        expected_miss_ids=[honda_doc_id],
    )


def test_all_datatypes_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car (single OBJECT at root)."""
    collection = make_new_collection(collection_factory)
    toyota = make_car()
    honda = _honda_civic_for_datatypes_test()

    toyota_doc_id = collection.data.insert({"car": toyota})
    honda_doc_id = collection.data.insert({"car": honda})

    _assert_each_datatype_filter_selects(
        collection,
        source_car=toyota,
        cars_path="car",
        expected_match_ids=[toyota_doc_id],
        expected_miss_ids=[honda_doc_id],
    )


# ===========================================================================
# Scenario: same-element AND correlates per-car
# ===========================================================================
#
# `make=Toyota AND color=red` must be satisfied by the SAME car. Shape A
# exercises real per-element correlation inside cars[]. Shape B and Shape C
# are degenerate (only one car per doc) and act as smoke tests for AND
# dispatch on a single-OBJECT subtree.


def test_same_element_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — same-element AND inside country.garage.cars[].

    The garage and country are single OBJECTs, so the only split shape available
    is two cars sitting side by side in cars[]."""
    collection = make_new_collection(collection_factory)

    match_doc_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
        ])),
    })
    split_within_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ])),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.cars.make").equal("Toyota"),
        Filter.by_property("country.garage.cars.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_doc_id in ids, "single red Toyota should match"
    assert split_within_cars_id not in ids, (
        "Toyota+blue and Honda+red split across cars — no single car has both"
    )


def test_same_element_and_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — AND on the single-OBJECT car at country.garage.car.

    Degenerate (only one car per doc) — smoke test for AND dispatch on a
    single-OBJECT subtree."""
    collection = make_new_collection(collection_factory)

    match_doc_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Toyota", color="red"),
        )),
    })
    mismatch_color_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Toyota", color="blue"),
        )),
    })
    mismatch_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Honda", color="red"),
        )),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.car.make").equal("Toyota"),
        Filter.by_property("country.garage.car.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_doc_id in ids, "red Toyota single car should match"
    assert mismatch_color_id not in ids, "blue Toyota — color mismatch must not match"
    assert mismatch_make_id not in ids, "red Honda — make mismatch must not match"


def test_same_element_and_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — AND on the single-OBJECT car at root. Degenerate (one car)."""
    collection = make_new_collection(collection_factory)

    match_doc_id = collection.data.insert({"car": make_car(make="Toyota", color="red")})
    mismatch_color_id = collection.data.insert({"car": make_car(make="Toyota", color="blue")})
    mismatch_make_id = collection.data.insert({"car": make_car(make="Honda", color="red")})

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("car.make").equal("Toyota"),
        Filter.by_property("car.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_doc_id in ids, "red Toyota at root should match"
    assert mismatch_color_id not in ids, "blue Toyota — color mismatch must not match"
    assert mismatch_make_id not in ids, "red Honda — make mismatch must not match"


# ===========================================================================
# Scenario: arr[N] positional pin on cars[]
# ===========================================================================
#
# Only Shape A applies — Shape B and Shape C have no array to pin.


def test_arr_n_pin_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars[0].make selects only docs whose first
    car in country.garage.cars matches."""
    collection = make_new_collection(collection_factory)

    pinned_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota"),
            make_car(make="Honda"),
        ])),
    })
    pinned_miss_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda"),
            make_car(make="Toyota"),
        ])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars[0].make").equal("Toyota"),
    ).objects
    ids = {o.uuid for o in result}
    assert pinned_match_id in ids, "Toyota at cars[0] should match"
    assert pinned_miss_id not in ids, "Toyota at cars[1] must not satisfy cars[0] pin"


# ===========================================================================
# Scenario: arr[N] pin combined with same-element AND
# ===========================================================================
#
# Only Shape A applies.


def test_arr_n_pin_with_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — pinned same-element AND inside country.garage.cars[].
    Both leaves must be satisfied by cars[0]."""
    collection = make_new_collection(collection_factory)

    pinned_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])),
    })
    split_within_pinned_car_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ])),
    })
    match_at_wrong_index_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda", color="red"),
            make_car(make="Toyota", color="red"),
        ])),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.cars[0].make").equal("Toyota"),
        Filter.by_property("country.garage.cars[0].color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert pinned_match_id in ids, "Toyota+red at cars[0] should match"
    assert split_within_pinned_car_id not in ids, (
        "cars[0] satisfies make but not color (blue) — must not match"
    )
    assert match_at_wrong_index_id not in ids, (
        "cars[1] is red Toyota but pin is to cars[0] — must not match"
    )


# ===========================================================================
# Scenario: arr[N] pin combined with OR
# ===========================================================================
#
# Only Shape A applies.


def test_arr_n_pin_with_or_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — pinned OR inside country.garage.cars[]. cars[0] must
    satisfy at least one of the two leaves."""
    collection = make_new_collection(collection_factory)

    pinned_both_leaves_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])),
    })
    pinned_only_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ])),
    })
    pinned_only_color_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda", color="red"),
            make_car(make="Toyota", color="blue"),
        ])),
    })
    pinned_satisfies_neither_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda", color="blue"),
            make_car(make="Toyota", color="red"),
        ])),
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.by_property("country.garage.cars[0].make").equal("Toyota"),
        Filter.by_property("country.garage.cars[0].color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert pinned_both_leaves_id in ids, "cars[0] satisfies both → match"
    assert pinned_only_make_id in ids, "cars[0] satisfies make (Toyota) → match"
    assert pinned_only_color_id in ids, "cars[0] satisfies color (red) → match"
    assert pinned_satisfies_neither_id not in ids, (
        "cars[0] satisfies neither; cars[1] satisfies both but pin is to cars[0]"
    )


# ===========================================================================
# Scenario: OR of correlated ANDs
# ===========================================================================
#
# `(make=Toyota AND color=red) OR (make=Honda AND color=blue)` — each AND
# requires a SINGLE car to satisfy both leaves. Cross-group splits (Toyota+blue
# AND Honda+red) must not match. Shape A exercises real correlation; Shape B
# and Shape C are degenerate (one car per doc) so the cross-group split case
# disappears, but the OR-of-AND dispatch path is still exercised.


def test_or_of_correlated_ands_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — OR of correlated ANDs under country.garage.cars[]."""
    collection = make_new_collection(collection_factory)

    group1_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
        ])),
    })
    group2_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda", color="blue"),
        ])),
    })
    both_groups_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])),
    })
    cross_group_split_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ])),
    })
    unrelated_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Ford", color="green"),
        ])),
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("country.garage.cars.make").equal("Toyota"),
            Filter.by_property("country.garage.cars.color").equal("red"),
        ]),
        Filter.all_of([
            Filter.by_property("country.garage.cars.make").equal("Honda"),
            Filter.by_property("country.garage.cars.color").equal("blue"),
        ]),
    ])).objects
    ids = {o.uuid for o in result}
    assert group1_match_id in ids, "single red Toyota satisfies group 1"
    assert group2_match_id in ids, "single blue Honda satisfies group 2"
    assert both_groups_match_id in ids, "two cars, one per group → match"
    assert cross_group_split_id not in ids, (
        "leaves split across groups (Toyota+blue, Honda+red) — no single car "
        "satisfies either AND group, so OR must not match"
    )
    assert unrelated_id not in ids, "Ford+green satisfies neither group"


def test_or_of_correlated_ands_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — OR of ANDs on the single-OBJECT car. Degenerate (one car);
    the cross-group-split case can't exist, so the docs that would fail the
    correlation in Shape A simply match-neither-group here."""
    collection = make_new_collection(collection_factory)

    group1_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Toyota", color="red"),
        )),
    })
    group2_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Honda", color="blue"),
        )),
    })
    cross_group_make_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Toyota", color="blue"),
        )),
    })
    cross_group_color_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Honda", color="red"),
        )),
    })
    unrelated_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Ford", color="green"),
        )),
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("country.garage.car.make").equal("Toyota"),
            Filter.by_property("country.garage.car.color").equal("red"),
        ]),
        Filter.all_of([
            Filter.by_property("country.garage.car.make").equal("Honda"),
            Filter.by_property("country.garage.car.color").equal("blue"),
        ]),
    ])).objects
    ids = {o.uuid for o in result}
    assert group1_match_id in ids, "red Toyota matches group 1"
    assert group2_match_id in ids, "blue Honda matches group 2"
    assert cross_group_make_only_id not in ids, "blue Toyota matches neither group"
    assert cross_group_color_only_id not in ids, "red Honda matches neither group"
    assert unrelated_id not in ids, "green Ford matches neither group"


def test_or_of_correlated_ands_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — OR of ANDs on the single-OBJECT car at root. Degenerate (one car)."""
    collection = make_new_collection(collection_factory)

    group1_match_id = collection.data.insert({"car": make_car(make="Toyota", color="red")})
    group2_match_id = collection.data.insert({"car": make_car(make="Honda", color="blue")})
    cross_group_make_only_id = collection.data.insert({"car": make_car(make="Toyota", color="blue")})
    cross_group_color_only_id = collection.data.insert({"car": make_car(make="Honda", color="red")})
    unrelated_id = collection.data.insert({"car": make_car(make="Ford", color="green")})

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("car.make").equal("Toyota"),
            Filter.by_property("car.color").equal("red"),
        ]),
        Filter.all_of([
            Filter.by_property("car.make").equal("Honda"),
            Filter.by_property("car.color").equal("blue"),
        ]),
    ])).objects
    ids = {o.uuid for o in result}
    assert group1_match_id in ids
    assert group2_match_id in ids
    assert cross_group_make_only_id not in ids
    assert cross_group_color_only_id not in ids
    assert unrelated_id not in ids


# ===========================================================================
# Scenario: IsNull on a nested leaf — existential per-element semantics
# ===========================================================================
#
# Shape A still has per-element existential (∃ car with/without make). Shape B
# and Shape C reduce to flat IS NULL: one car per doc, so the leaf is either
# present or absent.


def test_is_null_on_leaf_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.make IS NULL / IS NOT NULL with
    existential per-car semantics."""
    collection = make_new_collection(collection_factory)

    all_have_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota"),
            make_car(make="Honda"),
        ])),
    })
    some_missing_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota"),
            omit_fields(make_car(), "make"),
        ])),
    })
    none_have_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            omit_fields(make_car(), "make"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    is_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.make").is_none(True),
    ).objects}
    assert all_have_make_id not in is_null_ids, "all cars have make → IS NULL must not match"
    assert some_missing_make_id in is_null_ids, "∃ car without make → IS NULL matches"
    assert none_have_make_id in is_null_ids, "all cars lack make → IS NULL matches"
    assert empty_cars_id not in is_null_ids, "empty cars[] is vacuous → IS NULL must not match"

    is_not_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.make").is_none(False),
    ).objects}
    assert all_have_make_id in is_not_null_ids, "all cars have make → IS NOT NULL matches"
    assert some_missing_make_id in is_not_null_ids, "∃ car with make → IS NOT NULL matches"
    assert none_have_make_id not in is_not_null_ids, "no car has make → IS NOT NULL must not match"
    assert empty_cars_id not in is_not_null_ids, "empty cars[] is vacuous → IS NOT NULL must not match"


def test_is_null_on_leaf_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.make IS NULL / IS NOT NULL on a single
    OBJECT. Reduces to flat semantics (one value, present or absent)."""
    collection = make_new_collection(collection_factory)

    has_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Toyota"))),
    })
    missing_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=omit_fields(make_car(), "make"))),
    })

    is_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.make").is_none(True),
    ).objects}
    assert has_make_id not in is_null_ids, "car has make → IS NULL must not match"
    assert missing_make_id in is_null_ids, "car lacks make → IS NULL matches"

    is_not_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.make").is_none(False),
    ).objects}
    assert has_make_id in is_not_null_ids, "car has make → IS NOT NULL matches"
    assert missing_make_id not in is_not_null_ids, "car lacks make → IS NOT NULL must not match"


def test_is_null_on_leaf_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.make IS NULL / IS NOT NULL on a single OBJECT at root."""
    collection = make_new_collection(collection_factory)

    has_make_id = collection.data.insert({"car": make_car(make="Toyota")})
    missing_make_id = collection.data.insert({"car": omit_fields(make_car(), "make")})

    is_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("car.make").is_none(True),
    ).objects}
    assert has_make_id not in is_null_ids, "car has make → IS NULL must not match"
    assert missing_make_id in is_null_ids, "car lacks make → IS NULL matches"

    is_not_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("car.make").is_none(False),
    ).objects}
    assert has_make_id in is_not_null_ids, "car has make → IS NOT NULL matches"
    assert missing_make_id not in is_not_null_ids, "car lacks make → IS NOT NULL must not match"


# ===========================================================================
# Scenario: ContainsAll on a scalar-array leaf — same-element semantics
# ===========================================================================
#
# Shape A: real same-element semantics — both listed values must live on the
# same car's model_codes. Shape B/C: single car per doc, so the same-element
# rule trivially holds (no split possible across cars).


def test_contains_all_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.model_codes contains_all requires one
    car's model_codes to hold all listed values."""
    collection = make_new_collection(collection_factory)

    one_car_has_both_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020", "HYB-PRO"]),
        ])),
    })
    one_car_has_one_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"]),
        ])),
    })
    split_across_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"]),
            make_car(model_codes=["HYB-PRO"]),
        ])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.model_codes")
        .contains_all(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert one_car_has_both_id in ids, "single car with both values → match"
    assert one_car_has_one_id not in ids, "missing one value → must not match"
    assert split_across_cars_id not in ids, (
        "values split across two cars → must not match (same-element rule)"
    )


def test_contains_all_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.model_codes contains_all on the single
    car's scalar-array leaf."""
    collection = make_new_collection(collection_factory)

    has_both_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020", "HYB-PRO"]),
        )),
    })
    has_one_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020"]),
        )),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.model_codes")
        .contains_all(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert has_both_id in ids, "car holds both values → match"
    assert has_one_id not in ids, "car missing one value → no match"


def test_contains_all_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.model_codes contains_all at root."""
    collection = make_new_collection(collection_factory)

    has_both_id = collection.data.insert({"car": make_car(model_codes=["CAM-2020", "HYB-PRO"])})
    has_one_id = collection.data.insert({"car": make_car(model_codes=["CAM-2020"])})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("car.model_codes").contains_all(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert has_both_id in ids, "car holds both values → match"
    assert has_one_id not in ids, "car missing one value → no match"


# ===========================================================================
# Scenario: ContainsAny on a scalar-array leaf — natural existential
# ===========================================================================


def test_contains_any_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.model_codes contains_any: ∃ car has
    any listed value (cross-car distribution still matches)."""
    collection = make_new_collection(collection_factory)

    one_car_has_one_listed_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020", "OUTSIDE-A"]),
        ])),
    })
    split_listed_values_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"]),
            make_car(model_codes=["HYB-PRO"]),
        ])),
    })
    all_values_outside_list_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["OUTSIDE-A"]),
            make_car(model_codes=["OUTSIDE-B"]),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.model_codes")
        .contains_any(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert one_car_has_one_listed_id in ids, "car owns CAM-2020 → match"
    assert split_listed_values_id in ids, (
        "values split across cars (car1=CAM-2020, car2=HYB-PRO) → match "
        "(any car with any listed value is enough)"
    )
    assert all_values_outside_list_id not in ids, "no car owns any listed value → no match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_contains_any_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.model_codes contains_any."""
    collection = make_new_collection(collection_factory)

    has_listed_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020", "OUTSIDE-A"]),
        )),
    })
    all_outside_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["OUTSIDE-A", "OUTSIDE-B"]),
        )),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.model_codes")
        .contains_any(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert has_listed_id in ids, "car owns CAM-2020 → match"
    assert all_outside_id not in ids, "no listed value → no match"


def test_contains_any_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.model_codes contains_any at root."""
    collection = make_new_collection(collection_factory)

    has_listed_id = collection.data.insert({"car": make_car(model_codes=["CAM-2020", "OUTSIDE-A"])})
    all_outside_id = collection.data.insert({"car": make_car(model_codes=["OUTSIDE-A", "OUTSIDE-B"])})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("car.model_codes").contains_any(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert has_listed_id in ids, "car owns CAM-2020 → match"
    assert all_outside_id not in ids, "no listed value → no match"


# ===========================================================================
# Scenario: ContainsNone on a scalar-array leaf — per-tag-element existential
# ===========================================================================
#
# Shape A still exercises per-tag-element existential across multiple cars.
# Shape B/C have one car per doc, so the "mixed inside/outside on different
# cars" subcase collapses to "mixed inside/outside on the same car".


def test_contains_none_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.model_codes contains_none: ∃ tag outside
    the list anywhere across all cars."""
    collection = make_new_collection(collection_factory)

    all_values_in_list_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020", "HYB-PRO"]),
        ])),
    })
    all_values_in_list_split_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"]),
            make_car(model_codes=["HYB-PRO"]),
        ])),
    })
    has_outside_value_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["OUTSIDE-X"]),
        ])),
    })
    mixed_inside_and_outside_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"]),
            make_car(model_codes=["OUTSIDE-X"]),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.model_codes")
        .contains_none(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert all_values_in_list_id not in ids, "every tag is in the list → no match"
    assert all_values_in_list_split_id not in ids, (
        "every tag is in the list (split across cars) → no match"
    )
    assert has_outside_value_id in ids, "∃ tag outside the list → match"
    assert mixed_inside_and_outside_id in ids, (
        "∃ OUTSIDE-X tag → match, even though CAM-2020 is also present "
        "(per-tag-element existential, not universal)"
    )
    assert empty_cars_id not in ids, (
        "empty cars[] is vacuous — no model_code positions to evaluate → no match"
    )


def test_contains_none_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.model_codes contains_none with
    per-tag-element existential semantics on the single car's array."""
    collection = make_new_collection(collection_factory)

    all_in_list_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020", "HYB-PRO"]),
        )),
    })
    has_outside_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["OUTSIDE-X"]),
        )),
    })
    mixed_inside_and_outside_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020", "OUTSIDE-X"]),
        )),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.model_codes")
        .contains_none(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert all_in_list_id not in ids, "every tag in the list → no match"
    assert has_outside_id in ids, "∃ tag outside the list → match"
    assert mixed_inside_and_outside_id in ids, (
        "∃ OUTSIDE-X tag → match, even though CAM-2020 is also present"
    )


def test_contains_none_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.model_codes contains_none at root."""
    collection = make_new_collection(collection_factory)

    all_in_list_id = collection.data.insert({"car": make_car(model_codes=["CAM-2020", "HYB-PRO"])})
    has_outside_id = collection.data.insert({"car": make_car(model_codes=["OUTSIDE-X"])})
    mixed_inside_and_outside_id = collection.data.insert({
        "car": make_car(model_codes=["CAM-2020", "OUTSIDE-X"]),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("car.model_codes").contains_none(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert all_in_list_id not in ids, "every tag in the list → no match"
    assert has_outside_id in ids, "∃ tag outside the list → match"
    assert mixed_inside_and_outside_id in ids, (
        "∃ OUTSIDE-X tag → match, even though CAM-2020 is also present"
    )


# ===========================================================================
# Scenario: multi-token Equal on a tokenized text leaf
# ===========================================================================
#
# `cars.model = "Camry Hybrid"` tokenizes to ["camry", "hybrid"]. Both tokens
# must hit the same car's `model` field. Shape A exercises the cross-car
# split; Shape B/C have one car so the same-element rule is trivially held.


def test_multi_token_equal_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.model = 'Camry Hybrid' requires both
    tokens on the same car."""
    collection = make_new_collection(collection_factory)

    both_tokens_one_car_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model="Camry Hybrid"),
        ])),
    })
    single_token_one_car_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model="Camry"),
        ])),
    })
    tokens_split_across_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model="Camry"),
            make_car(model="Hybrid"),
        ])),
    })
    unrelated_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model="Civic LX"),
        ])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.model").equal("Camry Hybrid"),
    ).objects
    ids = {o.uuid for o in result}
    assert both_tokens_one_car_id in ids, "both tokens on the same car → match"
    assert single_token_one_car_id not in ids, "only 'camry' present → no match"
    assert tokens_split_across_cars_id not in ids, (
        "'camry' on car 0 and 'hybrid' on car 1 — same-element rule on tokens"
    )
    assert unrelated_id not in ids, "neither token present → no match"


def test_multi_token_equal_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.model = 'Camry Hybrid' on the single car."""
    collection = make_new_collection(collection_factory)

    both_tokens_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(model="Camry Hybrid"))),
    })
    single_token_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(model="Camry"))),
    })
    unrelated_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(model="Civic LX"))),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.model").equal("Camry Hybrid"),
    ).objects
    ids = {o.uuid for o in result}
    assert both_tokens_id in ids, "both tokens on the car → match"
    assert single_token_id not in ids, "only 'camry' present → no match"
    assert unrelated_id not in ids, "neither token present → no match"


def test_multi_token_equal_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.model = 'Camry Hybrid' at root."""
    collection = make_new_collection(collection_factory)

    both_tokens_id = collection.data.insert({"car": make_car(model="Camry Hybrid")})
    single_token_id = collection.data.insert({"car": make_car(model="Camry")})
    unrelated_id = collection.data.insert({"car": make_car(model="Civic LX")})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("car.model").equal("Camry Hybrid"),
    ).objects
    ids = {o.uuid for o in result}
    assert both_tokens_id in ids, "both tokens on the car → match"
    assert single_token_id not in ids, "only 'camry' present → no match"
    assert unrelated_id not in ids, "neither token present → no match"


# ===========================================================================
# Scenario: NOT of a correlated AND
# ===========================================================================
#
# Filter: `NOT (cars.make=Toyota AND cars.color=red)` — existential per-car:
# the doc matches when ∃ a car that does NOT satisfy the inner AND.


def test_not_of_correlated_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — NOT of a correlated AND inside country.garage.cars[]."""
    collection = make_new_collection(collection_factory)

    only_satisfying_car_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
        ])),
    })
    single_violating_car_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda", color="blue"),
        ])),
    })
    mixed_satisfying_and_violating_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])),
    })
    all_satisfying_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Toyota", color="red"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("country.garage.cars.make").equal("Toyota"),
        Filter.by_property("country.garage.cars.color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert only_satisfying_car_id not in ids, "every car satisfies → no match"
    assert single_violating_car_id in ids, "Honda+blue violates → match"
    assert mixed_satisfying_and_violating_id in ids, "Honda violates → match"
    assert all_satisfying_cars_id not in ids, "every car satisfies → no match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_not_of_correlated_and_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — NOT(car.make=Toyota AND car.color=red) on single OBJECT.

    Degenerate (one car) — smoke test for NOT-of-AND on a single-OBJECT subtree."""
    collection = make_new_collection(collection_factory)

    car_satisfies_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Toyota", color="red"),
        )),
    })
    car_violates_color_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Toyota", color="blue"),
        )),
    })
    car_violates_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Honda", color="red"),
        )),
    })
    car_violates_both_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(make="Honda", color="blue"),
        )),
    })

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("country.garage.car.make").equal("Toyota"),
        Filter.by_property("country.garage.car.color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert car_satisfies_id not in ids, "car satisfies inner AND → NOT excludes"
    assert car_violates_color_id in ids, "color=blue violates the AND → NOT matches"
    assert car_violates_make_id in ids, "make=Honda violates the AND → NOT matches"
    assert car_violates_both_id in ids, "both leaves violate → NOT matches"


def test_not_of_correlated_and_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — NOT(car.make=Toyota AND car.color=red) at root."""
    collection = make_new_collection(collection_factory)

    car_satisfies_id = collection.data.insert({"car": make_car(make="Toyota", color="red")})
    car_violates_color_id = collection.data.insert({"car": make_car(make="Toyota", color="blue")})
    car_violates_make_id = collection.data.insert({"car": make_car(make="Honda", color="red")})
    car_violates_both_id = collection.data.insert({"car": make_car(make="Honda", color="blue")})

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("car.make").equal("Toyota"),
        Filter.by_property("car.color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert car_satisfies_id not in ids
    assert car_violates_color_id in ids
    assert car_violates_make_id in ids
    assert car_violates_both_id in ids


# ===========================================================================
# Scenario: IsNull combined with a value leaf inside a correlated AND
# ===========================================================================
#
# Filter: `cars.color=red AND cars.make IS NULL` — same-element rule applies
# to IsNull-vs-value AND too.


def test_is_null_in_correlated_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — IsNull + value leaf, correlated per car under
    country.garage.cars[]."""
    collection = make_new_collection(collection_factory)

    match_same_car_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            omit_fields(make_car(color="red"), "make"),
        ])),
    })
    only_color_satisfied_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(color="red"),
        ])),
    })
    only_is_null_satisfied_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            omit_fields(make_car(color="blue"), "make"),
        ])),
    })
    split_across_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(color="red"),
            omit_fields(make_car(color="blue"), "make"),
        ])),
    })
    match_with_distractor_car_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            omit_fields(make_car(color="red"), "make"),
            make_car(color="blue"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.cars.color").equal("red"),
        Filter.by_property("country.garage.cars.make").is_none(True),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_same_car_id in ids, "single car has color=red and no make → match"
    assert only_color_satisfied_id not in ids, "make=Toyota not null → no match"
    assert only_is_null_satisfied_id not in ids, "color=blue doesn't match → no match"
    assert split_across_cars_id not in ids, (
        "color on one car, make-null on a different car — same-element rule fails"
    )
    assert match_with_distractor_car_id in ids, "first car satisfies both → match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_is_null_in_correlated_and_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — IsNull + value leaf on single-OBJECT car."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=omit_fields(make_car(color="red"), "make"),
        )),
    })
    only_color_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(color="red"),
        )),
    })
    only_is_null_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=omit_fields(make_car(color="blue"), "make"),
        )),
    })
    mismatch_both_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(color="blue"),
        )),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.car.color").equal("red"),
        Filter.by_property("country.garage.car.make").is_none(True),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids, "color=red and make is null → match"
    assert only_color_id not in ids, "color matches but make not null → no match"
    assert only_is_null_id not in ids, "make null but color doesn't match → no match"
    assert mismatch_both_id not in ids, "color doesn't match and make not null → no match"


def test_is_null_in_correlated_and_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — IsNull + value leaf on single-OBJECT car at root."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({"car": omit_fields(make_car(color="red"), "make")})
    only_color_id = collection.data.insert({"car": make_car(color="red")})
    only_is_null_id = collection.data.insert({"car": omit_fields(make_car(color="blue"), "make")})
    mismatch_both_id = collection.data.insert({"car": make_car(color="blue")})

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("car.color").equal("red"),
        Filter.by_property("car.make").is_none(True),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert only_color_id not in ids
    assert only_is_null_id not in ids
    assert mismatch_both_id not in ids


# ===========================================================================
# Scenario: NOT of a pinned correlated AND
# ===========================================================================
#
# Only Shape A applies — Shape B/C have no array to pin.
#
# Filter: `NOT (cars[0].make=Toyota AND cars[0].color=red)` — existential
# over the cars[0] position. Carries a known pin-lift gap (TODO below).


def test_not_of_pinned_correlated_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — NOT of a pinned correlated AND inside country.garage.cars[]."""
    collection = make_new_collection(collection_factory)

    pinned_satisfies_and_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
        ])),
    })
    pinned_only_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="blue"),
        ])),
    })
    pinned_only_color_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda", color="red"),
        ])),
    })
    pinned_neither_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda", color="blue"),
        ])),
    })
    match_at_wrong_index_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda", color="blue"),
            make_car(make="Toyota", color="red"),
        ])),
    })
    pinned_satisfies_with_distractor_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("country.garage.cars[0].make").equal("Toyota"),
        Filter.by_property("country.garage.cars[0].color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert pinned_satisfies_and_id not in ids, "cars[0] satisfies the inner AND → NOT excludes"
    assert pinned_only_make_id in ids, "cars[0] only matches make → NOT matches"
    assert pinned_only_color_id in ids, "cars[0] only matches color → NOT matches"
    assert pinned_neither_id in ids, "cars[0] matches neither → NOT matches"
    assert match_at_wrong_index_id in ids, (
        "cars[0] is Honda+blue (violates); cars[1] satisfies but pin restricts to [0] → match"
    )
    # TODO aliszka:nested_filtering: flip to `not in ids` once
    # NOT-of-compound lifts the arr[N] pin into the inverted universe.
    # Same shape as L0 — cars[0] satisfies the inner AND so the doc must
    # not match under the design contract, but the current impl ignores
    # the pin and the extra Honda+blue car at cars[1] flips the doc in.
    assert pinned_satisfies_with_distractor_id in ids, (
        "current impl: extra car at cars[1] leaks the doc through NOT-of-compound "
        "with arr[N] pin. Should flip to NOT match after pin-lift fix."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


# ===========================================================================
# Scenario: Pinned IsNull on a leaf inside a pinned position
# ===========================================================================
#
# Only Shape A applies.
#
# Filter: `cars[1].make IS NULL` — strict existential currently drops docs
# lacking a cars[1] (TODO below).


def test_pinned_is_null_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars[1].make IS NULL pinned-IsNull."""
    collection = make_new_collection(collection_factory)

    cars1_present_with_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota"),
            make_car(make="Honda"),
        ])),
    })
    cars1_present_no_make_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota"),
            omit_fields(make_car(), "make"),
        ])),
    })
    cars1_missing_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars[1].make").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    assert cars1_present_with_make_id not in ids, (
        "cars[1] present with make → no match (strict + recovery agree)"
    )
    assert cars1_present_no_make_id in ids, (
        "cars[1] present, make is null → match (strict + recovery agree)"
    )
    # TODO aliszka:nested_filtering: flip to `in ids` once the pinned-
    # IsNull recovery lands. Under user-intent semantics a doc lacking
    # cars[1] should match (the position is "null"). The strict
    # existential rule currently requires the position to exist and
    # drops vacuously.
    assert cars1_missing_id not in ids, (
        "current impl (strict existential): cars[1] doesn't exist → "
        "vacuous drop. Should flip to `in ids` after the pinned-IsNull "
        "recovery brings in user-intent semantics."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


# ===========================================================================
# Scenario: IsNull on an object[] sub-property
# ===========================================================================
#
# Filter: `<cars-path>.tires IS NULL` — Shape A carries the known encoding
# gap for IsNull on array-typed sub-properties. Shape B/C have the tires[]
# array under a single-OBJECT car; whether the gap manifests there too is
# observed empirically.


def test_is_null_on_object_array_subprop_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.tires IS NULL."""
    collection = make_new_collection(collection_factory)

    car_with_tires_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(tires=[{"width": 215, "brand": "Michelin"}]),
        ])),
    })
    car_no_tires_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            omit_fields(make_car(), "tires"),
        ])),
    })
    car_empty_tires_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(tires=[]),
        ])),
    })
    mixed_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(tires=[{"width": 215, "brand": "Michelin"}]),
            omit_fields(make_car(), "tires"),
        ])),
    })
    car_with_multiple_tires_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(tires=[
                {"width": 215, "brand": "Michelin"},
                {"width": 215, "brand": "Michelin"},
            ]),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.tires").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    # TODO aliszka:nested_filtering: flip to `not in ids` once the
    # multi-element / multi-leaf encoding gap for IsNull on array-typed
    # sub-properties is closed. Under the design contract this car has
    # tires → tires not null → no match.
    assert car_with_tires_id in ids, (
        "current impl: encoding-gap over-inclusion. Should flip to `not in ids` after fix."
    )
    assert car_no_tires_id in ids, "car has no tires → ∃ car with null tires → match"
    assert car_empty_tires_id in ids, "car has empty tires → ∃ car with null tires → match"
    assert mixed_cars_id in ids, "second car has no tires → match"
    # TODO aliszka:nested_filtering: flip to `not in ids` once the gap
    # is closed. Same as car_with_tires_id.
    assert car_with_multiple_tires_id in ids, (
        "current impl: encoding-gap over-inclusion. Should flip after fix."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_is_null_on_object_array_subprop_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.tires IS NULL on the single car's tires[].

    The encoding gap (multi-leaf-per-element variant) manifests here too:
    a car with tires still flags as IS NULL match. Confirmed empirically;
    TODO markers below mirror the Shape A / L0 convention."""
    collection = make_new_collection(collection_factory)

    car_with_tires_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(tires=[{"width": 215, "brand": "Michelin"}]),
        )),
    })
    car_no_tires_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=omit_fields(make_car(), "tires"),
        )),
    })
    car_empty_tires_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(tires=[]),
        )),
    })
    car_with_multiple_tires_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(tires=[
                {"width": 215, "brand": "Michelin"},
                {"width": 215, "brand": "Michelin"},
            ]),
        )),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.tires").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    # TODO aliszka:nested_filtering: flip to `not in ids` once the
    # multi-element / multi-leaf encoding gap for IsNull on array-typed
    # sub-properties is closed. The gap manifests on single-OBJECT
    # parents too (this test confirms): a car with tires still flags
    # as IS NULL match.
    assert car_with_tires_id in ids, (
        "current impl: encoding-gap over-inclusion. Should flip after fix."
    )
    assert car_no_tires_id in ids, "car has no tires → tires null → match"
    assert car_empty_tires_id in ids, "car has empty tires → tires null → match"
    # TODO aliszka:nested_filtering: flip to `not in ids` once the gap
    # is closed. Same as car_with_tires_id.
    assert car_with_multiple_tires_id in ids, (
        "current impl: encoding-gap over-inclusion. Should flip after fix."
    )


def test_is_null_on_object_array_subprop_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.tires IS NULL on the root single car's tires[].

    Same encoding-gap manifestation as Shape B."""
    collection = make_new_collection(collection_factory)

    car_with_tires_id = collection.data.insert({
        "car": make_car(tires=[{"width": 215, "brand": "Michelin"}]),
    })
    car_no_tires_id = collection.data.insert({"car": omit_fields(make_car(), "tires")})
    car_empty_tires_id = collection.data.insert({"car": make_car(tires=[])})
    car_with_multiple_tires_id = collection.data.insert({
        "car": make_car(tires=[
            {"width": 215, "brand": "Michelin"},
            {"width": 215, "brand": "Michelin"},
        ]),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("car.tires").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    # TODO aliszka:nested_filtering: flip to `not in ids` once the
    # multi-element / multi-leaf encoding gap for IsNull on array-typed
    # sub-properties is closed. Encoding gap manifests at the root
    # single-OBJECT layout too.
    assert car_with_tires_id in ids, (
        "current impl: encoding-gap over-inclusion. Should flip after fix."
    )
    assert car_no_tires_id in ids, "car has no tires → tires null → match"
    assert car_empty_tires_id in ids, "car has empty tires → tires null → match"
    # TODO aliszka:nested_filtering: flip to `not in ids` once the gap
    # is closed. Same as car_with_tires_id.
    assert car_with_multiple_tires_id in ids, (
        "current impl: encoding-gap over-inclusion. Should flip after fix."
    )


# ===========================================================================
# Scenario: invalid filters surface a server error
# ===========================================================================
#
# Filters that violate the schema or filter syntax should produce a clear
# error from the server. One test covers all common shapes against the new
# collection.


def test_invalid_filter_returns_server_error(collection_factory: CollectionFactory) -> None:
    """Each invalid filter shape produces a WeaviateQueryError with a
    message that explains what's wrong, against the object-intermediates
    collection."""
    collection = make_new_collection(collection_factory)
    collection.data.insert({"car": make_car()})

    # Non-existent root property.
    with pytest.raises(WeaviateQueryError, match="no such prop"):
        collection.query.fetch_objects(
            filters=Filter.by_property("nonexistent_root").equal("X"),
        )

    # Filter on a nested-object property without a sub-property — must use
    # dot notation. `car` is OBJECT.
    with pytest.raises(WeaviateQueryError, match="dot notation"):
        collection.query.fetch_objects(
            filters=Filter.by_property("car").equal("X"),
        )

    # Non-existent sub-property along an otherwise-valid nested path.
    with pytest.raises(WeaviateQueryError, match="sub-property .* not found"):
        collection.query.fetch_objects(
            filters=Filter.by_property("car.nonexistent").equal("X"),
        )

    # arr[N] indexing applied to a non-array sub-property (country.name is
    # text, not text[]).
    with pytest.raises(WeaviateQueryError, match="indexing requires an array type"):
        collection.query.fetch_objects(
            filters=Filter.by_property("country.name[0]").equal("X"),
        )

    # Filter terminates on a sub-property that is itself an object[],
    # not a primitive leaf — car.tires is object[].
    with pytest.raises(WeaviateQueryError, match="filtering on object types is not supported"):
        collection.query.fetch_objects(
            filters=Filter.by_property("car.tires").equal("X"),
        )

    # Path tries to descend past a scalar leaf — car.make is text but the
    # filter tries to navigate further.
    with pytest.raises(WeaviateQueryError, match=r"must be object or object\[\]"):
        collection.query.fetch_objects(
            filters=Filter.by_property("car.make.foo").equal("X"),
        )

    # len(...) length filter on a nested property root.
    with pytest.raises(WeaviateQueryError, match="property length filtering is not supported"):
        collection.query.fetch_objects(
            filters=Filter.by_property("car", length=True).equal(5),
        )


# ===========================================================================
# Scenario: comparison operators (gt / lt / gte / lte / like) on nested leaves
# ===========================================================================


def test_comparison_operators_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — comparison operators on country.garage.cars leaves."""
    collection = make_new_collection(collection_factory)

    toyota_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[make_car()])),
    })
    honda_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[_honda_2015()])),
    })
    mixed_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[make_car(), _honda_2015()])),
    })

    _assert_comparison_operator_sweep_selects(
        collection,
        cars_path="country.garage.cars",
        high_only_ids=[toyota_id],
        low_only_ids=[honda_id],
        mixed_ids=[mixed_id],
    )


def test_comparison_operators_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — comparison operators on country.garage.car (single OBJECT)."""
    collection = make_new_collection(collection_factory)

    toyota_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car())),
    })
    honda_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=_honda_2015())),
    })

    _assert_comparison_operator_sweep_selects(
        collection,
        cars_path="country.garage.car",
        high_only_ids=[toyota_id],
        low_only_ids=[honda_id],
    )


def test_comparison_operators_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — comparison operators on car (single OBJECT at root)."""
    collection = make_new_collection(collection_factory)

    toyota_id = collection.data.insert({"car": make_car()})
    honda_id = collection.data.insert({"car": _honda_2015()})

    _assert_comparison_operator_sweep_selects(
        collection,
        cars_path="car",
        high_only_ids=[toyota_id],
        low_only_ids=[honda_id],
    )


# ===========================================================================
# Scenario: ContainsAll combined with a scalar leaf inside a correlated AND
# ===========================================================================
#
# Filter: `cars.model_codes contains_all [a, b] AND cars.color = red`


def test_contains_all_with_equal_in_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — ContainsAll on model_codes + Equal on color, correlated per car."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020", "HYB-PRO"], color="red"),
        ])),
    })
    contains_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020", "HYB-PRO"], color="blue"),
        ])),
    })
    equal_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"], color="red"),
        ])),
    })
    split_across_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020", "HYB-PRO"], color="blue"),
            make_car(model_codes=["CAM-2020"], color="red"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.cars.model_codes").contains_all(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("country.garage.cars.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids, "single car has both codes and color=red → match"
    assert contains_only_id not in ids, "car has both codes but color=blue → no match"
    assert equal_only_id not in ids, "car has color=red but only one listed code → no match"
    assert split_across_cars_id not in ids, (
        "one car has codes, another has color — same-element rule fails → no match"
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_contains_all_with_equal_in_and_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — ContainsAll + Equal on single-OBJECT car."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020", "HYB-PRO"], color="red"),
        )),
    })
    contains_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020", "HYB-PRO"], color="blue"),
        )),
    })
    equal_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020"], color="red"),
        )),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.car.model_codes").contains_all(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("country.garage.car.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert contains_only_id not in ids, "color mismatch → no match"
    assert equal_only_id not in ids, "missing one code → no match"


def test_contains_all_with_equal_in_and_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — ContainsAll + Equal on single-OBJECT car at root."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "car": make_car(model_codes=["CAM-2020", "HYB-PRO"], color="red"),
    })
    contains_only_id = collection.data.insert({
        "car": make_car(model_codes=["CAM-2020", "HYB-PRO"], color="blue"),
    })
    equal_only_id = collection.data.insert({
        "car": make_car(model_codes=["CAM-2020"], color="red"),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("car.model_codes").contains_all(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("car.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert contains_only_id not in ids
    assert equal_only_id not in ids


# ===========================================================================
# Scenario: ContainsAny combined with a scalar leaf inside a correlated AND
# ===========================================================================


def test_contains_any_with_equal_in_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — ContainsAny + Equal correlated per car."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"], color="red"),
        ])),
    })
    contains_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"], color="blue"),
        ])),
    })
    equal_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["OUTSIDE"], color="red"),
        ])),
    })
    split_across_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"], color="blue"),
            make_car(model_codes=["OUTSIDE"], color="red"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.cars.model_codes").contains_any(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("country.garage.cars.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids, "single car has listed code and color=red → match"
    assert contains_only_id not in ids, "color wrong → no match"
    assert equal_only_id not in ids, "no listed code → no match"
    assert split_across_cars_id not in ids, "leaves split across cars → no match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_contains_any_with_equal_in_and_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — ContainsAny + Equal on single-OBJECT car."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020"], color="red"),
        )),
    })
    contains_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020"], color="blue"),
        )),
    })
    equal_only_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["OUTSIDE"], color="red"),
        )),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.car.model_codes").contains_any(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("country.garage.car.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert contains_only_id not in ids
    assert equal_only_id not in ids


def test_contains_any_with_equal_in_and_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — ContainsAny + Equal on single-OBJECT car at root."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({"car": make_car(model_codes=["CAM-2020"], color="red")})
    contains_only_id = collection.data.insert({"car": make_car(model_codes=["CAM-2020"], color="blue")})
    equal_only_id = collection.data.insert({"car": make_car(model_codes=["OUTSIDE"], color="red")})

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("car.model_codes").contains_any(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("car.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert contains_only_id not in ids
    assert equal_only_id not in ids


# ===========================================================================
# Scenario: ContainsNone combined with a scalar leaf inside a correlated AND
# ===========================================================================


def test_contains_none_with_equal_in_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — ContainsNone + Equal correlated per car."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["OUTSIDE"], color="red"),
        ])),
    })
    outside_tag_but_wrong_color_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["OUTSIDE"], color="blue"),
        ])),
    })
    all_in_list_but_color_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["CAM-2020"], color="red"),
        ])),
    })
    split_across_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model_codes=["OUTSIDE"], color="blue"),
            make_car(model_codes=["CAM-2020"], color="red"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.cars.model_codes").contains_none(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("country.garage.cars.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids, "single car has outside tag and color=red → match"
    assert outside_tag_but_wrong_color_id not in ids, "color wrong → no match"
    assert all_in_list_but_color_match_id not in ids, "no outside tag → no match"
    assert split_across_cars_id not in ids, (
        "outside tag on one car, color on another — same-element fails → no match"
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_contains_none_with_equal_in_and_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — ContainsNone + Equal on single-OBJECT car."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["OUTSIDE"], color="red"),
        )),
    })
    outside_tag_but_wrong_color_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["OUTSIDE"], color="blue"),
        )),
    })
    all_in_list_but_color_match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(model_codes=["CAM-2020"], color="red"),
        )),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garage.car.model_codes").contains_none(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("country.garage.car.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert outside_tag_but_wrong_color_id not in ids
    assert all_in_list_but_color_match_id not in ids


def test_contains_none_with_equal_in_and_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — ContainsNone + Equal on single-OBJECT car at root."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({"car": make_car(model_codes=["OUTSIDE"], color="red")})
    outside_tag_but_wrong_color_id = collection.data.insert({"car": make_car(model_codes=["OUTSIDE"], color="blue")})
    all_in_list_but_color_match_id = collection.data.insert({"car": make_car(model_codes=["CAM-2020"], color="red")})

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("car.model_codes").contains_none(["CAM-2020", "HYB-PRO"]),
        Filter.by_property("car.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert outside_tag_but_wrong_color_id not in ids
    assert all_in_list_but_color_match_id not in ids


# ===========================================================================
# Scenario: mixed flat + nested predicates inside a correlated AND
# ===========================================================================
#
# Filter: `category = "premium" AND <cars-path>.make = "Toyota"`


def test_flat_and_nested_in_and_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — category (flat) + country.garage.cars.make (nested) in AND."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Toyota")])),
    })
    flat_only_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Honda")])),
    })
    nested_only_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Toyota")])),
    })
    neither_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Honda")])),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("category").equal("premium"),
        Filter.by_property("country.garage.cars.make").equal("Toyota"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert flat_only_id not in ids
    assert nested_only_id not in ids
    assert neither_id not in ids


def test_flat_and_nested_in_and_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — category (flat) + country.garage.car.make (nested) in AND."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Toyota"))),
    })
    flat_only_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Honda"))),
    })
    nested_only_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Toyota"))),
    })
    neither_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Honda"))),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("category").equal("premium"),
        Filter.by_property("country.garage.car.make").equal("Toyota"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert flat_only_id not in ids
    assert nested_only_id not in ids
    assert neither_id not in ids


def test_flat_and_nested_in_and_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — category (flat) + car.make (nested) in AND."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({"category": "premium", "car": make_car(make="Toyota")})
    flat_only_id = collection.data.insert({"category": "premium", "car": make_car(make="Honda")})
    nested_only_id = collection.data.insert({"category": "economy", "car": make_car(make="Toyota")})
    neither_id = collection.data.insert({"category": "economy", "car": make_car(make="Honda")})

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("category").equal("premium"),
        Filter.by_property("car.make").equal("Toyota"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert flat_only_id not in ids
    assert nested_only_id not in ids
    assert neither_id not in ids


# ===========================================================================
# Scenario: flat OR nested
# ===========================================================================


def test_flat_or_nested_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — category (flat) OR country.garage.cars.make (nested)."""
    collection = make_new_collection(collection_factory)

    match_via_flat_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Honda")])),
    })
    match_via_nested_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Toyota")])),
    })
    match_via_both_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Toyota")])),
    })
    no_match_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Honda")])),
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.by_property("category").equal("premium"),
        Filter.by_property("country.garage.cars.make").equal("Toyota"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_via_flat_id in ids
    assert match_via_nested_id in ids
    assert match_via_both_id in ids
    assert no_match_id not in ids


def test_flat_or_nested_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — category (flat) OR country.garage.car.make (nested)."""
    collection = make_new_collection(collection_factory)

    match_via_flat_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Honda"))),
    })
    match_via_nested_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Toyota"))),
    })
    match_via_both_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Toyota"))),
    })
    no_match_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Honda"))),
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.by_property("category").equal("premium"),
        Filter.by_property("country.garage.car.make").equal("Toyota"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_via_flat_id in ids
    assert match_via_nested_id in ids
    assert match_via_both_id in ids
    assert no_match_id not in ids


def test_flat_or_nested_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — category (flat) OR car.make (nested)."""
    collection = make_new_collection(collection_factory)

    match_via_flat_id = collection.data.insert({"category": "premium", "car": make_car(make="Honda")})
    match_via_nested_id = collection.data.insert({"category": "economy", "car": make_car(make="Toyota")})
    match_via_both_id = collection.data.insert({"category": "premium", "car": make_car(make="Toyota")})
    no_match_id = collection.data.insert({"category": "economy", "car": make_car(make="Honda")})

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.by_property("category").equal("premium"),
        Filter.by_property("car.make").equal("Toyota"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_via_flat_id in ids
    assert match_via_nested_id in ids
    assert match_via_both_id in ids
    assert no_match_id not in ids


# ===========================================================================
# Scenario: OR of two mixed-flat-and-nested ANDs (deeper boolean tree)
# ===========================================================================
#
# Filter: `(category="premium" AND <cars>.make="Toyota")
#       OR (category="economy" AND <cars>.make="Honda")`


def test_or_of_mixed_correlated_ands_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — OR of two mixed-flat-and-nested AND groups."""
    collection = make_new_collection(collection_factory)

    premium_toyota_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Toyota")])),
    })
    economy_honda_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Honda")])),
    })
    premium_honda_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Honda")])),
    })
    economy_toyota_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Toyota")])),
    })
    premium_both_cars_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota"),
            make_car(make="Honda"),
        ])),
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("category").equal("premium"),
            Filter.by_property("country.garage.cars.make").equal("Toyota"),
        ]),
        Filter.all_of([
            Filter.by_property("category").equal("economy"),
            Filter.by_property("country.garage.cars.make").equal("Honda"),
        ]),
    ])).objects
    ids = {o.uuid for o in result}
    assert premium_toyota_id in ids, "group 1 (premium + Toyota) → match"
    assert economy_honda_id in ids, "group 2 (economy + Honda) → match"
    assert premium_honda_id not in ids, "neither group fully satisfied"
    assert economy_toyota_id not in ids, "neither group fully satisfied"
    assert premium_both_cars_id in ids, "group 1 via the Toyota car → match"


def test_or_of_mixed_correlated_ands_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — OR of mixed-flat-and-nested AND groups on single-OBJECT car."""
    collection = make_new_collection(collection_factory)

    premium_toyota_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Toyota"))),
    })
    economy_honda_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Honda"))),
    })
    premium_honda_id = collection.data.insert({
        "category": "premium",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Honda"))),
    })
    economy_toyota_id = collection.data.insert({
        "category": "economy",
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Toyota"))),
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("category").equal("premium"),
            Filter.by_property("country.garage.car.make").equal("Toyota"),
        ]),
        Filter.all_of([
            Filter.by_property("category").equal("economy"),
            Filter.by_property("country.garage.car.make").equal("Honda"),
        ]),
    ])).objects
    ids = {o.uuid for o in result}
    assert premium_toyota_id in ids
    assert economy_honda_id in ids
    assert premium_honda_id not in ids
    assert economy_toyota_id not in ids


def test_or_of_mixed_correlated_ands_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — OR of mixed-flat-and-nested AND groups on single-OBJECT car at root."""
    collection = make_new_collection(collection_factory)

    premium_toyota_id = collection.data.insert({"category": "premium", "car": make_car(make="Toyota")})
    economy_honda_id = collection.data.insert({"category": "economy", "car": make_car(make="Honda")})
    premium_honda_id = collection.data.insert({"category": "premium", "car": make_car(make="Honda")})
    economy_toyota_id = collection.data.insert({"category": "economy", "car": make_car(make="Toyota")})

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("category").equal("premium"),
            Filter.by_property("car.make").equal("Toyota"),
        ]),
        Filter.all_of([
            Filter.by_property("category").equal("economy"),
            Filter.by_property("car.make").equal("Honda"),
        ]),
    ])).objects
    ids = {o.uuid for o in result}
    assert premium_toyota_id in ids
    assert economy_honda_id in ids
    assert premium_honda_id not in ids
    assert economy_toyota_id not in ids


# ===========================================================================
# Scenario: multi-token query value in Contains
# ===========================================================================
#
# `cars.tags contains_any ["family hybrid"]` — multi-token query: both
# tokens must appear in the SAME tag entry within one car.


def test_contains_any_multi_token_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.tags contains_any ['family hybrid']."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(tags=["family hybrid car"]),
        ])),
    })
    tokens_split_across_tags_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(tags=["family sedan", "hybrid model"]),
        ])),
    })
    single_token_missing_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(tags=["family car"]),
        ])),
    })
    split_across_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(tags=["family car"]),
            make_car(tags=["hybrid model"]),
        ])),
    })
    empty_tags_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[make_car(tags=[])])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.tags").contains_any(["family hybrid"]),
    ).objects
    ids = {o.uuid for o in result}
    assert match_id in ids, "one tag 'family hybrid car' contains both tokens → match"
    assert tokens_split_across_tags_id not in ids, (
        "tokens in separate tag entries on the same car → no match"
    )
    assert single_token_missing_id not in ids
    assert split_across_cars_id not in ids
    assert empty_tags_id not in ids
    assert empty_cars_id not in ids


def test_contains_any_multi_token_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.tags contains_any ['family hybrid']."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(tags=["family hybrid car"]),
        )),
    })
    tokens_split_across_tags_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(tags=["family sedan", "hybrid model"]),
        )),
    })
    single_token_missing_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(
            car=make_car(tags=["family car"]),
        )),
    })
    empty_tags_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(tags=[]))),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.tags").contains_any(["family hybrid"]),
    ).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert tokens_split_across_tags_id not in ids, (
        "tokens in separate tag entries on the same car → no match"
    )
    assert single_token_missing_id not in ids
    assert empty_tags_id not in ids


def test_contains_any_multi_token_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.tags contains_any ['family hybrid']."""
    collection = make_new_collection(collection_factory)

    match_id = collection.data.insert({"car": make_car(tags=["family hybrid car"])})
    tokens_split_across_tags_id = collection.data.insert({
        "car": make_car(tags=["family sedan", "hybrid model"]),
    })
    single_token_missing_id = collection.data.insert({"car": make_car(tags=["family car"])})
    empty_tags_id = collection.data.insert({"car": make_car(tags=[])})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("car.tags").contains_any(["family hybrid"]),
    ).objects
    ids = {o.uuid for o in result}
    assert match_id in ids
    assert tokens_split_across_tags_id not in ids
    assert single_token_missing_id not in ids
    assert empty_tags_id not in ids


# ===========================================================================
# Scenario: NotEqual on a nested leaf — existential per-element
# ===========================================================================
#
# `cars.make != Toyota` — existential per-car: doc matches when ∃ a car
# whose make is not Toyota. Shape B/C have one car, so the existential
# collapses to "the car's make is not Toyota".


def test_not_equal_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.make != Toyota."""
    collection = make_new_collection(collection_factory)

    single_toyota_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Toyota")])),
    })
    single_honda_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[make_car(make="Honda")])),
    })
    mixed_toyota_and_honda_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Toyota"),
            make_car(make="Honda"),
        ])),
    })
    multiple_non_toyota_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(make="Honda"),
            make_car(make="Ford"),
        ])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.make").not_equal("Toyota"),
    ).objects
    ids = {o.uuid for o in result}
    assert single_toyota_id not in ids, "only car is Toyota → no match"
    assert single_honda_id in ids
    assert mixed_toyota_and_honda_id in ids, "∃ Honda → match"
    assert multiple_non_toyota_id in ids
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_not_equal_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.make != Toyota on single-OBJECT car."""
    collection = make_new_collection(collection_factory)

    toyota_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Toyota"))),
    })
    honda_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(make="Honda"))),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.make").not_equal("Toyota"),
    ).objects
    ids = {o.uuid for o in result}
    assert toyota_id not in ids, "car is Toyota → no match"
    assert honda_id in ids, "car is Honda → match"


def test_not_equal_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.make != Toyota on single-OBJECT car at root."""
    collection = make_new_collection(collection_factory)

    toyota_id = collection.data.insert({"car": make_car(make="Toyota")})
    honda_id = collection.data.insert({"car": make_car(make="Honda")})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("car.make").not_equal("Toyota"),
    ).objects
    ids = {o.uuid for o in result}
    assert toyota_id not in ids
    assert honda_id in ids


# ===========================================================================
# Scenario: Like with wildcard on a WORD-tokenized text leaf
# ===========================================================================
#
# `cars.model like "Hyb*"` — WORD-tokenized model. Pattern matches any
# token; "Camry Hybrid" matches via the 'hybrid' token even though the
# value doesn't start with "Hyb".


def test_like_word_tokenized_under_country_garage_cars(collection_factory: CollectionFactory) -> None:
    """Shape A — country.garage.cars.model like 'Hyb*'."""
    collection = make_new_collection(collection_factory)

    match_via_second_token_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model="Camry Hybrid"),
        ])),
    })
    match_via_first_token_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[
            make_car(model="Hybrid Sedan"),
        ])),
    })
    no_matching_token_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[make_car(model="Camry")])),
    })
    unrelated_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[make_car(model="Civic LX")])),
    })
    empty_cars_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(cars=[])),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.cars.model").like("Hyb*"),
    ).objects
    ids = {o.uuid for o in result}
    assert match_via_second_token_id in ids, "WORD-tokenization discriminator: 'hybrid' token matches"
    assert match_via_first_token_id in ids
    assert no_matching_token_id not in ids
    assert unrelated_id not in ids
    assert empty_cars_id not in ids


def test_like_word_tokenized_under_country_garage_car(collection_factory: CollectionFactory) -> None:
    """Shape B — country.garage.car.model like 'Hyb*'."""
    collection = make_new_collection(collection_factory)

    match_via_second_token_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(model="Camry Hybrid"))),
    })
    match_via_first_token_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(model="Hybrid Sedan"))),
    })
    no_matching_token_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(model="Camry"))),
    })
    unrelated_id = collection.data.insert({
        "country": make_new_country(garage=make_new_garage(car=make_car(model="Civic LX"))),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garage.car.model").like("Hyb*"),
    ).objects
    ids = {o.uuid for o in result}
    assert match_via_second_token_id in ids
    assert match_via_first_token_id in ids
    assert no_matching_token_id not in ids
    assert unrelated_id not in ids


def test_like_word_tokenized_at_root_car(collection_factory: CollectionFactory) -> None:
    """Shape C — car.model like 'Hyb*'."""
    collection = make_new_collection(collection_factory)

    match_via_second_token_id = collection.data.insert({"car": make_car(model="Camry Hybrid")})
    match_via_first_token_id = collection.data.insert({"car": make_car(model="Hybrid Sedan")})
    no_matching_token_id = collection.data.insert({"car": make_car(model="Camry")})
    unrelated_id = collection.data.insert({"car": make_car(model="Civic LX")})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("car.model").like("Hyb*"),
    ).objects
    ids = {o.uuid for o in result}
    assert match_via_second_token_id in ids
    assert match_via_first_token_id in ids
    assert no_matching_token_id not in ids
    assert unrelated_id not in ids
