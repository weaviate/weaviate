"""End-to-end tests for nested object property filtering.

One collection (NESTED_PROPS below) has three nested root paths defined side
by side, so we can probe the filter pipeline at three depths:

    L0:        cars[]                       — OBJECT_ARRAY at the root
    L2_object: country.garages[].cars[]     — single OBJECT at L0, OBJECT_ARRAY at L1/L2
    L2_array:  countries[].garages[].cars[] — OBJECT_ARRAY at every level

There's one test per scenario per variant. Each test spells out its doc shapes
in the test body so the reader can see exactly which path the cars under test
are placed at.

These tests prove the wire path: API write → DB index → filter dispatch →
result. Filter-correctness corner cases live in the DB-level nested-filtering
integration tests under adapters/repos/db/.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List

import pytest
from weaviate.classes.config import Configure, DataType, Property, Tokenization
from weaviate.collections.classes.filters import Filter
from weaviate.exceptions import WeaviateQueryError

from .conftest import CollectionFactory


# ---------------------------------------------------------------------------
# Schema — one collection with all three nested root paths
# ---------------------------------------------------------------------------

_TIRES_PROPS = [
    Property(name="width", data_type=DataType.INT),
    Property(name="brand", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
]

_CARS_PROPS = [
    Property(name="make", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
    Property(name="model", data_type=DataType.TEXT, tokenization=Tokenization.WORD),
    Property(name="color", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
    Property(name="year", data_type=DataType.INT),
    Property(name="price", data_type=DataType.NUMBER),
    Property(name="purchased", data_type=DataType.BOOL),
    Property(name="delivered_at", data_type=DataType.DATE),
    Property(name="vin", data_type=DataType.UUID),
    Property(name="model_codes", data_type=DataType.TEXT_ARRAY, tokenization=Tokenization.FIELD),
    Property(name="tags", data_type=DataType.TEXT_ARRAY, tokenization=Tokenization.WORD),
    Property(name="repair_years", data_type=DataType.INT_ARRAY),
    Property(name="fuel_levels", data_type=DataType.NUMBER_ARRAY),
    Property(name="warranty_flags", data_type=DataType.BOOL_ARRAY),
    Property(name="service_dates", data_type=DataType.DATE_ARRAY),
    Property(name="previous_owners", data_type=DataType.UUID_ARRAY),
    Property(name="tires", data_type=DataType.OBJECT_ARRAY, nested_properties=_TIRES_PROPS),
]

_GARAGES_PROPS = [
    Property(name="city", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
    Property(name="postcode", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
    Property(name="cars", data_type=DataType.OBJECT_ARRAY, nested_properties=_CARS_PROPS),
]

_COUNTRY_INNER = [
    Property(name="name", data_type=DataType.TEXT, tokenization=Tokenization.FIELD),
    Property(name="tags", data_type=DataType.TEXT_ARRAY, tokenization=Tokenization.FIELD),
    Property(name="cities", data_type=DataType.TEXT_ARRAY, tokenization=Tokenization.FIELD),
    Property(name="garages", data_type=DataType.OBJECT_ARRAY, nested_properties=_GARAGES_PROPS),
]

NESTED_PROPS: List[Property] = [
    Property(name="cars", data_type=DataType.OBJECT_ARRAY, nested_properties=_CARS_PROPS),
    Property(name="country", data_type=DataType.OBJECT, nested_properties=_COUNTRY_INNER),
    Property(name="countries", data_type=DataType.OBJECT_ARRAY, nested_properties=_COUNTRY_INNER),
]


# ---------------------------------------------------------------------------
# Sub-tree builders — building blocks only; tests wire them together inline.
# Every default is a realistic value so tests only spell out the fields they
# actually filter on.
# ---------------------------------------------------------------------------

DEFAULT_VIN = "11111111-1111-1111-1111-111111111111"
DEFAULT_OWNER_ALICE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DEFAULT_OWNER_BOB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def make_car(
    *,
    make: str = "Toyota",
    model: str = "Camry Hybrid",
    color: str = "red",
    year: int = 2020,
    price: float = 27500.0,
    purchased: bool = True,
    delivered_at: str = "2020-03-15T00:00:00Z",
    vin: str = DEFAULT_VIN,
    model_codes: List[str] = None,
    tags: List[str] = None,
    repair_years: List[int] = None,
    fuel_levels: List[float] = None,
    warranty_flags: List[bool] = None,
    service_dates: List[str] = None,
    previous_owners: List[str] = None,
    tires: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a complete car. Defaults: 2020 Toyota Camry Hybrid, red."""
    return {
        "make": make,
        "model": model,
        "color": color,
        "year": year,
        "price": price,
        "purchased": purchased,
        "delivered_at": delivered_at,
        "vin": vin,
        "model_codes": ["CAM-2020", "HYB-PRO"] if model_codes is None else model_codes,
        "tags": ["sedan hybrid", "family car"] if tags is None else tags,
        "repair_years": [2022, 2023] if repair_years is None else repair_years,
        "fuel_levels": [0.75, 0.50] if fuel_levels is None else fuel_levels,
        "warranty_flags": [True, False] if warranty_flags is None else warranty_flags,
        "service_dates": (
            ["2021-06-01T00:00:00Z", "2022-06-01T00:00:00Z"]
            if service_dates is None else service_dates
        ),
        "previous_owners": (
            [DEFAULT_OWNER_ALICE, DEFAULT_OWNER_BOB]
            if previous_owners is None else previous_owners
        ),
        "tires": (
            [{"width": 215, "brand": "Michelin"}, {"width": 215, "brand": "Michelin"}]
            if tires is None else tires
        ),
    }


def make_garage(
    *,
    city: str = "Amsterdam",
    postcode: str = "1011AB",
    cars: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a garage. Defaults: Amsterdam garage with one default car."""
    return {
        "city": city,
        "postcode": postcode,
        "cars": [make_car()] if cars is None else cars,
    }


def make_country(
    *,
    name: str = "Netherlands",
    tags: List[str] = None,
    cities: List[str] = None,
    garages: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a country. Defaults: Netherlands with one default Amsterdam garage."""
    return {
        "name": name,
        "tags": ["eu", "western-europe"] if tags is None else tags,
        "cities": ["Amsterdam", "Rotterdam"] if cities is None else cities,
        "garages": [make_garage()] if garages is None else garages,
    }


def omit_fields(doc: Dict[str, Any], *paths: str) -> Dict[str, Any]:
    """Return a deep copy of doc with the given dotted paths removed.

    Used for IsNull scenarios. Supports list indices like 'cars.0.price'.
    """
    out = copy.deepcopy(doc)
    for path in paths:
        parts = path.split(".")
        cur: Any = out
        for part in parts[:-1]:
            cur = cur[int(part)] if part.isdigit() else cur[part]
        last = parts[-1]
        if last.isdigit():
            del cur[int(last)]
        elif last in cur:
            del cur[last]
    return out


def make_collection(collection_factory: CollectionFactory):
    """Create the standard nested-properties collection used by every test."""
    return collection_factory(
        properties=NESTED_PROPS,
        vector_config=Configure.Vectors.self_provided(),
    )


# ===========================================================================
# Scenario: every leaf datatype is filterable
# ===========================================================================
#
# Insert a Toyota and a Honda under the variant's root path. For each datatype,
# run an Equal/ContainsAny filter against the Toyota's value and assert only
# the Toyota doc comes back.

# Honda Civic with clearly different values from the default Toyota for every
# datatype. Used as the "must not match" doc in every all-datatypes test.
def _honda_civic_for_datatypes_test() -> Dict[str, Any]:
    return make_car(
        make="Honda",
        model="Civic LX",
        color="blue",
        year=2015,
        price=18000.0,
        purchased=False,
        delivered_at="2015-09-01T00:00:00Z",
        vin="22222222-2222-2222-2222-222222222222",
        model_codes=["CIV-2015", "LX-STD"],
        tags=["compact economy", "city car"],
        repair_years=[2017, 2018],
        fuel_levels=[0.10, 0.30],
        warranty_flags=[False],
        service_dates=["2016-06-01T00:00:00Z"],
        previous_owners=["cccccccc-cccc-cccc-cccc-cccccccccccc"],
    )


def _assert_each_datatype_filter_selects(
    collection,
    *,
    source_car: Dict[str, Any],
    cars_path: str,
    expected_match_ids: List[str],
    expected_miss_ids: List[str],
) -> None:
    """Run one filter per leaf datatype on the cars schema and assert each
    returns the expected partition of docs.

    For every datatype the helper builds a filter from `source_car`'s value at
    `cars_path` (so the source car is, by construction, a match), then asserts
    every id in `expected_match_ids` appears in the result and every id in
    `expected_miss_ids` does not.
    """
    must_match = set(expected_match_ids)
    must_miss = set(expected_miss_ids)

    scalar_cases = [
        (f"{cars_path}.make", source_car["make"]),
        (f"{cars_path}.color", source_car["color"]),
        (f"{cars_path}.year", source_car["year"]),
        (f"{cars_path}.price", source_car["price"]),
        (f"{cars_path}.purchased", source_car["purchased"]),
        (f"{cars_path}.delivered_at", source_car["delivered_at"]),
        (f"{cars_path}.vin", source_car["vin"]),
    ]
    for filter_path, value in scalar_cases:
        result = collection.query.fetch_objects(
            filters=Filter.by_property(filter_path).equal(value),
        ).objects
        ids = {o.uuid for o in result}
        assert must_match.issubset(ids), f"{filter_path}=={value!r}: expected docs missing"
        assert must_miss.isdisjoint(ids), f"{filter_path}=={value!r}: unwanted docs returned"

    array_cases = [
        (f"{cars_path}.model_codes", [source_car["model_codes"][0]]),
        (f"{cars_path}.repair_years", [source_car["repair_years"][0]]),
        (f"{cars_path}.fuel_levels", [source_car["fuel_levels"][0]]),
        (f"{cars_path}.warranty_flags", [source_car["warranty_flags"][0]]),
        (f"{cars_path}.service_dates", [source_car["service_dates"][0]]),
        (f"{cars_path}.previous_owners", [source_car["previous_owners"][0]]),
    ]
    for filter_path, values in array_cases:
        result = collection.query.fetch_objects(
            filters=Filter.by_property(filter_path).contains_any(values),
        ).objects
        ids = {o.uuid for o in result}
        assert must_match.issubset(ids), (
            f"ContainsAny({filter_path}, {values!r}): expected docs missing"
        )
        assert must_miss.isdisjoint(ids), (
            f"ContainsAny({filter_path}, {values!r}): unwanted docs returned"
        )


def _assert_comparison_operator_sweep_selects(
    collection,
    *,
    cars_path: str,
    high_only_ids: List[str],
    low_only_ids: List[str],
    mixed_ids: List[str] = None,
) -> None:
    """Run a sweep of comparison operators on numeric / date / text leaves.

    Each "high" filter (year>2018, price>20000, etc.) matches docs that have
    at least one Toyota-style car (year=2020, price=27500, delivered_at=
    2020-03-15, make=Toyota). Each "low" filter (year<2018 etc.) matches
    docs with at least one Honda-style car (year=2015, price=18000,
    delivered_at=2015-09-01, make=Honda). A `mixed_ids` doc has both
    kinds of car and matches both polarities — passed in separately so
    every filter can include it on the match side.
    """
    high_only = set(high_only_ids)
    low_only = set(low_only_ids)
    mixed = set(mixed_ids or [])

    cases = [
        # (filt, expected_match, expected_miss, desc)
        (Filter.by_property(f"{cars_path}.year").greater_than(2018),
         high_only | mixed, low_only, "year > 2018"),
        (Filter.by_property(f"{cars_path}.year").less_than(2018),
         low_only | mixed, high_only, "year < 2018"),
        (Filter.by_property(f"{cars_path}.year").greater_or_equal(2020),
         high_only | mixed, low_only, "year >= 2020"),
        (Filter.by_property(f"{cars_path}.year").less_or_equal(2015),
         low_only | mixed, high_only, "year <= 2015"),
        (Filter.by_property(f"{cars_path}.price").greater_than(20000.0),
         high_only | mixed, low_only, "price > 20000"),
        (Filter.by_property(f"{cars_path}.price").less_than(20000.0),
         low_only | mixed, high_only, "price < 20000"),
        (Filter.by_property(f"{cars_path}.delivered_at").greater_than("2018-01-01T00:00:00Z"),
         high_only | mixed, low_only, "delivered_at > 2018-01-01"),
        (Filter.by_property(f"{cars_path}.delivered_at").less_than("2018-01-01T00:00:00Z"),
         low_only | mixed, high_only, "delivered_at < 2018-01-01"),
        (Filter.by_property(f"{cars_path}.make").like("Toy*"),
         high_only | mixed, low_only, "make like Toy*"),
        (Filter.by_property(f"{cars_path}.make").like("Hon*"),
         low_only | mixed, high_only, "make like Hon*"),
    ]
    for filt, expected_match, expected_miss, desc in cases:
        result = collection.query.fetch_objects(filters=filt).objects
        ids = {o.uuid for o in result}
        assert expected_match.issubset(ids), f"{desc}: expected match docs missing"
        assert expected_miss.isdisjoint(ids), f"{desc}: unwanted docs returned"


def test_all_datatypes_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars[] at the top of the doc."""
    collection = make_collection(collection_factory)
    toyota = make_car()
    honda = _honda_civic_for_datatypes_test()

    toyota_doc_id = collection.data.insert({"cars": [toyota]})
    honda_doc_id = collection.data.insert({"cars": [honda]})

    _assert_each_datatype_filter_selects(
        collection,
        source_car=toyota,
        cars_path="cars",
        expected_match_ids=[toyota_doc_id],
        expected_miss_ids=[honda_doc_id],
    )


def test_all_datatypes_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — country.garages[].cars[] (single OBJECT root)."""
    collection = make_collection(collection_factory)
    toyota = make_car()
    honda = _honda_civic_for_datatypes_test()

    toyota_doc_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[toyota])]),
    })
    honda_doc_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[honda])]),
    })

    _assert_each_datatype_filter_selects(
        collection,
        source_car=toyota,
        cars_path="country.garages.cars",
        expected_match_ids=[toyota_doc_id],
        expected_miss_ids=[honda_doc_id],
    )


def test_all_datatypes_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — countries[].garages[].cars[] (OBJECT_ARRAY at every level)."""
    collection = make_collection(collection_factory)
    toyota = make_car()
    honda = _honda_civic_for_datatypes_test()

    toyota_doc_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[toyota])])],
    })
    honda_doc_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[honda])])],
    })

    _assert_each_datatype_filter_selects(
        collection,
        source_car=toyota,
        cars_path="countries.garages.cars",
        expected_match_ids=[toyota_doc_id],
        expected_miss_ids=[honda_doc_id],
    )


# ===========================================================================
# Scenario: same-element AND correlates per-car
# ===========================================================================
#
# A `make=Toyota AND color=red` filter must require a SINGLE car to satisfy
# both leaves. Splitting the two attributes across separate elements at any
# level above the car (siblings within a garage, separate garages, separate
# countries) must NOT match.


def test_same_element_and_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars[] at root.

    Only one array exists above cars (cars[] itself), so the only split shape
    available is two cars sitting side by side in cars[].
    """
    collection = make_collection(collection_factory)

    match_doc_id = collection.data.insert({
        "cars": [make_car(make="Toyota", color="red")],
    })
    split_within_cars_id = collection.data.insert({
        "cars": [
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("cars.make").equal("Toyota"),
        Filter.by_property("cars.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_doc_id in ids, "single red Toyota should match"
    assert split_within_cars_id not in ids, "split across cars should not match"


def test_same_element_and_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — country.garages[].cars[].

    country is a single OBJECT, garages[] is the array above cars[]. Splits
    available: within one garage's cars[], or across two garages.
    """
    collection = make_collection(collection_factory)

    match_doc_id = collection.data.insert({
        "country": make_country(
            garages=[make_garage(cars=[make_car(make="Toyota", color="red")])],
        ),
    })
    split_within_garage_id = collection.data.insert({
        "country": make_country(
            garages=[make_garage(cars=[
                make_car(make="Toyota", color="blue"),
                make_car(make="Honda", color="red"),
            ])],
        ),
    })
    split_across_garages_id = collection.data.insert({
        "country": make_country(
            garages=[
                make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")]),
                make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="red")]),
            ],
        ),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(
            garages=[
                make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
                make_garage(city="Rotterdam", cars=[make_car(make="Ford", color="green")]),
            ],
        ),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garages.cars.make").equal("Toyota"),
        Filter.by_property("country.garages.cars.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_doc_id in ids, "single red Toyota should match"
    assert split_within_garage_id not in ids, "split within a garage should not match"
    assert split_across_garages_id not in ids, "split across garages should not match"
    assert match_via_one_garage_id in ids, (
        "Amsterdam has red Toyota, Rotterdam has unrelated car → match via Amsterdam"
    )


def test_same_element_and_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — countries[].garages[].cars[].

    OBJECT_ARRAY at every level, so splits are available within a garage,
    across garages in one country, or across countries.
    """
    collection = make_collection(collection_factory)

    match_doc_id = collection.data.insert({
        "countries": [make_country(
            garages=[make_garage(cars=[make_car(make="Toyota", color="red")])],
        )],
    })
    split_within_garage_id = collection.data.insert({
        "countries": [make_country(
            garages=[make_garage(cars=[
                make_car(make="Toyota", color="blue"),
                make_car(make="Honda", color="red"),
            ])],
        )],
    })
    split_across_garages_id = collection.data.insert({
        "countries": [make_country(
            garages=[
                make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")]),
                make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="red")]),
            ],
        )],
    })
    split_across_countries_id = collection.data.insert({
        "countries": [
            make_country(
                name="Netherlands",
                garages=[make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")])],
            ),
            make_country(
                name="Germany",
                garages=[make_garage(city="Berlin", cars=[make_car(make="Honda", color="red")])],
            ),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(
            garages=[
                make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
                make_garage(city="Rotterdam", cars=[make_car(make="Ford", color="green")]),
            ],
        )],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(
                name="Netherlands",
                garages=[make_garage(cars=[make_car(make="Toyota", color="red")])],
            ),
            make_country(
                name="Germany",
                garages=[make_garage(cars=[make_car(make="Ford", color="green")])],
            ),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("countries.garages.cars.make").equal("Toyota"),
        Filter.by_property("countries.garages.cars.color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_doc_id in ids, "single red Toyota should match"
    assert split_within_garage_id not in ids, "split within a garage should not match"
    assert split_across_garages_id not in ids, "split across garages should not match"
    assert split_across_countries_id not in ids, "split across countries should not match"
    assert match_via_one_garage_id in ids, (
        "Amsterdam has red Toyota, Rotterdam has unrelated car → match via Amsterdam"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands has red Toyota, Germany has unrelated car → match via Netherlands"
    )


# ===========================================================================
# Scenario: arr[N] positional pin on cars[]
# ===========================================================================
#
# `cars[N].make = X` must match only docs where the car at position N has
# make=X. A matching car at a different position must NOT make the doc match.
# The variant skips L2_object because that root has no array of cars to pin —
# cars[] only exists inside the country path on the array-rooted variants.


def test_arr_n_pin_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars[0].make selects only docs whose first car matches."""
    collection = make_collection(collection_factory)

    pinned_match_id = collection.data.insert({
        "cars": [
            make_car(make="Toyota"),
            make_car(make="Honda"),
        ],
    })
    pinned_miss_id = collection.data.insert({
        "cars": [
            make_car(make="Honda"),
            make_car(make="Toyota"),
        ],
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("cars[0].make").equal("Toyota"),
    ).objects
    ids = {o.uuid for o in result}
    assert pinned_match_id in ids, "Toyota at cars[0] should match"
    assert pinned_miss_id not in ids, "Toyota at cars[1] must not satisfy cars[0] pin"


def test_arr_n_pin_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — country.garages.cars[0].make pins on cars[] inside each garage.
    Toyota must be the first car in some garage's cars[] array."""
    collection = make_collection(collection_factory)

    pinned_match_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Toyota"),
            make_car(make="Honda"),
        ])]),
    })
    pinned_miss_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Honda"),
            make_car(make="Toyota"),
        ])]),
    })
    miss_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Honda"), make_car(make="Toyota")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Ford"), make_car(make="Toyota")]),
        ]),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda")]),
        ]),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars[0].make").equal("Toyota"),
    ).objects
    ids = {o.uuid for o in result}
    assert pinned_match_id in ids, "Toyota at cars[0] should match"
    assert pinned_miss_id not in ids, "Toyota at cars[1] must not satisfy cars[0] pin"
    assert miss_across_garages_id not in ids, (
        "no garage's cars[0] is Toyota (Toyota only at cars[1] in each) — must not match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's cars[0] is Toyota → match (Rotterdam is unrelated)"
    )


def test_arr_n_pin_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — countries.garages.cars[0].make pins on the cars[] array
    inside each garage. Toyota must be the first car in that array."""
    collection = make_collection(collection_factory)

    pinned_match_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Toyota"),
            make_car(make="Honda"),
        ])])],
    })
    pinned_miss_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Honda"),
            make_car(make="Toyota"),
        ])])],
    })
    miss_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Honda"), make_car(make="Toyota")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Ford"), make_car(make="Toyota")]),
        ])],
    })
    miss_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Honda"), make_car(make="Toyota"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Ford"), make_car(make="Toyota"),
            ])]),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda")]),
        ])],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[make_car(make="Toyota")])]),
            make_country(name="Germany", garages=[make_garage(cars=[make_car(make="Honda")])]),
        ],
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars[0].make").equal("Toyota"),
    ).objects
    ids = {o.uuid for o in result}
    assert pinned_match_id in ids, "Toyota at cars[0] should match"
    assert pinned_miss_id not in ids, "Toyota at cars[1] must not satisfy cars[0] pin"
    assert miss_across_garages_id not in ids, (
        "no garage's cars[0] is Toyota (Toyota only at cars[1] in each) — must not match"
    )
    assert miss_across_countries_id not in ids, (
        "no country's cars[0] is Toyota (Toyota only at cars[1] in each) — must not match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's cars[0] is Toyota → match (Rotterdam is unrelated)"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands' cars[0] is Toyota → match (Germany is unrelated)"
    )


# ===========================================================================
# Scenario: arr[N] pin combined with same-element AND
# ===========================================================================
#
# `cars[0].make = Toyota AND cars[0].color = red` must require both leaves to
# be satisfied by the SAME car AT POSITION 0. A car at position 0 satisfying
# only one leaf doesn't match. A car at position 1 satisfying both also
# doesn't match — the pin enforces position 0.


def test_arr_n_pin_with_and_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — pinned same-element AND. Both leaves must be satisfied by cars[0]."""
    collection = make_collection(collection_factory)

    pinned_match_id = collection.data.insert({
        "cars": [
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ],
    })
    split_within_pinned_car_id = collection.data.insert({
        "cars": [
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ],
    })
    match_at_wrong_index_id = collection.data.insert({
        "cars": [
            make_car(make="Honda", color="red"),
            make_car(make="Toyota", color="red"),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("cars[0].make").equal("Toyota"),
        Filter.by_property("cars[0].color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert pinned_match_id in ids, "Toyota+red at cars[0] should match"
    assert split_within_pinned_car_id not in ids, (
        "cars[0] satisfies make but not color (blue) — must not match"
    )
    assert match_at_wrong_index_id not in ids, (
        "cars[1] is red Toyota but pin is to cars[0] — must not match"
    )


def test_arr_n_pin_with_and_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — pinned same-element AND inside country.garages[]."""
    collection = make_collection(collection_factory)

    pinned_match_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])]),
    })
    split_within_pinned_car_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ])]),
    })
    match_at_wrong_index_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Honda", color="red"),
            make_car(make="Toyota", color="red"),
        ])]),
    })
    split_across_garages_pinned_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="red")]),
        ]),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Ford", color="green")]),
        ]),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garages.cars[0].make").equal("Toyota"),
        Filter.by_property("country.garages.cars[0].color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert pinned_match_id in ids, "Toyota+red at cars[0] should match"
    assert split_within_pinned_car_id not in ids, (
        "cars[0] satisfies make but not color (blue) — must not match"
    )
    assert match_at_wrong_index_id not in ids, (
        "cars[1] is red Toyota but pin is to cars[0] — must not match"
    )
    assert split_across_garages_pinned_id not in ids, (
        "each garage's cars[0] satisfies only one leaf — must not match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's cars[0] satisfies both leaves → match (Rotterdam is unrelated)"
    )


def test_arr_n_pin_with_and_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — pinned same-element AND inside countries[].garages[]."""
    collection = make_collection(collection_factory)

    pinned_match_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])])],
    })
    split_within_pinned_car_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ])])],
    })
    match_at_wrong_index_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Honda", color="red"),
            make_car(make="Toyota", color="red"),
        ])])],
    })
    split_across_garages_pinned_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="red")]),
        ])],
    })
    split_across_countries_pinned_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Toyota", color="blue"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Honda", color="red"),
            ])]),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Ford", color="green")]),
        ])],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Toyota", color="red"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Ford", color="green"),
            ])]),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("countries.garages.cars[0].make").equal("Toyota"),
        Filter.by_property("countries.garages.cars[0].color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert pinned_match_id in ids, "Toyota+red at cars[0] should match"
    assert split_within_pinned_car_id not in ids, (
        "cars[0] satisfies make but not color (blue) — must not match"
    )
    assert match_at_wrong_index_id not in ids, (
        "cars[1] is red Toyota but pin is to cars[0] — must not match"
    )
    assert split_across_garages_pinned_id not in ids, (
        "each garage's cars[0] satisfies only one leaf — must not match"
    )
    assert split_across_countries_pinned_id not in ids, (
        "each country's cars[0] satisfies only one leaf — must not match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's cars[0] satisfies both leaves → match (Rotterdam is unrelated)"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands' cars[0] satisfies both leaves → match (Germany is unrelated)"
    )


# ===========================================================================
# Scenario: arr[N] pin combined with OR
# ===========================================================================
#
# `cars[0].make = X OR cars[0].color = Y` requires the car at position 0
# to satisfy AT LEAST ONE of the two leaves. The pin applies to both
# operands — neither side may "borrow" a value from another position to
# satisfy the OR.


def test_arr_n_pin_with_or_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — pinned OR. cars[0] must satisfy at least one leaf; no leaf may
    satisfy by looking at a non-pinned position."""
    collection = make_collection(collection_factory)

    pinned_both_leaves_id = collection.data.insert({
        "cars": [
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ],
    })
    pinned_only_make_id = collection.data.insert({
        "cars": [
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ],
    })
    pinned_only_color_id = collection.data.insert({
        "cars": [
            make_car(make="Honda", color="red"),
            make_car(make="Toyota", color="blue"),
        ],
    })
    pinned_satisfies_neither_id = collection.data.insert({
        "cars": [
            make_car(make="Honda", color="blue"),
            make_car(make="Toyota", color="red"),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.by_property("cars[0].make").equal("Toyota"),
        Filter.by_property("cars[0].color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert pinned_both_leaves_id in ids, "cars[0] satisfies both → match"
    assert pinned_only_make_id in ids, "cars[0] satisfies make (Toyota) → match"
    assert pinned_only_color_id in ids, "cars[0] satisfies color (red) → match"
    assert pinned_satisfies_neither_id not in ids, (
        "cars[0] satisfies neither; cars[1] satisfies both but pin is to cars[0] — must not match"
    )


def test_arr_n_pin_with_or_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — pinned OR inside country.garages[]."""
    collection = make_collection(collection_factory)

    pinned_both_leaves_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])]),
    })
    pinned_only_make_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ])]),
    })
    pinned_only_color_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Honda", color="red"),
            make_car(make="Toyota", color="blue"),
        ])]),
    })
    pinned_satisfies_neither_id = collection.data.insert({
        "country": make_country(garages=[make_garage(cars=[
            make_car(make="Honda", color="blue"),
            make_car(make="Toyota", color="red"),
        ])]),
    })
    neither_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[
                make_car(make="Honda", color="blue"),
                make_car(make="Toyota", color="red"),
            ]),
            make_garage(city="Rotterdam", cars=[
                make_car(make="Ford", color="green"),
                make_car(make="Toyota", color="red"),
            ]),
        ]),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Ford", color="green")]),
        ]),
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.by_property("country.garages.cars[0].make").equal("Toyota"),
        Filter.by_property("country.garages.cars[0].color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert pinned_both_leaves_id in ids, "cars[0] satisfies both → match"
    assert pinned_only_make_id in ids, "cars[0] satisfies make (Toyota) → match"
    assert pinned_only_color_id in ids, "cars[0] satisfies color (red) → match"
    assert pinned_satisfies_neither_id not in ids, (
        "cars[0] satisfies neither; cars[1] satisfies both but pin is to cars[0] — must not match"
    )
    assert neither_across_garages_id not in ids, (
        "no garage's cars[0] satisfies either leaf (only cars[1] does) — must not match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's cars[0] satisfies make leaf → match (Rotterdam is unrelated)"
    )


def test_arr_n_pin_with_or_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — pinned OR inside countries[].garages[]."""
    collection = make_collection(collection_factory)

    pinned_both_leaves_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Toyota", color="red"),
            make_car(make="Honda", color="blue"),
        ])])],
    })
    pinned_only_make_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Toyota", color="blue"),
            make_car(make="Honda", color="red"),
        ])])],
    })
    pinned_only_color_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Honda", color="red"),
            make_car(make="Toyota", color="blue"),
        ])])],
    })
    pinned_satisfies_neither_id = collection.data.insert({
        "countries": [make_country(garages=[make_garage(cars=[
            make_car(make="Honda", color="blue"),
            make_car(make="Toyota", color="red"),
        ])])],
    })
    neither_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[
                make_car(make="Honda", color="blue"),
                make_car(make="Toyota", color="red"),
            ]),
            make_garage(city="Rotterdam", cars=[
                make_car(make="Ford", color="green"),
                make_car(make="Toyota", color="red"),
            ]),
        ])],
    })
    neither_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Honda", color="blue"),
                make_car(make="Toyota", color="red"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Ford", color="green"),
                make_car(make="Toyota", color="red"),
            ])]),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Ford", color="green")]),
        ])],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Toyota", color="blue"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Ford", color="green"),
            ])]),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.by_property("countries.garages.cars[0].make").equal("Toyota"),
        Filter.by_property("countries.garages.cars[0].color").equal("red"),
    ])).objects
    ids = {o.uuid for o in result}
    assert pinned_both_leaves_id in ids, "cars[0] satisfies both → match"
    assert pinned_only_make_id in ids, "cars[0] satisfies make (Toyota) → match"
    assert pinned_only_color_id in ids, "cars[0] satisfies color (red) → match"
    assert pinned_satisfies_neither_id not in ids, (
        "cars[0] satisfies neither; cars[1] satisfies both but pin is to cars[0] — must not match"
    )
    assert neither_across_garages_id not in ids, (
        "no garage's cars[0] satisfies either leaf (only cars[1] does) — must not match"
    )
    assert neither_across_countries_id not in ids, (
        "no country's cars[0] satisfies either leaf (only cars[1] does) — must not match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's cars[0] satisfies make leaf → match (Rotterdam is unrelated)"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands' cars[0] satisfies make leaf → match (Germany is unrelated)"
    )


# ===========================================================================
# Scenario: OR of correlated ANDs
# ===========================================================================
#
# Filter: `(cars.make=Toyota AND cars.color=red) OR (cars.make=Honda AND cars.color=blue)`
#
# Each AND group requires ONE car to satisfy both leaves of that group
# (same-element). The OR combines the groups: a doc matches if any AND
# group matches. Critically, the OR does NOT let leaves from different
# groups combine: a doc with cars=[{Toyota,blue},{Honda,red}] satisfies
# every individual leaf, but no single car satisfies any single AND group
# — so the doc must NOT match.


def test_or_of_correlated_ands_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — OR composes two per-car ANDs without breaking same-element correlation."""
    collection = make_collection(collection_factory)

    group1_match_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="red"),
    ]})
    group2_match_id = collection.data.insert({"cars": [
        make_car(make="Honda", color="blue"),
    ]})
    both_groups_match_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ]})
    cross_group_split_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="blue"),
        make_car(make="Honda", color="red"),
    ]})
    unrelated_id = collection.data.insert({"cars": [
        make_car(make="Ford", color="green"),
    ]})

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("cars.make").equal("Toyota"),
            Filter.by_property("cars.color").equal("red"),
        ]),
        Filter.all_of([
            Filter.by_property("cars.make").equal("Honda"),
            Filter.by_property("cars.color").equal("blue"),
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


def test_or_of_correlated_ands_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — OR of correlated ANDs under country.garages[]."""
    collection = make_collection(collection_factory)

    group1_match_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
    ])])})
    group2_match_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Honda", color="blue"),
    ])])})
    both_groups_match_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ])])})
    cross_group_split_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="blue"),
        make_car(make="Honda", color="red"),
    ])])})
    cross_group_split_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="red")]),
        ]),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Ford", color="green")]),
        ]),
    })
    unrelated_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Ford", color="green"),
    ])])})

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("country.garages.cars.make").equal("Toyota"),
            Filter.by_property("country.garages.cars.color").equal("red"),
        ]),
        Filter.all_of([
            Filter.by_property("country.garages.cars.make").equal("Honda"),
            Filter.by_property("country.garages.cars.color").equal("blue"),
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
    assert cross_group_split_across_garages_id not in ids, (
        "same cross-group leaf split, distributed across two garages → must not match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car satisfies group 1 (Toyota+red); Rotterdam unrelated → match"
    )
    assert unrelated_id not in ids, "Ford+green satisfies neither group"


def test_or_of_correlated_ands_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — OR of correlated ANDs under countries[].garages[]."""
    collection = make_collection(collection_factory)

    group1_match_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
    ])])]})
    group2_match_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Honda", color="blue"),
    ])])]})
    both_groups_match_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ])])]})
    cross_group_split_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="blue"),
        make_car(make="Honda", color="red"),
    ])])]})
    cross_group_split_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="blue")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="red")]),
        ])],
    })
    cross_group_split_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Toyota", color="blue"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Honda", color="red"),
            ])]),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Ford", color="green")]),
        ])],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Toyota", color="red"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Ford", color="green"),
            ])]),
        ],
    })
    unrelated_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Ford", color="green"),
    ])])]})

    result = collection.query.fetch_objects(filters=Filter.any_of([
        Filter.all_of([
            Filter.by_property("countries.garages.cars.make").equal("Toyota"),
            Filter.by_property("countries.garages.cars.color").equal("red"),
        ]),
        Filter.all_of([
            Filter.by_property("countries.garages.cars.make").equal("Honda"),
            Filter.by_property("countries.garages.cars.color").equal("blue"),
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
    assert cross_group_split_across_garages_id not in ids, (
        "same cross-group leaf split, distributed across two garages — no single "
        "car satisfies either AND group → must not match"
    )
    assert cross_group_split_across_countries_id not in ids, (
        "same cross-group leaf split, distributed across two countries → must not match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car satisfies group 1 (Toyota+red); Rotterdam unrelated → match"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands' car satisfies group 1 (Toyota+red); Germany unrelated → match"
    )
    assert unrelated_id not in ids, "Ford+green satisfies neither group"


# ===========================================================================
# Scenario: IsNull on a nested leaf — existential per-element semantics
# ===========================================================================
#
# `cars.make IS NULL` matches docs where ∃ car whose make is absent.
# `cars.make IS NOT NULL` matches docs where ∃ car whose make is present.
#
# Both filters can match the SAME doc (one car with make, another without).
# Docs with no cars at all (empty array, no positions to evaluate) are
# vacuous — they match NEITHER filter.


def test_is_null_on_leaf_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars.make IS NULL / IS NOT NULL with existential semantics."""
    collection = make_collection(collection_factory)

    all_have_make_id = collection.data.insert({"cars": [
        make_car(make="Toyota"),
        make_car(make="Honda"),
    ]})
    some_missing_make_id = collection.data.insert({"cars": [
        make_car(make="Toyota"),
        omit_fields(make_car(), "make"),
    ]})
    none_have_make_id = collection.data.insert({"cars": [
        omit_fields(make_car(), "make"),
    ]})
    empty_cars_id = collection.data.insert({"cars": []})

    is_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("cars.make").is_none(True),
    ).objects}
    assert all_have_make_id not in is_null_ids, "all cars have make → IS NULL must not match"
    assert some_missing_make_id in is_null_ids, "∃ car without make → IS NULL matches"
    assert none_have_make_id in is_null_ids, "all cars lack make → IS NULL matches"
    assert empty_cars_id not in is_null_ids, "empty cars[] is vacuous → IS NULL must not match"

    is_not_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("cars.make").is_none(False),
    ).objects}
    assert all_have_make_id in is_not_null_ids, "all cars have make → IS NOT NULL matches"
    assert some_missing_make_id in is_not_null_ids, "∃ car with make → IS NOT NULL matches"
    assert none_have_make_id not in is_not_null_ids, "no car has make → IS NOT NULL must not match"
    assert empty_cars_id not in is_not_null_ids, "empty cars[] is vacuous → IS NOT NULL must not match"


def test_is_null_on_leaf_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — country.garages.cars.make IS NULL / IS NOT NULL."""
    collection = make_collection(collection_factory)

    all_have_make_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
        make_car(make="Honda"),
    ])])})
    some_missing_make_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
        omit_fields(make_car(), "make"),
    ])])})
    none_have_make_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        omit_fields(make_car(), "make"),
    ])])})
    empty_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[])])})

    is_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars.make").is_none(True),
    ).objects}
    assert all_have_make_id not in is_null_ids, "all cars have make → IS NULL must not match"
    assert some_missing_make_id in is_null_ids, "∃ car without make → IS NULL matches"
    assert none_have_make_id in is_null_ids, "all cars lack make → IS NULL matches"
    assert empty_cars_id not in is_null_ids, "empty cars[] is vacuous → IS NULL must not match"

    is_not_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars.make").is_none(False),
    ).objects}
    assert all_have_make_id in is_not_null_ids, "all cars have make → IS NOT NULL matches"
    assert some_missing_make_id in is_not_null_ids, "∃ car with make → IS NOT NULL matches"
    assert none_have_make_id not in is_not_null_ids, "no car has make → IS NOT NULL must not match"
    assert empty_cars_id not in is_not_null_ids, "empty cars[] is vacuous → IS NOT NULL must not match"


def test_is_null_on_leaf_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — countries.garages.cars.make IS NULL / IS NOT NULL."""
    collection = make_collection(collection_factory)

    all_have_make_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
        make_car(make="Honda"),
    ])])]})
    some_missing_make_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
        omit_fields(make_car(), "make"),
    ])])]})
    none_have_make_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        omit_fields(make_car(), "make"),
    ])])]})
    empty_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[])])]})
    some_missing_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota")]),
            make_garage(city="Rotterdam", cars=[omit_fields(make_car(), "make")]),
        ])],
    })
    some_missing_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[make_car(make="Toyota")])]),
            make_country(name="Germany", garages=[make_garage(cars=[omit_fields(make_car(), "make")])]),
        ],
    })

    is_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars.make").is_none(True),
    ).objects}
    assert all_have_make_id not in is_null_ids, "all cars have make → IS NULL must not match"
    assert some_missing_make_id in is_null_ids, "∃ car without make → IS NULL matches"
    assert none_have_make_id in is_null_ids, "all cars lack make → IS NULL matches"
    assert empty_cars_id not in is_null_ids, "empty cars[] is vacuous → IS NULL must not match"
    assert some_missing_across_garages_id in is_null_ids, (
        "∃ car without make in some garage → IS NULL matches"
    )
    assert some_missing_across_countries_id in is_null_ids, (
        "∃ car without make in some country → IS NULL matches"
    )

    is_not_null_ids = {o.uuid for o in collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars.make").is_none(False),
    ).objects}
    assert all_have_make_id in is_not_null_ids, "all cars have make → IS NOT NULL matches"
    assert some_missing_make_id in is_not_null_ids, "∃ car with make → IS NOT NULL matches"
    assert none_have_make_id not in is_not_null_ids, "no car has make → IS NOT NULL must not match"
    assert empty_cars_id not in is_not_null_ids, "empty cars[] is vacuous → IS NOT NULL must not match"
    assert some_missing_across_garages_id in is_not_null_ids, (
        "∃ car with make in some garage → IS NOT NULL matches"
    )
    assert some_missing_across_countries_id in is_not_null_ids, (
        "∃ car with make in some country → IS NOT NULL matches"
    )


# ===========================================================================
# Scenario: ContainsAll on a scalar-array leaf — same-element semantics
# ===========================================================================
#
# `cars.model_codes contains_all [a, b]` matches docs where ∃ a single car
# whose model_codes array contains BOTH a and b. Splitting the two values
# across separate cars must NOT match — both values must live on the same car.


def test_contains_all_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars.model_codes contains_all requires one car to have all values."""
    collection = make_collection(collection_factory)

    one_car_has_both_id = collection.data.insert({"cars": [
        make_car(model_codes=["CAM-2020", "HYB-PRO"]),
    ]})
    one_car_has_one_id = collection.data.insert({"cars": [
        make_car(model_codes=["CAM-2020"]),
    ]})
    split_across_cars_id = collection.data.insert({"cars": [
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ]})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("cars.model_codes").contains_all(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert one_car_has_both_id in ids, "single car with both values → match"
    assert one_car_has_one_id not in ids, "missing one value → must not match"
    assert split_across_cars_id not in ids, (
        "values split across two cars → must not match (same-element rule)"
    )


def test_contains_all_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — country.garages.cars.model_codes contains_all same-element rule."""
    collection = make_collection(collection_factory)

    one_car_has_both_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020", "HYB-PRO"]),
    ])])})
    one_car_has_one_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
    ])])})
    split_across_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ])])})
    split_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["CAM-2020"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["HYB-PRO"])]),
        ]),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["CAM-2020", "HYB-PRO"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["OTHER"])]),
        ]),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars.model_codes")
        .contains_all(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert one_car_has_both_id in ids, "single car with both values → match"
    assert one_car_has_one_id not in ids, "missing one value → must not match"
    assert split_across_cars_id not in ids, (
        "values split across two cars → must not match (same-element rule)"
    )
    assert split_across_garages_id not in ids, (
        "values split across two garages — still no single car owns both → no match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car has both values; Rotterdam unrelated → match"
    )


def test_contains_all_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — countries.garages.cars.model_codes contains_all same-element rule."""
    collection = make_collection(collection_factory)

    one_car_has_both_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020", "HYB-PRO"]),
    ])])]})
    one_car_has_one_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
    ])])]})
    split_across_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ])])]})
    split_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["CAM-2020"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["HYB-PRO"])]),
        ])],
    })
    split_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(model_codes=["CAM-2020"]),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(model_codes=["HYB-PRO"]),
            ])]),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["CAM-2020", "HYB-PRO"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["OTHER"])]),
        ])],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(model_codes=["CAM-2020", "HYB-PRO"]),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(model_codes=["OTHER"]),
            ])]),
        ],
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars.model_codes")
        .contains_all(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert one_car_has_both_id in ids, "single car with both values → match"
    assert one_car_has_one_id not in ids, "missing one value → must not match"
    assert split_across_cars_id not in ids, (
        "values split across two cars → must not match (same-element rule)"
    )
    assert split_across_garages_id not in ids, (
        "values split across two garages — still no single car owns both → no match"
    )
    assert split_across_countries_id not in ids, (
        "values split across two countries — still no single car owns both → no match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car has both values; Rotterdam unrelated → match"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands' car has both values; Germany unrelated → match"
    )


# ===========================================================================
# Scenario: ContainsAny on a scalar-array leaf — natural existential
# ===========================================================================
#
# `cars.model_codes contains_any [a, b]` matches docs where ∃ a car has any
# of the listed values in its model_codes. Cross-car distribution (one car
# owns a, another owns b) still matches — any element owning any listed
# value is enough.


def test_contains_any_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars.model_codes contains_any: ∃ car has any listed value."""
    collection = make_collection(collection_factory)

    one_car_has_one_listed_id = collection.data.insert({"cars": [
        make_car(model_codes=["CAM-2020", "OUTSIDE-A"]),
    ]})
    split_listed_values_id = collection.data.insert({"cars": [
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ]})
    all_values_outside_list_id = collection.data.insert({"cars": [
        make_car(model_codes=["OUTSIDE-A"]),
        make_car(model_codes=["OUTSIDE-B"]),
    ]})
    empty_cars_id = collection.data.insert({"cars": []})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("cars.model_codes").contains_any(["CAM-2020", "HYB-PRO"]),
    ).objects
    ids = {o.uuid for o in result}
    assert one_car_has_one_listed_id in ids, "car owns CAM-2020 → match"
    assert split_listed_values_id in ids, (
        "values split across cars (car1=CAM-2020, car2=HYB-PRO) → match "
        "(any car with any listed value is enough)"
    )
    assert all_values_outside_list_id not in ids, "no car owns any listed value → no match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_contains_any_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — country.garages.cars.model_codes contains_any: ∃ car has any listed value."""
    collection = make_collection(collection_factory)

    one_car_has_one_listed_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020", "OUTSIDE-A"]),
    ])])})
    split_listed_values_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ])])})
    all_values_outside_list_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["OUTSIDE-A"]),
        make_car(model_codes=["OUTSIDE-B"]),
    ])])})
    empty_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[])])})
    split_listed_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["CAM-2020"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["HYB-PRO"])]),
        ]),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars.model_codes")
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
    assert split_listed_across_garages_id in ids, (
        "values split across two garages → match (any car owns any listed value)"
    )


def test_contains_any_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — countries.garages.cars.model_codes contains_any: ∃ car has any listed value."""
    collection = make_collection(collection_factory)

    one_car_has_one_listed_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020", "OUTSIDE-A"]),
    ])])]})
    split_listed_values_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ])])]})
    all_values_outside_list_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["OUTSIDE-A"]),
        make_car(model_codes=["OUTSIDE-B"]),
    ])])]})
    empty_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[])])]})
    split_listed_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["CAM-2020"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["HYB-PRO"])]),
        ])],
    })
    split_listed_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(model_codes=["CAM-2020"]),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(model_codes=["HYB-PRO"]),
            ])]),
        ],
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars.model_codes")
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
    assert split_listed_across_garages_id in ids, (
        "values split across two garages → match (any car owns any listed value)"
    )
    assert split_listed_across_countries_id in ids, (
        "values split across two countries → match (any car owns any listed value)"
    )


# ===========================================================================
# Scenario: ContainsNone on a scalar-array leaf — per-tag-element existential
# ===========================================================================
#
# `cars.model_codes contains_none [a, b]` matches docs where ∃ a model_code
# value (anywhere across all cars' model_codes arrays) that is NOT in the
# listed values. A car owning a listed value does NOT disqualify the doc
# if some other car (or even the same car) has another value outside the
# list. Only when EVERY model_code across all cars is in the listed values
# does the doc fail to match.
#
# Docs that have no model_code values at all (empty cars[], or cars with
# no model_codes set) drop — there's no value to be "outside the list".


def test_contains_none_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars.model_codes contains_none: ∃ tag outside the list semantics."""
    collection = make_collection(collection_factory)

    all_values_in_list_id = collection.data.insert({"cars": [
        make_car(model_codes=["CAM-2020", "HYB-PRO"]),
    ]})
    all_values_in_list_split_id = collection.data.insert({"cars": [
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ]})
    has_outside_value_id = collection.data.insert({"cars": [
        make_car(model_codes=["OUTSIDE-X"]),
    ]})
    mixed_inside_and_outside_id = collection.data.insert({"cars": [
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["OUTSIDE-X"]),
    ]})
    empty_cars_id = collection.data.insert({"cars": []})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("cars.model_codes").contains_none(["CAM-2020", "HYB-PRO"]),
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


def test_contains_none_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — same per-tag-element existential semantics inside country.garages[]."""
    collection = make_collection(collection_factory)

    all_values_in_list_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020", "HYB-PRO"]),
    ])])})
    all_values_in_list_split_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ])])})
    has_outside_value_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["OUTSIDE-X"]),
    ])])})
    mixed_inside_and_outside_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["OUTSIDE-X"]),
    ])])})
    all_in_list_split_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["CAM-2020"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["HYB-PRO"])]),
        ]),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["OUTSIDE-X"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["CAM-2020"])]),
        ]),
    })
    empty_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[])])})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars.model_codes")
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
    assert all_in_list_split_across_garages_id not in ids, (
        "every tag is in the list (split across garages) → no match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car has OUTSIDE-X → match (Rotterdam's CAM-2020 doesn't disqualify)"
    )
    assert empty_cars_id not in ids, (
        "empty cars[] is vacuous — no model_code positions to evaluate → no match"
    )


def test_contains_none_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — same per-tag-element existential semantics, deeper path."""
    collection = make_collection(collection_factory)

    all_values_in_list_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020", "HYB-PRO"]),
    ])])]})
    all_values_in_list_split_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["HYB-PRO"]),
    ])])]})
    has_outside_value_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["OUTSIDE-X"]),
    ])])]})
    mixed_inside_and_outside_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model_codes=["CAM-2020"]),
        make_car(model_codes=["OUTSIDE-X"]),
    ])])]})
    all_in_list_split_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["CAM-2020"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["HYB-PRO"])]),
        ])],
    })
    all_in_list_split_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(model_codes=["CAM-2020"]),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(model_codes=["HYB-PRO"]),
            ])]),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model_codes=["OUTSIDE-X"])]),
            make_garage(city="Rotterdam", cars=[make_car(model_codes=["CAM-2020"])]),
        ])],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(model_codes=["OUTSIDE-X"]),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(model_codes=["CAM-2020"]),
            ])]),
        ],
    })
    empty_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[])])]})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars.model_codes")
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
    assert all_in_list_split_across_garages_id not in ids, (
        "every tag is in the list (split across garages) → no match"
    )
    assert all_in_list_split_across_countries_id not in ids, (
        "every tag is in the list (split across countries) → no match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car has OUTSIDE-X → match (Rotterdam's CAM-2020 doesn't disqualify)"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands' car has OUTSIDE-X → match (Germany's CAM-2020 doesn't disqualify)"
    )
    assert empty_cars_id not in ids, (
        "empty cars[] is vacuous — no model_code positions to evaluate → no match"
    )


# ===========================================================================
# Scenario: multi-token Equal on a tokenized text leaf
# ===========================================================================
#
# `cars.model` is a WORD-tokenized text property. The filter value
# "Camry Hybrid" tokenizes to ["camry", "hybrid"]; both tokens must be
# present in the same car's `model` field for that car to satisfy the
# filter. Splitting tokens across separate cars must NOT match — the
# same-element rule that applies to AND across leaves also applies to
# AND across tokens within a single multi-token leaf.


def test_multi_token_equal_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars.model = 'Camry Hybrid' requires both tokens on the same car."""
    collection = make_collection(collection_factory)

    both_tokens_one_car_id = collection.data.insert({"cars": [
        make_car(model="Camry Hybrid"),
    ]})
    single_token_one_car_id = collection.data.insert({"cars": [
        make_car(model="Camry"),
    ]})
    tokens_split_across_cars_id = collection.data.insert({"cars": [
        make_car(model="Camry"),
        make_car(model="Hybrid"),
    ]})
    unrelated_id = collection.data.insert({"cars": [
        make_car(model="Civic LX"),
    ]})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("cars.model").equal("Camry Hybrid"),
    ).objects
    ids = {o.uuid for o in result}
    assert both_tokens_one_car_id in ids, "both tokens on the same car → match"
    assert single_token_one_car_id not in ids, "only 'camry' present, 'hybrid' missing → no match"
    assert tokens_split_across_cars_id not in ids, (
        "'camry' on car 0 and 'hybrid' on car 1 — same-element rule on tokens, must not match"
    )
    assert unrelated_id not in ids, "neither token present → no match"


def test_multi_token_equal_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — multi-token Equal inside country.garages.cars."""
    collection = make_collection(collection_factory)

    both_tokens_one_car_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model="Camry Hybrid"),
    ])])})
    single_token_one_car_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model="Camry"),
    ])])})
    tokens_split_across_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model="Camry"),
        make_car(model="Hybrid"),
    ])])})
    tokens_split_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model="Camry")]),
            make_garage(city="Rotterdam", cars=[make_car(model="Hybrid")]),
        ]),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model="Camry Hybrid")]),
            make_garage(city="Rotterdam", cars=[make_car(model="Civic LX")]),
        ]),
    })
    unrelated_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(model="Civic LX"),
    ])])})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars.model").equal("Camry Hybrid"),
    ).objects
    ids = {o.uuid for o in result}
    assert both_tokens_one_car_id in ids, "both tokens on the same car → match"
    assert single_token_one_car_id not in ids, "only 'camry' present, 'hybrid' missing → no match"
    assert tokens_split_across_cars_id not in ids, (
        "tokens split across cars in one garage — must not match (same-element on tokens)"
    )
    assert tokens_split_across_garages_id not in ids, (
        "tokens split across two garages — still no car has both tokens → no match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car has both tokens; Rotterdam unrelated → match"
    )
    assert unrelated_id not in ids, "neither token present → no match"


def test_multi_token_equal_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — multi-token Equal inside countries.garages.cars."""
    collection = make_collection(collection_factory)

    both_tokens_one_car_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model="Camry Hybrid"),
    ])])]})
    single_token_one_car_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model="Camry"),
    ])])]})
    tokens_split_across_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model="Camry"),
        make_car(model="Hybrid"),
    ])])]})
    tokens_split_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model="Camry")]),
            make_garage(city="Rotterdam", cars=[make_car(model="Hybrid")]),
        ])],
    })
    tokens_split_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[make_car(model="Camry")])]),
            make_country(name="Germany", garages=[make_garage(cars=[make_car(model="Hybrid")])]),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(model="Camry Hybrid")]),
            make_garage(city="Rotterdam", cars=[make_car(model="Civic LX")]),
        ])],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[make_car(model="Camry Hybrid")])]),
            make_country(name="Germany", garages=[make_garage(cars=[make_car(model="Civic LX")])]),
        ],
    })
    unrelated_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(model="Civic LX"),
    ])])]})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars.model").equal("Camry Hybrid"),
    ).objects
    ids = {o.uuid for o in result}
    assert both_tokens_one_car_id in ids, "both tokens on the same car → match"
    assert single_token_one_car_id not in ids, "only 'camry' present, 'hybrid' missing → no match"
    assert tokens_split_across_cars_id not in ids, (
        "tokens split across cars in one garage — must not match (same-element on tokens)"
    )
    assert tokens_split_across_garages_id not in ids, (
        "tokens split across two garages — still no car has both tokens → no match"
    )
    assert tokens_split_across_countries_id not in ids, (
        "tokens split across two countries — still no car has both tokens → no match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car has both tokens; Rotterdam unrelated → match"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands' car has both tokens; Germany unrelated → match"
    )
    assert unrelated_id not in ids, "neither token present → no match"


# ===========================================================================
# Scenario: NOT of a correlated AND
# ===========================================================================
#
# Filter: `NOT (cars.make=Toyota AND cars.color=red)`
#
# The inner AND correlates per car at the cars LCA. The NOT inverts at the
# same LCA, existentially: the doc matches when ∃ a car that does NOT
# satisfy the inner AND. A doc with cars=[{Toyota,red}, {Honda,blue}]
# matches — the Honda car violates the AND, satisfying the existential.
# Only docs where EVERY car satisfies the inner AND fail to match.
#
# Vacuous docs (empty cars[], or no cars at all) drop — there's no car
# position to evaluate the inverted predicate against.


def test_not_of_correlated_and_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — NOT(cars.make=Toyota AND cars.color=red) is existential per-car."""
    collection = make_collection(collection_factory)

    only_satisfying_car_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="red"),
    ]})
    single_violating_car_id = collection.data.insert({"cars": [
        make_car(make="Honda", color="blue"),
    ]})
    mixed_satisfying_and_violating_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ]})
    all_satisfying_cars_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="red"),
        make_car(make="Toyota", color="red"),
    ]})
    empty_cars_id = collection.data.insert({"cars": []})

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("cars.make").equal("Toyota"),
        Filter.by_property("cars.color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert only_satisfying_car_id not in ids, (
        "single Toyota+red car satisfies the AND → no violating car → no match"
    )
    assert single_violating_car_id in ids, "Honda+blue violates the AND → match"
    assert mixed_satisfying_and_violating_id in ids, (
        "Honda+blue car violates the AND → match (existential per-car)"
    )
    assert all_satisfying_cars_id not in ids, (
        "every car satisfies the AND → no violating car → no match"
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_not_of_correlated_and_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — NOT(country.garages.cars.make=T AND country.garages.cars.color=r)."""
    collection = make_collection(collection_factory)

    only_satisfying_car_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
    ])])})
    single_violating_car_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Honda", color="blue"),
    ])])})
    mixed_satisfying_and_violating_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ])])})
    all_satisfying_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
        make_car(make="Toyota", color="red"),
    ])])})
    empty_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[])])})
    mixed_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="blue")]),
        ]),
    })
    all_satisfying_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Toyota", color="red")]),
        ]),
    })

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("country.garages.cars.make").equal("Toyota"),
        Filter.by_property("country.garages.cars.color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert only_satisfying_car_id not in ids, "every car satisfies → no match"
    assert single_violating_car_id in ids, "Honda+blue violates → match"
    assert mixed_satisfying_and_violating_id in ids, "Honda violates → match"
    assert all_satisfying_cars_id not in ids, "every car satisfies → no match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"
    assert mixed_across_garages_id in ids, (
        "Rotterdam's car violates → match (existential spans garages)"
    )
    assert all_satisfying_across_garages_id not in ids, (
        "every car across all garages satisfies → no match"
    )


def test_not_of_correlated_and_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — NOT(countries.garages.cars.make=T AND countries.garages.cars.color=r)."""
    collection = make_collection(collection_factory)

    only_satisfying_car_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
    ])])]})
    single_violating_car_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Honda", color="blue"),
    ])])]})
    mixed_satisfying_and_violating_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ])])]})
    all_satisfying_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
        make_car(make="Toyota", color="red"),
    ])])]})
    empty_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[])])]})
    mixed_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="blue")]),
        ])],
    })
    all_satisfying_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Toyota", color="red")]),
        ])],
    })
    mixed_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[make_car(make="Toyota", color="red")])]),
            make_country(name="Germany", garages=[make_garage(cars=[make_car(make="Honda", color="blue")])]),
        ],
    })
    all_satisfying_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[make_car(make="Toyota", color="red")])]),
            make_country(name="Germany", garages=[make_garage(cars=[make_car(make="Toyota", color="red")])]),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("countries.garages.cars.make").equal("Toyota"),
        Filter.by_property("countries.garages.cars.color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert only_satisfying_car_id not in ids, "every car satisfies → no match"
    assert single_violating_car_id in ids, "Honda+blue violates → match"
    assert mixed_satisfying_and_violating_id in ids, "Honda violates → match"
    assert all_satisfying_cars_id not in ids, "every car satisfies → no match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"
    assert mixed_across_garages_id in ids, (
        "Rotterdam's car violates → match (existential spans garages)"
    )
    assert all_satisfying_across_garages_id not in ids, (
        "every car across all garages satisfies → no match"
    )
    assert mixed_across_countries_id in ids, (
        "Germany's car violates → match (existential spans countries)"
    )
    assert all_satisfying_across_countries_id not in ids, (
        "every car across all countries satisfies → no match"
    )


# ===========================================================================
# Scenario: IsNull combined with a value leaf inside a correlated AND
# ===========================================================================
#
# Filter: `cars.color = red AND cars.make IS NULL`
#
# Both leaves correlate per car at the cars LCA — the doc matches when ∃ a
# car that simultaneously has color=red AND no make field. Splitting the
# two leaves across separate cars (one car satisfies color, another
# satisfies the IsNull) does NOT match.


def test_is_null_in_correlated_and_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — IsNull + value leaf, correlated per car."""
    collection = make_collection(collection_factory)

    match_same_car_id = collection.data.insert({"cars": [
        omit_fields(make_car(color="red"), "make"),
    ]})
    only_color_satisfied_id = collection.data.insert({"cars": [
        make_car(color="red"),  # color matches, make defaults to "Toyota" (not null)
    ]})
    only_is_null_satisfied_id = collection.data.insert({"cars": [
        omit_fields(make_car(color="blue"), "make"),  # make null but color doesn't match
    ]})
    split_across_cars_id = collection.data.insert({"cars": [
        make_car(color="red"),                              # color matches, make=Toyota
        omit_fields(make_car(color="blue"), "make"),       # make null, color=blue
    ]})
    match_with_distractor_car_id = collection.data.insert({"cars": [
        omit_fields(make_car(color="red"), "make"),  # satisfies both
        make_car(color="blue"),                       # unrelated
    ]})
    empty_cars_id = collection.data.insert({"cars": []})

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("cars.color").equal("red"),
        Filter.by_property("cars.make").is_none(True),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_same_car_id in ids, "single car has color=red and no make → match"
    assert only_color_satisfied_id not in ids, "color=red but make=Toyota — make not null → no match"
    assert only_is_null_satisfied_id not in ids, "make null but color=blue → no match"
    assert split_across_cars_id not in ids, (
        "color=red on one car, make-null on a different car — same-element rule fails → no match"
    )
    assert match_with_distractor_car_id in ids, "first car satisfies both → match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_is_null_in_correlated_and_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — IsNull + value leaf inside country.garages.cars."""
    collection = make_collection(collection_factory)

    match_same_car_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        omit_fields(make_car(color="red"), "make"),
    ])])})
    only_color_satisfied_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(color="red"),
    ])])})
    only_is_null_satisfied_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        omit_fields(make_car(color="blue"), "make"),
    ])])})
    split_across_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(color="red"),
        omit_fields(make_car(color="blue"), "make"),
    ])])})
    empty_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[])])})
    split_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(color="red")]),
            make_garage(city="Rotterdam", cars=[omit_fields(make_car(color="blue"), "make")]),
        ]),
    })
    match_via_one_garage_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[omit_fields(make_car(color="red"), "make")]),
            make_garage(city="Rotterdam", cars=[make_car(color="blue")]),
        ]),
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("country.garages.cars.color").equal("red"),
        Filter.by_property("country.garages.cars.make").is_none(True),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_same_car_id in ids, "single car has color=red and no make → match"
    assert only_color_satisfied_id not in ids, "make not null → no match"
    assert only_is_null_satisfied_id not in ids, "color doesn't match → no match"
    assert split_across_cars_id not in ids, "leaves split across cars in same garage → no match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"
    assert split_across_garages_id not in ids, (
        "color in Amsterdam, make-null in Rotterdam — same-element rule fails → no match"
    )
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car satisfies both; Rotterdam unrelated → match"
    )


def test_is_null_in_correlated_and_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — IsNull + value leaf inside countries.garages.cars."""
    collection = make_collection(collection_factory)

    match_same_car_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        omit_fields(make_car(color="red"), "make"),
    ])])]})
    only_color_satisfied_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(color="red"),
    ])])]})
    only_is_null_satisfied_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        omit_fields(make_car(color="blue"), "make"),
    ])])]})
    split_across_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(color="red"),
        omit_fields(make_car(color="blue"), "make"),
    ])])]})
    empty_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[])])]})
    split_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(color="red")]),
            make_garage(city="Rotterdam", cars=[omit_fields(make_car(color="blue"), "make")]),
        ])],
    })
    split_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[make_car(color="red")])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                omit_fields(make_car(color="blue"), "make"),
            ])]),
        ],
    })
    match_via_one_garage_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[omit_fields(make_car(color="red"), "make")]),
            make_garage(city="Rotterdam", cars=[make_car(color="blue")]),
        ])],
    })
    match_via_one_country_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                omit_fields(make_car(color="red"), "make"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[make_car(color="blue")])]),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.all_of([
        Filter.by_property("countries.garages.cars.color").equal("red"),
        Filter.by_property("countries.garages.cars.make").is_none(True),
    ])).objects
    ids = {o.uuid for o in result}
    assert match_same_car_id in ids, "single car has color=red and no make → match"
    assert only_color_satisfied_id not in ids, "make not null → no match"
    assert only_is_null_satisfied_id not in ids, "color doesn't match → no match"
    assert split_across_cars_id not in ids, "leaves split across cars in same garage → no match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"
    assert split_across_garages_id not in ids, "leaves split across two garages → no match"
    assert split_across_countries_id not in ids, "leaves split across two countries → no match"
    assert match_via_one_garage_id in ids, (
        "Amsterdam's car satisfies both; Rotterdam unrelated → match"
    )
    assert match_via_one_country_id in ids, (
        "Netherlands' car satisfies both; Germany unrelated → match"
    )


# ===========================================================================
# Scenario: NOT of a pinned correlated AND
# ===========================================================================
#
# Filter: `NOT (cars[0].make=Toyota AND cars[0].color=red)`
#
# The inner AND correlates at the pinned cars[0] position. NOT inverts at
# the same LCA existentially: the doc matches when ∃ a cars[0] position
# (across all garages / countries) that does NOT satisfy the inner AND.
# A doc whose cars[0] satisfies the AND in every garage/country fails to
# match. A doc with no cars (empty array) is vacuous → no match.


def test_not_of_pinned_correlated_and_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — NOT(cars[0].make=Toyota AND cars[0].color=red)."""
    collection = make_collection(collection_factory)

    pinned_satisfies_and_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="red"),
    ]})
    pinned_only_make_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="blue"),
    ]})
    pinned_only_color_id = collection.data.insert({"cars": [
        make_car(make="Honda", color="red"),
    ]})
    pinned_neither_id = collection.data.insert({"cars": [
        make_car(make="Honda", color="blue"),
    ]})
    match_at_wrong_index_id = collection.data.insert({"cars": [
        make_car(make="Honda", color="blue"),
        make_car(make="Toyota", color="red"),
    ]})
    pinned_satisfies_with_distractor_id = collection.data.insert({"cars": [
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ]})
    empty_cars_id = collection.data.insert({"cars": []})

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("cars[0].make").equal("Toyota"),
        Filter.by_property("cars[0].color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert pinned_satisfies_and_id not in ids, "cars[0] satisfies the inner AND → NOT excludes"
    assert pinned_only_make_id in ids, "cars[0] only matches make (color=blue) → NOT matches"
    assert pinned_only_color_id in ids, "cars[0] only matches color (make=Honda) → NOT matches"
    assert pinned_neither_id in ids, "cars[0] matches neither → NOT matches"
    assert match_at_wrong_index_id in ids, (
        "cars[0] is Honda+blue (violates inner AND); cars[1] satisfies but the pin "
        "restricts evaluation to position 0 → NOT matches"
    )
    # TODO aliszka:nested_filtering: flip to `not in ids` once
    # NOT-of-compound lifts the arr[N] pin into the inverted universe.
    # Under the design contract cars[0]={Toyota, red} satisfies the
    # inner AND → no violating cars[0] anywhere → doc must not match.
    # The current impl evaluates each car (pin not lifted), so the
    # extra Honda+blue car at position 1 flips this doc in.
    assert pinned_satisfies_with_distractor_id in ids, (
        "cars[0] satisfies the inner AND but the doc still matches — current "
        "impl ignores the pin under NOT-of-compound and the extra car at "
        "cars[1] flips the doc in. See TODO above; should flip to NOT match "
        "after pin-lift fix."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_not_of_pinned_correlated_and_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — NOT of a pinned correlated AND inside country.garages.cars."""
    collection = make_collection(collection_factory)

    pinned_satisfies_and_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
    ])])})
    pinned_only_make_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="blue"),
    ])])})
    pinned_neither_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Honda", color="blue"),
    ])])})
    match_at_wrong_index_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Honda", color="blue"),
        make_car(make="Toyota", color="red"),
    ])])})
    pinned_satisfies_with_distractor_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ])])})
    empty_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[])])})
    all_garages_pinned_satisfy_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Toyota", color="red")]),
        ]),
    })
    one_garage_pinned_violates_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="blue")]),
        ]),
    })

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("country.garages.cars[0].make").equal("Toyota"),
        Filter.by_property("country.garages.cars[0].color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert pinned_satisfies_and_id not in ids, "cars[0] satisfies → NOT excludes"
    assert pinned_only_make_id in ids, "cars[0] only matches make → NOT matches"
    assert pinned_neither_id in ids, "cars[0] matches neither → NOT matches"
    assert match_at_wrong_index_id in ids, (
        "cars[0] is Honda+blue (violates); cars[1] satisfies but pin restricts to [0] → match"
    )
    # TODO aliszka:nested_filtering: flip to `not in ids` once
    # NOT-of-compound lifts the arr[N] pin into the inverted universe.
    # Same shape as the L0 test's pinned_satisfies_with_distractor —
    # cars[0] satisfies the inner AND so the doc must not match under
    # the design contract, but the current impl ignores the pin and
    # the extra Honda+blue car at cars[1] flips the doc in.
    assert pinned_satisfies_with_distractor_id in ids, (
        "current impl: extra car at cars[1] leaks the doc through NOT-of-compound "
        "with arr[N] pin. Should flip to NOT match after pin-lift fix."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"
    assert all_garages_pinned_satisfy_id not in ids, (
        "every garage's cars[0] satisfies → no violating cars[0] anywhere → no match"
    )
    assert one_garage_pinned_violates_id in ids, (
        "Rotterdam's cars[0] doesn't satisfy → ∃ violating cars[0] → match"
    )


def test_not_of_pinned_correlated_and_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — NOT of a pinned correlated AND inside countries.garages.cars."""
    collection = make_collection(collection_factory)

    pinned_satisfies_and_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
    ])])]})
    pinned_only_make_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="blue"),
    ])])]})
    pinned_neither_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Honda", color="blue"),
    ])])]})
    match_at_wrong_index_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Honda", color="blue"),
        make_car(make="Toyota", color="red"),
    ])])]})
    pinned_satisfies_with_distractor_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota", color="red"),
        make_car(make="Honda", color="blue"),
    ])])]})
    empty_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[])])]})
    all_garages_pinned_satisfy_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Toyota", color="red")]),
        ])],
    })
    one_garage_pinned_violates_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(make="Toyota", color="red")]),
            make_garage(city="Rotterdam", cars=[make_car(make="Honda", color="blue")]),
        ])],
    })
    all_countries_pinned_satisfy_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Toyota", color="red"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Toyota", color="red"),
            ])]),
        ],
    })
    one_country_pinned_violates_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(make="Toyota", color="red"),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                make_car(make="Honda", color="blue"),
            ])]),
        ],
    })

    result = collection.query.fetch_objects(filters=Filter.not_(Filter.all_of([
        Filter.by_property("countries.garages.cars[0].make").equal("Toyota"),
        Filter.by_property("countries.garages.cars[0].color").equal("red"),
    ]))).objects
    ids = {o.uuid for o in result}
    assert pinned_satisfies_and_id not in ids, "cars[0] satisfies → NOT excludes"
    assert pinned_only_make_id in ids, "cars[0] only matches make → NOT matches"
    assert pinned_neither_id in ids, "cars[0] matches neither → NOT matches"
    assert match_at_wrong_index_id in ids, (
        "cars[0] is Honda+blue (violates); cars[1] satisfies but pin restricts to [0] → match"
    )
    # TODO aliszka:nested_filtering: flip to `not in ids` once
    # NOT-of-compound lifts the arr[N] pin into the inverted universe.
    # Same shape as the L0/L2_object equivalents — cars[0] satisfies
    # the inner AND so the doc must not match, but the current impl
    # ignores the pin and the extra Honda+blue car at cars[1] flips
    # the doc in.
    assert pinned_satisfies_with_distractor_id in ids, (
        "current impl: extra car at cars[1] leaks the doc through NOT-of-compound "
        "with arr[N] pin. Should flip to NOT match after pin-lift fix."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"
    assert all_garages_pinned_satisfy_id not in ids, (
        "every garage's cars[0] satisfies → no match"
    )
    assert one_garage_pinned_violates_id in ids, (
        "Rotterdam's cars[0] doesn't satisfy → ∃ violating cars[0] → match"
    )
    assert all_countries_pinned_satisfy_id not in ids, (
        "every country's cars[0] satisfies → no match"
    )
    assert one_country_pinned_violates_id in ids, (
        "Germany's cars[0] doesn't satisfy → ∃ violating cars[0] → match"
    )


# ===========================================================================
# Scenario: Pinned IsNull on a leaf inside a pinned position
# ===========================================================================
#
# Filter: `cars[1].make IS NULL`
#
# The pin restricts evaluation to the car at position 1. The IsNull applies
# to that car's `make` field. Under the strict existential rule the filter
# matches when ∃ a cars[1] position where make is null — which requires the
# cars[1] position to actually exist.
#
# Two design polarities:
#   - Strict (current impl): cars[1] must exist AND its make must be null.
#     Docs lacking a cars[1] (only one car, or empty cars[]) drop —
#     vacuous, no position to evaluate.
#   - User-intent (deferred recovery): a doc that lacks a cars[1] also
#     "satisfies" the IS NULL (the position is null because it doesn't
#     exist). This is what users typically want from queries like
#     "addresses[1] IS NULL — find docs lacking a 2nd address."
#
# Tests below lock in the strict-existential behavior with TODO comments
# above the `cars1_missing_id` assertion: those should flip to `in ids`
# once the recovery lands (parent-scope existence keys or per-element
# iteration).


def test_pinned_is_null_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars[1].make IS NULL pinned-IsNull."""
    collection = make_collection(collection_factory)

    cars1_present_with_make_id = collection.data.insert({"cars": [
        make_car(make="Toyota"),
        make_car(make="Honda"),
    ]})
    cars1_present_no_make_id = collection.data.insert({"cars": [
        make_car(make="Toyota"),
        omit_fields(make_car(), "make"),
    ]})
    cars1_missing_id = collection.data.insert({"cars": [
        make_car(make="Toyota"),
    ]})
    empty_cars_id = collection.data.insert({"cars": []})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("cars[1].make").is_none(True),
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


def test_pinned_is_null_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — country.garages.cars[1].make IS NULL pinned-IsNull."""
    collection = make_collection(collection_factory)

    cars1_present_with_make_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
        make_car(make="Honda"),
    ])])})
    cars1_present_no_make_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
        omit_fields(make_car(), "make"),
    ])])})
    cars1_missing_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
    ])])})
    empty_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[])])})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars[1].make").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    assert cars1_present_with_make_id not in ids, (
        "cars[1] present with make → no match"
    )
    assert cars1_present_no_make_id in ids, (
        "cars[1] present, make is null → match"
    )
    # TODO aliszka:nested_filtering: flip to `in ids` once the pinned-
    # IsNull recovery lands. Same shape as L0 — strict existential
    # currently drops "no cars[1]" vacuously; user-intent semantics
    # would match.
    assert cars1_missing_id not in ids, (
        "current impl (strict existential): cars[1] doesn't exist → "
        "vacuous drop. Should flip after the pinned-IsNull recovery."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_pinned_is_null_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — countries.garages.cars[1].make IS NULL pinned-IsNull."""
    collection = make_collection(collection_factory)

    cars1_present_with_make_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
        make_car(make="Honda"),
    ])])]})
    cars1_present_no_make_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
        omit_fields(make_car(), "make"),
    ])])]})
    cars1_missing_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(make="Toyota"),
    ])])]})
    empty_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[])])]})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars[1].make").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    assert cars1_present_with_make_id not in ids, (
        "cars[1] present with make → no match"
    )
    assert cars1_present_no_make_id in ids, (
        "cars[1] present, make is null → match"
    )
    # TODO aliszka:nested_filtering: flip to `in ids` once the pinned-
    # IsNull recovery lands. Same shape as L0 — strict existential
    # currently drops "no cars[1]" vacuously; user-intent semantics
    # would match.
    assert cars1_missing_id not in ids, (
        "current impl (strict existential): cars[1] doesn't exist → "
        "vacuous drop. Should flip after the pinned-IsNull recovery."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


# ===========================================================================
# Scenario: IsNull on an object[] sub-property
# ===========================================================================
#
# Filter: `cars.tires IS NULL`
#
# `tires` is an object[] sub-property of `cars`. Under existential-per-
# element semantics at the cars LCA, the filter matches when ∃ a car
# whose tires array is absent or empty. A car with a non-empty tires
# array does NOT satisfy.
#
# This shape exercises a known encoding gap (multi-element / multi-leaf
# at sub-root LCAs). A single car with multiple tire entries should not
# match (tires present, not null), but the gap can cause contamination
# in some cases. Tests below assert observed current behavior; the
# relevant discriminator is annotated with a TODO if the impl disagrees
# with the design contract.


def test_is_null_on_object_array_subprop_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — cars.tires IS NULL (object[] sub-property)."""
    collection = make_collection(collection_factory)

    car_with_tires_id = collection.data.insert({"cars": [
        make_car(tires=[{"width": 215, "brand": "Michelin"}]),
    ]})
    car_no_tires_id = collection.data.insert({"cars": [
        omit_fields(make_car(), "tires"),
    ]})
    car_empty_tires_id = collection.data.insert({"cars": [
        make_car(tires=[]),
    ]})
    mixed_cars_id = collection.data.insert({"cars": [
        make_car(tires=[{"width": 215, "brand": "Michelin"}]),
        omit_fields(make_car(), "tires"),
    ]})
    car_with_multiple_tires_id = collection.data.insert({"cars": [
        make_car(tires=[
            {"width": 215, "brand": "Michelin"},
            {"width": 215, "brand": "Michelin"},
        ]),
    ]})
    empty_cars_id = collection.data.insert({"cars": []})

    result = collection.query.fetch_objects(
        filters=Filter.by_property("cars.tires").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    # TODO aliszka:nested_filtering: flip to `not in ids` once the
    # multi-element / multi-leaf encoding gap for IsNull on array-typed
    # sub-properties is closed. Under the design contract this car has
    # tires → tires not null → no match. Current impl over-includes
    # every doc that has any car (encoding contamination at sub-root
    # LCAs).
    assert car_with_tires_id in ids, (
        "current impl: encoding-gap over-inclusion — car with tires still flags "
        "the doc as matching IS NULL. Should flip to `not in ids` after fix."
    )
    assert car_no_tires_id in ids, "car has no tires field → ∃ car with null tires → match"
    assert car_empty_tires_id in ids, "car has empty tires → ∃ car with null tires → match"
    assert mixed_cars_id in ids, "second car has no tires → ∃ car with null tires → match"
    # TODO aliszka:nested_filtering: flip to `not in ids` once the gap
    # is closed. Same as car_with_tires_id — current impl flags every
    # doc with at least one car.
    assert car_with_multiple_tires_id in ids, (
        "current impl: encoding-gap over-inclusion — car with multiple tires "
        "still flags as IS NULL match. Should flip to `not in ids` after fix."
    )
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"


def test_is_null_on_object_array_subprop_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — country.garages.cars.tires IS NULL."""
    collection = make_collection(collection_factory)

    car_with_tires_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(tires=[{"width": 215, "brand": "Michelin"}]),
    ])])})
    car_no_tires_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        omit_fields(make_car(), "tires"),
    ])])})
    car_empty_tires_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(tires=[]),
    ])])})
    mixed_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(tires=[{"width": 215, "brand": "Michelin"}]),
        omit_fields(make_car(), "tires"),
    ])])})
    empty_cars_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[])])})
    mixed_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(tires=[{"width": 215, "brand": "Michelin"}])]),
            make_garage(city="Rotterdam", cars=[omit_fields(make_car(), "tires")]),
        ]),
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("country.garages.cars.tires").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    # TODO aliszka:nested_filtering: flip to `not in ids` once the
    # multi-element / multi-leaf encoding gap for IsNull on array-typed
    # sub-properties is closed. Same shape as L0 — car with tires
    # currently flags as IS NULL match (encoding-gap over-inclusion).
    assert car_with_tires_id in ids, (
        "current impl: encoding-gap over-inclusion. Should flip after fix."
    )
    assert car_no_tires_id in ids, "car has no tires → match"
    assert car_empty_tires_id in ids, "car has empty tires → match"
    assert mixed_cars_id in ids, "∃ car with no tires → match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"
    assert mixed_across_garages_id in ids, (
        "Rotterdam's car has no tires → ∃ car with null tires → match"
    )


def test_is_null_on_object_array_subprop_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — countries.garages.cars.tires IS NULL."""
    collection = make_collection(collection_factory)

    car_with_tires_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(tires=[{"width": 215, "brand": "Michelin"}]),
    ])])]})
    car_no_tires_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        omit_fields(make_car(), "tires"),
    ])])]})
    car_empty_tires_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(tires=[]),
    ])])]})
    mixed_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(tires=[{"width": 215, "brand": "Michelin"}]),
        omit_fields(make_car(), "tires"),
    ])])]})
    empty_cars_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[])])]})
    mixed_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car(tires=[{"width": 215, "brand": "Michelin"}])]),
            make_garage(city="Rotterdam", cars=[omit_fields(make_car(), "tires")]),
        ])],
    })
    mixed_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[
                make_car(tires=[{"width": 215, "brand": "Michelin"}]),
            ])]),
            make_country(name="Germany", garages=[make_garage(cars=[
                omit_fields(make_car(), "tires"),
            ])]),
        ],
    })

    result = collection.query.fetch_objects(
        filters=Filter.by_property("countries.garages.cars.tires").is_none(True),
    ).objects
    ids = {o.uuid for o in result}

    # TODO aliszka:nested_filtering: flip to `not in ids` once the
    # multi-element / multi-leaf encoding gap for IsNull on array-typed
    # sub-properties is closed. Same shape as L0 — car with tires
    # currently flags as IS NULL match (encoding-gap over-inclusion).
    assert car_with_tires_id in ids, (
        "current impl: encoding-gap over-inclusion. Should flip after fix."
    )
    assert car_no_tires_id in ids, "car has no tires → match"
    assert car_empty_tires_id in ids, "car has empty tires → match"
    assert mixed_cars_id in ids, "∃ car with no tires → match"
    assert empty_cars_id not in ids, "empty cars[] is vacuous → no match"
    assert mixed_across_garages_id in ids, (
        "Rotterdam's car has no tires → ∃ car with null tires → match"
    )
    assert mixed_across_countries_id in ids, (
        "Germany's car has no tires → ∃ car with null tires → match"
    )


# ===========================================================================
# Scenario: invalid filters surface a server error
# ===========================================================================
#
# Filters that violate the schema or filter syntax should produce a clear
# error from the server, not silently return wrong results. Covers four
# common validation paths.


def test_invalid_filter_returns_server_error(collection_factory: CollectionFactory) -> None:
    """Each invalid filter shape produces a WeaviateQueryError with a
    message that explains what's wrong."""
    collection = make_collection(collection_factory)
    collection.data.insert({"cars": [make_car()]})

    # Non-existent root property.
    with pytest.raises(WeaviateQueryError, match="no such prop"):
        collection.query.fetch_objects(
            filters=Filter.by_property("nonexistent_root").equal("X"),
        )

    # Filter on a nested-object property without a sub-property — must
    # use dot notation to reach a primitive leaf.
    with pytest.raises(WeaviateQueryError, match="dot notation"):
        collection.query.fetch_objects(
            filters=Filter.by_property("country").equal("X"),
        )

    # Non-existent sub-property along an otherwise-valid nested path.
    with pytest.raises(WeaviateQueryError, match="sub-property .* not found"):
        collection.query.fetch_objects(
            filters=Filter.by_property("cars.nonexistent").equal("X"),
        )

    # arr[N] indexing applied to a non-array sub-property (country.name
    # is text, not text[]).
    with pytest.raises(WeaviateQueryError, match="indexing requires an array type"):
        collection.query.fetch_objects(
            filters=Filter.by_property("country.name[0]").equal("X"),
        )

    # Filter terminates on a sub-property that is itself an object[],
    # not a primitive leaf — cars.tires is object[], must descend to a
    # primitive (e.g. cars.tires.width).
    with pytest.raises(WeaviateQueryError, match="filtering on object types is not supported"):
        collection.query.fetch_objects(
            filters=Filter.by_property("cars.tires").equal("X"),
        )

    # Path tries to descend past a scalar leaf — cars.make is text, but
    # the filter tries to navigate further (cars.make.foo). Intermediate
    # segments must be object or object[].
    with pytest.raises(WeaviateQueryError, match=r"must be object or object\[\]"):
        collection.query.fetch_objects(
            filters=Filter.by_property("cars.make.foo").equal("X"),
        )

    # len(...) length filter on a nested property root — length
    # filtering is not supported for nested properties.
    with pytest.raises(WeaviateQueryError, match="property length filtering is not supported"):
        collection.query.fetch_objects(
            filters=Filter.by_property("cars", length=True).equal(5),
        )


# ===========================================================================
# Scenario: comparison operators (gt / lt / gte / lte / like) on nested leaves
# ===========================================================================
#
# Each operator follows existential-at-cars-LCA semantics: the doc matches
# when ∃ a car whose leaf value satisfies the comparison. Coverage spans
# numeric (year — int, price — number), date (delivered_at) and text-with-
# pattern (make — Like).


def _honda_2015() -> Dict[str, Any]:
    """A Honda configured to be the low-value counterpart to the default Toyota."""
    return make_car(
        make="Honda",
        year=2015,
        price=18000.0,
        delivered_at="2015-09-01T00:00:00Z",
    )


def test_comparison_operators_at_root_cars(collection_factory: CollectionFactory) -> None:
    """L0 — gt/lt/gte/lte/like on nested cars leaves."""
    collection = make_collection(collection_factory)

    toyota_id = collection.data.insert({"cars": [make_car()]})
    honda_id = collection.data.insert({"cars": [_honda_2015()]})

    _assert_comparison_operator_sweep_selects(
        collection,
        cars_path="cars",
        high_only_ids=[toyota_id],
        low_only_ids=[honda_id],
    )


def test_comparison_operators_under_country_object(collection_factory: CollectionFactory) -> None:
    """L2_object — comparison operators inside country.garages.cars."""
    collection = make_collection(collection_factory)

    toyota_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        make_car(),
    ])])})
    honda_id = collection.data.insert({"country": make_country(garages=[make_garage(cars=[
        _honda_2015(),
    ])])})
    toyota_and_honda_across_garages_id = collection.data.insert({
        "country": make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car()]),
            make_garage(city="Rotterdam", cars=[_honda_2015()]),
        ]),
    })

    _assert_comparison_operator_sweep_selects(
        collection,
        cars_path="country.garages.cars",
        high_only_ids=[toyota_id],
        low_only_ids=[honda_id],
        mixed_ids=[toyota_and_honda_across_garages_id],
    )


def test_comparison_operators_under_countries_array(collection_factory: CollectionFactory) -> None:
    """L2_array — comparison operators inside countries.garages.cars."""
    collection = make_collection(collection_factory)

    toyota_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        make_car(),
    ])])]})
    honda_id = collection.data.insert({"countries": [make_country(garages=[make_garage(cars=[
        _honda_2015(),
    ])])]})
    toyota_and_honda_across_garages_id = collection.data.insert({
        "countries": [make_country(garages=[
            make_garage(city="Amsterdam", cars=[make_car()]),
            make_garage(city="Rotterdam", cars=[_honda_2015()]),
        ])],
    })
    toyota_and_honda_across_countries_id = collection.data.insert({
        "countries": [
            make_country(name="Netherlands", garages=[make_garage(cars=[make_car()])]),
            make_country(name="Germany", garages=[make_garage(cars=[_honda_2015()])]),
        ],
    })

    _assert_comparison_operator_sweep_selects(
        collection,
        cars_path="countries.garages.cars",
        high_only_ids=[toyota_id],
        low_only_ids=[honda_id],
        mixed_ids=[
            toyota_and_honda_across_garages_id,
            toyota_and_honda_across_countries_id,
        ],
    )
