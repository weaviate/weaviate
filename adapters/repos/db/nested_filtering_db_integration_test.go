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

//go:build integrationTest

package db

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

// TestNestedFilteringViaShardWritePath exercises nested property filtering
// end-to-end using the full production write and search pipelines:
//
//	write: repo.PutObject → putObjectLSM → updateInvertedIndexLSM
//	            → extendNestedInvertedIndicesLSM → RoaringSetAddBatch
//	read:  repo.Search → Searcher → extractNestedProp
//	            → docBitmapInvertedRoaringSet → MaskAllPositions
//
// A shared filterCase table is run against both sub-tests, with each case
// declaring its expected result set explicitly for each sub-test:
//
//   - doc123/124/125: design document reference objects stored as nestedObject
//     (DataTypeObject). All operators, deeply nested paths, AND/OR.
//   - doc998/999: same data stored as nestedArray (DataTypeObjectArray).
//     doc998 = [doc123Data] (one root), doc999 = [doc124Data, doc125Data]
//     (two roots) — verifies object[] behaves identically to object.
func TestNestedFilteringViaShardWritePath(t *testing.T) {
	const nestedClass = "Article"
	vTrue := true

	fullNestedProps := []*models.NestedProperty{
		{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{Name: "lastname", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{Name: "nicknames", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
			},
		},
		{
			Name:     "addresses",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{Name: "numbers", DataType: schema.DataTypeNumberArray.PropString(), IndexFilterable: &vTrue},
			},
		},
		{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
		{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
				{
					Name:     "tires",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
						{Name: "radiuses", DataType: schema.DataTypeIntArray.PropString(), IndexFilterable: &vTrue},
					},
				},
				{
					Name:     "accessories",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "type", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
					},
				},
				{Name: "colors", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word", IndexFilterable: &vTrue},
			},
		},
	}

	const (
		id123 = strfmt.UUID("00000000-0000-0000-0000-000000000123")
		id124 = strfmt.UUID("00000000-0000-0000-0000-000000000124")
		id125 = strfmt.UUID("00000000-0000-0000-0000-000000000125")
		id998 = strfmt.UUID("00000000-0000-0000-0000-000000000998")
		id999 = strfmt.UUID("00000000-0000-0000-0000-000000000999")
	)

	makeFilter := func(path string, op filters.Operator, vt schema.DataType, val any) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: op,
			Value:    &filters.Value{Type: vt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(a, b *filters.LocalFilter) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{*a.Root, *b.Root},
		}}
	}
	orFilter := func(a, b *filters.LocalFilter) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorOr,
			Operands: []filters.Clause{*a.Root, *b.Root},
		}}
	}

	// UUIDs used in the update sub-tests — defined here so filterCases can
	// reference them directly.
	const (
		id200 = strfmt.UUID("00000000-0000-0000-0000-000000000200")
		id201 = strfmt.UUID("00000000-0000-0000-0000-000000000201")
		id300 = strfmt.UUID("00000000-0000-0000-0000-000000000300")
	)

	// f builds a filter from a relative sub-path and operator.
	f := func(subPath string, op filters.Operator, vt schema.DataType, val any) func(string) *filters.LocalFilter {
		return func(p string) *filters.LocalFilter { return makeFilter(p+"."+subPath, op, vt, val) }
	}

	// filterCase holds the filter builder and the expected document IDs for
	// every sub-test operation directly — no derived helper methods needed.
	//
	// Object type sub-tests use doc123/id123, doc124/id124, doc125/id125.
	// Array type sub-tests use doc998/id998 (=[doc123Data]) and
	// doc999/id999 (=[doc124Data,doc125Data]).
	// Delete sub-tests remove doc123 / doc998 respectively.
	// Update sub-tests replace id201 (doc123Data→doc125Data) /
	// id300 ([doc123Data]→[doc124Data,doc125Data]).
	type filterCase struct {
		name             string
		filter           func(propName string) *filters.LocalFilter
		matchesObjectAdd []strfmt.UUID // object type: after write
		matchesArrayAdd  []strfmt.UUID // array type:  after write
		matchesObjectDel []strfmt.UUID // object type: after deleting doc123
		matchesArrayDel  []strfmt.UUID // array type:  after deleting doc998
		matchesObjectUpd []strfmt.UUID // object type: after updating id201 with doc125Data
		matchesArrayUpd  []strfmt.UUID // array type:  after updating id300 with [doc124,doc125]
	}

	e := []strfmt.UUID{} // empty — no match

	filterCases := []filterCase{
		// owner sub-properties
		{
			name: "owner.firstname Marsha", filter: f("owner.firstname", filters.OperatorEqual, schema.DataTypeText, "marsha"),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "owner.firstname Justin", filter: f("owner.firstname", filters.OperatorEqual, schema.DataTypeText, "justin"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "owner.firstname Anna", filter: f("owner.firstname", filters.OperatorEqual, schema.DataTypeText, "anna"),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "owner.nicknames Marshmallow", filter: f("owner.nicknames", filters.OperatorEqual, schema.DataTypeText, "marshmallow"),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "owner.nicknames watch", filter: f("owner.nicknames", filters.OperatorEqual, schema.DataTypeText, "watch"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		// addresses
		{
			name: "addresses.city Berlin", filter: f("addresses.city", filters.OperatorEqual, schema.DataTypeText, "berlin"),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "addresses.city Madrid", filter: f("addresses.city", filters.OperatorEqual, schema.DataTypeText, "madrid"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "addresses.city London", filter: f("addresses.city", filters.OperatorEqual, schema.DataTypeText, "london"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "addresses.city Paris", filter: f("addresses.city", filters.OperatorEqual, schema.DataTypeText, "paris"),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "addresses.city Munich no match", filter: f("addresses.city", filters.OperatorEqual, schema.DataTypeText, "munich"),
			matchesObjectAdd: e, matchesArrayAdd: e,
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "addresses.numbers == 1123", filter: f("addresses.numbers", filters.OperatorEqual, schema.DataTypeNumber, float64(1123)),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "addresses.numbers > 200", filter: f("addresses.numbers", filters.OperatorGreaterThan, schema.DataTypeNumber, float64(200)),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "addresses.numbers >= 124", filter: f("addresses.numbers", filters.OperatorGreaterThanEqual, schema.DataTypeNumber, float64(124)),
			matchesObjectAdd: []strfmt.UUID{id123, id124, id125}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id124, id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200, id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "addresses.numbers < 125", filter: f("addresses.numbers", filters.OperatorLessThan, schema.DataTypeNumber, float64(125)),
			matchesObjectAdd: []strfmt.UUID{id123, id124}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "addresses.numbers <= 125", filter: f("addresses.numbers", filters.OperatorLessThanEqual, schema.DataTypeNumber, float64(125)),
			matchesObjectAdd: []strfmt.UUID{id123, id124, id125}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id124, id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200, id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		// tags
		{
			name: "tags german", filter: f("tags", filters.OperatorEqual, schema.DataTypeText, "german"),
			matchesObjectAdd: []strfmt.UUID{id123, id124}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "tags electric", filter: f("tags", filters.OperatorEqual, schema.DataTypeText, "electric"),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "tags premium", filter: f("tags", filters.OperatorEqual, schema.DataTypeText, "premium"),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		// cars
		{
			name: "cars.make BMW", filter: f("cars.make", filters.OperatorEqual, schema.DataTypeText, "bmw"),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "cars.make Audi", filter: f("cars.make", filters.OperatorEqual, schema.DataTypeText, "audi"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.make Kia", filter: f("cars.make", filters.OperatorEqual, schema.DataTypeText, "kia"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.make Tesla", filter: f("cars.make", filters.OperatorEqual, schema.DataTypeText, "tesla"),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.colors white", filter: f("cars.colors", filters.OperatorEqual, schema.DataTypeText, "white"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.accessories.type charger", filter: f("cars.accessories.type", filters.OperatorEqual, schema.DataTypeText, "charger"),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		// tires.width — deeply nested int, all four comparison operators
		{
			name: "cars.tires.width == 225", filter: f("cars.tires.width", filters.OperatorEqual, schema.DataTypeInt, 225),
			matchesObjectAdd: []strfmt.UUID{id123, id124}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.width == 205", filter: f("cars.tires.width", filters.OperatorEqual, schema.DataTypeInt, 205),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.width == 245", filter: f("cars.tires.width", filters.OperatorEqual, schema.DataTypeInt, 245),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.width > 240", filter: f("cars.tires.width", filters.OperatorGreaterThan, schema.DataTypeInt, 240),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.width >= 225", filter: f("cars.tires.width", filters.OperatorGreaterThanEqual, schema.DataTypeInt, 225),
			matchesObjectAdd: []strfmt.UUID{id123, id124, id125}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id124, id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200, id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.width < 200", filter: f("cars.tires.width", filters.OperatorLessThan, schema.DataTypeInt, 200),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.width <= 205", filter: f("cars.tires.width", filters.OperatorLessThanEqual, schema.DataTypeInt, 205),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.width <= 225", filter: f("cars.tires.width", filters.OperatorLessThanEqual, schema.DataTypeInt, 225),
			matchesObjectAdd: []strfmt.UUID{id123, id124}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		// tires.radiuses — int array
		{
			name: "cars.tires.radiuses == 19", filter: f("cars.tires.radiuses", filters.OperatorEqual, schema.DataTypeInt, 19),
			matchesObjectAdd: []strfmt.UUID{id123, id125}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.radiuses == 17", filter: f("cars.tires.radiuses", filters.OperatorEqual, schema.DataTypeInt, 17),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.tires.radiuses == 20", filter: f("cars.tires.radiuses", filters.OperatorEqual, schema.DataTypeInt, 20),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		// compound AND / OR
		{
			name: "Berlin AND BMW",
			filter: func(p string) *filters.LocalFilter {
				return andFilter(makeFilter(p+".addresses.city", filters.OperatorEqual, schema.DataTypeText, "berlin"),
					makeFilter(p+".cars.make", filters.OperatorEqual, schema.DataTypeText, "bmw"))
			},
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "Anna AND Tesla",
			filter: func(p string) *filters.LocalFilter {
				return andFilter(makeFilter(p+".owner.firstname", filters.OperatorEqual, schema.DataTypeText, "anna"),
					makeFilter(p+".cars.make", filters.OperatorEqual, schema.DataTypeText, "tesla"))
			},
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "Berlin AND Justin (empty)",
			filter: func(p string) *filters.LocalFilter {
				return andFilter(makeFilter(p+".addresses.city", filters.OperatorEqual, schema.DataTypeText, "berlin"),
					makeFilter(p+".owner.firstname", filters.OperatorEqual, schema.DataTypeText, "justin"))
			},
			matchesObjectAdd: e, matchesArrayAdd: e,
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "Berlin OR Paris",
			filter: func(p string) *filters.LocalFilter {
				return orFilter(makeFilter(p+".addresses.city", filters.OperatorEqual, schema.DataTypeText, "berlin"),
					makeFilter(p+".addresses.city", filters.OperatorEqual, schema.DataTypeText, "paris"))
			},
			matchesObjectAdd: []strfmt.UUID{id123, id125}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		// LIKE operator — exercises the prefix-bounded cursor in RowReaderRoaringSet.
		// Word tokenization lowercases input, so patterns must also be lowercase.
		{
			name: "owner.firstname LIKE mar* (prefix)", filter: f("owner.firstname", filters.OperatorLike, schema.DataTypeText, "mar*"),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "owner.firstname LIKE *ustin (suffix)", filter: f("owner.firstname", filters.OperatorLike, schema.DataTypeText, "*ustin"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "owner.firstname LIKE ann* (prefix)", filter: f("owner.firstname", filters.OperatorLike, schema.DataTypeText, "ann*"),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "owner.firstname LIKE *a (Marsha and Anna)", filter: f("owner.firstname", filters.OperatorLike, schema.DataTypeText, "*a"),
			matchesObjectAdd: []strfmt.UUID{id123, id125}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "addresses.city LIKE ber*", filter: f("addresses.city", filters.OperatorLike, schema.DataTypeText, "ber*"),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "cars.make LIKE bm*", filter: f("cars.make", filters.OperatorLike, schema.DataTypeText, "bm*"),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			name: "cars.make LIKE *ia (Kia)", filter: f("cars.make", filters.OperatorLike, schema.DataTypeText, "*ia"),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			name: "cars.make LIKE tesl*", filter: f("cars.make", filters.OperatorLike, schema.DataTypeText, "tesl*"),
			matchesObjectAdd: []strfmt.UUID{id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		// NotEqual — deny-list semantics: fetches matching positions, strips to
		// docIDs, then inverts against all documents in the shard.
		{
			// "marsha" removed after update (id201 becomes Anna) → deny list empty
			// → all remaining docs returned.
			name: "owner.firstname != marsha", filter: f("owner.firstname", filters.OperatorNotEqual, schema.DataTypeText, "marsha"),
			matchesObjectAdd: []strfmt.UUID{id124, id125}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124, id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200, id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		// ContainsAny / ContainsAll / ContainsNone — decomposed into individual
		// Equal clauses by extractContains, each re-dispatched through
		// extractPropValuePair which routes them to extractNestedProp.
		{
			// ContainsAny: german(id123,id124) ∪ electric(id125) = all three
			name: "tags ContainsAny [german, electric]", filter: f("tags", filters.ContainsAny, schema.DataTypeText, []string{"german", "electric"}),
			matchesObjectAdd: []strfmt.UUID{id123, id124, id125}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id124, id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200, id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			// ContainsAny: premium(id123) ∪ electric(id125) = {id123,id125}
			name: "tags ContainsAny [premium, electric]", filter: f("tags", filters.ContainsAny, schema.DataTypeText, []string{"premium", "electric"}),
			matchesObjectAdd: []strfmt.UUID{id123, id125}, matchesArrayAdd: []strfmt.UUID{id998, id999},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			// ContainsAll: german(id123,id124) ∩ sedan(id124) = {id124}
			name: "tags ContainsAll [german, sedan]", filter: f("tags", filters.ContainsAll, schema.DataTypeText, []string{"german", "sedan"}),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: []strfmt.UUID{id999},
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: []strfmt.UUID{id999},
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: []strfmt.UUID{id300},
		},
		{
			// ContainsAll: german(id123,id124) ∩ premium(id123) = {id123}
			name: "tags ContainsAll [german, premium]", filter: f("tags", filters.ContainsAll, schema.DataTypeText, []string{"german", "premium"}),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			// ContainsNone: NOT(electric(id125) ∪ sedan(id124)) = {id123}
			// Array: electric and sedan both in id999 → NOT({id999}) = {id998}
			name: "tags ContainsNone [electric, sedan]", filter: f("tags", filters.ContainsNone, schema.DataTypeText, []string{"electric", "sedan"}),
			matchesObjectAdd: []strfmt.UUID{id123}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: e, matchesArrayDel: e,
			matchesObjectUpd: e, matchesArrayUpd: e,
		},
		{
			// ContainsNone: NOT(premium(id123) ∪ electric(id125)) = {id124}
			// After delete/update: premium gone, electric still present.
			name: "tags ContainsNone [premium, electric]", filter: f("tags", filters.ContainsNone, schema.DataTypeText, []string{"premium", "electric"}),
			matchesObjectAdd: []strfmt.UUID{id124}, matchesArrayAdd: e,
			matchesObjectDel: []strfmt.UUID{id124}, matchesArrayDel: e,
			matchesObjectUpd: []strfmt.UUID{id200}, matchesArrayUpd: e,
		},
		{
			// Step 10a deny-list operates at document level: doc999 contains Justin
			// in element 0, so the whole document is excluded even though element 1
			// (Anna) would satisfy the condition. Element-level precision requires
			// Step 10b. After array delete/update the only remaining document also
			// contains Justin, so all array results are empty.
			name: "owner.firstname != justin", filter: f("owner.firstname", filters.OperatorNotEqual, schema.DataTypeText, "justin"),
			matchesObjectAdd: []strfmt.UUID{id123, id125}, matchesArrayAdd: []strfmt.UUID{id998},
			matchesObjectDel: []strfmt.UUID{id125}, matchesArrayDel: e,
			matchesObjectUpd: []strfmt.UUID{id201}, matchesArrayUpd: e,
		},
	}

	// Shared document data. doc999 reuses doc124Data and doc125Data as its
	// two root elements, so defining them once avoids duplication.
	doc123Data := map[string]any{
		"name": "subdoc_123",
		"owner": map[string]any{
			"firstname": "Marsha", "lastname": "Mallow",
			"nicknames": []any{"Marshmallow", "M&M"},
		},
		"addresses": []any{
			map[string]any{"city": "Berlin", "postcode": "10115", "numbers": []any{float64(123), float64(1123)}},
		},
		"tags": []any{"german", "premium"},
		"cars": []any{
			map[string]any{
				"make":   "BMW",
				"tires":  []any{map[string]any{"width": float64(225), "radiuses": []any{float64(18), float64(19)}}},
				"colors": []any{"black", "orange"},
			},
		},
	}
	doc124Data := map[string]any{
		"name": "subdoc_124",
		"owner": map[string]any{
			"firstname": "Justin", "lastname": "Time",
			"nicknames": []any{"watch"},
		},
		"addresses": []any{
			map[string]any{"city": "Madrid", "postcode": "28001", "numbers": []any{float64(124)}},
			map[string]any{"city": "London", "postcode": "SW1"},
		},
		"tags": []any{"german", "japanese", "sedan"},
		"cars": []any{
			map[string]any{
				"make": "Audi",
				"tires": []any{
					map[string]any{"width": float64(205), "radiuses": []any{float64(17), float64(18)}},
					map[string]any{"width": float64(225)},
				},
			},
			map[string]any{
				"make":   "Kia",
				"tires":  []any{map[string]any{"width": float64(195), "radiuses": []any{}}},
				"colors": []any{"white"},
			},
		},
	}
	doc125Data := map[string]any{
		"name": "subdoc_125",
		"owner": map[string]any{
			"firstname": "Anna", "lastname": "Wanna",
		},
		"addresses": []any{
			map[string]any{"city": "Paris", "postcode": "75001", "numbers": []any{float64(125)}},
		},
		"tags": []any{"electric"},
		"cars": []any{
			map[string]any{
				"make":        "Tesla",
				"tires":       []any{map[string]any{"width": float64(245), "radiuses": []any{float64(18), float64(19), float64(20)}}},
				"accessories": []any{map[string]any{"type": "charger"}, map[string]any{"type": "mats"}},
				"colors":      []any{"yellow"},
			},
		},
	}
	// doc123 (Marsha/Berlin/BMW), doc124 (Justin/Madrid+London/Audi+Kia),
	// doc125 (Anna/Paris/Tesla) — primary design document examples.
	t.Run("doc123 doc124 doc125 object type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		docs := []models.Object{
			{Class: nestedClass, ID: id123, Properties: map[string]any{"nestedObject": doc123Data}},
			{Class: nestedClass, ID: id124, Properties: map[string]any{"nestedObject": doc124Data}},
			{Class: nestedClass, ID: id125, Properties: map[string]any{"nestedObject": doc125Data}},
		}

		for i := range docs {
			require.NoError(t, db.PutObject(ctx, &docs[i], nil, nil, nil, nil, 0))
		}

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{
				ClassName:  nestedClass,
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    f,
			})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesObjectAdd, search(t, tc.filter("nestedObject")))
			})
		}
	})

	// Both doc998 and doc999 use nestedArray: object[].
	// doc998 wraps doc123Data as a single root element — same data as doc123 but
	// via object[] rather than object, verifying the two types are equivalent.
	// doc999 wraps doc124Data and doc125Data as two root elements.
	t.Run("doc998 one root doc999 two roots object array", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		docs := []models.Object{
			// doc998: doc123Data as a single-root object[] — same data as doc123 in sub-test 1.
			{Class: nestedClass, ID: id998, Properties: map[string]any{"nestedArray": []any{doc123Data}}},
			// doc999: doc124Data + doc125Data as two-root object[].
			{Class: nestedClass, ID: id999, Properties: map[string]any{"nestedArray": []any{doc124Data, doc125Data}}},
		}

		for i := range docs {
			require.NoError(t, db.PutObject(ctx, &docs[i], nil, nil, nil, nil, 0))
		}

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{
				ClassName:  nestedClass,
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    f,
			})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesArrayAdd, search(t, tc.filter("nestedArray")))
			})
		}
	})

	// After deleting doc123, all filters that previously matched it must now
	// return only doc124/doc125 — verifying the delete path removes all
	// nested positions correctly.
	t.Run("doc123/124/125 delete doc123 object type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		for _, obj := range []models.Object{
			{Class: nestedClass, ID: id123, Properties: map[string]any{"nestedObject": doc123Data}},
			{Class: nestedClass, ID: id124, Properties: map[string]any{"nestedObject": doc124Data}},
			{Class: nestedClass, ID: id125, Properties: map[string]any{"nestedObject": doc125Data}},
		} {
			require.NoError(t, db.PutObject(ctx, &obj, nil, nil, nil, nil, 0))
		}

		require.NoError(t, db.DeleteObject(ctx, nestedClass, id123, time.Now(), nil, "", 0))

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: f})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesObjectDel, search(t, tc.filter("nestedObject")))
			})
		}
	})

	// After deleting doc998, filters must return only doc999 — verifying the
	// delete path works correctly for the object[] type as well.
	t.Run("doc998/999 delete doc998 array type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		for _, obj := range []models.Object{
			{Class: nestedClass, ID: id998, Properties: map[string]any{"nestedArray": []any{doc123Data}}},
			{Class: nestedClass, ID: id999, Properties: map[string]any{"nestedArray": []any{doc124Data, doc125Data}}},
		} {
			require.NoError(t, db.PutObject(ctx, &obj, nil, nil, nil, nil, 0))
		}

		require.NoError(t, db.DeleteObject(ctx, nestedClass, id998, time.Now(), nil, "", 0))

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: f})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesArrayDel, search(t, tc.filter("nestedArray")))
			})
		}
	})

	// Update test: id201 is written with doc123Data, id200 with doc124Data, then
	// id201 is updated to doc125Data. After the update:
	//   id200 = doc124Data  (unchanged)
	//   id201 = doc125Data  (was doc123Data, now replaced)
	// Filters must no longer return any doc123Data results, exercising that the
	// delete-then-reindex path in updateInvertedIndexLSM removes old positions.
	t.Run("update doc123→doc125 object type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		// Insert id201 = doc123Data and id200 = doc124Data.
		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id201, Properties: map[string]any{"nestedObject": doc123Data}}, nil, nil, nil, nil, 0))
		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id200, Properties: map[string]any{"nestedObject": doc124Data}}, nil, nil, nil, nil, 0))

		// Update id201: replace doc123Data with doc125Data.
		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id201, Properties: map[string]any{"nestedObject": doc125Data}}, nil, nil, nil, nil, 0))

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: f})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesObjectUpd, search(t, tc.filter("nestedObject")))
			})
		}
	})

	// Update test for the array type: id300 is written with [doc123Data] (same
	// as doc998), then updated to [doc124Data, doc125Data] (same as doc999).
	// After the update id300 holds doc999Data; doc123Data is gone. Filters must
	// return id300 wherever they previously matched doc999 data, and nothing
	// where they previously matched doc998 data.
	t.Run("update doc998→doc999 array type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		// Insert id300 = [doc123Data] (doc998 equivalent).
		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id300, Properties: map[string]any{"nestedArray": []any{doc123Data}}}, nil, nil, nil, nil, 0))

		// Update id300: replace with [doc124Data, doc125Data] (doc999 equivalent).
		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id300, Properties: map[string]any{"nestedArray": []any{doc124Data, doc125Data}}}, nil, nil, nil, nil, 0))

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: f})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesArrayUpd, search(t, tc.filter("nestedArray")))
			})
		}
	})

	makeBatch := func(objs ...models.Object) objects.BatchObjects {
		batch := make(objects.BatchObjects, len(objs))
		for i := range objs {
			batch[i] = objects.BatchObject{OriginalIndex: i, Object: &objs[i], UUID: objs[i].ID}
		}
		return batch
	}

	putBatch := func(t *testing.T, db *DB, ctx context.Context, batch objects.BatchObjects) {
		t.Helper()
		res, err := db.BatchPutObjects(ctx, batch, nil, 0)
		require.NoError(t, err)
		for _, r := range res {
			require.NoError(t, r.Err)
		}
	}

	// Batch write: same as the individual-write add sub-tests but all documents
	// inserted in a single BatchPutObjects call, verifying the batch write path
	// exercises the same nested index pipeline.
	t.Run("batch write doc123/124/125 object type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		putBatch(t, db, ctx, makeBatch(
			models.Object{Class: nestedClass, ID: id123, Properties: map[string]any{"nestedObject": doc123Data}},
			models.Object{Class: nestedClass, ID: id124, Properties: map[string]any{"nestedObject": doc124Data}},
			models.Object{Class: nestedClass, ID: id125, Properties: map[string]any{"nestedObject": doc125Data}},
		))

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: f})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesObjectAdd, search(t, tc.filter("nestedObject")))
			})
		}
	})

	t.Run("batch write doc998/999 array type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		putBatch(t, db, ctx, makeBatch(
			models.Object{Class: nestedClass, ID: id998, Properties: map[string]any{"nestedArray": []any{doc123Data}}},
			models.Object{Class: nestedClass, ID: id999, Properties: map[string]any{"nestedArray": []any{doc124Data, doc125Data}}},
		))

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: f})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesArrayAdd, search(t, tc.filter("nestedArray")))
			})
		}
	})

	// Batch update: a second BatchPutObjects call with the same UUID overwrites
	// the existing document, exercising the delete-then-reindex path just as
	// individual PutObject updates do.
	t.Run("batch update doc123→doc125 object type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		putBatch(t, db, ctx, makeBatch(
			models.Object{Class: nestedClass, ID: id201, Properties: map[string]any{"nestedObject": doc123Data}},
			models.Object{Class: nestedClass, ID: id200, Properties: map[string]any{"nestedObject": doc124Data}},
		))
		putBatch(t, db, ctx, makeBatch(
			models.Object{Class: nestedClass, ID: id201, Properties: map[string]any{"nestedObject": doc125Data}},
		))

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: f})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesObjectUpd, search(t, tc.filter("nestedObject")))
			})
		}
	})

	t.Run("batch update doc998→doc999 array type", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		putBatch(t, db, ctx, makeBatch(
			models.Object{Class: nestedClass, ID: id300, Properties: map[string]any{"nestedArray": []any{doc123Data}}},
		))
		putBatch(t, db, ctx, makeBatch(
			models.Object{Class: nestedClass, ID: id300, Properties: map[string]any{"nestedArray": []any{doc124Data, doc125Data}}},
		))

		search := func(t *testing.T, f *filters.LocalFilter) []strfmt.UUID {
			t.Helper()
			res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: f})
			require.NoError(t, err)
			ids := make([]strfmt.UUID, len(res))
			for i, r := range res {
				ids[i] = r.ID
			}
			return ids
		}

		for _, tc := range filterCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.ElementsMatch(t, tc.matchesArrayUpd, search(t, tc.filter("nestedArray")))
			})
		}
	})
}
