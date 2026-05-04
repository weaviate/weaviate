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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
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

		deletedDocID := getDocID(t, db, nestedClass, id123)
		require.NoError(t, db.DeleteObject(ctx, nestedClass, id123, time.Now(), nil, "", 0))

		assertNoGhostEntries(t, db, nestedClass, "nestedObject", deletedDocID)

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

		deletedDocID := getDocID(t, db, nestedClass, id998)
		require.NoError(t, db.DeleteObject(ctx, nestedClass, id998, time.Now(), nil, "", 0))

		assertNoGhostEntries(t, db, nestedClass, "nestedArray", deletedDocID)

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

	// Update test with vector change (object type): using a different vector forces
	// docIDChanged=true, which fully abandons the old docID. assertNoGhostEntries
	// then verifies the old docID is completely absent from all nested buckets —
	// the same strong check used for deletes. Without vector change the docID is
	// preserved and the same docID legitimately appears in new keys, so this
	// check cannot be used.
	t.Run("update doc123→doc125 object type with vector", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		vecA := []float32{1, 0, 0}
		vecB := []float32{0, 1, 0}

		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id201, Properties: map[string]any{"nestedObject": doc123Data}}, vecA, nil, nil, nil, 0))
		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id200, Properties: map[string]any{"nestedObject": doc124Data}}, vecA, nil, nil, nil, 0))

		oldDocID := getDocID(t, db, nestedClass, id201)

		// Different vector forces docIDChanged=true — old docID is fully abandoned.
		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id201, Properties: map[string]any{"nestedObject": doc125Data}}, vecB, nil, nil, nil, 0))

		assertNoGhostEntries(t, db, nestedClass, "nestedObject", oldDocID)

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

	// Update test with vector change (array type): same rationale as the object
	// type variant above — vector change forces docIDChanged=true.
	t.Run("update doc998→doc999 array type with vector", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: fullNestedProps},
			},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()

		vecA := []float32{1, 0, 0}
		vecB := []float32{0, 1, 0}

		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id300, Properties: map[string]any{"nestedArray": []any{doc123Data}}}, vecA, nil, nil, nil, 0))

		oldDocID := getDocID(t, db, nestedClass, id300)

		// Different vector forces docIDChanged=true — old docID is fully abandoned.
		require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: id300, Properties: map[string]any{"nestedArray": []any{doc124Data, doc125Data}}}, vecB, nil, nil, nil, 0))

		assertNoGhostEntries(t, db, nestedClass, "nestedArray", oldDocID)

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

// getDocID returns the internal docID for an object identified by its UUID.
func getDocID(t *testing.T, db *DB, className string, id strfmt.UUID) uint64 {
	t.Helper()
	index := db.indices[indexID(schema.ClassName(className))]
	require.NotNil(t, index, "index %q not found", className)
	var (
		docID uint64
		found bool
	)
	err := index.IterateShards(context.Background(), func(_ *Index, shard ShardLike) error {
		obj, err := shard.ObjectByID(context.Background(), id, search.SelectProperties{}, additional.Properties{})
		if err != nil || obj == nil {
			return err
		}
		docID = obj.DocID
		found = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "object %q not found in any shard of class %q", id, className)
	return docID
}

// assertNoGhostEntries scans all entries in both the filterable and meta nested
// buckets for propName and asserts that deletedDocID does not appear in any
// position. This verifies that delete properly cleaned up all nested index entries.
func assertNoGhostEntries(t *testing.T, db *DB, className, propName string, deletedDocID uint64) {
	t.Helper()
	index := db.indices[indexID(schema.ClassName(className))]
	require.NotNil(t, index, "index %q not found", className)

	bucketNames := []string{
		helpers.BucketNestedFromPropNameLSM(propName),
		helpers.BucketNestedMetaFromPropNameLSM(propName),
	}

	err := index.IterateShards(context.Background(), func(_ *Index, shard ShardLike) error {
		for _, bucketName := range bucketNames {
			bucket := shard.Store().Bucket(bucketName)
			if bucket == nil {
				continue
			}
			func() {
				c := bucket.CursorRoaringSet()
				defer c.Close()
				for k, bm := c.First(); k != nil; k, bm = c.Next() {
					for _, pos := range bm.ToArray() {
						if invnested.DecodeDocID(pos) == deletedDocID {
							t.Errorf("ghost entry in bucket %q: position %d references deleted docID %d",
								bucketName, pos, deletedDocID)
						}
					}
				}
			}()
		}
		return nil
	})
	require.NoError(t, err)
}

// allDatatypesNestedProps returns a schema with one leaf property of every
// supported scalar and scalar-array type, suitable for all-datatype tests.
func allDatatypesNestedProps(vTrue *bool) []*models.NestedProperty {
	return []*models.NestedProperty{
		{Name: "text", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: vTrue},
		{Name: "integer", DataType: schema.DataTypeInt.PropString(), IndexFilterable: vTrue},
		{Name: "number", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: vTrue},
		{Name: "boolean", DataType: schema.DataTypeBoolean.PropString(), IndexFilterable: vTrue},
		{Name: "date", DataType: schema.DataTypeDate.PropString(), IndexFilterable: vTrue},
		{Name: "uuid", DataType: schema.DataTypeUUID.PropString(), IndexFilterable: vTrue},
		{Name: "texts", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word", IndexFilterable: vTrue},
		{Name: "integers", DataType: schema.DataTypeIntArray.PropString(), IndexFilterable: vTrue},
		{Name: "numbers", DataType: schema.DataTypeNumberArray.PropString(), IndexFilterable: vTrue},
		{Name: "booleans", DataType: schema.DataTypeBooleanArray.PropString(), IndexFilterable: vTrue},
		{Name: "dates", DataType: schema.DataTypeDateArray.PropString(), IndexFilterable: vTrue},
		{Name: "uuids", DataType: schema.DataTypeUUIDArray.PropString(), IndexFilterable: vTrue},
	}
}

// allDatatypesAPIValues returns nested property values as they arrive from the
// JSON/API path: arrays as []any with JSON-typed elements (no enrichSchemaTypes
// applied), scalars as Go primitives. Date and UUID values are strings; numeric
// values are float64 (JSON number).
func allDatatypesAPIValues() map[string]any {
	return map[string]any{
		"text":    "hello world",
		"integer": float64(42),
		"number":  float64(3.14),
		"boolean": true,
		"date":    "2024-01-15T00:00:00Z",
		"uuid":    "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		// arrays as []any — the form produced by json.Unmarshal before enrichSchemaTypes
		"texts":    []any{"foo", "bar"},
		"integers": []any{float64(1), float64(2)},
		"numbers":  []any{float64(1.1), float64(2.2)},
		"booleans": []any{true, false},
		"dates":    []any{"2024-01-15T00:00:00Z", "2024-06-01T00:00:00Z"},
		"uuids":    []any{"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "550e8400-e29b-41d4-a716-446655440000"},
	}
}

// TestNestedFilteringAllDatatypesAPIPath verifies that AnalyzeObject correctly
// analyzes all supported scalar and scalar-array datatypes when values arrive
// in JSON/API form — arrays as []any with JSON-native element types, scalars as
// Go primitives — without any DB round-trip. The object is analyzed in-memory
// before being stored, exercising the write-side analysis path. Both
// DataTypeObject and DataTypeObjectArray are tested.
func TestNestedFilteringAllDatatypesAPIPath(t *testing.T) {
	const nestedClass = "AllTypes"
	const objID = strfmt.UUID("00000000-0000-0000-0000-000000000001")
	vTrue := true

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{
				Name:             "obj",
				DataType:         schema.DataTypeObject.PropString(),
				NestedProperties: allDatatypesNestedProps(&vTrue),
			},
			{
				Name:             "objArray",
				DataType:         schema.DataTypeObjectArray.PropString(),
				NestedProperties: allDatatypesNestedProps(&vTrue),
			},
		},
	}

	// Create DB only for schema access — the object is NOT written to the DB.
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	ctx := context.Background()

	obj := storobj.FromObject(&models.Object{
		Class: nestedClass, ID: objID,
		Properties: map[string]any{
			"obj":      allDatatypesAPIValues(),
			"objArray": []any{allDatatypesAPIValues()},
		},
	}, nil, nil, nil)

	allPaths := []string{
		"text", "integer", "number", "boolean", "date", "uuid",
		"texts", "integers", "numbers", "booleans", "dates", "uuids",
	}

	index := db.indices[indexID(schema.ClassName(nestedClass))]
	require.NotNil(t, index)
	err := index.IterateShards(ctx, func(_ *Index, shard ShardLike) error {
		// Analyze the in-memory object — values are in JSON/API form ([]any,
		// float64, string, bool) with no binary round-trip applied.
		_, _, nestedProps, err := shard.AnalyzeObject(obj)
		require.NoError(t, err)
		require.Len(t, nestedProps, 2, "expected NestedProperty for both 'obj' and 'objArray'")

		for _, np := range nestedProps {
			assert.True(t, np.HasFilterableIndex)
			paths := make(map[string]int)
			for _, v := range np.Values {
				paths[v.Path]++
			}
			for _, p := range allPaths {
				assert.Positive(t, paths[p], "prop %q: expected Values entries for path %q", np.Name, p)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// TestNestedFilteringAllDatatypesFilter verifies that all supported scalar and
// scalar-array datatypes produce correctly searchable index entries. Writes an
// object with API-typed values and runs a filter query for each type, asserting
// the object is returned. Both DataTypeObject and DataTypeObjectArray are tested.
func TestNestedFilteringAllDatatypesFilter(t *testing.T) {
	const nestedClass = "AllTypes"
	const objID = strfmt.UUID("00000000-0000-0000-0000-000000000001")
	vTrue := true

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{
				Name:             "obj",
				DataType:         schema.DataTypeObject.PropString(),
				NestedProperties: allDatatypesNestedProps(&vTrue),
			},
			{
				Name:             "objArray",
				DataType:         schema.DataTypeObjectArray.PropString(),
				NestedProperties: allDatatypesNestedProps(&vTrue),
			},
		},
	}

	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	ctx := context.Background()

	require.NoError(t, db.PutObject(ctx, &models.Object{
		Class: nestedClass, ID: objID,
		Properties: map[string]any{
			"obj":      allDatatypesAPIValues(),
			"objArray": []any{allDatatypesAPIValues()},
		},
	}, nil, nil, nil, nil, 0))

	mustParseDate := func(s string) time.Time {
		t.Helper()
		parsed, err := time.Parse(time.RFC3339, s)
		require.NoError(t, err)
		return parsed
	}
	type filterCase struct {
		name    string
		subPath string
		op      filters.Operator
		vt      schema.DataType
		val     any
	}
	cases := []filterCase{
		{"text scalar", "text", filters.OperatorEqual, schema.DataTypeText, "hello"},
		{"integer scalar", "integer", filters.OperatorEqual, schema.DataTypeInt, 42},
		{"number scalar", "number", filters.OperatorEqual, schema.DataTypeNumber, float64(3.14)},
		{"boolean scalar", "boolean", filters.OperatorEqual, schema.DataTypeBoolean, true},
		{"date scalar", "date", filters.OperatorEqual, schema.DataTypeDate, mustParseDate("2024-01-15T00:00:00Z")},
		{"uuid scalar", "uuid", filters.OperatorEqual, schema.DataTypeText, "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"text array", "texts", filters.OperatorEqual, schema.DataTypeText, "foo"},
		{"integer array", "integers", filters.OperatorEqual, schema.DataTypeInt, 1},
		{"number array", "numbers", filters.OperatorEqual, schema.DataTypeNumber, float64(1.1)},
		{"boolean array", "booleans", filters.OperatorEqual, schema.DataTypeBoolean, true},
		{"date array", "dates", filters.OperatorEqual, schema.DataTypeDate, mustParseDate("2024-01-15T00:00:00Z")},
		{"uuid array", "uuids", filters.OperatorEqual, schema.DataTypeText, "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
	}
	searchFn := func(f *filters.LocalFilter) []strfmt.UUID {
		t.Helper()
		res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 10}, Filters: f})
		require.NoError(t, err)
		ids := make([]strfmt.UUID, len(res))
		for i, r := range res {
			ids[i] = r.ID
		}
		return ids
	}
	for _, propName := range []string{"obj", "objArray"} {
		propName := propName
		t.Run(propName, func(t *testing.T) {
			for _, tc := range cases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					f := &filters.LocalFilter{Root: &filters.Clause{
						Operator: tc.op,
						Value:    &filters.Value{Type: tc.vt, Value: tc.val},
						On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(propName + "." + tc.subPath)},
					}}
					assert.ElementsMatch(t, []strfmt.UUID{objID}, searchFn(f))
				})
			}
		})
	}
}

// TestNestedFilteringAllDatatypesDBReadBack verifies that after a binary
// round-trip (write → storobj.FromBinary → enrichSchemaTypes), AnalyzeObject
// correctly re-analyzes all supported scalar and scalar-array types for both
// DataTypeObject and DataTypeObjectArray. enrichSchemaTypes converts []any
// arrays to typed slices ([]string, []float64, []bool), exercising the
// defensive typed-slice cases in walkScalarArray.
func TestNestedFilteringAllDatatypesDBReadBack(t *testing.T) {
	const nestedClass = "AllTypes"
	const objID = strfmt.UUID("00000000-0000-0000-0000-000000000001")
	vTrue := true

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{
				Name:             "obj",
				DataType:         schema.DataTypeObject.PropString(),
				NestedProperties: allDatatypesNestedProps(&vTrue),
			},
			{
				Name:             "objArray",
				DataType:         schema.DataTypeObjectArray.PropString(),
				NestedProperties: allDatatypesNestedProps(&vTrue),
			},
		},
	}

	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	ctx := context.Background()

	require.NoError(t, db.PutObject(ctx, &models.Object{
		Class: nestedClass, ID: objID,
		Properties: map[string]any{
			"obj":      allDatatypesAPIValues(),
			"objArray": []any{allDatatypesAPIValues()},
		},
	}, nil, nil, nil, nil, 0))

	// Read back the object — properties go through storobj.FromBinary →
	// json.Unmarshal → enrichSchemaTypes, converting []any arrays to typed
	// slices: []string for text/date/uuid, []float64 for int/number, []bool.
	allPaths := []string{
		"text", "integer", "number", "boolean", "date", "uuid",
		"texts", "integers", "numbers", "booleans", "dates", "uuids",
	}

	index := db.indices[indexID(schema.ClassName(nestedClass))]
	require.NotNil(t, index)
	err := index.IterateShards(ctx, func(_ *Index, shard ShardLike) error {
		obj, err := shard.ObjectByID(ctx, objID, search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, obj)

		_, _, nestedProps, err := shard.AnalyzeObject(obj)
		require.NoError(t, err)
		require.Len(t, nestedProps, 2, "expected NestedProperty for both 'obj' and 'objArray'")

		for _, np := range nestedProps {
			assert.True(t, np.HasFilterableIndex)
			paths := make(map[string]int)
			for _, v := range np.Values {
				paths[v.Path]++
			}
			for _, p := range allPaths {
				assert.Positive(t, paths[p], "prop %q: expected Values entries for path %q after binary round-trip", np.Name, p)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// TestNestedFilteringTokenizationCorrelatedAnd exercises the multi-token
// tokenization path end-to-end through the production write+search pipeline.
//
// The hand-built integration tests for tokenization construct propValuePair
// trees directly with childrenFromTokenization=true. This test instead writes
// real text values (e.g. "new york") that the analyzer tokenizes at write
// time, and runs real text filters that the searcher tokenizes at query time.
// It verifies that:
//
//  1. Write-time tokenization stores all tokens of a single value occurrence
//     at the SAME parent-element position (the invariant hardcoded at
//     objects_nested.go analyzeNestedValue line 247: Positions: pv.Positions)
//  2. Search-time tokenization in buildNestedTextFilterPair produces the same
//     decomposition as the analyzer
//  3. The pvp shape produced by buildNestedTextFilterPair + groupNestedByProp
//     (multi-token wrapper as a child of an outer correlated AND) is correctly
//     resolved by the recursive resolver — this shape is NOT directly tested by
//     the hand-built integration tests, which model a different wrapper shape
//  4. Same-element correlation works across the multi-token wrapper child plus
//     a sibling leaf condition
//
// Sub-tests cover:
//   - word_tokenization: 2-token filter ("new york") with sibling postcode
//   - field_tokenization: same filter under field-tokenization (whole string one token)
//   - word_three_token_filter: 3-token filter ("new york city") — stronger AndAll
//   - word_token_order_independence: filter "york new" — proves order doesn't matter
//   - word_no_sibling_condition: filter without postcode — different pvp shape (no
//     groupNestedByProp wrapper; buildNestedTextFilterPair sits at the top)
//   - word_repeated_tokens_filter: filter "new new york" — duplicate tokens collapse
func TestNestedFilteringTokenizationCorrelatedAnd(t *testing.T) {
	const (
		nestedClass = "Article"
		topProp     = "addresses"

		idMatch                 = strfmt.UUID("00000000-0000-0000-0000-000000000001")
		idNoMatchSplit          = strfmt.UUID("00000000-0000-0000-0000-000000000002")
		idNoMatchPostcode       = strfmt.UUID("00000000-0000-0000-0000-000000000003")
		idNoMatchDifferentAddr  = strfmt.UUID("00000000-0000-0000-0000-000000000004")
		idNoMatchExtraToken     = strfmt.UUID("00000000-0000-0000-0000-000000000005")
		idMatchExtraStoredToken = strfmt.UUID("00000000-0000-0000-0000-000000000006")
		idMatchDuplicateAddrs   = strfmt.UUID("00000000-0000-0000-0000-000000000007")
	)
	vTrue := true

	// makeClass builds the test class with the requested tokenization for both
	// city and postcode. Tokenization is set explicitly because
	// createTestDatabaseWithClass bypasses setNestedPropertiesDefaults.
	makeClass := func(tok string) *models.Class {
		return &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{
				{
					Name:     topProp,
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
						{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
					},
				},
			},
		}
	}

	// addressDocs is the shared dataset. Element layout is identical across all
	// sub-tests; what differs is how the analyzer tokenizes the text values at
	// write time and how the searcher tokenizes the filter value at query time.
	addressDocs := []struct {
		id        strfmt.UUID
		addresses []any
	}{
		{
			id: idMatch,
			addresses: []any{
				map[string]any{"city": "new york", "postcode": "10115"},
			},
		},
		{
			// Tokens of "new york" split across two elements; postcode lives in
			// the element holding only "new". Under word tokenization the city
			// tokens land at different leaves, so AndAll on tokens cannot match
			// in either element. Under field tokenization neither element has
			// the literal "new york" string so the city condition itself fails.
			id: idNoMatchSplit,
			addresses: []any{
				map[string]any{"city": "new", "postcode": "10115"},
				map[string]any{"city": "york"},
			},
		},
		{
			id: idNoMatchPostcode,
			addresses: []any{
				map[string]any{"city": "new york"},
			},
		},
		{
			// City matches in element[0]; postcode matches in element[1] —
			// different addresses, so same-element correlation must reject.
			id: idNoMatchDifferentAddr,
			addresses: []any{
				map[string]any{"city": "new york"},
				map[string]any{"postcode": "10115"},
			},
		},
		{
			// Under word tokenization "new yorkville" → ["new","yorkville"] so
			// the "york" token from the filter has no match. Under field
			// tokenization the literal "new york" filter value also doesn't
			// match the literal "new yorkville" stored value.
			id: idNoMatchExtraToken,
			addresses: []any{
				map[string]any{"city": "new yorkville", "postcode": "10115"},
			},
		},
		{
			// Stored value has more tokens than the 2-token filter. Under word
			// tokenization the filter's [new, york] are both at the same leaf
			// (the "new york city" value's element); the extra "city" token
			// doesn't break AndAll. Under field tokenization the whole string
			// "new york city" is one token that doesn't equal "new york".
			id: idMatchExtraStoredToken,
			addresses: []any{
				map[string]any{"city": "new york city", "postcode": "10115"},
			},
		},
		{
			// Two addresses each independently satisfy the filter. The result
			// must contain the doc exactly once (dedup at the docID level).
			id: idMatchDuplicateAddrs,
			addresses: []any{
				map[string]any{"city": "new york", "postcode": "10115"},
				map[string]any{"city": "new york", "postcode": "10115"},
			},
		},
	}

	makeFilter := func(path string, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(a, b *filters.LocalFilter) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{*a.Root, *b.Root},
		}}
	}

	// runScenario writes the dataset and runs the given filter.
	runScenario := func(t *testing.T, tokenization string, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), makeClass(tokenization))
		ctx := context.Background()

		for _, d := range addressDocs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id,
				Properties: map[string]any{topProp: d.addresses},
			}, nil, nil, nil, nil, 0))
		}

		res, err := db.Search(ctx, dto.GetParams{
			ClassName:  nestedClass,
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    filter,
		})
		require.NoError(t, err)

		got := make([]strfmt.UUID, len(res))
		for i, r := range res {
			got[i] = r.ID
		}
		assert.ElementsMatch(t, want, got)
	}

	cityAndPostcode := andFilter(
		makeFilter("addresses.city", "new york"),
		makeFilter("addresses.postcode", "10115"),
	)

	// word_tokenization: filter tokens [new, york] need same-leaf positions
	// AND a sibling postcode at the same address. idMatchExtraStoredToken
	// matches because [new, york] are both at the leaf of "new york city";
	// idMatchDuplicateAddrs matches once via dedup.
	t.Run("word_tokenization", func(t *testing.T) {
		runScenario(t, models.NestedPropertyTokenizationWord, cityAndPostcode,
			[]strfmt.UUID{idMatch, idMatchExtraStoredToken, idMatchDuplicateAddrs})
	})

	// field_tokenization: filter is one token "new york". Only idMatch and
	// idMatchDuplicateAddrs have stored values literally equal to "new york".
	// idMatchExtraStoredToken has "new york city" which is a distinct token.
	t.Run("field_tokenization", func(t *testing.T) {
		runScenario(t, models.NestedPropertyTokenizationField, cityAndPostcode,
			[]strfmt.UUID{idMatch, idMatchDuplicateAddrs})
	})

	// word_three_token_filter: filter tokens [new, york, city] — only
	// idMatchExtraStoredToken stores "new york city" with all three tokens at
	// the same leaf and the matching postcode at the same address.
	t.Run("word_three_token_filter", func(t *testing.T) {
		filter := andFilter(
			makeFilter("addresses.city", "new york city"),
			makeFilter("addresses.postcode", "10115"),
		)
		runScenario(t, models.NestedPropertyTokenizationWord, filter,
			[]strfmt.UUID{idMatchExtraStoredToken})
	})

	// word_token_order_independence: filter value "york new" tokenizes to the
	// same set as "new york". Tokens at storage are unordered per leaf so the
	// filter must produce identical results regardless of token order.
	t.Run("word_token_order_independence", func(t *testing.T) {
		filter := andFilter(
			makeFilter("addresses.city", "york new"),
			makeFilter("addresses.postcode", "10115"),
		)
		runScenario(t, models.NestedPropertyTokenizationWord, filter,
			[]strfmt.UUID{idMatch, idMatchExtraStoredToken, idMatchDuplicateAddrs})
	})

	// word_no_sibling_condition: filter has only the multi-token text
	// condition; no sibling. The pvp shape skips groupNestedByProp wrapping —
	// buildNestedTextFilterPair's wrapper sits at the top of the pvp tree.
	// idNoMatchPostcode and idNoMatchDifferentAddr now match because there is
	// no postcode constraint to fail on.
	t.Run("word_no_sibling_condition", func(t *testing.T) {
		filter := makeFilter("addresses.city", "new york")
		runScenario(t, models.NestedPropertyTokenizationWord, filter,
			[]strfmt.UUID{
				idMatch,
				idNoMatchPostcode,
				idNoMatchDifferentAddr,
				idMatchExtraStoredToken,
				idMatchDuplicateAddrs,
			})
	})

	// word_repeated_tokens_filter: filter value "new new york" has duplicate
	// "new" token. AndAll naturally collapses duplicates; the effective filter
	// is [new, york]. Result equals word_tokenization.
	t.Run("word_repeated_tokens_filter", func(t *testing.T) {
		filter := andFilter(
			makeFilter("addresses.city", "new new york"),
			makeFilter("addresses.postcode", "10115"),
		)
		runScenario(t, models.NestedPropertyTokenizationWord, filter,
			[]strfmt.UUID{idMatch, idMatchExtraStoredToken, idMatchDuplicateAddrs})
	})
}

// TestNestedFilteringF13CorrelatedAndDifferentCarsSameGarage exercises a
// correlated AND filter where two conditions target different cars[N] indices
// inside the same garages array:
//
//	garages.cars[0].make = "honda" AND garages.cars[1].model = "civic"
//
// Same-garage semantics: a doc matches when SOME garage has cars[0].make="honda"
// AND that same garage has cars[1].model="civic". The dataset probes the
// position-precision boundaries — same-garage vs cross-garage, exact index vs
// neighboring index, value at wrong field — across both sparse and densely
// populated documents.
func TestNestedFilteringF13CorrelatedAndDifferentCarsSameGarage(t *testing.T) {
	const nestedClass = "F13"
	vTrue := true

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: &vTrue},
					{
						Name:     "cars",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: &vTrue},
							{Name: "model", DataType: schema.DataTypeText.PropString(), Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: &vTrue},
						},
					},
				},
			},
		},
	}

	// car / garage builders for compact dataset literals.
	car := func(props ...string) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i]] = props[i+1]
		}
		return out
	}
	garage := func(cars ...map[string]any) map[string]any {
		anyCars := make([]any, len(cars))
		for i, c := range cars {
			anyCars[i] = c
		}
		return map[string]any{"cars": anyCars}
	}
	garageWithCity := func(city string) map[string]any {
		return map[string]any{"city": city}
	}

	const (
		idMatchMinimal                = strfmt.UUID("00000000-0000-0000-0000-000000000001")
		idMatchTwoCarsBoth            = strfmt.UUID("00000000-0000-0000-0000-000000000002")
		idMatchExtraCar               = strfmt.UUID("00000000-0000-0000-0000-000000000003")
		idMatchSecondGarage           = strfmt.UUID("00000000-0000-0000-0000-000000000004")
		idNoMatchSplitGarages         = strfmt.UUID("00000000-0000-0000-0000-000000000005")
		idNoMatchSwapped              = strfmt.UUID("00000000-0000-0000-0000-000000000006")
		idNoMatchOnlyMakeNoSecondCar  = strfmt.UUID("00000000-0000-0000-0000-000000000007")
		idNoMatchOnlyMakeWrongModel   = strfmt.UUID("00000000-0000-0000-0000-000000000008")
		idNoMatchWrongMakeRightModel  = strfmt.UUID("00000000-0000-0000-0000-000000000009")
		idNoMatchAllWrong             = strfmt.UUID("00000000-0000-0000-0000-00000000000a")
		idNoMatchNoCars               = strfmt.UUID("00000000-0000-0000-0000-00000000000b")
		idNoMatchThirdCarMatchesModel = strfmt.UUID("00000000-0000-0000-0000-00000000000c")

		// Rich variants: every garage has 3 cars with both make and model populated.
		// Rejection (when applicable) is due to value mismatch only — never due to
		// missing fields, missing cars, or sparse garages. Filler values (toyota,
		// kia, bmw, etc.) are guaranteed to never match the filter.
		idRichMatchG0                  = strfmt.UUID("00000000-0000-0000-0000-00000000000d")
		idRichMatchG1                  = strfmt.UUID("00000000-0000-0000-0000-00000000000e")
		idRichMatchG2                  = strfmt.UUID("00000000-0000-0000-0000-00000000000f")
		idRichNoMatchAllFiller         = strfmt.UUID("00000000-0000-0000-0000-000000000010")
		idRichNoMatchOnlyMakeMatches   = strfmt.UUID("00000000-0000-0000-0000-000000000011")
		idRichNoMatchOnlyModelMatches  = strfmt.UUID("00000000-0000-0000-0000-000000000012")
		idRichNoMatchSplitAcrossGars   = strfmt.UUID("00000000-0000-0000-0000-000000000013")
		idRichNoMatchSwapped           = strfmt.UUID("00000000-0000-0000-0000-000000000014")
		idRichNoMatchHondaCivicWrong   = strfmt.UUID("00000000-0000-0000-0000-000000000015")
		idRichNoMatchCivicOnlyAtCars2  = strfmt.UUID("00000000-0000-0000-0000-000000000016")
		idAbsentMissingModelField      = strfmt.UUID("00000000-0000-0000-0000-000000000017")

		idRichNoMatchCrossConfusion     = strfmt.UUID("00000000-0000-0000-0000-000000000018")
		idRichMatchHondaAtMultiplePos   = strfmt.UUID("00000000-0000-0000-0000-000000000019")
		idRichMatchTwoGaragesBoth       = strfmt.UUID("00000000-0000-0000-0000-00000000001a")
		idAbsentEmptyCarsArray          = strfmt.UUID("00000000-0000-0000-0000-00000000001b")
		idRichMatchHeterogeneousShapes  = strfmt.UUID("00000000-0000-0000-0000-00000000001c")
		idRichNoMatchCivicAsMake        = strfmt.UUID("00000000-0000-0000-0000-00000000001d")
	)

	// fillerGarage builds a 3-car garage that cannot satisfy either condition of
	// the filter (no honda, no civic). All cars carry both make and model.
	fillerGarage := func() map[string]any {
		return garage(
			car("make", "toyota", "model", "corolla"),
			car("make", "kia", "model", "sportage"),
			car("make", "bmw", "model", "x3"),
		)
	}
	// matchingGarage builds a 3-car garage that satisfies BOTH filter conditions
	// at the right indices: cars[0].make=honda, cars[1].model=civic. Other slots
	// (cars[0].model, cars[1].make, cars[2]) carry filler values.
	matchingGarage := func() map[string]any {
		return garage(
			car("make", "honda", "model", "corolla"),
			car("make", "kia", "model", "civic"),
			car("make", "bmw", "model", "x3"),
		)
	}

	docs := []struct {
		id      strfmt.UUID
		garages []any
		note    string
	}{
		{
			id:      idMatchMinimal,
			garages: []any{garage(car("make", "honda"), car("model", "civic"))},
			note:    "minimal same-garage match: cars[0].make=honda, cars[1].model=civic",
		},
		{
			id: idMatchTwoCarsBoth,
			garages: []any{garage(
				car("make", "honda", "model", "civic"),
				car("make", "honda", "model", "civic"),
			)},
			note: "both cars carry both fields; cars[0].make and cars[1].model still match",
		},
		{
			id: idMatchExtraCar,
			garages: []any{garage(
				car("make", "honda"),
				car("model", "civic"),
				car("make", "kia", "model", "sportage"),
			)},
			note: "cars[2] beyond the filter range is irrelevant",
		},
		{
			id: idMatchSecondGarage,
			garages: []any{
				garage(car("make", "toyota")),
				garage(car("make", "honda"), car("model", "civic")),
			},
			note: "match satisfied by the second garage element",
		},
		{
			id: idNoMatchSplitGarages,
			garages: []any{
				garage(car("make", "honda")),
				garage(car("make", "x"), car("model", "civic")),
			},
			note: "headline regression: cars[0].make in garage[0]; cars[1].model in garage[1] — split must reject",
		},
		{
			id:      idNoMatchSwapped,
			garages: []any{garage(car("model", "civic"), car("make", "honda"))},
			note:    "values exist but at swapped indices: cars[0].model=civic, cars[1].make=honda",
		},
		{
			id:      idNoMatchOnlyMakeNoSecondCar,
			garages: []any{garage(car("make", "honda"))},
			note:    "cars[1] absent → cars[1].model group resolves to empty",
		},
		{
			id: idNoMatchOnlyMakeWrongModel,
			garages: []any{garage(
				car("make", "honda"),
				car("make", "kia", "model", "sportage"),
			)},
			note: "cars[1] exists but with non-matching model — proves we don't accept any cars[1]",
		},
		{
			id: idNoMatchWrongMakeRightModel,
			garages: []any{garage(
				car("make", "toyota", "model", "corolla"),
				car("make", "kia", "model", "civic"),
			)},
			note: "cars[1].model right; cars[0].make wrong",
		},
		{
			id: idNoMatchAllWrong,
			garages: []any{garage(
				car("make", "toyota", "model", "corolla"),
				car("make", "kia", "model", "sportage"),
			)},
			note: "both cars exist with non-matching values",
		},
		{
			id:      idNoMatchNoCars,
			garages: []any{garageWithCity("berlin")},
			note:    "garage with no cars at all",
		},
		{
			id: idNoMatchThirdCarMatchesModel,
			garages: []any{garage(
				car("make", "honda"),
				car("make", "kia", "model", "sportage"),
				car("model", "civic"),
			)},
			note: "cars[2].model=civic but filter targets cars[1] — must not match through cars[2]",
		},

		// Rich match cases: 3 garages × 3 cars, all cars carry both make and model.
		// One garage matches; the other two are pure filler with non-matching values.
		{
			id:      idRichMatchG0,
			garages: []any{matchingGarage(), fillerGarage(), fillerGarage()},
			note:    "rich: 3×3, match in first garage with surrounding filler",
		},
		{
			id:      idRichMatchG1,
			garages: []any{fillerGarage(), matchingGarage(), fillerGarage()},
			note:    "rich: 3×3, match in middle garage",
		},
		{
			id:      idRichMatchG2,
			garages: []any{fillerGarage(), fillerGarage(), matchingGarage()},
			note:    "rich: 3×3, match in last garage — proves dispatch checks all garages",
		},

		// Rich no-match cases: every garage is fully populated (3 cars with both
		// fields) so rejection is purely due to value mismatch at the targeted
		// positions, not due to missing fields, missing cars, or sparse data.
		{
			id:      idRichNoMatchAllFiller,
			garages: []any{fillerGarage(), fillerGarage(), fillerGarage()},
			note:    "rich: 3×3 all filler — no honda or civic anywhere; pure value mismatch",
		},
		{
			id: idRichNoMatchOnlyMakeMatches,
			garages: []any{
				garage(
					car("make", "honda", "model", "corolla"),
					car("make", "kia", "model", "sportage"),
					car("make", "bmw", "model", "x3"),
				),
				fillerGarage(),
				fillerGarage(),
			},
			note: "rich: cars[0].make=honda satisfied at g0; civic missing entirely (every model is filler)",
		},
		{
			id: idRichNoMatchOnlyModelMatches,
			garages: []any{
				garage(
					car("make", "toyota", "model", "corolla"),
					car("make", "kia", "model", "civic"),
					car("make", "bmw", "model", "x3"),
				),
				fillerGarage(),
				fillerGarage(),
			},
			note: "rich: cars[1].model=civic satisfied at g0; honda missing entirely (every make is filler)",
		},
		{
			id: idRichNoMatchSplitAcrossGars,
			garages: []any{
				garage(
					car("make", "honda", "model", "corolla"),
					car("make", "kia", "model", "sportage"),
					car("make", "bmw", "model", "x3"),
				),
				garage(
					car("make", "toyota", "model", "corolla"),
					car("make", "audi", "model", "civic"),
					car("make", "ford", "model", "focus"),
				),
				fillerGarage(),
			},
			note: "rich: cars[0].make=honda in g0; cars[1].model=civic in g1 — split across garages, both fully populated",
		},
		{
			id: idRichNoMatchSwapped,
			garages: []any{
				garage(
					car("make", "toyota", "model", "civic"),
					car("make", "honda", "model", "corolla"),
					car("make", "kia", "model", "sportage"),
				),
				fillerGarage(),
				fillerGarage(),
			},
			note: "rich: g0 has both honda and civic but at swapped indices (cars[0].model=civic, cars[1].make=honda)",
		},
		{
			id: idRichNoMatchHondaCivicWrong,
			garages: []any{
				garage(
					car("make", "toyota", "model", "corolla"),
					car("make", "honda", "model", "sportage"),
					car("make", "kia", "model", "civic"),
				),
				fillerGarage(),
				fillerGarage(),
			},
			note: "rich: honda at cars[1] (wrong slot for make), civic at cars[2] (wrong slot for model) — values present but never at filtered positions",
		},
		{
			id: idRichNoMatchCivicOnlyAtCars2,
			garages: []any{
				garage(
					car("make", "honda", "model", "corolla"),
					car("make", "kia", "model", "sportage"),
					car("make", "toyota", "model", "civic"),
				),
				fillerGarage(),
				fillerGarage(),
			},
			note: "rich: cars[0].make=honda satisfied; civic exists but only at cars[2] — must not satisfy cars[1].model",
		},

		// Absent-data variant: every car has make but no model field at all.
		// Different code path from idNoMatchOnlyMakeNoSecondCar (which has only
		// 1 car) and idNoMatchNoCars (no cars array).
		{
			id: idAbsentMissingModelField,
			garages: []any{garage(
				map[string]any{"make": "honda"},
				map[string]any{"make": "kia"},
				map[string]any{"make": "bmw"},
			)},
			note: "absent: 3 cars with make but no model field — cars[1].model group resolves empty",
		},

		// Field/value cross-confusion: every car has the values "honda" and
		// "civic" present but on the WRONG fields. Filter requires make=honda;
		// every make is "civic" → reject. Proves the engine binds value to the
		// correct field, not just any field.
		{
			id: idRichNoMatchCrossConfusion,
			garages: []any{
				garage(
					car("make", "civic", "model", "honda"),
					car("make", "civic", "model", "honda"),
					car("make", "civic", "model", "honda"),
				),
				fillerGarage(),
				fillerGarage(),
			},
			note: "field/value cross-confusion: honda and civic present everywhere but on swapped fields",
		},

		// Honda at multiple positions: cars[0], cars[1], cars[2] all have
		// make=honda. cars[0].make=honda ✓, cars[1].model=civic ✓ → match.
		// Proves having the make value at multiple positions doesn't break the
		// cars[1] check.
		{
			id: idRichMatchHondaAtMultiplePos,
			garages: []any{
				garage(
					car("make", "honda", "model", "corolla"),
					car("make", "honda", "model", "civic"),
					car("make", "honda", "model", "x3"),
				),
				fillerGarage(),
				fillerGarage(),
			},
			note: "every car in g0 has make=honda; cars[1].model=civic — match despite extra honda noise",
		},

		// Two garages both fully match — defensive dedup check. Result must
		// contain the doc exactly once, not duplicated per matching garage.
		{
			id: idRichMatchTwoGaragesBoth,
			garages: []any{
				matchingGarage(),
				matchingGarage(),
				fillerGarage(),
			},
			note: "g0 and g1 both satisfy the filter — result must contain doc exactly once (dedup)",
		},

		// Empty cars array — distinct from missing field (idAbsentMissingModelField)
		// and missing array (idNoMatchNoCars). Every garage explicitly carries
		// cars=[]. cars[1].model group resolves empty.
		{
			id: idAbsentEmptyCarsArray,
			garages: []any{
				map[string]any{"cars": []any{}},
				map[string]any{"cars": []any{}},
				map[string]any{"cars": []any{}},
			},
			note: "absent: every garage has explicit empty cars array",
		},

		// Heterogeneous garage shapes: g0 has 2 cars, g1 has 5 cars
		// (matching at cars[0]+cars[1] with 3 extra cars), g2 has 4 cars.
		// Proves the engine handles non-uniform garage sizes and matches in a
		// 5-car garage exactly the same as in a 3-car garage.
		{
			id: idRichMatchHeterogeneousShapes,
			garages: []any{
				garage(
					car("make", "toyota", "model", "corolla"),
					car("make", "kia", "model", "sportage"),
				),
				garage(
					car("make", "honda", "model", "corolla"),
					car("make", "kia", "model", "civic"),
					car("make", "bmw", "model", "x3"),
					car("make", "audi", "model", "a4"),
					car("make", "ford", "model", "focus"),
				),
				garage(
					car("make", "nissan", "model", "sentra"),
					car("make", "hyundai", "model", "elantra"),
					car("make", "volvo", "model", "xc60"),
					car("make", "ford", "model", "focus"),
				),
			},
			note: "heterogeneous: 2 / 5 / 4 cars per garage; match in middle 5-car garage",
		},

		// Civic-as-make value confusion: every car has make="civic" and
		// model="civic". Filter cars[0].make=honda fails (make is civic, not
		// honda) — proves the position binding holds even when the filter value
		// (civic) is repeated across many fields under the wrong field name.
		{
			id: idRichNoMatchCivicAsMake,
			garages: []any{
				garage(
					car("make", "civic", "model", "civic"),
					car("make", "civic", "model", "civic"),
					car("make", "civic", "model", "civic"),
				),
				fillerGarage(),
				fillerGarage(),
			},
			note: "civic-as-make: cars[1].model=civic satisfied but cars[0].make=civic ≠ honda → reject",
		},
	}

	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	ctx := context.Background()

	for _, d := range docs {
		require.NoError(t, db.PutObject(ctx, &models.Object{
			Class: nestedClass, ID: d.id,
			Properties: map[string]any{"garages": d.garages},
		}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
	}

	makeFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(a, b *filters.LocalFilter) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{*a.Root, *b.Root},
		}}
	}

	filter := andFilter(
		makeFilter("garages.cars[0].make", "honda"),
		makeFilter("garages.cars[1].model", "civic"),
	)

	res, err := db.Search(ctx, dto.GetParams{
		ClassName:  nestedClass,
		Pagination: &filters.Pagination{Limit: 100},
		Filters:    filter,
	})
	require.NoError(t, err)

	got := make([]strfmt.UUID, len(res))
	for i, r := range res {
		got[i] = r.ID
	}

	want := []strfmt.UUID{
		idMatchMinimal,
		idMatchTwoCarsBoth,
		idMatchExtraCar,
		idMatchSecondGarage,
		idRichMatchG0,
		idRichMatchG1,
		idRichMatchG2,
		idRichMatchHondaAtMultiplePos,
		idRichMatchTwoGaragesBoth,
		idRichMatchHeterogeneousShapes,
	}
	assert.ElementsMatch(t, want, got)
}

// TestNestedFilteringF15CorrelatedAndDifferentRootCountries exercises a
// correlated AND filter where two conditions target different root-level
// element indices (countries[0] and countries[1]):
//
//	countries[0].garages.city = "berlin" AND countries[1].garages.postcode = "10115"
//
// Independent-clause semantics: a doc matches when countries[0] has SOME garage
// with city="berlin" AND countries[1] has SOME garage with postcode="10115".
// The two clauses are about different root elements so they don't need to share
// any sub-tree state — the doc just needs both root sub-trees to be populated
// at the right indices with the right values.
func TestNestedFilteringF15CorrelatedAndDifferentRootCountries(t *testing.T) {
	const nestedClass = "F15"
	vTrue := true

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{
				Name:     "countries",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:     "garages",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: &vTrue},
							{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: &vTrue},
						},
					},
				},
			},
		},
	}

	garage := func(props ...string) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i]] = props[i+1]
		}
		return out
	}
	country := func(garages ...map[string]any) map[string]any {
		anyGarages := make([]any, len(garages))
		for i, g := range garages {
			anyGarages[i] = g
		}
		return map[string]any{"garages": anyGarages}
	}

	// fillerGarage carries non-matching city + postcode; both fields populated.
	fillerGarage := func() map[string]any {
		return garage("city", "munich", "postcode", "80331")
	}
	// fillerCountry has 3 filler garages — guaranteed not to satisfy either condition.
	fillerCountry := func() map[string]any {
		return country(fillerGarage(), fillerGarage(), fillerGarage())
	}

	const (
		idMatchMinimal                 = strfmt.UUID("00000000-0000-0000-0000-000000000001")
		idMatchExtraCountry            = strfmt.UUID("00000000-0000-0000-0000-000000000002")
		idMatchSameValuesBothCountries = strfmt.UUID("00000000-0000-0000-0000-000000000003")
		idMatchEachCountryManyGarages  = strfmt.UUID("00000000-0000-0000-0000-000000000004")
		idMatchAllCountriesAlsoBoth    = strfmt.UUID("00000000-0000-0000-0000-000000000005")
		idNoMatchAllFiller             = strfmt.UUID("00000000-0000-0000-0000-000000000006")
		idNoMatchOnlyCity              = strfmt.UUID("00000000-0000-0000-0000-000000000007")
		idNoMatchOnlyPostcode          = strfmt.UUID("00000000-0000-0000-0000-000000000008")
		idNoMatchSwappedCountries      = strfmt.UUID("00000000-0000-0000-0000-000000000009")
		idNoMatchOnlyOneCountry        = strfmt.UUID("00000000-0000-0000-0000-00000000000a")
		idNoMatchBothInCountry0        = strfmt.UUID("00000000-0000-0000-0000-00000000000b")
		idNoMatchCrossConfusion        = strfmt.UUID("00000000-0000-0000-0000-00000000000c")
		idNoMatchValuesAtCountry2      = strfmt.UUID("00000000-0000-0000-0000-00000000000d")
		idAbsentNoCountries            = strfmt.UUID("00000000-0000-0000-0000-00000000000e")
		idAbsentEmptyGarages           = strfmt.UUID("00000000-0000-0000-0000-00000000000f")

		idNoMatchNoGaragesField                = strfmt.UUID("00000000-0000-0000-0000-000000000010")
		idMatchHeterogeneousCountries          = strfmt.UUID("00000000-0000-0000-0000-000000000011")
		idMatchEveryGarageMatchesPerCountry    = strfmt.UUID("00000000-0000-0000-0000-000000000012")
		idMatchCountry2AlsoMatches             = strfmt.UUID("00000000-0000-0000-0000-000000000013")
		idNoMatchInvertedFieldsAcrossCountries = strfmt.UUID("00000000-0000-0000-0000-000000000014")
	)

	docs := []struct {
		id        strfmt.UUID
		countries []any
		note      string
	}{
		{
			id: idMatchMinimal,
			countries: []any{
				country(garage("city", "berlin")),
				country(garage("postcode", "10115")),
			},
			note: "minimal: countries[0] has city=berlin, countries[1] has postcode=10115",
		},
		{
			id: idMatchExtraCountry,
			countries: []any{
				country(garage("city", "berlin")),
				country(garage("postcode", "10115")),
				fillerCountry(),
			},
			note: "extra country at index 2 doesn't break the match",
		},
		{
			id: idMatchSameValuesBothCountries,
			countries: []any{
				country(garage("city", "berlin", "postcode", "10115")),
				country(garage("city", "berlin", "postcode", "10115")),
			},
			note: "both countries carry both fields; condition still satisfied at the right indices",
		},
		{
			id: idMatchEachCountryManyGarages,
			countries: []any{
				country(fillerGarage(), garage("city", "berlin", "postcode", "80331"), fillerGarage()),
				country(fillerGarage(), garage("city", "munich", "postcode", "10115"), fillerGarage()),
			},
			note: "each country has 3 garages; matching garage is in the middle of each country's list",
		},
		{
			id: idMatchAllCountriesAlsoBoth,
			countries: []any{
				country(
					garage("city", "berlin", "postcode", "10115"),
					garage("city", "berlin", "postcode", "10115"),
					garage("city", "berlin", "postcode", "10115"),
				),
				country(
					garage("city", "berlin", "postcode", "10115"),
					garage("city", "berlin", "postcode", "10115"),
					garage("city", "berlin", "postcode", "10115"),
				),
			},
			note: "every garage in every country has both fields — saturated match data",
		},

		{
			id:        idNoMatchAllFiller,
			countries: []any{fillerCountry(), fillerCountry(), fillerCountry()},
			note:      "no berlin or 10115 anywhere — pure value mismatch",
		},
		{
			id: idNoMatchOnlyCity,
			countries: []any{
				country(garage("city", "berlin", "postcode", "80331"), fillerGarage()),
				fillerCountry(),
				fillerCountry(),
			},
			note: "countries[0] has city=berlin; postcode=10115 missing entirely",
		},
		{
			id: idNoMatchOnlyPostcode,
			countries: []any{
				fillerCountry(),
				country(garage("city", "munich", "postcode", "10115"), fillerGarage()),
				fillerCountry(),
			},
			note: "countries[1] has postcode=10115; city=berlin missing entirely",
		},
		{
			id: idNoMatchSwappedCountries,
			countries: []any{
				country(garage("city", "munich", "postcode", "10115")),
				country(garage("city", "berlin", "postcode", "80331")),
			},
			note: "values present at swapped country indices: 10115 at countries[0], berlin at countries[1]",
		},
		{
			id: idNoMatchOnlyOneCountry,
			countries: []any{
				country(garage("city", "berlin", "postcode", "10115")),
			},
			note: "single country with both values — countries[1] does not exist",
		},
		{
			id: idNoMatchBothInCountry0,
			countries: []any{
				country(garage("city", "berlin", "postcode", "10115"), fillerGarage()),
				fillerCountry(),
			},
			note: "both values in countries[0]; countries[1] has no postcode=10115",
		},
		{
			id: idNoMatchCrossConfusion,
			countries: []any{
				country(garage("city", "10115", "postcode", "berlin")),
				country(garage("city", "10115", "postcode", "berlin")),
			},
			note: "values present but on swapped fields (city=10115, postcode=berlin) — field/value binding must hold",
		},
		{
			id: idNoMatchValuesAtCountry2,
			countries: []any{
				fillerCountry(),
				fillerCountry(),
				country(garage("city", "berlin", "postcode", "10115")),
			},
			note: "both values exist but only at countries[2] — must not satisfy countries[0]/countries[1] constraints",
		},

		{
			id:        idAbsentNoCountries,
			countries: []any{},
			note:      "absent: empty countries array",
		},
		{
			id: idAbsentEmptyGarages,
			countries: []any{
				map[string]any{"garages": []any{}},
				map[string]any{"garages": []any{}},
			},
			note: "absent: countries exist but garages arrays are empty",
		},

		// Country with no garages field at all — distinct from empty array.
		// countries[1] is an empty object; the garages key is absent entirely.
		{
			id: idNoMatchNoGaragesField,
			countries: []any{
				country(garage("city", "berlin", "postcode", "10115")),
				map[string]any{},
			},
			note: "countries[1] has no garages field at all (vs empty array)",
		},

		// Heterogeneous country shapes: countries[0] has 5 garages with the
		// matching city in the middle; countries[1] has just 1 garage with the
		// matching postcode. Proves engine handles non-uniform country sizes.
		{
			id: idMatchHeterogeneousCountries,
			countries: []any{
				country(
					fillerGarage(),
					fillerGarage(),
					garage("city", "berlin", "postcode", "80331"),
					fillerGarage(),
					fillerGarage(),
				),
				country(garage("city", "munich", "postcode", "10115")),
			},
			note: "heterogeneous: countries[0] has 5 garages, countries[1] has 1 — match works across mixed shapes",
		},

		// Every garage in the matching country has the matching value (parallel
		// to F13's honda-at-multiple-positions). Tests that having the matching
		// value at every sub-position doesn't confuse the resolver.
		{
			id: idMatchEveryGarageMatchesPerCountry,
			countries: []any{
				country(
					garage("city", "berlin", "postcode", "80331"),
					garage("city", "berlin", "postcode", "80331"),
					garage("city", "berlin", "postcode", "80331"),
				),
				country(
					garage("city", "munich", "postcode", "10115"),
					garage("city", "munich", "postcode", "10115"),
					garage("city", "munich", "postcode", "10115"),
				),
			},
			note: "every garage in countries[0] has city=berlin; every garage in countries[1] has postcode=10115",
		},

		// countries[2] also satisfies both conditions — defensive dedup at
		// root level. Result must contain doc exactly once, not duplicated.
		{
			id: idMatchCountry2AlsoMatches,
			countries: []any{
				country(garage("city", "berlin", "postcode", "80331")),
				country(garage("city", "munich", "postcode", "10115")),
				country(garage("city", "berlin", "postcode", "10115")),
			},
			note: "countries[0] satisfies city, countries[1] satisfies postcode, countries[2] satisfies both — must dedup",
		},

		// Inverted fields between countries: countries[0] has only the
		// postcode field set (with the value the filter wants at countries[1]);
		// countries[1] has only the city field set (with the value the filter
		// wants at countries[0]). Both values exist in the doc but neither is
		// at the right country index. Distinct from idNoMatchSwappedCountries
		// (which has both fields populated everywhere) — here the relevant
		// field is structurally absent at the right country.
		{
			id: idNoMatchInvertedFieldsAcrossCountries,
			countries: []any{
				country(garage("postcode", "10115")),
				country(garage("city", "berlin")),
			},
			note: "countries[0] has only postcode=10115; countries[1] has only city=berlin — values present but at wrong country indices",
		},
	}

	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	ctx := context.Background()

	for _, d := range docs {
		require.NoError(t, db.PutObject(ctx, &models.Object{
			Class: nestedClass, ID: d.id,
			Properties: map[string]any{"countries": d.countries},
		}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
	}

	makeFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(a, b *filters.LocalFilter) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{*a.Root, *b.Root},
		}}
	}

	filter := andFilter(
		makeFilter("countries[0].garages.city", "berlin"),
		makeFilter("countries[1].garages.postcode", "10115"),
	)

	res, err := db.Search(ctx, dto.GetParams{
		ClassName:  nestedClass,
		Pagination: &filters.Pagination{Limit: 100},
		Filters:    filter,
	})
	require.NoError(t, err)

	got := make([]strfmt.UUID, len(res))
	for i, r := range res {
		got[i] = r.ID
	}

	want := []strfmt.UUID{
		idMatchMinimal,
		idMatchExtraCountry,
		idMatchSameValuesBothCountries,
		idMatchEachCountryManyGarages,
		idMatchAllCountriesAlsoBoth,
		idMatchHeterogeneousCountries,
		idMatchEveryGarageMatchesPerCountry,
		idMatchCountry2AlsoMatches,
	}
	assert.ElementsMatch(t, want, got)
}

// TestNestedFilteringF16CorrelatedAndDifferentRootGaragesWithSameCarSubs
// exercises a correlated AND filter where two pairs of conditions target
// different root-level garages indices, with each pair requiring same-car
// correlation inside its garage:
//
//	garages[0].cars.make = "honda"   AND garages[0].cars.tires.width = 205 AND
//	garages[1].cars.make = "ferrari" AND garages[1].cars.tires.width = 225
//
// Combines two semantic axes:
//   - root-level independence (garages[0] sub-tree is independent of garages[1])
//   - same-car correlation within each garage (make and tires.width must be
//     at the same car; tires.width can be in any of that car's tires)
//
// A doc matches when SOME garages[0] has a car with make="honda" AND that same
// car has a tire with width=205, AND SOME garages[1] has a car with
// make="ferrari" AND that same car has a tire with width=225.
func TestNestedFilteringF16CorrelatedAndDifferentRootGaragesWithSameCarSubs(t *testing.T) {
	const nestedClass = "F16"
	vTrue := true

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:     "cars",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: &vTrue},
							{
								Name:     "tires",
								DataType: schema.DataTypeObjectArray.PropString(),
								NestedProperties: []*models.NestedProperty{
									{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
								},
							},
						},
					},
				},
			},
		},
	}

	tire := func(width int) map[string]any {
		return map[string]any{"width": width}
	}
	carWithTires := func(makeName string, tires ...map[string]any) map[string]any {
		anyTires := make([]any, len(tires))
		for i, t := range tires {
			anyTires[i] = t
		}
		return map[string]any{"make": makeName, "tires": anyTires}
	}
	garage := func(cars ...map[string]any) map[string]any {
		anyCars := make([]any, len(cars))
		for i, c := range cars {
			anyCars[i] = c
		}
		return map[string]any{"cars": anyCars}
	}

	// fillerCar carries a non-matching make and a single non-matching tire.
	fillerCar := func() map[string]any {
		return carWithTires("toyota", tire(175))
	}
	// fillerGarage has 3 filler cars — guaranteed not to satisfy either subfilter.
	fillerGarage := func() map[string]any {
		return garage(fillerCar(), fillerCar(), fillerCar())
	}
	// matchingG0Car is a single car satisfying both garages[0] subfilter conditions.
	matchingG0Car := func() map[string]any {
		return carWithTires("honda", tire(205))
	}
	// matchingG1Car is a single car satisfying both garages[1] subfilter conditions.
	matchingG1Car := func() map[string]any {
		return carWithTires("ferrari", tire(225))
	}
	matchingG0 := func() map[string]any { return garage(matchingG0Car()) }
	matchingG1 := func() map[string]any { return garage(matchingG1Car()) }

	const (
		idMatchMinimal                  = strfmt.UUID("00000000-0000-0000-0000-000000000001")
		idMatchExtraGarages             = strfmt.UUID("00000000-0000-0000-0000-000000000002")
		idMatchMatchingCarSurroundedByFillers = strfmt.UUID("00000000-0000-0000-0000-000000000003")
		idMatchEveryCarMatchesInBothG   = strfmt.UUID("00000000-0000-0000-0000-000000000004")
		idMatchHeterogeneousGarages     = strfmt.UUID("00000000-0000-0000-0000-000000000005")
		idMatchMatchingCarHasManyTires  = strfmt.UUID("00000000-0000-0000-0000-000000000006")
		idMatchG2AlsoMatchesG0          = strfmt.UUID("00000000-0000-0000-0000-000000000007")

		idNoMatchG0SplitMakeAndWidth    = strfmt.UUID("00000000-0000-0000-0000-000000000008")
		idNoMatchG1SplitMakeAndWidth    = strfmt.UUID("00000000-0000-0000-0000-000000000009")
		idNoMatchG0AllCarsSplit         = strfmt.UUID("00000000-0000-0000-0000-00000000000a")

		idNoMatchSplitAcrossGarages     = strfmt.UUID("00000000-0000-0000-0000-00000000000b")
		idNoMatchSwappedGarages         = strfmt.UUID("00000000-0000-0000-0000-00000000000c")
		idNoMatchValuesAtG2Only         = strfmt.UUID("00000000-0000-0000-0000-00000000000d")
		idNoMatchInvertedAcrossGarages  = strfmt.UUID("00000000-0000-0000-0000-00000000000e")

		idNoMatchAllFiller              = strfmt.UUID("00000000-0000-0000-0000-00000000000f")
		idNoMatchOnlyMakeInG0           = strfmt.UUID("00000000-0000-0000-0000-000000000010")
		idNoMatchOnlyWidthInG0          = strfmt.UUID("00000000-0000-0000-0000-000000000011")
		idNoMatchMixedSplits            = strfmt.UUID("00000000-0000-0000-0000-000000000012")

		idAbsentNoGarages               = strfmt.UUID("00000000-0000-0000-0000-000000000013")
		idAbsentOnlyOneGarage           = strfmt.UUID("00000000-0000-0000-0000-000000000014")
		idAbsentG0EmptyObject           = strfmt.UUID("00000000-0000-0000-0000-000000000015")
		idAbsentG0NoCarsArray           = strfmt.UUID("00000000-0000-0000-0000-000000000016")
		idAbsentMatchingCarNoTires      = strfmt.UUID("00000000-0000-0000-0000-000000000017")
		idAbsentMatchingCarEmptyTires   = strfmt.UUID("00000000-0000-0000-0000-000000000018")

		idMatchHondaAtAllCarsOneHasWidth = strfmt.UUID("00000000-0000-0000-0000-000000000019")
		idMatchMultipleSameCarsMatchInG0 = strfmt.UUID("00000000-0000-0000-0000-00000000001a")
	)

	docs := []struct {
		id      strfmt.UUID
		garages []any
		note    string
	}{
		{
			id:      idMatchMinimal,
			garages: []any{matchingG0(), matchingG1()},
			note:    "minimal: each garage has one car satisfying its subfilter",
		},
		{
			id:      idMatchExtraGarages,
			garages: []any{matchingG0(), matchingG1(), fillerGarage(), fillerGarage()},
			note:    "extra garages beyond [1] are irrelevant",
		},
		{
			id: idMatchMatchingCarSurroundedByFillers,
			garages: []any{
				garage(fillerCar(), matchingG0Car(), fillerCar()),
				garage(fillerCar(), matchingG1Car(), fillerCar()),
			},
			note: "matching car at index 1 of 3 in each garage",
		},
		{
			id: idMatchEveryCarMatchesInBothG,
			garages: []any{
				garage(matchingG0Car(), matchingG0Car(), matchingG0Car()),
				garage(matchingG1Car(), matchingG1Car(), matchingG1Car()),
			},
			note: "every car satisfies the per-garage subfilter — saturated match",
		},
		{
			id: idMatchHeterogeneousGarages,
			garages: []any{
				garage(fillerCar(), fillerCar(), matchingG0Car(), fillerCar(), fillerCar()),
				garage(matchingG1Car()),
			},
			note: "heterogeneous: g0 has 5 cars (match in middle), g1 has 1 car",
		},
		{
			id: idMatchMatchingCarHasManyTires,
			garages: []any{
				garage(carWithTires("honda", tire(175), tire(205), tire(215), tire(235))),
				garage(carWithTires("ferrari", tire(195), tire(215), tire(225), tire(235))),
			},
			note: "matching car has 4 tires; one matches the required width",
		},
		{
			id: idMatchG2AlsoMatchesG0,
			garages: []any{
				matchingG0(),
				matchingG1(),
				matchingG0(),
			},
			note: "garages[0] and garages[2] both have honda+205; result must dedup to single doc",
		},

		// Same-car correlation within a garage — headline F16-specific cases.
		// In each, the make and tires.width values needed for one garage's
		// subfilter exist but in DIFFERENT cars within that garage.
		{
			id: idNoMatchG0SplitMakeAndWidth,
			garages: []any{
				garage(
					carWithTires("honda", tire(175)),
					carWithTires("toyota", tire(205)),
				),
				matchingG1(),
			},
			note: "g0 has make=honda in cars[0] but tires.width=205 only in cars[1] — different cars; same-car correlation must reject",
		},
		{
			id: idNoMatchG1SplitMakeAndWidth,
			garages: []any{
				matchingG0(),
				garage(
					carWithTires("ferrari", tire(235)),
					carWithTires("bmw", tire(225)),
				),
			},
			note: "symmetric: g1 has make=ferrari and tires.width=225 in different cars",
		},
		{
			id: idNoMatchG0AllCarsSplit,
			garages: []any{
				garage(
					carWithTires("honda", tire(175), tire(195)),
					carWithTires("toyota", tire(205), tire(215)),
					carWithTires("honda", tire(235), tire(175)),
				),
				matchingG1(),
			},
			note: "g0 has multiple honda cars but none with width=205; the only width=205 lives in a non-honda car",
		},

		// Cross-garage rejection: condition pairs not satisfied across garages.
		{
			id: idNoMatchSplitAcrossGarages,
			garages: []any{
				matchingG0(),
				fillerGarage(),
			},
			note: "g0 satisfied; g1 has neither ferrari nor 225",
		},
		{
			id: idNoMatchSwappedGarages,
			garages: []any{
				matchingG1(), // g0 has ferrari+225 (which the filter expects at g1)
				matchingG0(), // g1 has honda+205 (which the filter expects at g0)
			},
			note: "right value pairs at swapped garage indices",
		},
		{
			id: idNoMatchValuesAtG2Only,
			garages: []any{
				fillerGarage(),
				fillerGarage(),
				matchingG0(),
				matchingG1(),
			},
			note: "matching value pairs exist at garages[2]+[3] but not at garages[0]+[1]",
		},
		{
			id: idNoMatchInvertedAcrossGarages,
			garages: []any{
				// g0 has only the make value the filter wants at g1 (ferrari);
				// no width=205 anywhere in g0.
				garage(carWithTires("ferrari", tire(215)), carWithTires("ferrari", tire(235))),
				// g1 has only the make value the filter wants at g0 (honda);
				// no width=225 anywhere in g1.
				garage(carWithTires("honda", tire(195)), carWithTires("honda", tire(175))),
			},
			note: "each garage has only the make value the OTHER garage's filter expects, with no matching widths anywhere — distinct from Swapped (which has full pairs)",
		},

		// Rich value mismatch — every garage fully populated, neither subfilter satisfied.
		{
			id: idNoMatchAllFiller,
			garages: []any{
				fillerGarage(),
				fillerGarage(),
				fillerGarage(),
			},
			note: "all filler — no honda, no ferrari, no 205, no 225 anywhere",
		},
		{
			id: idNoMatchOnlyMakeInG0,
			garages: []any{
				garage(
					carWithTires("honda", tire(175)),
					carWithTires("honda", tire(195)),
					carWithTires("honda", tire(215)),
				),
				matchingG1(),
			},
			note: "g0 has make=honda everywhere but no tires.width=205 in any car",
		},
		{
			id: idNoMatchOnlyWidthInG0,
			garages: []any{
				garage(
					carWithTires("toyota", tire(205)),
					carWithTires("kia", tire(205)),
					carWithTires("bmw", tire(205)),
				),
				matchingG1(),
			},
			note: "g0 has tires.width=205 everywhere but no make=honda in any car",
		},
		// Mixed splits: BOTH g0 and g1 have same-car correlation broken
		// simultaneously. Tests that the dispatch correctly fans into 2 groups
		// and each group independently rejects.
		{
			id: idNoMatchMixedSplits,
			garages: []any{
				garage(
					carWithTires("honda", tire(175)),
					carWithTires("toyota", tire(205)),
				),
				garage(
					carWithTires("ferrari", tire(235)),
					carWithTires("bmw", tire(225)),
				),
			},
			note: "both g0 and g1 have same-car splits — neither subfilter satisfied at any single car",
		},

		// Absent-data variants probing distinct missing-structure code paths.
		{
			id:      idAbsentNoGarages,
			garages: []any{},
			note:    "absent: empty garages array",
		},
		{
			id:      idAbsentOnlyOneGarage,
			garages: []any{matchingG0()},
			note:    "absent: only garages[0] exists; garages[1] missing",
		},
		{
			id: idAbsentG0EmptyObject,
			garages: []any{
				map[string]any{}, // no cars field at all
				matchingG1(),
			},
			note: "absent: garages[0] is empty object (no cars field); garages[1] perfect",
		},
		{
			id: idAbsentG0NoCarsArray,
			garages: []any{
				map[string]any{"cars": []any{}},
				matchingG1(),
			},
			note: "absent: garages[0] has explicit empty cars array",
		},
		{
			id: idAbsentMatchingCarNoTires,
			garages: []any{
				garage(map[string]any{"make": "honda"}), // no tires field on the honda car
				matchingG1(),
			},
			note: "absent: g0's honda car has no tires field at all",
		},
		{
			id: idAbsentMatchingCarEmptyTires,
			garages: []any{
				garage(map[string]any{"make": "honda", "tires": []any{}}),
				matchingG1(),
			},
			note: "absent: g0's honda car has explicit empty tires array (vs missing tires field)",
		},

		// Match: every car in g0 has make=honda but only one car has tires.width=205.
		// Same-car correlation must select that specific car. g1 perfect.
		{
			id: idMatchHondaAtAllCarsOneHasWidth,
			garages: []any{
				garage(
					carWithTires("honda", tire(175)),
					carWithTires("honda", tire(205)),
					carWithTires("honda", tire(235)),
				),
				matchingG1(),
			},
			note: "every car in g0 has honda; only middle car has width=205 — same-car correlation finds it",
		},

		// Match: multiple cars in g0 each independently satisfy the g0 subfilter
		// (each has both make=honda AND tires.width=205). Within-garage dedup
		// parallel — result must contain doc once, not multiple times per match.
		{
			id: idMatchMultipleSameCarsMatchInG0,
			garages: []any{
				garage(
					carWithTires("honda", tire(205)),
					carWithTires("honda", tire(205)),
					carWithTires("honda", tire(205)),
				),
				matchingG1(),
			},
			note: "g0 has 3 cars each independently satisfying the subfilter; result must dedup",
		},
	}

	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	ctx := context.Background()

	for _, d := range docs {
		require.NoError(t, db.PutObject(ctx, &models.Object{
			Class: nestedClass, ID: d.id,
			Properties: map[string]any{"garages": d.garages},
		}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
	}

	makeTextFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	makeIntFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andClauses := func(lfs ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(lfs))
		for i, lf := range lfs {
			operands[i] = *lf.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: operands,
		}}
	}

	filter := andClauses(
		makeTextFilter("garages[0].cars.make", "honda"),
		makeIntFilter("garages[0].cars.tires.width", 205),
		makeTextFilter("garages[1].cars.make", "ferrari"),
		makeIntFilter("garages[1].cars.tires.width", 225),
	)

	res, err := db.Search(ctx, dto.GetParams{
		ClassName:  nestedClass,
		Pagination: &filters.Pagination{Limit: 100},
		Filters:    filter,
	})
	require.NoError(t, err)

	got := make([]strfmt.UUID, len(res))
	for i, r := range res {
		got[i] = r.ID
	}

	want := []strfmt.UUID{
		idMatchMinimal,
		idMatchExtraGarages,
		idMatchMatchingCarSurroundedByFillers,
		idMatchEveryCarMatchesInBothG,
		idMatchHeterogeneousGarages,
		idMatchMatchingCarHasManyTires,
		idMatchG2AlsoMatchesG0,
		idMatchHondaAtAllCarsOneHasWidth,
		idMatchMultipleSameCarsMatchInG0,
	}
	assert.ElementsMatch(t, want, got)
}
