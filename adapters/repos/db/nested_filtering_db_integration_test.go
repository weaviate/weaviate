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
	"fmt"
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
		idRichMatchG0                 = strfmt.UUID("00000000-0000-0000-0000-00000000000d")
		idRichMatchG1                 = strfmt.UUID("00000000-0000-0000-0000-00000000000e")
		idRichMatchG2                 = strfmt.UUID("00000000-0000-0000-0000-00000000000f")
		idRichNoMatchAllFiller        = strfmt.UUID("00000000-0000-0000-0000-000000000010")
		idRichNoMatchOnlyMakeMatches  = strfmt.UUID("00000000-0000-0000-0000-000000000011")
		idRichNoMatchOnlyModelMatches = strfmt.UUID("00000000-0000-0000-0000-000000000012")
		idRichNoMatchSplitAcrossGars  = strfmt.UUID("00000000-0000-0000-0000-000000000013")
		idRichNoMatchSwapped          = strfmt.UUID("00000000-0000-0000-0000-000000000014")
		idRichNoMatchHondaCivicWrong  = strfmt.UUID("00000000-0000-0000-0000-000000000015")
		idRichNoMatchCivicOnlyAtCars2 = strfmt.UUID("00000000-0000-0000-0000-000000000016")
		idAbsentMissingModelField     = strfmt.UUID("00000000-0000-0000-0000-000000000017")

		idRichNoMatchCrossConfusion    = strfmt.UUID("00000000-0000-0000-0000-000000000018")
		idRichMatchHondaAtMultiplePos  = strfmt.UUID("00000000-0000-0000-0000-000000000019")
		idRichMatchTwoGaragesBoth      = strfmt.UUID("00000000-0000-0000-0000-00000000001a")
		idAbsentEmptyCarsArray         = strfmt.UUID("00000000-0000-0000-0000-00000000001b")
		idRichMatchHeterogeneousShapes = strfmt.UUID("00000000-0000-0000-0000-00000000001c")
		idRichNoMatchCivicAsMake       = strfmt.UUID("00000000-0000-0000-0000-00000000001d")
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
		idMatchMinimal                        = strfmt.UUID("00000000-0000-0000-0000-000000000001")
		idMatchExtraGarages                   = strfmt.UUID("00000000-0000-0000-0000-000000000002")
		idMatchMatchingCarSurroundedByFillers = strfmt.UUID("00000000-0000-0000-0000-000000000003")
		idMatchEveryCarMatchesInBothG         = strfmt.UUID("00000000-0000-0000-0000-000000000004")
		idMatchHeterogeneousGarages           = strfmt.UUID("00000000-0000-0000-0000-000000000005")
		idMatchMatchingCarHasManyTires        = strfmt.UUID("00000000-0000-0000-0000-000000000006")
		idMatchG2AlsoMatchesG0                = strfmt.UUID("00000000-0000-0000-0000-000000000007")

		idNoMatchG0SplitMakeAndWidth = strfmt.UUID("00000000-0000-0000-0000-000000000008")
		idNoMatchG1SplitMakeAndWidth = strfmt.UUID("00000000-0000-0000-0000-000000000009")
		idNoMatchG0AllCarsSplit      = strfmt.UUID("00000000-0000-0000-0000-00000000000a")

		idNoMatchSplitAcrossGarages    = strfmt.UUID("00000000-0000-0000-0000-00000000000b")
		idNoMatchSwappedGarages        = strfmt.UUID("00000000-0000-0000-0000-00000000000c")
		idNoMatchValuesAtG2Only        = strfmt.UUID("00000000-0000-0000-0000-00000000000d")
		idNoMatchInvertedAcrossGarages = strfmt.UUID("00000000-0000-0000-0000-00000000000e")

		idNoMatchAllFiller     = strfmt.UUID("00000000-0000-0000-0000-00000000000f")
		idNoMatchOnlyMakeInG0  = strfmt.UUID("00000000-0000-0000-0000-000000000010")
		idNoMatchOnlyWidthInG0 = strfmt.UUID("00000000-0000-0000-0000-000000000011")
		idNoMatchMixedSplits   = strfmt.UUID("00000000-0000-0000-0000-000000000012")

		idAbsentNoGarages             = strfmt.UUID("00000000-0000-0000-0000-000000000013")
		idAbsentOnlyOneGarage         = strfmt.UUID("00000000-0000-0000-0000-000000000014")
		idAbsentG0EmptyObject         = strfmt.UUID("00000000-0000-0000-0000-000000000015")
		idAbsentG0NoCarsArray         = strfmt.UUID("00000000-0000-0000-0000-000000000016")
		idAbsentMatchingCarNoTires    = strfmt.UUID("00000000-0000-0000-0000-000000000017")
		idAbsentMatchingCarEmptyTires = strfmt.UUID("00000000-0000-0000-0000-000000000018")

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

// TestNestedFilteringF17DeeperCorrelatedAndDifferentCarsSameGarage exercises
// a deeper-schema variant of F13:
//
//	countries.garages.cars[0].make = "honda" AND countries.garages.cars[1].model = "civic"
//
// By the rule "conditions sharing an unconstrained path prefix apply to the
// same element at that prefix", this matches docs where the same countries[N]
// AND the same garages[M] within it have both cars[0].make=honda and
// cars[1].model=civic. Different garages within the same country must NOT
// satisfy the filter — the LCA above the conflict is countries.garages, not
// just countries.
//
// Two test groups:
//
//   - Cross-garage: F13's full case set wrapped in a single country. Behaviour
//     must mirror F13 case-for-case — adding a country wrapper above the F13
//     root must not change which docs match.
//   - Cross-country: data distributed across multiple countries such that no
//     single country has both filter conditions satisfied at the same garage.
func TestNestedFilteringF17DeeperCorrelatedAndDifferentCarsSameGarage(t *testing.T) {
	const nestedClass = "F17"
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
			},
		},
	}

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
	country := func(garages ...map[string]any) map[string]any {
		anyGarages := make([]any, len(garages))
		for i, g := range garages {
			anyGarages[i] = g
		}
		return map[string]any{"garages": anyGarages}
	}
	fillerGarage := func() map[string]any {
		return garage(
			car("make", "toyota", "model", "corolla"),
			car("make", "kia", "model", "sportage"),
			car("make", "bmw", "model", "x3"),
		)
	}
	matchingGarage := func() map[string]any {
		return garage(
			car("make", "honda", "model", "corolla"),
			car("make", "kia", "model", "civic"),
			car("make", "bmw", "model", "x3"),
		)
	}
	fillerCountry := func() map[string]any {
		return country(fillerGarage(), fillerGarage(), fillerGarage())
	}

	const (
		// Cross-garage cases mirror F13 verbatim with a single country wrapper.
		// Names match F13's so the parallel is explicit.
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

		idRichMatchG0                 = strfmt.UUID("00000000-0000-0000-0000-00000000000d")
		idRichMatchG1                 = strfmt.UUID("00000000-0000-0000-0000-00000000000e")
		idRichMatchG2                 = strfmt.UUID("00000000-0000-0000-0000-00000000000f")
		idRichNoMatchAllFiller        = strfmt.UUID("00000000-0000-0000-0000-000000000010")
		idRichNoMatchOnlyMakeMatches  = strfmt.UUID("00000000-0000-0000-0000-000000000011")
		idRichNoMatchOnlyModelMatches = strfmt.UUID("00000000-0000-0000-0000-000000000012")
		idRichNoMatchSplitAcrossGars  = strfmt.UUID("00000000-0000-0000-0000-000000000013")
		idRichNoMatchSwapped          = strfmt.UUID("00000000-0000-0000-0000-000000000014")
		idRichNoMatchHondaCivicWrong  = strfmt.UUID("00000000-0000-0000-0000-000000000015")
		idRichNoMatchCivicOnlyAtCars2 = strfmt.UUID("00000000-0000-0000-0000-000000000016")
		idAbsentMissingModelField     = strfmt.UUID("00000000-0000-0000-0000-000000000017")

		idRichNoMatchCrossConfusion    = strfmt.UUID("00000000-0000-0000-0000-000000000018")
		idRichMatchHondaAtMultiplePos  = strfmt.UUID("00000000-0000-0000-0000-000000000019")
		idRichMatchTwoGaragesBoth      = strfmt.UUID("00000000-0000-0000-0000-00000000001a")
		idAbsentEmptyCarsArray         = strfmt.UUID("00000000-0000-0000-0000-00000000001b")
		idRichMatchHeterogeneousShapes = strfmt.UUID("00000000-0000-0000-0000-00000000001c")
		idRichNoMatchCivicAsMake       = strfmt.UUID("00000000-0000-0000-0000-00000000001d")

		// Cross-country cases: data distributed across countries such that no
		// single country has both filter conditions satisfied at the same garage.
		idCntrSplitMinimal          = strfmt.UUID("00000000-0000-0000-0000-00000000001e")
		idCntrSplitRich             = strfmt.UUID("00000000-0000-0000-0000-00000000001f")
		idCntrSplitAcrossMany       = strfmt.UUID("00000000-0000-0000-0000-000000000020")
		idCntrEachHasF13NoMatch     = strfmt.UUID("00000000-0000-0000-0000-000000000021")
		idCntrValuesAtWrongCarSlots = strfmt.UUID("00000000-0000-0000-0000-000000000022")

		idCntrMatchInC1             = strfmt.UUID("00000000-0000-0000-0000-000000000023")
		idCntrMatchInC2             = strfmt.UUID("00000000-0000-0000-0000-000000000024")
		idCntrMatchTwoCountriesBoth = strfmt.UUID("00000000-0000-0000-0000-000000000025")
		idCntrMatchHeterogeneous    = strfmt.UUID("00000000-0000-0000-0000-000000000026")

		idAbsentNoCountries           = strfmt.UUID("00000000-0000-0000-0000-000000000027")
		idAbsentCountryNoGaragesField = strfmt.UUID("00000000-0000-0000-0000-000000000028")
		idAbsentCountryEmptyGarages   = strfmt.UUID("00000000-0000-0000-0000-000000000029")
	)

	docs := []struct {
		id        strfmt.UUID
		countries []any
		note      string
	}{
		// ----- Cross-garage cases: F13 verbatim, wrapped in one country -----

		{
			id: idMatchMinimal,
			countries: []any{country(garage(
				car("make", "honda"),
				car("model", "civic"),
			))},
			note: "minimal same-garage match: cars[0].make=honda, cars[1].model=civic",
		},
		{
			id: idMatchTwoCarsBoth,
			countries: []any{country(garage(
				car("make", "honda", "model", "civic"),
				car("make", "honda", "model", "civic"),
			))},
			note: "both cars carry both fields; cars[0].make and cars[1].model still match",
		},
		{
			id: idMatchExtraCar,
			countries: []any{country(garage(
				car("make", "honda"),
				car("model", "civic"),
				car("make", "kia", "model", "sportage"),
			))},
			note: "cars[2] beyond the filter range is irrelevant",
		},
		{
			id: idMatchSecondGarage,
			countries: []any{country(
				garage(car("make", "toyota")),
				garage(car("make", "honda"), car("model", "civic")),
			)},
			note: "match satisfied by the second garage element",
		},
		{
			id: idNoMatchSplitGarages,
			countries: []any{country(
				garage(car("make", "honda")),
				garage(car("make", "x"), car("model", "civic")),
			)},
			note: "headline regression: cars[0].make in garage[0]; cars[1].model in garage[1] — split must reject",
		},
		{
			id:        idNoMatchSwapped,
			countries: []any{country(garage(car("model", "civic"), car("make", "honda")))},
			note:      "values exist but at swapped indices: cars[0].model=civic, cars[1].make=honda",
		},
		{
			id:        idNoMatchOnlyMakeNoSecondCar,
			countries: []any{country(garage(car("make", "honda")))},
			note:      "cars[1] absent → cars[1].model group resolves to empty",
		},
		{
			id: idNoMatchOnlyMakeWrongModel,
			countries: []any{country(garage(
				car("make", "honda"),
				car("make", "kia", "model", "sportage"),
			))},
			note: "cars[1] exists but with non-matching model — proves we don't accept any cars[1]",
		},
		{
			id: idNoMatchWrongMakeRightModel,
			countries: []any{country(garage(
				car("make", "toyota", "model", "corolla"),
				car("make", "kia", "model", "civic"),
			))},
			note: "cars[1].model right; cars[0].make wrong",
		},
		{
			id: idNoMatchAllWrong,
			countries: []any{country(garage(
				car("make", "toyota", "model", "corolla"),
				car("make", "kia", "model", "sportage"),
			))},
			note: "both cars exist with non-matching values",
		},
		{
			id:        idNoMatchNoCars,
			countries: []any{country(garageWithCity("berlin"))},
			note:      "garage with no cars at all",
		},
		{
			id: idNoMatchThirdCarMatchesModel,
			countries: []any{country(garage(
				car("make", "honda"),
				car("make", "kia", "model", "sportage"),
				car("model", "civic"),
			))},
			note: "cars[2].model=civic but filter targets cars[1] — must not match through cars[2]",
		},

		// Rich match cases: 3 garages × 3 cars in one country, all cars carry
		// both fields. One garage matches; the other two are pure filler.
		{
			id:        idRichMatchG0,
			countries: []any{country(matchingGarage(), fillerGarage(), fillerGarage())},
			note:      "rich: 3×3, match in first garage with surrounding filler",
		},
		{
			id:        idRichMatchG1,
			countries: []any{country(fillerGarage(), matchingGarage(), fillerGarage())},
			note:      "rich: 3×3, match in middle garage",
		},
		{
			id:        idRichMatchG2,
			countries: []any{country(fillerGarage(), fillerGarage(), matchingGarage())},
			note:      "rich: 3×3, match in last garage",
		},

		// Rich no-match cases: every garage fully populated; rejection is
		// purely due to value mismatch at the targeted positions.
		{
			id:        idRichNoMatchAllFiller,
			countries: []any{country(fillerGarage(), fillerGarage(), fillerGarage())},
			note:      "rich: 3×3 all filler — no honda or civic anywhere",
		},
		{
			id: idRichNoMatchOnlyMakeMatches,
			countries: []any{country(
				garage(
					car("make", "honda", "model", "corolla"),
					car("make", "kia", "model", "sportage"),
					car("make", "bmw", "model", "x3"),
				),
				fillerGarage(),
				fillerGarage(),
			)},
			note: "rich: cars[0].make=honda satisfied at g0; civic missing entirely",
		},
		{
			id: idRichNoMatchOnlyModelMatches,
			countries: []any{country(
				garage(
					car("make", "toyota", "model", "corolla"),
					car("make", "kia", "model", "civic"),
					car("make", "bmw", "model", "x3"),
				),
				fillerGarage(),
				fillerGarage(),
			)},
			note: "rich: cars[1].model=civic satisfied at g0; honda missing entirely",
		},
		{
			id: idRichNoMatchSplitAcrossGars,
			countries: []any{country(
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
			)},
			note: "rich: cars[0].make=honda in g0; cars[1].model=civic in g1 — split across garages",
		},
		{
			id: idRichNoMatchSwapped,
			countries: []any{country(
				garage(
					car("make", "toyota", "model", "civic"),
					car("make", "honda", "model", "corolla"),
					car("make", "kia", "model", "sportage"),
				),
				fillerGarage(),
				fillerGarage(),
			)},
			note: "rich: g0 has both honda and civic but at swapped indices",
		},
		{
			id: idRichNoMatchHondaCivicWrong,
			countries: []any{country(
				garage(
					car("make", "toyota", "model", "corolla"),
					car("make", "honda", "model", "sportage"),
					car("make", "kia", "model", "civic"),
				),
				fillerGarage(),
				fillerGarage(),
			)},
			note: "rich: honda at cars[1], civic at cars[2] — values present but never at filtered positions",
		},
		{
			id: idRichNoMatchCivicOnlyAtCars2,
			countries: []any{country(
				garage(
					car("make", "honda", "model", "corolla"),
					car("make", "kia", "model", "sportage"),
					car("make", "toyota", "model", "civic"),
				),
				fillerGarage(),
				fillerGarage(),
			)},
			note: "rich: cars[0].make=honda satisfied; civic only at cars[2]",
		},

		{
			id: idAbsentMissingModelField,
			countries: []any{country(garage(
				map[string]any{"make": "honda"},
				map[string]any{"make": "kia"},
				map[string]any{"make": "bmw"},
			))},
			note: "absent: 3 cars with make but no model field",
		},

		{
			id: idRichNoMatchCrossConfusion,
			countries: []any{country(
				garage(
					car("make", "civic", "model", "honda"),
					car("make", "civic", "model", "honda"),
					car("make", "civic", "model", "honda"),
				),
				fillerGarage(),
				fillerGarage(),
			)},
			note: "field/value cross-confusion: honda and civic on swapped fields",
		},

		{
			id: idRichMatchHondaAtMultiplePos,
			countries: []any{country(
				garage(
					car("make", "honda", "model", "corolla"),
					car("make", "honda", "model", "civic"),
					car("make", "honda", "model", "x3"),
				),
				fillerGarage(),
				fillerGarage(),
			)},
			note: "every car in g0 has make=honda; cars[1].model=civic — match despite extra honda noise",
		},

		{
			id:        idRichMatchTwoGaragesBoth,
			countries: []any{country(matchingGarage(), matchingGarage(), fillerGarage())},
			note:      "g0 and g1 both satisfy the filter — dedup",
		},

		{
			id: idAbsentEmptyCarsArray,
			countries: []any{country(
				map[string]any{"cars": []any{}},
				map[string]any{"cars": []any{}},
				map[string]any{"cars": []any{}},
			)},
			note: "absent: every garage has explicit empty cars array",
		},

		{
			id: idRichMatchHeterogeneousShapes,
			countries: []any{country(
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
			)},
			note: "heterogeneous: 2 / 5 / 4 cars per garage; match in middle 5-car garage",
		},

		{
			id: idRichNoMatchCivicAsMake,
			countries: []any{country(
				garage(
					car("make", "civic", "model", "civic"),
					car("make", "civic", "model", "civic"),
					car("make", "civic", "model", "civic"),
				),
				fillerGarage(),
				fillerGarage(),
			)},
			note: "civic-as-make: cars[1].model=civic satisfied but cars[0].make=civic ≠ honda",
		},

		// ----- Cross-country cases: data spread across countries -----

		{
			id: idCntrSplitMinimal,
			countries: []any{
				country(garage(car("make", "honda"))),
				country(garage(car("make", "x"), car("model", "civic"))),
			},
			note: "minimal cross-country split: country[0] only honda, country[1] only civic",
		},
		{
			id: idCntrSplitRich,
			countries: []any{
				country(
					garage(
						car("make", "honda", "model", "corolla"),
						car("make", "kia", "model", "sportage"),
						car("make", "bmw", "model", "x3"),
					),
					fillerGarage(),
					fillerGarage(),
				),
				country(
					garage(
						car("make", "toyota", "model", "corolla"),
						car("make", "kia", "model", "civic"),
						car("make", "bmw", "model", "x3"),
					),
					fillerGarage(),
					fillerGarage(),
				),
				fillerCountry(),
			},
			note: "rich cross-country split: 3×3 per country; honda only in country[0], civic only in country[1]",
		},
		{
			id: idCntrSplitAcrossMany,
			countries: []any{
				country(
					garage(
						car("make", "honda", "model", "corolla"),
						car("make", "kia", "model", "sportage"),
						car("make", "bmw", "model", "x3"),
					),
					fillerGarage(),
					fillerGarage(),
				),
				country(
					garage(
						car("make", "toyota", "model", "corolla"),
						car("make", "kia", "model", "civic"),
						car("make", "bmw", "model", "x3"),
					),
					fillerGarage(),
					fillerGarage(),
				),
				country(
					garage(
						car("make", "audi", "model", "civic"),
						car("make", "ford", "model", "focus"),
						car("make", "nissan", "model", "sentra"),
					),
					fillerGarage(),
					fillerGarage(),
				),
			},
			note: "honda only in country[0]; civic spread across countries[1] and [2]",
		},
		{
			id: idCntrEachHasF13NoMatch,
			countries: []any{
				// country[0]: F13's split-garages no-match shape — make in g0, civic in g1
				country(
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
				),
				// country[1]: F13's swap no-match shape — cars[0].model=civic, cars[1].make=honda
				country(
					garage(
						car("make", "toyota", "model", "civic"),
						car("make", "honda", "model", "corolla"),
						car("make", "kia", "model", "sportage"),
					),
					fillerGarage(),
					fillerGarage(),
				),
				fillerCountry(),
			},
			note: "each country independently fails F13: country[0] has F13-split, country[1] has F13-swap",
		},
		{
			id: idCntrValuesAtWrongCarSlots,
			countries: []any{
				// country[0]: honda exists but at cars[1] (wrong slot for make)
				country(
					garage(
						car("make", "toyota", "model", "corolla"),
						car("make", "honda", "model", "sportage"),
						car("make", "bmw", "model", "x3"),
					),
					fillerGarage(),
					fillerGarage(),
				),
				// country[1]: civic exists but at cars[2] (wrong slot for model)
				country(
					garage(
						car("make", "toyota", "model", "corolla"),
						car("make", "kia", "model", "sportage"),
						car("make", "bmw", "model", "civic"),
					),
					fillerGarage(),
					fillerGarage(),
				),
				fillerCountry(),
			},
			note: "values exist across countries but always at wrong cars[N] slots",
		},

		// Cross-country match cases: matching garage at non-first country
		// positions and dedup across multiple matching countries.
		{
			id:        idCntrMatchInC1,
			countries: []any{fillerCountry(), country(matchingGarage(), fillerGarage(), fillerGarage()), fillerCountry()},
			note:      "match in middle country — engine must check all countries",
		},
		{
			id:        idCntrMatchInC2,
			countries: []any{fillerCountry(), fillerCountry(), country(matchingGarage(), fillerGarage(), fillerGarage())},
			note:      "match in last country — engine must check all countries",
		},
		{
			id: idCntrMatchTwoCountriesBoth,
			countries: []any{
				country(matchingGarage(), fillerGarage(), fillerGarage()),
				country(matchingGarage(), fillerGarage(), fillerGarage()),
				fillerCountry(),
			},
			note: "two countries each have a matching garage — result must dedup to single doc",
		},
		{
			id: idCntrMatchHeterogeneous,
			countries: []any{
				country(fillerGarage()),
				country(
					fillerGarage(),
					fillerGarage(),
					matchingGarage(),
					fillerGarage(),
					fillerGarage(),
				),
				country(fillerGarage(), fillerGarage(), fillerGarage(), fillerGarage()),
			},
			note: "heterogeneous: 1 / 5 / 4 garages per country; match in middle country's middle garage",
		},

		// Country-level absent-data variants.
		{
			id:        idAbsentNoCountries,
			countries: []any{},
			note:      "absent: empty countries array",
		},
		{
			id: idAbsentCountryNoGaragesField,
			countries: []any{
				map[string]any{},
				map[string]any{},
			},
			note: "absent: countries exist but neither has a garages field",
		},
		{
			id: idAbsentCountryEmptyGarages,
			countries: []any{
				map[string]any{"garages": []any{}},
				map[string]any{"garages": []any{}},
			},
			note: "absent: countries exist with explicit empty garages arrays",
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
		makeFilter("countries.garages.cars[0].make", "honda"),
		makeFilter("countries.garages.cars[1].model", "civic"),
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
		idCntrMatchInC1,
		idCntrMatchInC2,
		idCntrMatchTwoCountriesBoth,
		idCntrMatchHeterogeneous,
	}
	assert.ElementsMatch(t, want, got)
}

// TestNestedFilteringIsNullWithArrNInCorrelatedAnd exercises correlated AND
// filters where one condition pins to a specific array index (arr[N]) and the
// other is an IsNull check on a property in the same scope. Coverage spans
// three arr[N] depths (root countries[N], intermediate garages[N], deepest
// cars[N]) crossed with IsNull polarity (true/false), plus dual-IsNull
// patterns (no positive value condition).
//
// At each constraint level multiple IsNull depths are exercised where they
// make sense: IsNull on a leaf, on the intermediate object[] (e.g. cars), and
// on the outer intermediate object[] (e.g. garages). This probes _exists key
// generation across arr[N]-restricted scopes and the resolver's ability to
// combine arr[N] restriction with both leaf-level and intermediate-level
// IsNull.
func TestNestedFilteringIsNullWithArrNInCorrelatedAnd(t *testing.T) {
	const nestedClass = "IsNullArrN"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{
				Name:     "countries",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
					{
						Name:     "garages",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
							{
								Name:     "cars",
								DataType: schema.DataTypeObjectArray.PropString(),
								NestedProperties: []*models.NestedProperty{
									{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
									{Name: "model", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
								},
							},
						},
					},
				},
			},
		},
	}

	car := func(props ...string) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i]] = props[i+1]
		}
		return out
	}
	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}

	valueFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	isNullFilter := func(path string, isNull bool) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: isNull},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(a, b *filters.LocalFilter) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{*a.Root, *b.Root},
		}}
	}

	type docDef struct {
		id        strfmt.UUID
		countries []any
		note      string
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id,
				Properties: map[string]any{"countries": d.countries},
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// uuids returns sub-test-scoped UUIDs starting from 0x01.
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	// Reusable filler car: non-matching make and model populated.
	fillerCar := func() map[string]any { return car("make", "toyota", "model", "corolla") }

	// ----- Constraint at countries[1] -----

	// 1a: countries[1].name = "germany" AND countries[1].garages.cars.model IS NULL
	// TODO aliszka:nested_filtering: this asserts CURRENT universal IsNull
	// semantics on the deep path `countries[1].garages.cars.model`. When
	// the planned existential IsNull rewrite lands, expectations flip:
	// idMatchNoGarages/idMatchEmptyGarages/idMatchGarageNoCars STOP
	// matching (vacuous matches go away); idNoMatchModelInOtherGarage
	// STARTS matching (cross-garage absent satisfies existential). Could
	// also be obviated by explicit ANY/ALL/NONE quantifiers.
	t.Run("regression_1a_countries_value_and_isNull_cars_model", func(t *testing.T) {
		idMatch := uuid(1)
		idMatchNoGarages := uuid(2)
		idMatchEmptyGarages := uuid(3)
		idMatchGarageNoCars := uuid(4)
		idMatchModelOnlyInOtherCntr := uuid(5)
		idNoMatchModelInCntr1 := uuid(6)
		idNoMatchModelInOtherGarage := uuid(7)
		idNoMatchWrongName := uuid(8)
		idNoMatchGermanyAtCntr0 := uuid(9)
		idAbsentCntr1Empty := uuid(10)
		idAbsentOneCountry := uuid(11)

		fillerCntr := func() map[string]any {
			return map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})}
		}

		docs := []docDef{
			{id: idMatch, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
			}, note: "countries[1].name=germany; one car with only make field — no model anywhere → match"},
			{id: idMatchNoGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany"},
			}, note: "countries[1].name=germany; no garages → no model anywhere → match"},
			{id: idMatchEmptyGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": []any{}},
			}, note: "countries[1].name=germany; garages=[] → match"},
			{id: idMatchGarageNoCars, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})},
			}, note: "countries[1].name=germany; garage with city only, no cars → match"},
			{id: idMatchModelOnlyInOtherCntr, countries: []any{
				map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})},
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
			}, note: "model in countries[0] but countries[1] has no model → match (IsNull restricted to countries[1])"},
			{id: idNoMatchModelInCntr1, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})},
			}, note: "countries[1] has model present → no match"},
			{id: idNoMatchModelInOtherGarage, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(
					map[string]any{"cars": asArr(car("make", "x"))},
					map[string]any{"cars": asArr(car("model", "y"))},
				)},
			}, note: "countries[1] g0 no model, but g1 has model → no match (IsNull is per countries[1] scope)"},
			{id: idNoMatchWrongName, countries: []any{
				fillerCntr(),
				map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
			}, note: "countries[1].name=france ≠ germany → no match"},
			{id: idNoMatchGermanyAtCntr0, countries: []any{
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
				map[string]any{"name": "france"},
			}, note: "germany at countries[0]; countries[1].name=france → no match"},
			{id: idAbsentCntr1Empty, countries: []any{
				fillerCntr(),
				map[string]any{},
			}, note: "countries[1] is empty {} (no name) → no match"},
			{id: idAbsentOneCountry, countries: []any{
				map[string]any{"name": "germany"},
			}, note: "only countries[0]; countries[1] missing → no match"},
		}

		filter := andFilter(
			valueFilter("countries[1].name", "germany"),
			isNullFilter("countries[1].garages.cars.model", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchNoGarages, idMatchEmptyGarages, idMatchGarageNoCars, idMatchModelOnlyInOtherCntr})
	})

	// 1b: countries[1].name = "germany" AND countries[1].garages.cars IS NULL
	t.Run("1b_countries_value_and_isNull_cars", func(t *testing.T) {
		idMatchNoGarages := uuid(1)
		idMatchEmptyGarages := uuid(2)
		idMatchGarageNoCars := uuid(3)
		idMatchEmptyCarsArray := uuid(4)
		idMatchCarsOnlyInOtherCntr := uuid(5)
		idNoMatchCarsPresent := uuid(6)
		idNoMatchCarsInOtherGarage := uuid(7)
		idNoMatchWrongName := uuid(8)
		idAbsentCntr1Empty := uuid(9)

		fillerCntr := func() map[string]any {
			return map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})}
		}

		docs := []docDef{
			{id: idMatchNoGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany"},
			}, note: "countries[1].name=germany; no garages → no cars → match"},
			{id: idMatchEmptyGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": []any{}},
			}, note: "countries[1].name=germany; garages=[] → no cars → match"},
			{id: idMatchGarageNoCars, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})},
			}, note: "garage with no cars field → match"},
			{id: idMatchEmptyCarsArray, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": []any{}})},
			}, note: "garage with cars=[] → no cars → match"},
			{id: idMatchCarsOnlyInOtherCntr, countries: []any{
				map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})},
				map[string]any{"name": "germany"},
			}, note: "cars in countries[0] but countries[1] has no cars → match"},
			{id: idNoMatchCarsPresent, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})},
			}, note: "countries[1] has cars → no match"},
			{id: idNoMatchCarsInOtherGarage, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"cars": asArr(fillerCar())},
				)},
			}, note: "countries[1] g0 has no cars but g1 does → no match (cars exist within countries[1])"},
			{id: idNoMatchWrongName, countries: []any{
				fillerCntr(),
				map[string]any{"name": "france"},
			}, note: "countries[1].name=france → no match"},
			{id: idAbsentCntr1Empty, countries: []any{
				fillerCntr(),
				map[string]any{},
			}, note: "countries[1] empty (no name) → no match"},
		}

		filter := andFilter(
			valueFilter("countries[1].name", "germany"),
			isNullFilter("countries[1].garages.cars", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchNoGarages, idMatchEmptyGarages, idMatchGarageNoCars, idMatchEmptyCarsArray, idMatchCarsOnlyInOtherCntr})
	})

	// 1c: countries[1].name = "germany" AND countries[1].garages IS NULL
	t.Run("1c_countries_value_and_isNull_garages", func(t *testing.T) {
		idMatchNoGarages := uuid(1)
		idMatchEmptyGarages := uuid(2)
		idMatchGaragesOnlyInOtherCntr := uuid(3)
		idNoMatchGaragesPresent := uuid(4)
		idNoMatchGaragesNoCars := uuid(5)
		idNoMatchWrongName := uuid(6)
		idAbsentCntr1Empty := uuid(7)

		fillerCntr := func() map[string]any {
			return map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})}
		}

		docs := []docDef{
			{id: idMatchNoGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany"},
			}, note: "countries[1] has no garages field → match"},
			{id: idMatchEmptyGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": []any{}},
			}, note: "countries[1] has garages=[] → no garages elements → match"},
			{id: idMatchGaragesOnlyInOtherCntr, countries: []any{
				map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})},
				map[string]any{"name": "germany"},
			}, note: "garages in countries[0] only → match"},
			{id: idNoMatchGaragesPresent, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})},
			}, note: "countries[1] has garage → no match"},
			{id: idNoMatchGaragesNoCars, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{})},
			}, note: "countries[1] has empty garage element → no match (garage exists)"},
			{id: idNoMatchWrongName, countries: []any{
				fillerCntr(),
				map[string]any{"name": "france"},
			}, note: "countries[1].name=france → no match"},
			{id: idAbsentCntr1Empty, countries: []any{
				fillerCntr(),
				map[string]any{},
			}, note: "countries[1] empty → no match"},
		}

		filter := andFilter(
			valueFilter("countries[1].name", "germany"),
			isNullFilter("countries[1].garages", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchNoGarages, idMatchEmptyGarages, idMatchGaragesOnlyInOtherCntr})
	})

	// 2a: countries[1].name = "germany" AND countries[1].garages.cars.model IS NOT NULL
	t.Run("2a_countries_value_and_isNotNull_cars_model", func(t *testing.T) {
		idMatchModelInCntr1 := uuid(1)
		idMatchModelInG1 := uuid(2)
		idMatchModelInCars2 := uuid(3)
		idMatchCrossConfusion := uuid(4)
		idNoMatchNoModel := uuid(5)
		idNoMatchModelOnlyInOtherCntr := uuid(6)
		idNoMatchWrongName := uuid(7)
		idNoMatchNoGarages := uuid(8)
		idAbsentCntr1Empty := uuid(9)

		fillerCntr := func() map[string]any {
			return map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})}
		}

		docs := []docDef{
			{id: idMatchModelInCntr1, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})},
			}, note: "countries[1] has model present → match"},
			{id: idMatchModelInG1, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(
					map[string]any{"cars": asArr(car("make", "x"))},
					map[string]any{"cars": asArr(car("model", "y"))},
				)},
			}, note: "countries[1] g1 has model → match (model exists somewhere in countries[1])"},
			{id: idMatchModelInCars2, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"), car("make", "y"), car("model", "z"))})},
			}, note: "countries[1] cars[2] has model → match"},
			{id: idMatchCrossConfusion, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "civic", "model", "honda"))})},
			}, note: "model present (value 'honda' on model field) → match (IsNotNull only checks presence)"},
			{id: idNoMatchNoModel, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
			}, note: "countries[1] has no model anywhere → no match"},
			{id: idNoMatchModelOnlyInOtherCntr, countries: []any{
				map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})},
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
			}, note: "model in countries[0] only → no match (IsNotNull restricted to countries[1])"},
			{id: idNoMatchWrongName, countries: []any{
				fillerCntr(),
				map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})},
			}, note: "countries[1].name=france → no match"},
			{id: idNoMatchNoGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany"},
			}, note: "germany but no garages → no model → no match"},
			{id: idAbsentCntr1Empty, countries: []any{
				fillerCntr(),
				map[string]any{},
			}, note: "countries[1] empty → no match"},
		}

		filter := andFilter(
			valueFilter("countries[1].name", "germany"),
			isNullFilter("countries[1].garages.cars.model", false),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchModelInCntr1, idMatchModelInG1, idMatchModelInCars2, idMatchCrossConfusion})
	})

	// 2b: countries[1].name = "germany" AND countries[1].garages.cars IS NOT NULL
	t.Run("2b_countries_value_and_isNotNull_cars", func(t *testing.T) {
		idMatchCarsPresent := uuid(1)
		idMatchCarsInG1 := uuid(2)
		idNoMatchNoCars := uuid(3)
		idNoMatchEmptyCars := uuid(4)
		idNoMatchCarsOnlyInOtherCntr := uuid(5)
		idNoMatchWrongName := uuid(6)
		idAbsentCntr1Empty := uuid(7)

		fillerCntr := func() map[string]any {
			return map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})}
		}

		docs := []docDef{
			{id: idMatchCarsPresent, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})},
			}, note: "countries[1] has cars → match"},
			{id: idMatchCarsInG1, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"cars": asArr(fillerCar())},
				)},
			}, note: "countries[1] g0 no cars; g1 has cars → match"},
			{id: idNoMatchNoCars, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})},
			}, note: "garages have no cars → no match"},
			{id: idNoMatchEmptyCars, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": []any{}})},
			}, note: "garages have cars=[] → no match (no car elements)"},
			{id: idNoMatchCarsOnlyInOtherCntr, countries: []any{
				map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})},
				map[string]any{"name": "germany"},
			}, note: "cars in countries[0] only → no match"},
			{id: idNoMatchWrongName, countries: []any{
				fillerCntr(),
				map[string]any{"name": "france"},
			}, note: "wrong name → no match"},
			{id: idAbsentCntr1Empty, countries: []any{
				fillerCntr(),
				map[string]any{},
			}, note: "empty countries[1] → no match"},
		}

		filter := andFilter(
			valueFilter("countries[1].name", "germany"),
			isNullFilter("countries[1].garages.cars", false),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchCarsPresent, idMatchCarsInG1})
	})

	// 2c: countries[1].name = "germany" AND countries[1].garages IS NOT NULL
	t.Run("2c_countries_value_and_isNotNull_garages", func(t *testing.T) {
		idMatchGaragePresent := uuid(1)
		idMatchEmptyGarageElement := uuid(2)
		idNoMatchNoGarages := uuid(3)
		idNoMatchEmptyGarages := uuid(4)
		idNoMatchGaragesOnlyInOtherCntr := uuid(5)
		idNoMatchWrongName := uuid(6)
		idAbsentCntr1Empty := uuid(7)

		fillerCntr := func() map[string]any {
			return map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})}
		}

		docs := []docDef{
			{id: idMatchGaragePresent, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})},
			}, note: "countries[1] has garage → match"},
			{id: idMatchEmptyGarageElement, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": asArr(map[string]any{})},
			}, note: "countries[1] has empty garage element → match (garage element exists)"},
			{id: idNoMatchNoGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany"},
			}, note: "no garages field → no match"},
			{id: idNoMatchEmptyGarages, countries: []any{
				fillerCntr(),
				map[string]any{"name": "germany", "garages": []any{}},
			}, note: "garages=[] → no garage elements → no match"},
			{id: idNoMatchGaragesOnlyInOtherCntr, countries: []any{
				map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(fillerCar())})},
				map[string]any{"name": "germany"},
			}, note: "garages in countries[0] only → no match"},
			{id: idNoMatchWrongName, countries: []any{
				fillerCntr(),
				map[string]any{"name": "france"},
			}, note: "wrong name → no match"},
			{id: idAbsentCntr1Empty, countries: []any{
				fillerCntr(),
				map[string]any{},
			}, note: "empty countries[1] → no match"},
		}

		filter := andFilter(
			valueFilter("countries[1].name", "germany"),
			isNullFilter("countries[1].garages", false),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchGaragePresent, idMatchEmptyGarageElement})
	})

	// 7: countries[1].garages.cars.make IS NOT NULL AND countries[1].garages.cars.model IS NULL
	//
	// Semantics note: both conditions reference paths that share the same
	// unconstrained deepest ancestor (garages.cars) below the constrained
	// countries[1]. Same-element correlation therefore lands at cars level —
	// the filter matches when SOME car within countries[1] has make present and
	// model absent at THAT same car. Compare sub-test 1a where the value
	// condition lives at countries[1] direct (no shared unconstrained ancestor
	// with the IsNull condition); there same-element falls back to countries[1]
	// level and IsNull becomes "no model anywhere within countries[1]".
	t.Run("7_countries_dual_isNull_make_present_model_absent", func(t *testing.T) {
		idMatchMakeOnly := uuid(1)
		idMatchMakeMultiCars := uuid(2)
		idMatchMakeInG0NoModel := uuid(3)
		idMatchSwapAcrossCntrs := uuid(4)
		idNoMatchModelPresent := uuid(5)
		idNoMatchNoMake := uuid(6)
		idNoMatchNoCars := uuid(7)
		idNoMatchEveryCarHasModel := uuid(8)
		idAbsentCntr1Empty := uuid(9)

		fillerCntr := func() map[string]any {
			return map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})}
		}

		docs := []docDef{
			{id: idMatchMakeOnly, countries: []any{
				fillerCntr(),
				map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
			}, note: "cars[0] in countries[1] has make, no model → match (per-cars same-element)"},
			{id: idMatchMakeMultiCars, countries: []any{
				fillerCntr(),
				map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "x"), car("make", "y"))})},
			}, note: "multiple cars in countries[1] each have make+nomodel → match"},
			{id: idMatchMakeInG0NoModel, countries: []any{
				fillerCntr(),
				map[string]any{"garages": asArr(
					map[string]any{"cars": asArr(car("make", "x"))},
					map[string]any{"cars": asArr(car("model", "y"))},
				)},
			}, note: "g0.cars[0] has make+nomodel (satisfies per-cars); g1.cars[0]'s model excludes only that specific car → match"},
			{id: idMatchSwapAcrossCntrs, countries: []any{
				map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("model", "y"))})},
				map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
			}, note: "countries[0] has model, countries[1] has make+nomodel car → match"},
			{id: idNoMatchModelPresent, countries: []any{
				fillerCntr(),
				map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})},
			}, note: "single car has both make and model → exclude removes that car; no other cars → no match"},
			{id: idNoMatchNoMake, countries: []any{
				fillerCntr(),
				map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("model", "y"))})},
			}, note: "only model, no make anywhere → make IS NOT NULL fails at every car → no match"},
			{id: idNoMatchNoCars, countries: []any{
				fillerCntr(),
				map[string]any{"garages": asArr(map[string]any{"city": "berlin"})},
			}, note: "no cars → no car-element to satisfy → no match"},
			{id: idNoMatchEveryCarHasModel, countries: []any{
				fillerCntr(),
				map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "x", "model", "y"),
					car("make", "a", "model", "b"),
				)})},
			}, note: "every car in countries[1] has both make and model → exclude removes every car → no match"},
			{id: idAbsentCntr1Empty, countries: []any{
				fillerCntr(),
				map[string]any{},
			}, note: "empty countries[1] → no match"},
		}

		filter := andFilter(
			isNullFilter("countries[1].garages.cars.make", false),
			isNullFilter("countries[1].garages.cars.model", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchMakeOnly, idMatchMakeMultiCars, idMatchMakeInG0NoModel, idMatchSwapAcrossCntrs})
	})

	// ----- Constraint at countries.garages[1] -----

	// 3a: countries.garages[1].city = "berlin" AND countries.garages[1].cars.model IS NULL
	// TODO aliszka:nested_filtering: this asserts CURRENT universal IsNull
	// semantics on the deep path `countries.garages[1].cars.model`. When
	// the planned existential IsNull rewrite lands, vacuous matches
	// (no cars / no model anywhere) are removed and cross-car-with-model
	// matches are added. A missing discriminator doc should be added at
	// that time.
	t.Run("regression_3a_garages_value_and_isNull_cars_model", func(t *testing.T) {
		idMatchMinimal := uuid(1)
		idMatchNoCars := uuid(2)
		idMatchEmptyCars := uuid(3)
		idMatchModelOnlyInG0 := uuid(4)
		idNoMatchModelInG1 := uuid(5)
		idNoMatchWrongCity := uuid(6)
		idNoMatchBerlinAtG0 := uuid(7)
		idAbsentG1Empty := uuid(8)

		docs := []docDef{
			{id: idMatchMinimal, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": asArr(car("make", "x"))},
			)}}, note: "garages[1]: city=berlin, cars only have make → match"},
			{id: idMatchNoCars, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin"},
			)}}, note: "garages[1]: city=berlin, no cars → match"},
			{id: idMatchEmptyCars, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": []any{}},
			)}}, note: "garages[1]: city=berlin, cars=[] → match"},
			{id: idMatchModelOnlyInG0, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich", "cars": asArr(car("make", "x", "model", "y"))},
				map[string]any{"city": "berlin", "cars": asArr(car("make", "x"))},
			)}}, note: "model only in garages[0]; garages[1] has no model → match (IsNull restricted to garages[1])"},
			{id: idNoMatchModelInG1, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": asArr(car("make", "x", "model", "y"))},
			)}}, note: "garages[1].cars[0].model present → no match"},
			{id: idNoMatchWrongCity, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "paris", "cars": asArr(car("make", "x"))},
			)}}, note: "garages[1].city=paris → no match"},
			{id: idNoMatchBerlinAtG0, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "berlin"},
				map[string]any{"city": "paris", "cars": asArr(car("make", "x"))},
			)}}, note: "berlin at garages[0]; garages[1].city=paris → no match"},
			{id: idAbsentG1Empty, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{},
			)}}, note: "garages[1] empty (no city) → no match"},
		}

		filter := andFilter(
			valueFilter("countries.garages[1].city", "berlin"),
			isNullFilter("countries.garages[1].cars.model", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchMinimal, idMatchNoCars, idMatchEmptyCars, idMatchModelOnlyInG0})
	})

	// 3b: countries.garages[1].city = "berlin" AND countries.garages[1].cars IS NULL
	t.Run("3b_garages_value_and_isNull_cars", func(t *testing.T) {
		idMatchNoCars := uuid(1)
		idMatchEmptyCars := uuid(2)
		idMatchCarsOnlyInG0 := uuid(3)
		idNoMatchCarsPresent := uuid(4)
		idNoMatchWrongCity := uuid(5)
		idAbsentG1Empty := uuid(6)

		docs := []docDef{
			{id: idMatchNoCars, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin"},
			)}}, note: "garages[1]: berlin, no cars → match"},
			{id: idMatchEmptyCars, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": []any{}},
			)}}, note: "garages[1]: berlin, cars=[] → match"},
			{id: idMatchCarsOnlyInG0, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich", "cars": asArr(fillerCar())},
				map[string]any{"city": "berlin"},
			)}}, note: "cars only in garages[0]; garages[1] has no cars → match"},
			{id: idNoMatchCarsPresent, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": asArr(fillerCar())},
			)}}, note: "garages[1] has cars → no match"},
			{id: idNoMatchWrongCity, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "paris"},
			)}}, note: "wrong city → no match"},
			{id: idAbsentG1Empty, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{},
			)}}, note: "garages[1] empty → no match"},
		}

		filter := andFilter(
			valueFilter("countries.garages[1].city", "berlin"),
			isNullFilter("countries.garages[1].cars", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchNoCars, idMatchEmptyCars, idMatchCarsOnlyInG0})
	})

	// 4a: countries.garages[1].city = "berlin" AND countries.garages[1].cars.model IS NOT NULL
	t.Run("4a_garages_value_and_isNotNull_cars_model", func(t *testing.T) {
		idMatchModelInG1 := uuid(1)
		idMatchModelInDeepCar := uuid(2)
		idMatchCrossConfusion := uuid(3)
		idNoMatchNoModel := uuid(4)
		idNoMatchNoCars := uuid(5)
		idNoMatchModelOnlyInG0 := uuid(6)
		idNoMatchWrongCity := uuid(7)
		idAbsentG1Empty := uuid(8)

		docs := []docDef{
			{id: idMatchModelInG1, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": asArr(car("make", "x", "model", "y"))},
			)}}, note: "garages[1] has model → match"},
			{id: idMatchModelInDeepCar, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": asArr(car("make", "x"), car("make", "y"), car("model", "z"))},
			)}}, note: "garages[1].cars[2] has model → match"},
			{id: idMatchCrossConfusion, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": asArr(car("make", "civic", "model", "honda"))},
			)}}, note: "garages[1] has model field present (with value 'honda') → match"},
			{id: idNoMatchNoModel, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": asArr(car("make", "x"))},
			)}}, note: "garages[1] has no model → no match"},
			{id: idNoMatchNoCars, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin"},
			)}}, note: "garages[1] has no cars → no model → no match"},
			{id: idNoMatchModelOnlyInG0, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich", "cars": asArr(car("make", "x", "model", "y"))},
				map[string]any{"city": "berlin", "cars": asArr(car("make", "x"))},
			)}}, note: "model only in garages[0]; garages[1] has no model → no match"},
			{id: idNoMatchWrongCity, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "paris", "cars": asArr(car("make", "x", "model", "y"))},
			)}}, note: "wrong city → no match"},
			{id: idAbsentG1Empty, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{},
			)}}, note: "garages[1] empty → no match"},
		}

		filter := andFilter(
			valueFilter("countries.garages[1].city", "berlin"),
			isNullFilter("countries.garages[1].cars.model", false),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchModelInG1, idMatchModelInDeepCar, idMatchCrossConfusion})
	})

	// 4b: countries.garages[1].city = "berlin" AND countries.garages[1].cars IS NOT NULL
	t.Run("4b_garages_value_and_isNotNull_cars", func(t *testing.T) {
		idMatchCarsPresent := uuid(1)
		idNoMatchNoCars := uuid(2)
		idNoMatchEmptyCars := uuid(3)
		idNoMatchCarsOnlyInG0 := uuid(4)
		idNoMatchWrongCity := uuid(5)
		idAbsentG1Empty := uuid(6)

		docs := []docDef{
			{id: idMatchCarsPresent, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": asArr(fillerCar())},
			)}}, note: "garages[1] has cars → match"},
			{id: idNoMatchNoCars, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin"},
			)}}, note: "garages[1] no cars → no match"},
			{id: idNoMatchEmptyCars, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "berlin", "cars": []any{}},
			)}}, note: "garages[1] cars=[] → no match"},
			{id: idNoMatchCarsOnlyInG0, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich", "cars": asArr(fillerCar())},
				map[string]any{"city": "berlin"},
			)}}, note: "cars in garages[0] only → no match"},
			{id: idNoMatchWrongCity, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{"city": "paris", "cars": asArr(fillerCar())},
			)}}, note: "wrong city → no match"},
			{id: idAbsentG1Empty, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"city": "munich"},
				map[string]any{},
			)}}, note: "garages[1] empty → no match"},
		}

		filter := andFilter(
			valueFilter("countries.garages[1].city", "berlin"),
			isNullFilter("countries.garages[1].cars", false),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchCarsPresent})
	})

	// 8: countries.garages[1].cars.make IS NOT NULL AND countries.garages[1].cars.model IS NULL
	t.Run("8_garages_dual_isNull_make_present_model_absent", func(t *testing.T) {
		idMatchMakeOnly := uuid(1)
		idMatchMakeAndModelInG0 := uuid(2)
		idNoMatchModelInG1 := uuid(3)
		idNoMatchNoMake := uuid(4)
		idNoMatchNoCars := uuid(5)
		idAbsentG1Empty := uuid(6)

		docs := []docDef{
			{id: idMatchMakeOnly, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"cars": asArr(fillerCar())},
				map[string]any{"cars": asArr(car("make", "x"))},
			)}}, note: "garages[1].cars: make present, no model → match"},
			{id: idMatchMakeAndModelInG0, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"cars": asArr(car("make", "x", "model", "y"))},
				map[string]any{"cars": asArr(car("make", "x"))},
			)}}, note: "model in garages[0] only; garages[1] has only make → match"},
			{id: idNoMatchModelInG1, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"cars": asArr(fillerCar())},
				map[string]any{"cars": asArr(car("make", "x", "model", "y"))},
			)}}, note: "garages[1] has both → no match (model present)"},
			{id: idNoMatchNoMake, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"cars": asArr(fillerCar())},
				map[string]any{"cars": asArr(car("model", "y"))},
			)}}, note: "garages[1]: only model, no make → no match"},
			{id: idNoMatchNoCars, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"cars": asArr(fillerCar())},
				map[string]any{"city": "berlin"},
			)}}, note: "garages[1] no cars → make IS NOT NULL fails → no match"},
			{id: idAbsentG1Empty, countries: []any{map[string]any{"garages": asArr(
				map[string]any{"cars": asArr(fillerCar())},
				map[string]any{},
			)}}, note: "garages[1] empty → no match"},
		}

		filter := andFilter(
			isNullFilter("countries.garages[1].cars.make", false),
			isNullFilter("countries.garages[1].cars.model", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchMakeOnly, idMatchMakeAndModelInG0})
	})

	// ----- Constraint at countries.garages.cars[1] -----

	// 5: cars[1].make = "honda" AND cars[1].model IS NULL
	// TODO aliszka:nested_filtering: this asserts CURRENT universal IsNull
	// semantics on the deep path `cars[1].model`. When the planned
	// existential IsNull rewrite lands, expectations flip — vacuous
	// matches removed, cross-element absent matches added.
	t.Run("regression_5_cars_value_and_isNull_model", func(t *testing.T) {
		idMatchMinimal := uuid(1)
		idMatchExtraCar := uuid(2)
		idNoMatchModelPresent := uuid(3)
		idNoMatchWrongMake := uuid(4)
		idNoMatchHondaAtCars0 := uuid(5)
		idNoMatchCrossConfusion := uuid(6)
		idNoMatchSplitMakeModel := uuid(7)
		idAbsentCars1Missing := uuid(8)
		idAbsentNoCars := uuid(9)

		docs := []docDef{
			{id: idMatchMinimal, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("make", "honda"),
			)})}}, note: "cars[1]: make=honda, no model → match"},
			{id: idMatchExtraCar, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("make", "honda"),
				car("make", "y", "model", "z"),
			)})}}, note: "cars[1] satisfies; cars[2]'s model doesn't matter → match"},
			{id: idNoMatchModelPresent, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("make", "honda", "model", "civic"),
			)})}}, note: "cars[1] has model → no match"},
			{id: idNoMatchWrongMake, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("make", "toyota"),
			)})}}, note: "cars[1].make=toyota → no match"},
			{id: idNoMatchHondaAtCars0, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "honda"),
				car("make", "toyota"),
			)})}}, note: "honda at cars[0]; cars[1] doesn't satisfy → no match"},
			{id: idNoMatchCrossConfusion, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("make", "civic", "model", "honda"),
			)})}}, note: "cars[1]: values swapped on fields (make=civic, model=honda) → no match (make≠honda + model present)"},
			{id: idNoMatchSplitMakeModel, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "honda"),
				car("model", "civic"),
			)})}}, note: "cars[0] has honda, cars[1] has model → no match (cars[1].make≠honda)"},
			{id: idAbsentCars1Missing, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "honda"),
			)})}}, note: "cars=[honda] only — cars[1] missing → no match"},
			{id: idAbsentNoCars, countries: []any{map[string]any{"garages": asArr(map[string]any{"city": "berlin"})}}, note: "no cars → no match"},
		}

		filter := andFilter(
			valueFilter("countries.garages.cars[1].make", "honda"),
			isNullFilter("countries.garages.cars[1].model", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchMinimal, idMatchExtraCar})
	})

	// 6: cars[1].make = "honda" AND cars[1].model IS NOT NULL
	t.Run("6_cars_value_and_isNotNull_model", func(t *testing.T) {
		idMatchMinimal := uuid(1)
		idMatchExtraField := uuid(2)
		idNoMatchModelAbsent := uuid(3)
		idNoMatchWrongMake := uuid(4)
		idNoMatchHondaAtCars0 := uuid(5)
		idNoMatchModelAtCars0Only := uuid(6)
		idAbsentCars1Missing := uuid(7)

		docs := []docDef{
			{id: idMatchMinimal, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("make", "honda", "model", "civic"),
			)})}}, note: "cars[1]: make=honda, model present → match"},
			{id: idMatchExtraField, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				fillerCar(),
				car("make", "honda", "model", "any-value"),
				fillerCar(),
			)})}}, note: "cars[1] satisfies, surrounded by fillers → match"},
			{id: idNoMatchModelAbsent, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("make", "honda"),
			)})}}, note: "cars[1] has make=honda but no model → no match"},
			{id: idNoMatchWrongMake, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("make", "toyota", "model", "civic"),
			)})}}, note: "cars[1].make=toyota → no match"},
			{id: idNoMatchHondaAtCars0, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "honda", "model", "civic"),
				car("make", "toyota"),
			)})}}, note: "honda+model at cars[0]; cars[1] doesn't satisfy → no match"},
			{id: idNoMatchModelAtCars0Only, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x", "model", "y"),
				car("make", "honda"),
			)})}}, note: "model at cars[0]; cars[1].model absent → no match"},
			{id: idAbsentCars1Missing, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "honda", "model", "civic"),
			)})}}, note: "cars[1] missing → no match"},
		}

		filter := andFilter(
			valueFilter("countries.garages.cars[1].make", "honda"),
			isNullFilter("countries.garages.cars[1].model", false),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchMinimal, idMatchExtraField})
	})

	// 9: cars[1].make IS NOT NULL AND cars[1].model IS NULL
	t.Run("9_cars_dual_isNull_make_present_model_absent", func(t *testing.T) {
		idMatchMakeOnly := uuid(1)
		idMatchMakeAtCars1 := uuid(2)
		idNoMatchModelPresent := uuid(3)
		idNoMatchNoMake := uuid(4)
		idNoMatchSwapAcrossCars := uuid(5)
		idAbsentCars1Missing := uuid(6)
		idAbsentNoCars := uuid(7)

		docs := []docDef{
			{id: idMatchMakeOnly, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x", "model", "y"),
				car("make", "honda"),
			)})}}, note: "cars[1]: make present, no model → match"},
			{id: idMatchMakeAtCars1, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("model", "y"),
				car("make", "z"),
			)})}}, note: "cars[1]: make present, no model; cars[0] has model but doesn't matter → match"},
			{id: idNoMatchModelPresent, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				fillerCar(),
				car("make", "x", "model", "y"),
			)})}}, note: "cars[1] has both → no match (model present)"},
			{id: idNoMatchNoMake, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				fillerCar(),
				car("model", "y"),
			)})}}, note: "cars[1]: only model, no make → no match"},
			{id: idNoMatchSwapAcrossCars, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
				car("model", "y"),
			)})}}, note: "make at cars[0], model at cars[1] → cars[1] has no make → no match"},
			{id: idAbsentCars1Missing, countries: []any{map[string]any{"garages": asArr(map[string]any{"cars": asArr(
				car("make", "x"),
			)})}}, note: "cars[1] missing → no match"},
			{id: idAbsentNoCars, countries: []any{map[string]any{"garages": asArr(map[string]any{"city": "berlin"})}}, note: "no cars → no match"},
		}

		filter := andFilter(
			isNullFilter("countries.garages.cars[1].make", false),
			isNullFilter("countries.garages.cars[1].model", true),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchMakeOnly, idMatchMakeAtCars1})
	})
}

// TestNestedFilteringIsNullStandalone exercises IS NULL / IS NOT NULL filters
// outside of correlated AND, end-to-end through the production write+search
// pipeline.
//
// Coverage:
//   - For both DataTypeObject (top-level "nested") and DataTypeObjectArray
//     (top-level "nestedArray") root properties.
//   - IsNull on the top-level nested property itself (e.g. "nested" or
//     "nestedArray") — checks whether the nested object/array exists at all.
//   - IsNull on a direct nested-array property of the root (e.g. "nested.addresses")
//     — checks whether that array exists.
//   - IsNull on a scalar leaf inside a nested array (e.g. "nested.addresses.city")
//     — universal semantics under the current resolver.
//   - IsNull on a scalar-array leaf (e.g. "nested.addresses.tags") — same
//     universal semantics, but exercises a different write-path code branch
//     (walkScalarArray instead of walkObject).
//   - For nestedArray: aggregation across multiple top-level array elements.
//   - Docs that don't set the top-level nested property at all.
//
// This documents the current universal IsNull-on-deep-path behaviour. The
// planned existential IsNull rewrite would change some of these assertions
// before release.
func TestNestedFilteringIsNullStandalone(t *testing.T) {
	const nestedClass = "Article"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	addressesProps := []*models.NestedProperty{
		{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
	}
	rootProps := []*models.NestedProperty{
		{
			Name:             "addresses",
			DataType:         schema.DataTypeObjectArray.PropString(),
			NestedProperties: addressesProps,
		},
	}

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "nested", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
			{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootProps},
		},
	}

	addrCity := func(city string) map[string]any { return map[string]any{"city": city} }
	addrTags := func(tags ...string) map[string]any {
		anyTags := make([]any, len(tags))
		for i, t := range tags {
			anyTags[i] = t
		}
		return map[string]any{"tags": anyTags}
	}
	addrCityAndTags := func(city string, tags ...string) map[string]any {
		anyTags := make([]any, len(tags))
		for i, t := range tags {
			anyTags[i] = t
		}
		return map[string]any{"city": city, "tags": anyTags}
	}
	addrEmpty := func() map[string]any { return map[string]any{} }
	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}

	isNullFilter := func(path string, isNull bool) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: isNull},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id,
				Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	// ----- DataTypeObject (top-level "nested") -----
	t.Run("nested_object", func(t *testing.T) {
		idAddrWithCity := uuid(1)
		idAddrNoCity := uuid(2)
		idMixedCities := uuid(3)
		idEmptyAddresses := uuid(4)
		idNoAddresses := uuid(5)
		idNoNestedProp := uuid(6)
		idAddrWithTags := uuid(7)
		idAddrCityAndTags := uuid(8)
		idMixedTagsAndCities := uuid(9)

		docs := []docDef{
			{id: idAddrWithCity, props: map[string]any{
				"nested": map[string]any{"addresses": asArr(addrCity("berlin"))},
			}, note: "nested.addresses=[{city:berlin}]"},
			{id: idAddrNoCity, props: map[string]any{
				"nested": map[string]any{"addresses": asArr(addrEmpty())},
			}, note: "nested.addresses=[{}] — no city, no tags"},
			{id: idMixedCities, props: map[string]any{
				"nested": map[string]any{"addresses": asArr(addrCity("munich"), addrEmpty(), addrCity("paris"))},
			}, note: "mixed addresses with city, no tags"},
			{id: idEmptyAddresses, props: map[string]any{
				"nested": map[string]any{"addresses": []any{}},
			}, note: "nested.addresses=[]"},
			{id: idNoAddresses, props: map[string]any{
				"nested": map[string]any{},
			}, note: "nested={} — no addresses field"},
			{id: idNoNestedProp, props: map[string]any{}, note: "props={} — no nested prop at all"},
			{id: idAddrWithTags, props: map[string]any{
				"nested": map[string]any{"addresses": asArr(addrTags("foo", "bar"))},
			}, note: "addresses=[{tags:[foo,bar]}] — tags present, no city"},
			{id: idAddrCityAndTags, props: map[string]any{
				"nested": map[string]any{"addresses": asArr(addrCityAndTags("madrid", "x"))},
			}, note: "addresses=[{city:madrid, tags:[x]}] — both present"},
			{id: idMixedTagsAndCities, props: map[string]any{
				"nested": map[string]any{"addresses": asArr(addrCity("munich"), addrTags("a"), addrEmpty())},
			}, note: "mixed: city in [0], tags in [1], empty in [2]"},
		}

		t.Run("nested_is_not_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nested", false),
				[]strfmt.UUID{
					idAddrWithCity, idAddrNoCity, idMixedCities,
					idEmptyAddresses, idNoAddresses,
					idAddrWithTags, idAddrCityAndTags, idMixedTagsAndCities,
				})
		})

		t.Run("nested_is_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nested", true),
				[]strfmt.UUID{idNoNestedProp})
		})

		t.Run("addresses_is_not_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nested.addresses", false),
				[]strfmt.UUID{
					idAddrWithCity, idAddrNoCity, idMixedCities,
					idAddrWithTags, idAddrCityAndTags, idMixedTagsAndCities,
				})
		})

		t.Run("addresses_is_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nested.addresses", true),
				[]strfmt.UUID{idEmptyAddresses, idNoAddresses, idNoNestedProp})
		})

		t.Run("addresses_city_is_not_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nested.addresses.city", false),
				[]strfmt.UUID{
					idAddrWithCity, idMixedCities,
					idAddrCityAndTags, idMixedTagsAndCities,
				})
		})

		// TODO aliszka:nested_filtering: locks in CURRENT universal IsNull
		// on the deep path `nested.addresses.city`. When the planned
		// existential IsNull rewrite lands, idMixedCities and
		// idMixedTagsAndCities (some address has city, some doesn't)
		// would match — only docs with NO addresses or where every
		// address has city should be excluded under existential.
		t.Run("regression_addresses_city_is_null_universal", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nested.addresses.city", true),
				[]strfmt.UUID{
					idAddrNoCity, idEmptyAddresses, idNoAddresses, idNoNestedProp,
					idAddrWithTags,
				})
		})

		t.Run("addresses_tags_is_not_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nested.addresses.tags", false),
				[]strfmt.UUID{idAddrWithTags, idAddrCityAndTags, idMixedTagsAndCities})
		})

		// TODO aliszka:nested_filtering: locks in CURRENT universal IsNull on
		// `nested.addresses.tags` (text[]). Under the existential IsNull
		// rewrite, idMixedTagsAndCities (some address has tags, some
		// doesn't) would match. Verify scalar-array IsNull semantics in
		// the rewrite plan.
		t.Run("regression_addresses_tags_is_null_universal", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nested.addresses.tags", true),
				[]strfmt.UUID{
					idAddrWithCity, idAddrNoCity, idMixedCities,
					idEmptyAddresses, idNoAddresses, idNoNestedProp,
				})
		})
	})

	// ----- DataTypeObjectArray (top-level "nestedArray") -----
	t.Run("nested_array", func(t *testing.T) {
		idArrAddrWithCity := uuid(1)
		idArrAddrNoCity := uuid(2)
		idArrMixedAcrossElems := uuid(3)
		idArrCityInSecondElem := uuid(4)
		idArrEmptyAddresses := uuid(5)
		idArrNoAddresses := uuid(6)
		idArrEmptyTopLevel := uuid(7)
		idArrNoNestedArrayProp := uuid(8)
		idArrAddrWithTags := uuid(9)
		idArrAddrCityAndTags := uuid(10)
		idArrTagsInSecondElem := uuid(11)

		docs := []docDef{
			{id: idArrAddrWithCity, props: map[string]any{
				"nestedArray": asArr(map[string]any{"addresses": asArr(addrCity("berlin"))}),
			}, note: "single root with addresses+city"},
			{id: idArrAddrNoCity, props: map[string]any{
				"nestedArray": asArr(map[string]any{"addresses": asArr(addrEmpty())}),
			}, note: "single root with addresses (no city, no tags)"},
			{id: idArrMixedAcrossElems, props: map[string]any{
				"nestedArray": asArr(
					map[string]any{"addresses": asArr(addrCity("paris"))},
					map[string]any{"addresses": asArr(addrEmpty())},
				),
			}, note: "two roots: first with city, second without"},
			{id: idArrCityInSecondElem, props: map[string]any{
				"nestedArray": asArr(
					map[string]any{},
					map[string]any{"addresses": asArr(addrCity("madrid"))},
				),
			}, note: "city in nestedArray[1] only — cross-root-element aggregation"},
			{id: idArrEmptyAddresses, props: map[string]any{
				"nestedArray": asArr(map[string]any{"addresses": []any{}}),
			}, note: "single root with addresses=[]"},
			{id: idArrNoAddresses, props: map[string]any{
				"nestedArray": asArr(map[string]any{}),
			}, note: "single root without addresses field"},
			{id: idArrEmptyTopLevel, props: map[string]any{
				"nestedArray": []any{},
			}, note: "nestedArray=[] — empty top-level array"},
			{id: idArrNoNestedArrayProp, props: map[string]any{}, note: "props={} — no nestedArray prop"},
			{id: idArrAddrWithTags, props: map[string]any{
				"nestedArray": asArr(map[string]any{"addresses": asArr(addrTags("foo"))}),
			}, note: "addresses=[{tags:[foo]}] — tags only"},
			{id: idArrAddrCityAndTags, props: map[string]any{
				"nestedArray": asArr(map[string]any{"addresses": asArr(addrCityAndTags("oslo", "y"))}),
			}, note: "addresses=[{city:oslo, tags:[y]}]"},
			{id: idArrTagsInSecondElem, props: map[string]any{
				"nestedArray": asArr(
					map[string]any{},
					map[string]any{"addresses": asArr(addrTags("z"))},
				),
			}, note: "tags only in nestedArray[1] — cross-root-element"},
		}

		t.Run("nestedArray_is_not_null", func(t *testing.T) {
			// Empty top-level array and missing prop produce no _exists positions
			// for "nestedArray" so they don't show up here.
			runScenario(t, docs, isNullFilter("nestedArray", false),
				[]strfmt.UUID{
					idArrAddrWithCity, idArrAddrNoCity, idArrMixedAcrossElems, idArrCityInSecondElem,
					idArrEmptyAddresses, idArrNoAddresses,
					idArrAddrWithTags, idArrAddrCityAndTags, idArrTagsInSecondElem,
				})
		})

		t.Run("nestedArray_is_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nestedArray", true),
				[]strfmt.UUID{idArrEmptyTopLevel, idArrNoNestedArrayProp})
		})

		t.Run("addresses_is_not_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nestedArray.addresses", false),
				[]strfmt.UUID{
					idArrAddrWithCity, idArrAddrNoCity, idArrMixedAcrossElems, idArrCityInSecondElem,
					idArrAddrWithTags, idArrAddrCityAndTags, idArrTagsInSecondElem,
				})
		})

		t.Run("addresses_is_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nestedArray.addresses", true),
				[]strfmt.UUID{idArrEmptyAddresses, idArrNoAddresses, idArrEmptyTopLevel, idArrNoNestedArrayProp})
		})

		t.Run("addresses_city_is_not_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nestedArray.addresses.city", false),
				[]strfmt.UUID{
					idArrAddrWithCity, idArrMixedAcrossElems, idArrCityInSecondElem,
					idArrAddrCityAndTags,
				})
		})

		// TODO aliszka:nested_filtering: docs_array variant of universal
		// IsNull on `nestedArray.addresses.city`. Locks in CURRENT
		// behavior; flips under the existential IsNull rewrite.
		// Discriminator docs (idArrMixedAcrossElems etc.) document
		// expected post-rewrite matches.
		t.Run("regression_addresses_city_is_null_universal", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nestedArray.addresses.city", true),
				[]strfmt.UUID{
					idArrAddrNoCity, idArrEmptyAddresses, idArrNoAddresses, idArrEmptyTopLevel, idArrNoNestedArrayProp,
					idArrAddrWithTags, idArrTagsInSecondElem,
				})
		})

		t.Run("addresses_tags_is_not_null", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nestedArray.addresses.tags", false),
				[]strfmt.UUID{idArrAddrWithTags, idArrAddrCityAndTags, idArrTagsInSecondElem})
		})

		// TODO aliszka:nested_filtering: docs_array variant of universal
		// IsNull on `nestedArray.addresses.tags` (text[]). Locks in
		// CURRENT behavior; flips under the existential IsNull rewrite.
		t.Run("regression_addresses_tags_is_null_universal", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("nestedArray.addresses.tags", true),
				[]strfmt.UUID{
					idArrAddrWithCity, idArrAddrNoCity, idArrMixedAcrossElems, idArrCityInSecondElem,
					idArrEmptyAddresses, idArrNoAddresses, idArrEmptyTopLevel, idArrNoNestedArrayProp,
				})
		})
	})
}

// TestNestedFilteringIsNullInCorrelatedAnd exercises IsNull / IsNotNull
// combined with a positive condition (or another IsNull) inside a correlated
// AND, end-to-end through the production write+search pipeline. Coverage is
// duplicated under both DataTypeObject (top-level "country") and
// DataTypeObjectArray (top-level "countries") root properties, and across
// three nesting depths: root-level fields, L1 (garages) fields, and L2 (cars)
// fields. At each depth we test:
//
//   - value + IsNull=false (positive value AND sibling property present)
//   - value + IsNull=true  (positive value AND sibling property absent)
//
// At L2 we also test:
//
//   - dual IsNull (one IS NOT NULL + one IS NULL, no positive value)
//   - tokenization compound + IsNull
//   - direct contradiction on the same property (always empty)
//
// Same-element correlation requires both conditions in correlated AND to be
// satisfied at the same array element of the deepest unconstrained ancestor —
// "same root" for root-level conditions, "same garage" for L1, "same car"
// for L2.
func TestNestedFilteringIsNullInCorrelatedAnd(t *testing.T) {
	const nestedClass = "IsNullCorr"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	// Both top-level prop variants share the same nested schema:
	// {name, capital} + garages [{city, postcode} + cars [{make, model}]].
	rootProps := []*models.NestedProperty{
		{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "capital", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{
			Name:     "garages",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name:     "cars",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						// Word-tokenized so multi-token tests work for sub-test 8.
						{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: &vTrue},
						{Name: "model", DataType: schema.DataTypeText.PropString(), Tokenization: models.NestedPropertyTokenizationWord, IndexFilterable: &vTrue},
						{Name: "year", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
						{
							Name:     "tires",
							DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "width", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
							},
						},
					},
				},
			},
		},
	}

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "country", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
			{Name: "countries", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	car := func(props ...string) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i]] = props[i+1]
		}
		return out
	}

	valueFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	isNullFilter := func(path string, isNull bool) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: isNull},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(a, b *filters.LocalFilter) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: []filters.Clause{*a.Root, *b.Root},
		}}
	}
	andFilterN := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: operands,
		}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id,
				Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	// ---------- DataTypeObject (top-level "country") ----------
	t.Run("country_object", func(t *testing.T) {
		// Sub-test 1: country.name = "germany" AND country.capital IS NOT NULL
		t.Run("root_value_isNotNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchExtraFields := uuid(2)
			idNoMatchNoCapital := uuid(3)
			idNoMatchWrongName := uuid(4)
			idNoMatchEmptyCountry := uuid(5)
			idNoMatchNoCountryProp := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"name": "germany", "capital": "berlin"}}, note: "name=germany, capital=berlin"},
				{id: idMatchExtraFields, props: map[string]any{"country": map[string]any{"name": "germany", "capital": "berlin", "garages": asArr(map[string]any{"city": "munich"})}}, note: "extra fields don't break the match"},
				{id: idNoMatchNoCapital, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "name=germany but capital absent"},
				{id: idNoMatchWrongName, props: map[string]any{"country": map[string]any{"name": "france", "capital": "paris"}}, note: "wrong name"},
				{id: idNoMatchEmptyCountry, props: map[string]any{"country": map[string]any{}}, note: "empty country"},
				{id: idNoMatchNoCountryProp, props: map[string]any{}, note: "no country prop"},
			}
			filter := andFilter(valueFilter("country.name", "germany"), isNullFilter("country.capital", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraFields})
		})

		// Sub-test 2: country.name = "germany" AND country.capital IS NULL
		t.Run("root_value_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchWithGarages := uuid(2)
			idNoMatchCapitalPresent := uuid(3)
			idNoMatchWrongName := uuid(4)
			idNoMatchNoName := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "name=germany, no capital"},
				{id: idMatchWithGarages, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "munich"})}}, note: "name=germany, no capital but has garages"},
				{id: idNoMatchCapitalPresent, props: map[string]any{"country": map[string]any{"name": "germany", "capital": "berlin"}}, note: "capital present → no match"},
				{id: idNoMatchWrongName, props: map[string]any{"country": map[string]any{"name": "france"}}, note: "wrong name"},
				{id: idNoMatchNoName, props: map[string]any{"country": map[string]any{"capital": "berlin"}}, note: "no name → no match"},
			}
			filter := andFilter(valueFilter("country.name", "germany"), isNullFilter("country.capital", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchWithGarages})
		})

		// Sub-test 3: country.garages.city = "berlin" AND country.garages.postcode IS NOT NULL
		t.Run("L1_value_isNotNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchSecondGarage := uuid(2)
			idMatchExtraGarages := uuid(3)
			idNoMatchSplit := uuid(4)
			idNoMatchWrongCity := uuid(5)
			idNoMatchNoPostcode := uuid(6)
			idNoMatchNoGarages := uuid(7)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin", "postcode": "10115"})}}, note: "garage with city=berlin, postcode present"},
				{id: idMatchSecondGarage, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"city": "munich"},
					map[string]any{"city": "berlin", "postcode": "10115"},
				)}}, note: "match in second garage"},
				{id: idMatchExtraGarages, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"city": "berlin", "postcode": "10115"},
					map[string]any{"city": "paris", "postcode": "75000"},
					map[string]any{"city": "munich"},
				)}}, note: "first garage matches; others irrelevant"},
				{id: idNoMatchSplit, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"city": "munich", "postcode": "80331"},
				)}}, note: "berlin in g0; postcode in g1 — different garages"},
				{id: idNoMatchWrongCity, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "munich", "postcode": "80331"})}}, note: "wrong city"},
				{id: idNoMatchNoPostcode, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin"})}}, note: "berlin but no postcode"},
				{id: idNoMatchNoGarages, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "no garages"},
			}
			filter := andFilter(valueFilter("country.garages.city", "berlin"), isNullFilter("country.garages.postcode", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchSecondGarage, idMatchExtraGarages})
		})

		// Sub-test 4: country.garages.city = "berlin" AND country.garages.postcode IS NULL
		t.Run("L1_value_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchWithCars := uuid(2)
			idNoMatchPostcodePresent := uuid(3)
			idNoMatchWrongCity := uuid(4)
			idNoMatchSplit := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin"})}}, note: "garage with city=berlin, no postcode"},
				{id: idMatchWithCars, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})}}, note: "city=berlin, no postcode, has cars"},
				{id: idNoMatchPostcodePresent, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin", "postcode": "10115"})}}, note: "city=berlin AND postcode → no match"},
				{id: idNoMatchWrongCity, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "munich"})}}, note: "wrong city"},
				{id: idNoMatchSplit, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"city": "berlin", "postcode": "10115"},
					map[string]any{"city": "munich"},
				)}}, note: "g0 has berlin+postcode; g1 has no postcode but wrong city — no garage satisfies"},
			}
			filter := andFilter(valueFilter("country.garages.city", "berlin"), isNullFilter("country.garages.postcode", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchWithCars})
		})

		// Sub-test 5: country.garages.cars.make = "honda" AND country.garages.cars.model IS NOT NULL
		t.Run("L2_value_isNotNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchSecondCar := uuid(2)
			idNoMatchSplit := uuid(3)
			idNoMatchWrongMake := uuid(4)
			idNoMatchNoModel := uuid(5)
			idNoMatchSplitGarages := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})}}, note: "honda+civic at same car"},
				{id: idMatchSecondCar, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "toyota", "model", "corolla"),
					car("make", "honda", "model", "civic"),
				)})}}, note: "second car satisfies"},
				{id: idNoMatchSplit, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda"),
					car("model", "civic"),
				)})}}, note: "honda in c0, model in c1 — different cars"},
				{id: idNoMatchWrongMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota", "model", "corolla"))})}}, note: "wrong make"},
				{id: idNoMatchNoModel, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "honda but no model"},
				{id: idNoMatchSplitGarages, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"cars": asArr(car("make", "honda"))},
					map[string]any{"cars": asArr(car("model", "civic"))},
				)}}, note: "honda in g0.cars; model in g1.cars — different cars across garages"},
			}
			filter := andFilter(valueFilter("country.garages.cars.make", "honda"), isNullFilter("country.garages.cars.model", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchSecondCar})
		})

		// Sub-test 6: country.garages.cars.make = "honda" AND country.garages.cars.model IS NULL
		t.Run("L2_value_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchExtraCar := uuid(2)
			idNoMatchModelPresent := uuid(3)
			idNoMatchSplit := uuid(4)
			idNoMatchWrongMake := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "honda, no model"},
				{id: idMatchExtraCar, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda"),
					car("make", "toyota", "model", "corolla"),
				)})}}, note: "first car satisfies (honda, no model)"},
				{id: idNoMatchModelPresent, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})}}, note: "honda but model present"},
				{id: idNoMatchSplit, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda", "model", "civic"),
					car("make", "toyota"),
				)})}}, note: "honda in c0 has model; honda absent in c1"},
				{id: idNoMatchWrongMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota"))})}}, note: "wrong make"},
			}
			filter := andFilter(valueFilter("country.garages.cars.make", "honda"), isNullFilter("country.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraCar})
		})

		// Sub-test 7: dual IsNull at L2 — make IS NOT NULL AND model IS NULL
		t.Run("L2_dual_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchExtraCar := uuid(2)
			idNoMatchModelPresent := uuid(3)
			idNoMatchNoMake := uuid(4)
			idNoMatchEveryCarHasModel := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "make present, no model"},
				{id: idMatchExtraCar, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "kia", "model", "sportage"),
					car("make", "toyota"),
				)})}}, note: "second car has make but no model"},
				{id: idNoMatchModelPresent, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})}}, note: "single car has both"},
				{id: idNoMatchNoMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("model", "civic"))})}}, note: "model only, no make"},
				{id: idNoMatchEveryCarHasModel, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda", "model", "civic"),
					car("make", "toyota", "model", "corolla"),
				)})}}, note: "every car has both"},
			}
			filter := andFilter(isNullFilter("country.garages.cars.make", false), isNullFilter("country.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraCar})
		})

		// Sub-test 8: tokenization + IsNull at L2
		// make = "honda civic" (tokenizes to [honda, civic]) AND model IS NULL
		t.Run("L2_tokenization_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idNoMatchModelPresent := uuid(2)
			idNoMatchTokensSplit := uuid(3)
			idNoMatchOneTokenMissing := uuid(4)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda civic"))})}}, note: "make='honda civic' tokens at same leaf, no model"},
				{id: idNoMatchModelPresent, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda civic", "model", "anything"))})}}, note: "tokens align but model present"},
				{id: idNoMatchTokensSplit, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda"),
					car("make", "civic"),
				)})}}, note: "tokens split across cars — AndAll fails"},
				{id: idNoMatchOneTokenMissing, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "only one token; civic missing"},
			}
			filter := andFilter(valueFilter("country.garages.cars.make", "honda civic"), isNullFilter("country.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})

		// Sub-test 9: contradiction — IS NULL AND IS NOT NULL on same property
		t.Run("L2_contradiction", func(t *testing.T) {
			idDoc := uuid(1)
			docs := []docDef{
				{id: idDoc, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})}}, note: "any doc; filter is contradictory"},
			}
			filter := andFilter(
				isNullFilter("country.garages.cars.model", true),
				isNullFilter("country.garages.cars.model", false),
			)
			runScenario(t, docs, filter, []strfmt.UUID{})
		})

		// Sub-test 10 (cross-level): country.name = "germany" AND country.garages.cars.model IS NULL
		// Value condition at root scope; IsNull at deeper L2. Under universal
		// semantics the IsNull exclude is applied at rootDoc level — "no model
		// anywhere within this country."
		t.Run("crossLevel_root_value_isNull_at_L2", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchEmpty := uuid(2)
			idMatchNoCarsAtAll := uuid(3)
			idNoMatchModelExists := uuid(4)
			idNoMatchModelInOtherGarage := uuid(5)
			idNoMatchWrongName := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})}}, note: "germany; cars have only make, no model anywhere"},
				{id: idMatchEmpty, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "germany; no garages → vacuous match (universal)"},
				{id: idMatchNoCarsAtAll, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})}}, note: "germany; garage has no cars"},
				{id: idNoMatchModelExists, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})}}, note: "germany has model"},
				{id: idNoMatchModelInOtherGarage, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(
					map[string]any{"cars": asArr(car("make", "x"))},
					map[string]any{"cars": asArr(car("model", "y"))},
				)}}, note: "germany has model in second garage's car"},
				{id: idNoMatchWrongName, props: map[string]any{"country": map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})}}, note: "wrong name"},
			}
			filter := andFilter(valueFilter("country.name", "germany"), isNullFilter("country.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchEmpty, idMatchNoCarsAtAll})
		})

		// Sub-test 11 (cross-level): country.name = "germany" AND country.garages.cars.model IS NOT NULL
		// Existential reading is natural here — model exists somewhere within country.
		t.Run("crossLevel_root_value_isNotNull_at_L2", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchModelInDeepCar := uuid(2)
			idNoMatchNoModel := uuid(3)
			idNoMatchWrongName := uuid(4)
			idNoMatchNoCars := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})}}, note: "germany has model in cars"},
				{id: idMatchModelInDeepCar, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(
					map[string]any{"cars": asArr(car("make", "x"))},
					map[string]any{"cars": asArr(car("make", "y"), car("model", "z"))},
				)}}, note: "model exists in g1.cars[1]"},
				{id: idNoMatchNoModel, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})}}, note: "germany; no model anywhere"},
				{id: idNoMatchWrongName, props: map[string]any{"country": map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})}}, note: "wrong name"},
				{id: idNoMatchNoCars, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "germany; no cars at all → no model"},
			}
			filter := andFilter(valueFilter("country.name", "germany"), isNullFilter("country.garages.cars.model", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchModelInDeepCar})
		})

		// Sub-test 12 (cross-level): country.garages.city = "berlin" AND country.garages.cars.make IS NULL
		// Value at L1 (garages); IsNull on L2 descendant. Under current universal
		// semantics the IsNull exclude is applied at rootDoc level (country
		// scope). The country with city=berlin somewhere must have NO cars.make
		// anywhere within it — even in a different garage. The existential
		// rewrite would change this to per-garage scope.
		t.Run("crossLevel_L1_value_isNull_at_L2", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchEmptyCars := uuid(2)
			idNoMatchMakeInOtherGarage := uuid(3)
			idNoMatchMakeInBerlinGarage := uuid(4)
			idNoMatchWrongCity := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin"})}}, note: "berlin garage; no cars anywhere in country"},
				{id: idMatchEmptyCars, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": []any{}})}}, note: "berlin garage with empty cars array; no cars.make anywhere"},
				{id: idNoMatchMakeInOtherGarage, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"city": "munich", "cars": asArr(car("make", "honda"))},
				)}}, note: "make exists in country (other garage) — universal exclude rejects (existential would match)"},
				{id: idNoMatchMakeInBerlinGarage, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})}}, note: "berlin garage has cars.make"},
				{id: idNoMatchWrongCity, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "munich"})}}, note: "wrong city"},
			}
			filter := andFilter(valueFilter("country.garages.city", "berlin"), isNullFilter("country.garages.cars.make", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchEmptyCars})
		})

		// Sub-test 13 (no-positive / rootAnchor path):
		// country.garages.cars.make IS NULL AND country.garages.cars.model IS NULL
		// No positive leaf to seed the plan — resolver uses the rootAnchor
		// (_exists.""). Result: docs with at least one position not in either
		// excluded _exists set. Note: under the analyzer's DFS leaf encoding,
		// scalar fields at intermediate levels share the leaf positions of their
		// descendants — so a country with both name and a make-only car shares
		// the make's leaf for both, and the AndNot of make removes that shared
		// leaf, leaving no surviving position.
		t.Run("L2_all_isNull_no_positive", func(t *testing.T) {
			idMatchCarWithNeither := uuid(1)
			idMatchMixedCars := uuid(2)
			idMatchOtherFieldSurvives := uuid(3)
			idNoMatchSharedLeafExcluded := uuid(4)
			idNoMatchOnlyCarsWithMake := uuid(5)
			idNoMatchOnlyCarsWithBoth := uuid(6)
			docs := []docDef{
				{id: idMatchCarWithNeither, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{})})}}, note: "single car with neither make nor model"},
				{id: idMatchMixedCars, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"), map[string]any{})})}}, note: "second car has neither field — fresh leaf survives"},
				{id: idMatchOtherFieldSurvives, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "name has fresh leaf (no descendants to share with) — survives"},
				{id: idNoMatchSharedLeafExcluded, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "name shares its leaf with the cars descendants under DFS encoding; the make exclude removes the shared leaf"},
				{id: idNoMatchOnlyCarsWithMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "only leaf is the make leaf — excluded"},
				{id: idNoMatchOnlyCarsWithBoth, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})}}, note: "every leaf in cars.make or cars.model"},
			}
			filter := andFilter(isNullFilter("country.garages.cars.make", true), isNullFilter("country.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatchCarWithNeither, idMatchMixedCars, idMatchOtherFieldSurvives})
		})

		// Sub-test 14 (3-condition AND):
		// country.garages.cars.make = "honda" AND country.garages.cars.model IS NOT NULL
		// AND country.garages.cars.year IS NULL
		// Same-car correlation: same car has make=honda AND model present AND year absent.
		t.Run("L2_three_condition", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchSecondCar := uuid(2)
			idNoMatchYearPresent := uuid(3)
			idNoMatchNoModel := uuid(4)
			idNoMatchSplit := uuid(5)
			idNoMatchWrongMake := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})}}, note: "honda + model + no year"},
				{id: idMatchSecondCar, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "toyota", "model", "corolla", "year", "2020"),
					car("make", "honda", "model", "civic"),
				)})}}, note: "second car satisfies all three"},
				{id: idNoMatchYearPresent, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic", "year", "2020"))})}}, note: "year present"},
				{id: idNoMatchNoModel, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "no model"},
				{id: idNoMatchSplit, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda", "year", "2020"),
					car("model", "civic"),
				)})}}, note: "honda+year in c0; model in c1 — split across cars"},
				{id: idNoMatchWrongMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota", "model", "corolla"))})}}, note: "wrong make"},
			}
			filter := andFilterN(
				valueFilter("country.garages.cars.make", "honda"),
				isNullFilter("country.garages.cars.model", false),
				isNullFilter("country.garages.cars.year", true),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchSecondCar})
		})

		// Sub-test 15: country.name = "germany" AND country.garages IS NOT NULL
		// Array-prop IsNotNull at root, paired with a sibling root scalar.
		t.Run("root_value_arrayIsNotNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchMultiple := uuid(2)
			idNoMatchNoGarages := uuid(3)
			idNoMatchWrongName := uuid(4)
			idNoMatchEmpty := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})}}, note: "germany with one garage"},
				{id: idMatchMultiple, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "munich"}, map[string]any{"city": "berlin"})}}, note: "germany with multiple garages"},
				{id: idNoMatchNoGarages, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "germany but no garages prop"},
				{id: idNoMatchWrongName, props: map[string]any{"country": map[string]any{"name": "france", "garages": asArr(map[string]any{"city": "paris"})}}, note: "wrong name"},
				{id: idNoMatchEmpty, props: map[string]any{"country": map[string]any{}}, note: "empty country"},
			}
			filter := andFilter(valueFilter("country.name", "germany"), isNullFilter("country.garages", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchMultiple})
		})

		// Sub-test 16: country.name = "germany" AND country.garages IS NULL
		// Array-prop IsNull at root — distinct code path from leaf-IsNull (no
		// synthetic IS NOT NULL rewrite).
		t.Run("root_value_arrayIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchOtherFields := uuid(2)
			idNoMatchHasGarages := uuid(3)
			idNoMatchWrongName := uuid(4)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "germany with no garages"},
				{id: idMatchOtherFields, props: map[string]any{"country": map[string]any{"name": "germany", "capital": "berlin"}}, note: "germany with capital but no garages"},
				{id: idNoMatchHasGarages, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})}}, note: "germany has garages"},
				{id: idNoMatchWrongName, props: map[string]any{"country": map[string]any{"name": "france"}}, note: "wrong name"},
			}
			filter := andFilter(valueFilter("country.name", "germany"), isNullFilter("country.garages", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchOtherFields})
		})

		// Sub-test 17 (cross-level): country.name = "germany" AND country.garages.cars IS NULL
		// Root scalar + L1 intermediate-array IsNull. Universal at country
		// scope: no cars anywhere within country.
		t.Run("crossLevel_root_value_intermediateArrayIsNull", func(t *testing.T) {
			idMatchNoCarsAnywhere := uuid(1)
			idMatchNoGarages := uuid(2)
			idMatchAllGaragesNoCars := uuid(3)
			idNoMatchHasCars := uuid(4)
			idNoMatchCarsInOtherGarage := uuid(5)
			idNoMatchWrongName := uuid(6)
			docs := []docDef{
				{id: idMatchNoCarsAnywhere, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})}}, note: "garage with no cars"},
				{id: idMatchNoGarages, props: map[string]any{"country": map[string]any{"name": "germany"}}, note: "germany; no garages → vacuous match"},
				{id: idMatchAllGaragesNoCars, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"}, map[string]any{"city": "munich"})}}, note: "multiple garages, none with cars"},
				{id: idNoMatchHasCars, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "garage has cars"},
				{id: idNoMatchCarsInOtherGarage, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"}, map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "cars exist in another garage"},
				{id: idNoMatchWrongName, props: map[string]any{"country": map[string]any{"name": "france"}}, note: "wrong name"},
			}
			filter := andFilter(valueFilter("country.name", "germany"), isNullFilter("country.garages.cars", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatchNoCarsAnywhere, idMatchNoGarages, idMatchAllGaragesNoCars})
		})

		// Sub-test 18 (inverse cross-level): country.garages.cars.make = "honda"
		// AND country.name IS NULL. Deep value + shallower leaf IsNull.
		t.Run("inverseCrossLevel_L2_value_root_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idNoMatchNameSet := uuid(2)
			idNoMatchWrongMake := uuid(3)
			idNoMatchNoCars := uuid(4)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "honda exists; no name"},
				{id: idNoMatchNameSet, props: map[string]any{"country": map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "honda exists but name is set"},
				{id: idNoMatchWrongMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota"))})}}, note: "wrong make; no name"},
				{id: idNoMatchNoCars, props: map[string]any{"country": map[string]any{"capital": "berlin"}}, note: "no cars; no name"},
			}
			filter := andFilter(valueFilter("country.garages.cars.make", "honda"), isNullFilter("country.name", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})

		// Sub-test 19 (mixed): country.garages.cars.make = "honda" AND
		// country.garages.postcode IS NULL. L2 value + L1 leaf IsNull. Under
		// current universal IsNull semantics, "postcode IS NULL" applies at
		// country scope: the country must have no postcode anywhere AND honda
		// must exist in some car. Same-garage correlation is not enforced
		// across mixed L1/L2 conditions.
		t.Run("mixed_L2_value_L1_leafIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchHondaInSecondGarage := uuid(2)
			idNoMatchPostcodePresent := uuid(3)
			idNoMatchPostcodeInOtherGarage := uuid(4)
			idNoMatchWrongMake := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "garage with honda, no postcode"},
				{id: idMatchHondaInSecondGarage, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"cars": asArr(car("make", "toyota"))},
					map[string]any{"cars": asArr(car("make", "honda"))},
				)}}, note: "honda exists in g1; no postcode anywhere"},
				{id: idNoMatchPostcodePresent, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"postcode": "10115", "cars": asArr(car("make", "honda"))})}}, note: "honda garage has postcode"},
				{id: idNoMatchPostcodeInOtherGarage, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"cars": asArr(car("make", "honda"))},
					map[string]any{"postcode": "10115"},
				)}}, note: "honda in g0 (no postcode), but postcode in g1 — universal at country rejects"},
				{id: idNoMatchWrongMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota"))})}}, note: "wrong make"},
			}
			filter := andFilter(valueFilter("country.garages.cars.make", "honda"), isNullFilter("country.garages.postcode", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchHondaInSecondGarage})
		})

		// Sub-test 20 (no-positive / rootAnchor with three excludes):
		// country.garages.cars.make IS NULL AND country.garages.cars.model IS NULL
		// AND country.garages.cars.year IS NULL. Stress-tests rootAnchor with 3 excludes.
		t.Run("L2_three_isNull_no_positive", func(t *testing.T) {
			idMatchCarWithNothing := uuid(1)
			idMatchOtherFieldSurvives := uuid(2)
			idNoMatchHasMake := uuid(3)
			idNoMatchHasYear := uuid(4)
			idNoMatchAllSet := uuid(5)
			docs := []docDef{
				{id: idMatchCarWithNothing, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{})})}}, note: "single car with no fields"},
				{id: idMatchOtherFieldSurvives, props: map[string]any{"country": map[string]any{"capital": "berlin"}}, note: "capital has fresh leaf (no descendants)"},
				{id: idNoMatchHasMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "only leaf in cars.make exclude"},
				{id: idNoMatchHasYear, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("year", "2020"))})}}, note: "only leaf in cars.year exclude"},
				{id: idNoMatchAllSet, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic", "year", "2020"))})}}, note: "every leaf in some exclude"},
			}
			filter := andFilterN(
				isNullFilter("country.garages.cars.make", true),
				isNullFilter("country.garages.cars.model", true),
				isNullFilter("country.garages.cars.year", true),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatchCarWithNothing, idMatchOtherFieldSurvives})
		})

		// Sub-test 21: country.garages.city = "berlin" AND country.garages.cars IS NULL.
		// L1 leaf + L1 intermediate-array IsNull. Array-prop IsNull is universal
		// at country scope (no synthetic rewrite for array-prop IsNull) — the
		// country containing a berlin garage must have no cars anywhere. Within
		// a single country, same-garage correlation does not apply.
		t.Run("L1_value_intermediateArrayIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchMultipleGarages := uuid(2)
			idNoMatchHasCarsSameGarage := uuid(3)
			idNoMatchHasCarsOtherGarage := uuid(4)
			idNoMatchWrongCity := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin"})}}, note: "berlin garage, no cars anywhere"},
				{id: idMatchMultipleGarages, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"city": "munich"},
					map[string]any{"city": "berlin"},
				)}}, note: "berlin in g1; no cars in any garage"},
				{id: idNoMatchHasCarsSameGarage, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})}}, note: "berlin garage has cars"},
				{id: idNoMatchHasCarsOtherGarage, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"city": "munich", "cars": asArr(car("make", "honda"))},
				)}}, note: "berlin garage has no cars but other garage does — universal at country rejects"},
				{id: idNoMatchWrongCity, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "munich"})}}, note: "wrong city"},
			}
			filter := andFilter(valueFilter("country.garages.city", "berlin"), isNullFilter("country.garages.cars", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchMultipleGarages})
		})

		// Sub-test 22: country.garages.cars.make = "honda" AND
		// country.garages.cars.tires IS NULL. L2 leaf + L2 intermediate-array
		// IsNull. Array-prop IsNull is universal at country scope: the country
		// must have no tires anywhere AND honda must exist in some car. Within
		// a single country, same-car correlation does not apply.
		t.Run("L2_value_intermediateArrayIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchMultipleCars := uuid(2)
			idNoMatchHasTiresSameCar := uuid(3)
			idNoMatchHasTiresOtherCar := uuid(4)
			idNoMatchWrongMake := uuid(5)
			tire := func(width string) map[string]any { return map[string]any{"width": width} }
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})}}, note: "honda car, no tires anywhere"},
				{id: idMatchMultipleCars, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "toyota"),
					car("make", "honda"),
				)})}}, note: "honda in c1; no tires anywhere"},
				{id: idNoMatchHasTiresSameCar, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{"make": "honda", "tires": asArr(tire("205"))})})}}, note: "honda car has tires"},
				{id: idNoMatchHasTiresOtherCar, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda"),
					map[string]any{"make": "toyota", "tires": asArr(tire("205"))},
				)})}}, note: "honda car has no tires but other car does — universal at country rejects"},
				{id: idNoMatchWrongMake, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota"))})}}, note: "wrong make"},
			}
			filter := andFilter(valueFilter("country.garages.cars.make", "honda"), isNullFilter("country.garages.cars.tires", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchMultipleCars})
		})

		// Sub-test 23 (cross-level L1+L2): country.garages.city = "berlin" AND
		// country.garages.cars.tires IS NULL. L1 leaf + L2 intermediate-array
		// IsNull. Universal at country scope: berlin garage exists somewhere
		// AND no tires anywhere in the country.
		t.Run("crossLevel_L1_value_intermediateArrayIsNull_at_L2", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchCarsNoTires := uuid(2)
			idNoMatchTiresInBerlinCar := uuid(3)
			idNoMatchTiresInOtherGarage := uuid(4)
			idNoMatchWrongCity := uuid(5)
			tire := func(width string) map[string]any { return map[string]any{"width": width} }
			docs := []docDef{
				{id: idMatch, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin"})}}, note: "berlin garage, no tires anywhere"},
				{id: idMatchCarsNoTires, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})}}, note: "berlin garage with cars but cars have no tires"},
				{id: idNoMatchTiresInBerlinCar, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(map[string]any{"make": "honda", "tires": asArr(tire("205"))})})}}, note: "tires in berlin garage's car"},
				{id: idNoMatchTiresInOtherGarage, props: map[string]any{"country": map[string]any{"garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"city": "munich", "cars": asArr(map[string]any{"make": "honda", "tires": asArr(tire("205"))})},
				)}}, note: "tires in other garage — universal at country scope rejects"},
				{id: idNoMatchWrongCity, props: map[string]any{"country": map[string]any{"garages": asArr(map[string]any{"city": "munich"})}}, note: "wrong city"},
			}
			filter := andFilter(valueFilter("country.garages.city", "berlin"), isNullFilter("country.garages.cars.tires", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchCarsNoTires})
		})
	})

	// ---------- DataTypeObjectArray (top-level "countries") ----------
	t.Run("countries_array", func(t *testing.T) {
		// Sub-test 1: countries.name = "germany" AND countries.capital IS NOT NULL
		t.Run("root_value_isNotNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchExtraFields := uuid(2)
			idMatchSecondCountry := uuid(3)
			idMatchExtraCountries := uuid(4)
			idNoMatchSplit := uuid(5)
			idNoMatchWrongName := uuid(6)
			idNoMatchNoCapital := uuid(7)
			idNoMatchEmptyCountries := uuid(8)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "capital": "berlin"})}, note: "single country: germany+berlin"},
				{id: idMatchExtraFields, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "capital": "berlin", "garages": asArr(map[string]any{"city": "munich"})})}, note: "germany country with extra populated fields"},
				{id: idMatchSecondCountry, props: map[string]any{"countries": asArr(
					map[string]any{"name": "france", "capital": "paris"},
					map[string]any{"name": "germany", "capital": "berlin"},
				)}, note: "second country satisfies"},
				{id: idMatchExtraCountries, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany", "capital": "berlin"},
					map[string]any{"name": "france"},
					map[string]any{"name": "italy", "capital": "rome"},
				)}, note: "first country matches; others irrelevant"},
				{id: idNoMatchSplit, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany"},
					map[string]any{"capital": "berlin"},
				)}, note: "name and capital in different countries — same-element fails"},
				{id: idNoMatchWrongName, props: map[string]any{"countries": asArr(map[string]any{"name": "france", "capital": "paris"})}, note: "wrong name"},
				{id: idNoMatchNoCapital, props: map[string]any{"countries": asArr(map[string]any{"name": "germany"})}, note: "no capital"},
				{id: idNoMatchEmptyCountries, props: map[string]any{"countries": []any{}}, note: "empty countries"},
			}
			filter := andFilter(valueFilter("countries.name", "germany"), isNullFilter("countries.capital", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraFields, idMatchSecondCountry, idMatchExtraCountries})
		})

		// Sub-test 2: countries.name = "germany" AND countries.capital IS NULL
		t.Run("root_value_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchWithGarages := uuid(2)
			idMatchInSecondPos := uuid(3)
			idNoMatchCapitalPresent := uuid(4)
			idNoMatchSplitCapitalElsewhere := uuid(5)
			idNoMatchWrongName := uuid(6)
			idNoMatchNoName := uuid(7)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"name": "germany"})}, note: "single country: germany, no capital"},
				{id: idMatchWithGarages, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "munich"})})}, note: "germany, no capital but has garages"},
				{id: idMatchInSecondPos, props: map[string]any{"countries": asArr(
					map[string]any{"name": "france", "capital": "paris"},
					map[string]any{"name": "germany"},
				)}, note: "germany in [1] without capital"},
				{id: idNoMatchCapitalPresent, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "capital": "berlin"})}, note: "germany has capital"},
				{id: idNoMatchSplitCapitalElsewhere, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany", "capital": "berlin"},
					map[string]any{"name": "france"},
				)}, note: "germany country has capital — same-element rejects"},
				{id: idNoMatchWrongName, props: map[string]any{"countries": asArr(map[string]any{"name": "france"})}, note: "wrong name"},
				{id: idNoMatchNoName, props: map[string]any{"countries": asArr(map[string]any{"capital": "berlin"})}, note: "country has capital but no name"},
			}
			filter := andFilter(valueFilter("countries.name", "germany"), isNullFilter("countries.capital", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchWithGarages, idMatchInSecondPos})
		})

		// Sub-test 3: countries.garages.city = "berlin" AND countries.garages.postcode IS NOT NULL
		t.Run("L1_value_isNotNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchSecondGarage := uuid(2)
			idMatchExtraGarages := uuid(3)
			idMatchInSecondCountry := uuid(4)
			idNoMatchSplitWithinCountry := uuid(5)
			idNoMatchSplitAcrossCountries := uuid(6)
			idNoMatchWrongCity := uuid(7)
			idNoMatchNoPostcode := uuid(8)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin", "postcode": "10115"})})}, note: "single country, garage with both fields"},
				{id: idMatchSecondGarage, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"city": "munich"},
					map[string]any{"city": "berlin", "postcode": "10115"},
				)})}, note: "single country; second garage satisfies"},
				{id: idMatchExtraGarages, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"city": "berlin", "postcode": "10115"},
					map[string]any{"city": "paris", "postcode": "75000"},
					map[string]any{"city": "munich"},
				)})}, note: "first garage matches; others irrelevant"},
				{id: idMatchInSecondCountry, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"city": "munich"})},
					map[string]any{"garages": asArr(map[string]any{"city": "berlin", "postcode": "10115"})},
				)}, note: "second country has matching garage"},
				{id: idNoMatchSplitWithinCountry, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"city": "munich", "postcode": "80331"},
				)})}, note: "berlin in g0; postcode in g1 — different garages within same country"},
				{id: idNoMatchSplitAcrossCountries, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"city": "berlin"})},
					map[string]any{"garages": asArr(map[string]any{"city": "munich", "postcode": "80331"})},
				)}, note: "berlin and postcode in different countries"},
				{id: idNoMatchWrongCity, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "munich", "postcode": "80331"})})}, note: "wrong city"},
				{id: idNoMatchNoPostcode, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin"})})}, note: "berlin but no postcode"},
			}
			filter := andFilter(valueFilter("countries.garages.city", "berlin"), isNullFilter("countries.garages.postcode", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchSecondGarage, idMatchExtraGarages, idMatchInSecondCountry})
		})

		// Sub-test 4: countries.garages.city = "berlin" AND countries.garages.postcode IS NULL
		t.Run("L1_value_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchWithCars := uuid(2)
			idMatchInSecondCountry := uuid(3)
			idNoMatchPostcodePresent := uuid(4)
			idNoMatchSplit := uuid(5)
			idNoMatchWrongCity := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin"})})}, note: "garage city=berlin, no postcode"},
				{id: idMatchWithCars, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})})}, note: "city=berlin, no postcode, has cars"},
				{id: idMatchInSecondCountry, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"city": "munich", "postcode": "80331"})},
					map[string]any{"garages": asArr(map[string]any{"city": "berlin"})},
				)}, note: "second country has matching garage"},
				{id: idNoMatchPostcodePresent, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin", "postcode": "10115"})})}, note: "city=berlin but postcode present"},
				{id: idNoMatchSplit, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"city": "berlin", "postcode": "10115"},
					map[string]any{"city": "munich"},
				)})}, note: "g0 has berlin+postcode; g1 has wrong city — no garage satisfies both"},
				{id: idNoMatchWrongCity, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "munich"})})}, note: "wrong city"},
			}
			filter := andFilter(valueFilter("countries.garages.city", "berlin"), isNullFilter("countries.garages.postcode", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchWithCars, idMatchInSecondCountry})
		})

		// Sub-test 5: countries.garages.cars.make = "honda" AND countries.garages.cars.model IS NOT NULL
		t.Run("L2_value_isNotNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchSecondCar := uuid(2)
			idMatchInSecondCountry := uuid(3)
			idNoMatchSplitCars := uuid(4)
			idNoMatchSplitGarages := uuid(5)
			idNoMatchSplitAcrossCountries := uuid(6)
			idNoMatchWrongMake := uuid(7)
			idNoMatchNoModel := uuid(8)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})})}, note: "single car with both"},
				{id: idMatchSecondCar, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "toyota", "model", "corolla"),
					car("make", "honda", "model", "civic"),
				)})})}, note: "second car satisfies"},
				{id: idMatchInSecondCountry, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota", "model", "corolla"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})},
				)}, note: "second country has matching car"},
				{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda"),
					car("model", "civic"),
				)})})}, note: "honda in cars[0], model in cars[1] — different cars"},
				{id: idNoMatchSplitGarages, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"cars": asArr(car("make", "honda"))},
					map[string]any{"cars": asArr(car("model", "civic"))},
				)})}, note: "honda in g0.cars; model in g1.cars — different cars across garages within country"},
				{id: idNoMatchSplitAcrossCountries, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("model", "civic"))})},
				)}, note: "honda and model in different countries"},
				{id: idNoMatchWrongMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota", "model", "corolla"))})})}, note: "wrong make"},
				{id: idNoMatchNoModel, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "honda but no model"},
			}
			filter := andFilter(valueFilter("countries.garages.cars.make", "honda"), isNullFilter("countries.garages.cars.model", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchSecondCar, idMatchInSecondCountry})
		})

		// Sub-test 6: countries.garages.cars.make = "honda" AND countries.garages.cars.model IS NULL
		t.Run("L2_value_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchExtraCar := uuid(2)
			idMatchInSecondCountry := uuid(3)
			idNoMatchModelPresent := uuid(4)
			idNoMatchSplit := uuid(5)
			idNoMatchWrongMake := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "single car: make=honda, no model"},
				{id: idMatchExtraCar, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda"),
					car("make", "toyota", "model", "corolla"),
				)})})}, note: "first car satisfies; extra car irrelevant"},
				{id: idMatchInSecondCountry, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota", "model", "corolla"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})},
				)}, note: "second country has matching car"},
				{id: idNoMatchModelPresent, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})})}, note: "honda has model"},
				{id: idNoMatchSplit, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda", "model", "civic"),
					car("make", "toyota"),
				)})})}, note: "honda has model in cars[0]; cars[1] has no make"},
				{id: idNoMatchWrongMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota"))})})}, note: "wrong make"},
			}
			filter := andFilter(valueFilter("countries.garages.cars.make", "honda"), isNullFilter("countries.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraCar, idMatchInSecondCountry})
		})

		// Sub-test 7: dual IsNull at L2
		t.Run("L2_dual_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchExtraCar := uuid(2)
			idMatchAcrossCountries := uuid(3)
			idNoMatchModelPresent := uuid(4)
			idNoMatchNoMake := uuid(5)
			idNoMatchEveryCarHasModel := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "make present, no model"},
				{id: idMatchExtraCar, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "kia", "model", "sportage"),
					car("make", "toyota"),
				)})})}, note: "second car has make but no model"},
				{id: idMatchAcrossCountries, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("model", "civic"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "kia"))})},
				)}, note: "second country has car satisfying same-element"},
				{id: idNoMatchModelPresent, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})})}, note: "single car has both"},
				{id: idNoMatchNoMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("model", "civic"))})})}, note: "no make"},
				{id: idNoMatchEveryCarHasModel, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda", "model", "civic"),
					car("make", "toyota", "model", "corolla"),
				)})})}, note: "every car has both → no satisfying car"},
			}
			filter := andFilter(isNullFilter("countries.garages.cars.make", false), isNullFilter("countries.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraCar, idMatchAcrossCountries})
		})

		// Sub-test 8: tokenization + IsNull at L2
		t.Run("L2_tokenization_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchInSecondCountry := uuid(2)
			idNoMatchModelPresent := uuid(3)
			idNoMatchTokensSplit := uuid(4)
			idNoMatchOneTokenMissing := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda civic"))})})}, note: "tokens at same leaf, no model"},
				{id: idMatchInSecondCountry, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota corolla"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda civic"))})},
				)}, note: "second country satisfies"},
				{id: idNoMatchModelPresent, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda civic", "model", "anything"))})})}, note: "tokens align but model present"},
				{id: idNoMatchTokensSplit, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda"),
					car("make", "civic"),
				)})})}, note: "tokens split across cars"},
				{id: idNoMatchOneTokenMissing, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "only one token (honda); civic missing"},
			}
			filter := andFilter(valueFilter("countries.garages.cars.make", "honda civic"), isNullFilter("countries.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchInSecondCountry})
		})

		// Sub-test 9: contradiction
		t.Run("L2_contradiction", func(t *testing.T) {
			idDoc := uuid(1)
			docs := []docDef{
				{id: idDoc, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})})}, note: "any doc; filter is contradictory"},
			}
			filter := andFilter(
				isNullFilter("countries.garages.cars.model", true),
				isNullFilter("countries.garages.cars.model", false),
			)
			runScenario(t, docs, filter, []strfmt.UUID{})
		})

		// Sub-test 10 (cross-level): countries.name = "germany" AND countries.garages.cars.model IS NULL
		// Same-element correlation at root (countries) — the country with
		// name=germany must have no model anywhere within (universal at country scope).
		t.Run("crossLevel_root_value_isNull_at_L2", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchModelInOtherCntr := uuid(2)
			idMatchEmptyCntr := uuid(3)
			idMatchNoCarsAtAll := uuid(4)
			idNoMatchModelInGermany := uuid(5)
			idNoMatchModelInDeepCar := uuid(6)
			idNoMatchSplitNameAndCntr := uuid(7)
			idNoMatchWrongName := uuid(8)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})})}, note: "germany; no model"},
				{id: idMatchModelInOtherCntr, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany"},
					map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("model", "y"))})},
				)}, note: "germany has no model; model is in france country"},
				{id: idMatchEmptyCntr, props: map[string]any{"countries": asArr(map[string]any{"name": "germany"})}, note: "germany; no garages → vacuous match"},
				{id: idMatchNoCarsAtAll, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})})}, note: "germany; garage with no cars"},
				{id: idNoMatchModelInGermany, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})})}, note: "germany has model"},
				{id: idNoMatchModelInDeepCar, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(
					map[string]any{"cars": asArr(car("make", "x"))},
					map[string]any{"cars": asArr(car("model", "y"))},
				)})}, note: "germany has model in second garage"},
				{id: idNoMatchSplitNameAndCntr, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})},
					map[string]any{"name": "france"},
				)}, note: "germany country has model — fails despite france country having no model"},
				{id: idNoMatchWrongName, props: map[string]any{"countries": asArr(map[string]any{"name": "france"})}, note: "wrong name"},
			}
			filter := andFilter(valueFilter("countries.name", "germany"), isNullFilter("countries.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchModelInOtherCntr, idMatchEmptyCntr, idMatchNoCarsAtAll})
		})

		// Sub-test 11 (cross-level): countries.name = "germany" AND countries.garages.cars.model IS NOT NULL
		t.Run("crossLevel_root_value_isNotNull_at_L2", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchSecondCntr := uuid(2)
			idNoMatchModelInOtherCntr := uuid(3)
			idNoMatchNoModel := uuid(4)
			idNoMatchWrongName := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})})}, note: "germany has model"},
				{id: idMatchSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
					map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "z", "model", "y"))})},
				)}, note: "germany country in second position satisfies"},
				{id: idNoMatchModelInOtherCntr, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "x"))})},
					map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("model", "y"))})},
				)}, note: "model only in france; germany has no model"},
				{id: idNoMatchNoModel, props: map[string]any{"countries": asArr(map[string]any{"name": "germany"})}, note: "no model anywhere"},
				{id: idNoMatchWrongName, props: map[string]any{"countries": asArr(map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "x", "model", "y"))})})}, note: "wrong name"},
			}
			filter := andFilter(valueFilter("countries.name", "germany"), isNullFilter("countries.garages.cars.model", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchSecondCntr})
		})

		// Sub-test 12 (cross-level): countries.garages.city = "berlin" AND countries.garages.cars.make IS NULL
		// Value at L1; IsNull on L2 descendant. Under current universal semantics
		// the IsNull exclude is applied at root (countries) level — the country
		// containing the berlin garage must have NO cars.make ANYWHERE within
		// (any of its garages). Existential rewrite would change this to
		// per-garage scope.
		t.Run("crossLevel_L1_value_isNull_at_L2", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchEmptyCars := uuid(2)
			idMatchInSecondCntr := uuid(3)
			idMatchSplitAcrossCountries := uuid(4)
			idNoMatchMakeInOtherGarageSameCntr := uuid(5)
			idNoMatchMakeInBerlinGarage := uuid(6)
			idNoMatchWrongCity := uuid(7)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin"})})}, note: "berlin garage; no make anywhere"},
				{id: idMatchEmptyCars, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": []any{}})})}, note: "berlin garage with empty cars array; no cars.make anywhere"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"city": "munich", "cars": asArr(car("make", "honda"))})},
					map[string]any{"garages": asArr(map[string]any{"city": "berlin"})},
				)}, note: "second country has berlin garage with no make; first country (with make) is independent"},
				{id: idMatchSplitAcrossCountries, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})},
					map[string]any{"garages": asArr(map[string]any{"city": "berlin"})},
				)}, note: "second country has berlin garage with no make — independently satisfies"},
				{id: idNoMatchMakeInOtherGarageSameCntr, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"city": "munich", "cars": asArr(car("make", "honda"))},
				)})}, note: "country has make in munich garage — universal at country scope rejects (existential would match via berlin garage)"},
				{id: idNoMatchMakeInBerlinGarage, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})})}, note: "berlin garage has cars.make"},
				{id: idNoMatchWrongCity, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "munich"})})}, note: "wrong city"},
			}
			filter := andFilter(valueFilter("countries.garages.city", "berlin"), isNullFilter("countries.garages.cars.make", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchEmptyCars, idMatchInSecondCntr, idMatchSplitAcrossCountries})
		})

		// Sub-test 13 (no-positive / rootAnchor path):
		// countries.garages.cars.make IS NULL AND countries.garages.cars.model IS NULL
		t.Run("L2_all_isNull_no_positive", func(t *testing.T) {
			idMatchCarWithNeither := uuid(1)
			idMatchInSecondCntr := uuid(2)
			idMatchOtherFieldSurvives := uuid(3)
			idNoMatchAllCarsWithMake := uuid(4)
			idNoMatchSharedLeafExcluded := uuid(5)
			idNoMatchOnlyCarsWithBoth := uuid(6)
			docs := []docDef{
				{id: idMatchCarWithNeither, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{})})})}, note: "single car with neither field"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{})})},
				)}, note: "second country has a car with neither field"},
				{id: idMatchOtherFieldSurvives, props: map[string]any{"countries": asArr(map[string]any{"name": "germany"})}, note: "name has fresh leaf (no descendants to share with) — survives"},
				{id: idNoMatchAllCarsWithMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "only leaf is in cars.make exclude"},
				{id: idNoMatchSharedLeafExcluded, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "name shares its leaf with the cars descendants under DFS encoding; the make exclude removes the shared leaf"},
				{id: idNoMatchOnlyCarsWithBoth, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})})}, note: "every leaf in cars.make or cars.model"},
			}
			filter := andFilter(isNullFilter("countries.garages.cars.make", true), isNullFilter("countries.garages.cars.model", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatchCarWithNeither, idMatchInSecondCntr, idMatchOtherFieldSurvives})
		})

		// Sub-test 14 (3-condition AND):
		// countries.garages.cars.make = "honda" AND countries.garages.cars.model IS NOT NULL
		// AND countries.garages.cars.year IS NULL
		t.Run("L2_three_condition", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchInSecondCntr := uuid(2)
			idNoMatchYearPresent := uuid(3)
			idNoMatchNoModel := uuid(4)
			idNoMatchSplit := uuid(5)
			idNoMatchWrongMake := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})})}, note: "honda + model + no year"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota", "model", "corolla", "year", "2020"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic"))})},
				)}, note: "second country has matching car"},
				{id: idNoMatchYearPresent, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic", "year", "2020"))})})}, note: "year present"},
				{id: idNoMatchNoModel, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "no model"},
				{id: idNoMatchSplit, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda", "year", "2020"),
					car("model", "civic"),
				)})})}, note: "honda+year in c0, model in c1 — split"},
				{id: idNoMatchWrongMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota", "model", "corolla"))})})}, note: "wrong make"},
			}
			filter := andFilterN(
				valueFilter("countries.garages.cars.make", "honda"),
				isNullFilter("countries.garages.cars.model", false),
				isNullFilter("countries.garages.cars.year", true),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchInSecondCntr})
		})

		// Sub-test 15: countries.name = "germany" AND countries.garages IS NOT NULL
		// Same-element correlation: a country named germany that has at least one garage.
		t.Run("root_value_arrayIsNotNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchInSecondCntr := uuid(2)
			idNoMatchSplit := uuid(3)
			idNoMatchNoGarages := uuid(4)
			idNoMatchWrongName := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})})}, note: "single country with garages"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"name": "france", "garages": asArr(map[string]any{"city": "paris"})},
					map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})},
				)}, note: "second country satisfies"},
				{id: idNoMatchSplit, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany"},
					map[string]any{"garages": asArr(map[string]any{"city": "berlin"})},
				)}, note: "name and garages in different countries"},
				{id: idNoMatchNoGarages, props: map[string]any{"countries": asArr(map[string]any{"name": "germany"})}, note: "germany without garages"},
				{id: idNoMatchWrongName, props: map[string]any{"countries": asArr(map[string]any{"name": "france", "garages": asArr(map[string]any{"city": "paris"})})}, note: "wrong name"},
			}
			filter := andFilter(valueFilter("countries.name", "germany"), isNullFilter("countries.garages", false))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchInSecondCntr})
		})

		// Sub-test 16: countries.name = "germany" AND countries.garages IS NULL
		// Same-element correlation: a country named germany that has no garages.
		t.Run("root_value_arrayIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchInSecondCntr := uuid(2)
			idNoMatchHasGarages := uuid(3)
			idNoMatchSplit := uuid(4)
			idNoMatchWrongName := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"name": "germany"})}, note: "germany no garages"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"name": "france", "garages": asArr(map[string]any{"city": "paris"})},
					map[string]any{"name": "germany"},
				)}, note: "germany country in [1] without garages"},
				{id: idNoMatchHasGarages, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})})}, note: "germany has garages"},
				{id: idNoMatchSplit, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})},
					map[string]any{"name": "france"},
				)}, note: "germany country has garages — same-element fails"},
				{id: idNoMatchWrongName, props: map[string]any{"countries": asArr(map[string]any{"name": "france"})}, note: "wrong name"},
			}
			filter := andFilter(valueFilter("countries.name", "germany"), isNullFilter("countries.garages", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchInSecondCntr})
		})

		// Sub-test 17 (cross-level): countries.name = "germany" AND countries.garages.cars IS NULL
		// Universal at countries (root) scope: the germany country must have no
		// cars anywhere within its garages.
		t.Run("crossLevel_root_value_intermediateArrayIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchNoGarages := uuid(2)
			idMatchCarsInOtherCntr := uuid(3)
			idNoMatchHasCars := uuid(4)
			idNoMatchCarsInOtherGarage := uuid(5)
			idNoMatchWrongName := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})})}, note: "germany; garage with no cars"},
				{id: idMatchNoGarages, props: map[string]any{"countries": asArr(map[string]any{"name": "germany"})}, note: "germany; no garages → vacuous match"},
				{id: idMatchCarsInOtherCntr, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany", "garages": asArr(map[string]any{"city": "berlin"})},
					map[string]any{"name": "france", "garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})},
				)}, note: "germany has no cars; cars in france"},
				{id: idNoMatchHasCars, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "germany has cars"},
				{id: idNoMatchCarsInOtherGarage, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"cars": asArr(car("make", "honda"))},
				)})}, note: "cars exist in germany country (other garage)"},
				{id: idNoMatchWrongName, props: map[string]any{"countries": asArr(map[string]any{"name": "france"})}, note: "wrong name"},
			}
			filter := andFilter(valueFilter("countries.name", "germany"), isNullFilter("countries.garages.cars", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchNoGarages, idMatchCarsInOtherCntr})
		})

		// Sub-test 18 (inverse cross-level): countries.garages.cars.make = "honda"
		// AND countries.name IS NULL. Same-element correlation: a country with
		// cars containing honda AND no name on that same country.
		t.Run("inverseCrossLevel_L2_value_root_isNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchInSecondCntr := uuid(2)
			idNoMatchNameSet := uuid(3)
			idNoMatchSplit := uuid(4)
			idNoMatchWrongMake := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "country with honda and no name"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"name": "france"},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})},
				)}, note: "second country: honda, no name"},
				{id: idNoMatchNameSet, props: map[string]any{"countries": asArr(map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "honda but name is set"},
				{id: idNoMatchSplit, props: map[string]any{"countries": asArr(
					map[string]any{"name": "germany", "garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})},
					map[string]any{"capital": "paris"},
				)}, note: "honda in named country; nameless country has no honda"},
				{id: idNoMatchWrongMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota"))})})}, note: "wrong make"},
			}
			filter := andFilter(valueFilter("countries.garages.cars.make", "honda"), isNullFilter("countries.name", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchInSecondCntr})
		})

		// Sub-test 19 (mixed): countries.garages.cars.make = "honda" AND
		// countries.garages.postcode IS NULL. Same-garage correlation.
		t.Run("mixed_L2_value_L1_leafIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchInSecondCntr := uuid(2)
			idNoMatchPostcodePresent := uuid(3)
			idNoMatchSplit := uuid(4)
			idNoMatchWrongMake := uuid(5)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "garage with honda, no postcode"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"postcode": "10115", "cars": asArr(car("make", "toyota"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})},
				)}, note: "second country has satisfying garage"},
				{id: idNoMatchPostcodePresent, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"postcode": "10115", "cars": asArr(car("make", "honda"))})})}, note: "honda garage has postcode"},
				{id: idNoMatchSplit, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"postcode": "10115", "cars": asArr(car("make", "honda"))},
					map[string]any{"cars": asArr(car("make", "toyota"))},
				)})}, note: "honda+postcode in g0; no-postcode+wrong-make in g1"},
				{id: idNoMatchWrongMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota"))})})}, note: "wrong make"},
			}
			filter := andFilter(valueFilter("countries.garages.cars.make", "honda"), isNullFilter("countries.garages.postcode", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchInSecondCntr})
		})

		// Sub-test 20 (no-positive / rootAnchor with three excludes).
		t.Run("L2_three_isNull_no_positive", func(t *testing.T) {
			idMatchCarWithNothing := uuid(1)
			idMatchInSecondCntr := uuid(2)
			idMatchOtherFieldSurvives := uuid(3)
			idNoMatchHasMake := uuid(4)
			idNoMatchHasYear := uuid(5)
			idNoMatchAllSet := uuid(6)
			docs := []docDef{
				{id: idMatchCarWithNothing, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{})})})}, note: "single car with no fields"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic", "year", "2020"))})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{})})},
				)}, note: "second country has a car with no fields"},
				{id: idMatchOtherFieldSurvives, props: map[string]any{"countries": asArr(map[string]any{"capital": "berlin"})}, note: "capital has fresh leaf"},
				{id: idNoMatchHasMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "only leaf in cars.make exclude"},
				{id: idNoMatchHasYear, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("year", "2020"))})})}, note: "only leaf in cars.year exclude"},
				{id: idNoMatchAllSet, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda", "model", "civic", "year", "2020"))})})}, note: "every leaf in some exclude"},
			}
			filter := andFilterN(
				isNullFilter("countries.garages.cars.make", true),
				isNullFilter("countries.garages.cars.model", true),
				isNullFilter("countries.garages.cars.year", true),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatchCarWithNothing, idMatchInSecondCntr, idMatchOtherFieldSurvives})
		})

		// Sub-test 21: countries.garages.city = "berlin" AND
		// countries.garages.cars IS NULL. Per-country correlation: each country
		// is evaluated independently for same-element. Within a single country,
		// the array-prop IsNull is universal — no cars anywhere within the
		// country containing the berlin garage.
		t.Run("L1_value_intermediateArrayIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchMultipleGarages := uuid(2)
			idMatchInSecondCntr := uuid(3)
			idNoMatchHasCarsSameGarage := uuid(4)
			idNoMatchHasCarsOtherGarageSameCntr := uuid(5)
			idNoMatchWrongCity := uuid(6)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin"})})}, note: "berlin garage, no cars"},
				{id: idMatchMultipleGarages, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"city": "munich"},
					map[string]any{"city": "berlin"},
				)})}, note: "berlin in g1; no cars in country"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"city": "munich", "cars": asArr(car("make", "honda"))})},
					map[string]any{"garages": asArr(map[string]any{"city": "berlin"})},
				)}, note: "second country independently satisfies (berlin, no cars in that country)"},
				{id: idNoMatchHasCarsSameGarage, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})})}, note: "berlin garage has cars"},
				{id: idNoMatchHasCarsOtherGarageSameCntr, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"city": "munich", "cars": asArr(car("make", "honda"))},
				)})}, note: "single country: berlin garage has no cars but other garage does — universal at country rejects"},
				{id: idNoMatchWrongCity, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "munich"})})}, note: "wrong city"},
			}
			filter := andFilter(valueFilter("countries.garages.city", "berlin"), isNullFilter("countries.garages.cars", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchMultipleGarages, idMatchInSecondCntr})
		})

		// Sub-test 22: countries.garages.cars.make = "honda" AND
		// countries.garages.cars.tires IS NULL. Per-country correlation; within
		// a single country, array-prop IsNull is universal — no tires anywhere
		// in the country that has honda.
		t.Run("L2_value_intermediateArrayIsNull", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchMultipleCars := uuid(2)
			idMatchInSecondCntr := uuid(3)
			idNoMatchHasTiresSameCar := uuid(4)
			idNoMatchHasTiresOtherCarSameCntr := uuid(5)
			idNoMatchWrongMake := uuid(6)
			tire := func(width string) map[string]any { return map[string]any{"width": width} }
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})})}, note: "honda car, no tires"},
				{id: idMatchMultipleCars, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "toyota"),
					car("make", "honda"),
				)})})}, note: "honda in c1; no tires in country"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{"make": "toyota", "tires": asArr(tire("205"))})})},
					map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "honda"))})},
				)}, note: "second country independently satisfies"},
				{id: idNoMatchHasTiresSameCar, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(map[string]any{"make": "honda", "tires": asArr(tire("205"))})})})}, note: "honda car has tires"},
				{id: idNoMatchHasTiresOtherCarSameCntr, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(
					car("make", "honda"),
					map[string]any{"make": "toyota", "tires": asArr(tire("205"))},
				)})})}, note: "single country: honda car has no tires but other car does — universal at country rejects"},
				{id: idNoMatchWrongMake, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"cars": asArr(car("make", "toyota"))})})}, note: "wrong make"},
			}
			filter := andFilter(valueFilter("countries.garages.cars.make", "honda"), isNullFilter("countries.garages.cars.tires", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchMultipleCars, idMatchInSecondCntr})
		})

		// Sub-test 23 (cross-level L1+L2): countries.garages.city = "berlin" AND
		// countries.garages.cars.tires IS NULL. Universal at country scope:
		// the country containing the berlin garage must have no tires anywhere
		// within it.
		t.Run("crossLevel_L1_value_intermediateArrayIsNull_at_L2", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchCarsNoTires := uuid(2)
			idMatchInSecondCntr := uuid(3)
			idNoMatchTiresInBerlinCar := uuid(4)
			idNoMatchTiresInOtherGarageSameCntr := uuid(5)
			idNoMatchWrongCity := uuid(6)
			tire := func(width string) map[string]any { return map[string]any{"width": width} }
			docs := []docDef{
				{id: idMatch, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin"})})}, note: "berlin garage, no tires anywhere"},
				{id: idMatchCarsNoTires, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(car("make", "honda"))})})}, note: "cars but no tires"},
				{id: idMatchInSecondCntr, props: map[string]any{"countries": asArr(
					map[string]any{"garages": asArr(map[string]any{"city": "munich", "cars": asArr(map[string]any{"make": "honda", "tires": asArr(tire("205"))})})},
					map[string]any{"garages": asArr(map[string]any{"city": "berlin"})},
				)}, note: "second country has berlin garage, no tires; first country independent"},
				{id: idNoMatchTiresInBerlinCar, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "berlin", "cars": asArr(map[string]any{"make": "honda", "tires": asArr(tire("205"))})})})}, note: "tires in berlin garage car"},
				{id: idNoMatchTiresInOtherGarageSameCntr, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(
					map[string]any{"city": "berlin"},
					map[string]any{"city": "munich", "cars": asArr(map[string]any{"make": "honda", "tires": asArr(tire("205"))})},
				)})}, note: "tires in other garage of same country — universal at country scope rejects"},
				{id: idNoMatchWrongCity, props: map[string]any{"countries": asArr(map[string]any{"garages": asArr(map[string]any{"city": "munich"})})}, note: "wrong city"},
			}
			filter := andFilter(valueFilter("countries.garages.city", "berlin"), isNullFilter("countries.garages.cars.tires", true))
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchCarsNoTires, idMatchInSecondCntr})
		})
	})
}

// TestNestedFilteringCorrelatedAndFilterExamples ports the G1-G6 / C1-C5
// "filter examples" coverage to the DB level. Each sub-test exercises a
// correlated AND of two or three conditions with explicit arr[N] indices
// and/or unconstrained paths, where same-element semantics must hold at the
// shared LCA. Each filter is run twice (forward and reversed condition order)
// to confirm bucketing/dispatch are order-independent.
//
// Schema mirrors filterExamplesClass:
//
//	garages (object[]): city, make, postcode, cars (object[]): make, model, color
//	countries (object[]): garages (same shape as above)
func TestNestedFilteringCorrelatedAndFilterExamples(t *testing.T) {
	const nestedClass = "FilterExamplesDB"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	carsProps := []*models.NestedProperty{
		{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "model", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
	}
	garagesProps := []*models.NestedProperty{
		{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "cars", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: carsProps},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "garages", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: garagesProps},
			{Name: "countries", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: []*models.NestedProperty{
				{Name: "garages", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: garagesProps},
			}},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	car := func(props ...string) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i]] = props[i+1]
		}
		return out
	}
	garage := func(fields map[string]any, cars ...map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range fields {
			out[k] = v
		}
		if len(cars) > 0 {
			out["cars"] = asArr(cars...)
		}
		return out
	}
	country := func(garages ...map[string]any) map[string]any {
		return map[string]any{"garages": asArr(garages...)}
	}

	valueFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andClauses := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: operands,
		}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}

	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	// runOrderings runs the filter twice — once with parts in given order,
	// once reversed — and asserts each yields want.
	runOrderings := func(t *testing.T, docs []docDef, parts []*filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
		}
		for _, ordering := range []string{"forward", "reverse"} {
			ordered := make([]*filters.LocalFilter, len(parts))
			if ordering == "forward" {
				copy(ordered, parts)
			} else {
				for i, p := range parts {
					ordered[len(parts)-1-i] = p
				}
			}
			res, err := db.Search(ctx, dto.GetParams{
				ClassName:  nestedClass,
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    andClauses(ordered...),
			})
			require.NoError(t, err, "ordering=%s", ordering)
			got := make([]strfmt.UUID, len(res))
			for i, r := range res {
				got[i] = r.ID
			}
			assert.ElementsMatch(t, want, got, "ordering=%s", ordering)
		}
	}

	// ----------------------------------------------------------------- G1-G6
	// Top-level "garages" root.

	// G1: garages[0].city = berlin AND garages[1].make = honda.
	// Different garage indices → docID-level AND, same-doc enforced.
	t.Run("G1_garages[0].city_AND_garages[1].make", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitDocs := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"make": "honda"}),
			)}, note: "g[0].city=berlin AND g[1].make=honda"},
			{id: idNoMatchSplitDocs, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"make": "toyota"}),
			)}, note: "g[1].make wrong"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[0].city", "berlin"),
			valueFilter("garages[1].make", "honda"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G2: garages.city = berlin AND garages[1].make = honda.
	// Unconstrained city scoped to garages[1] (same-element).
	t.Run("G2_garages.city_AND_garages[1].make", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplit := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "munich"}),
				garage(map[string]any{"city": "berlin", "make": "honda"}),
			)}, note: "g[1] has city=berlin AND make=honda"},
			{id: idNoMatchSplit, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"make": "honda"}),
			)}, note: "city in g[0], make in g[1] — same-element fails"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages.city", "berlin"),
			valueFilter("garages[1].make", "honda"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G3: garages.cars[1].make = honda AND garages.city = berlin.
	// Unconstrained city scoped to same garage as the constrained car.
	t.Run("G3_garages.cars[1].make_AND_garages.city", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplit := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}, car(), car("make", "honda")),
			)}, note: "same garage has city AND cars[1].make=honda"},
			{id: idNoMatchSplit, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{}, car(), car("make", "honda")),
			)}, note: "city in g[0], cars[1].make in g[1] — different garages"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages.cars[1].make", "honda"),
			valueFilter("garages.city", "berlin"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G4: garages.cars[1].make = honda AND garages.cars.model = civic.
	// Unconstrained model scoped to same car (cars[1]) via groupAndAll.
	t.Run("G4_garages.cars[1].make_AND_garages.cars.model", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplit := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil, car(), car("make", "honda", "model", "civic")),
			)}, note: "cars[1] has both"},
			{id: idNoMatchSplit, props: map[string]any{"garages": asArr(
				garage(nil, car("model", "civic"), car("make", "honda")),
			)}, note: "make in cars[1], model in cars[0] — different cars"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages.cars[1].make", "honda"),
			valueFilter("garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G5: garages.cars[1].make = honda AND garages.cars[2].color = red.
	// Different car indices but root+docID AND enforces same garage.
	t.Run("G5_garages.cars[1].make_AND_garages.cars[2].color", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitGarages := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil, car(), car("make", "honda"), car("color", "red")),
			)}, note: "g[0] has cars[1].make AND cars[2].color"},
			{id: idNoMatchSplitGarages, props: map[string]any{"garages": asArr(
				garage(nil, car(), car("make", "honda")),
				garage(nil, car(), car(), car("color", "red")),
			)}, note: "make in g[0].cars[1], color in g[1].cars[2] — different garages"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages.cars[1].make", "honda"),
			valueFilter("garages.cars[2].color", "red"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G6: garages.city = berlin AND garages.cars.make = honda AND
	// garages.cars.model = civic. Mixed-depth, no explicit indices.
	// Same garage AND same car (make+model leaf-precise via groupAndAll).
	t.Run("G6_garages.city_AND_garages.cars.{make,model}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}, car(), car("make", "honda", "model", "civic")),
			)}, note: "city + same-car make+model"},
			{id: idNoMatchSplitCars, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}, car("make", "honda"), car("model", "civic")),
			)}, note: "make in cars[0], model in cars[1] — different cars same garage"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages.city", "berlin"),
			valueFilter("garages.cars.make", "honda"),
			valueFilter("garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G7: garages[2].city = berlin AND garages.cars[1].make = honda.
	// Outer pinned to garages[2]; unconstrained cars[1] scoped to that same
	// garage via runIdxLoop on garages.
	t.Run("G7_garages[2].city_AND_garages.cars[1].make", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchMakeWrongGarage := uuid(2)
		idNoMatchCityWrongGarage := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil), garage(nil), garage(map[string]any{"city": "berlin"}, car(), car("make", "honda")),
			)}, note: "g[2] has city AND cars[1].make"},
			{id: idNoMatchMakeWrongGarage, props: map[string]any{"garages": asArr(
				garage(nil), garage(nil, car(), car("make", "honda")), garage(map[string]any{"city": "berlin"}),
			)}, note: "city in g[2], make in g[1] — different garages"},
			{id: idNoMatchCityWrongGarage, props: map[string]any{"garages": asArr(
				garage(nil), garage(map[string]any{"city": "berlin"}), garage(nil, car(), car("make", "honda")),
			)}, note: "city in g[1] (not g[2]); make in g[2]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[2].city", "berlin"),
			valueFilter("garages.cars[1].make", "honda"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G8: garages.cars.make = honda AND garages.cars.model = civic. Pure
	// unconstrained correlation at cars level; same-car required via
	// groupAndAll (no arr[N] anywhere).
	t.Run("G8_garages.cars.make_AND_garages.cars.model", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		idNoMatchSplitGarages := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil, car("make", "honda", "model", "civic")),
			)}, note: "single car has both"},
			{id: idNoMatchSplitCars, props: map[string]any{"garages": asArr(
				garage(nil, car("make", "honda"), car("model", "civic")),
			)}, note: "make in cars[0], model in cars[1] — different cars same garage"},
			{id: idNoMatchSplitGarages, props: map[string]any{"garages": asArr(
				garage(nil, car("make", "honda")),
				garage(nil, car("model", "civic")),
			)}, note: "make in g[0].cars, model in g[1].cars — different garages"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages.cars.make", "honda"),
			valueFilter("garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G9: garages[1].city = berlin AND garages[1].make = honda. Same arr[N]
	// index on both conditions; single-bucket split holds both conditions in
	// the same bucket.
	t.Run("G9_garages[1].city_AND_garages[1].make", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchAtIdx0 := uuid(2)
		idNoMatchSplitIndices := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil), garage(map[string]any{"city": "berlin", "make": "honda"}),
			)}, note: "g[1] has both"},
			{id: idNoMatchAtIdx0, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin", "make": "honda"}),
			)}, note: "both at g[0] — wrong index"},
			{id: idNoMatchSplitIndices, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}), garage(map[string]any{"make": "honda"}),
			)}, note: "city at g[0], make at g[1] — neither index has both"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[1].city", "berlin"),
			valueFilter("garages[1].make", "honda"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// G10: garages[0].city = berlin AND garages[1].make = honda AND
	// garages[2].postcode = "10115". Three different arr[N] indices →
	// three-way multi-bucket split at garages.
	t.Run("G10_garages[0].city_AND_garages[1].make_AND_garages[2].postcode", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchMissingThird := uuid(2)
		idNoMatchWrongIndex := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"make": "honda"}),
				garage(map[string]any{"postcode": "10115"}),
			)}, note: "all three indices satisfied"},
			{id: idNoMatchMissingThird, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"make": "honda"}),
			)}, note: "g[2] missing"},
			{id: idNoMatchWrongIndex, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"make": "honda", "postcode": "10115"}),
			)}, note: "postcode at g[1] not g[2]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[0].city", "berlin"),
			valueFilter("garages[1].make", "honda"),
			valueFilter("garages[2].postcode", "10115"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// ----------------------------------------------------------------- C1-C10
	// Top-level "countries" root. Each Cn mirrors Gn one level deeper:
	// G's "garages" → C's "countries.garages", G's "garages.cars" → C's
	// "countries.garages.cars".

	// C1 (mirror of G1): countries.garages[0].city = berlin AND
	// countries.garages[1].make = honda. Different garages within a country →
	// split at garages enforces same country implicitly via root+docID AND.
	t.Run("C1_countries.garages[0].city_AND_countries.garages[1].make", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCntr := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"}), garage(map[string]any{"make": "honda"})),
			)}, note: "country[0]: g[0].city AND g[1].make"},
			{id: idNoMatchSplitCntr, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"})),
				country(garage(nil), garage(map[string]any{"make": "honda"})),
			)}, note: "city in c[0], make in c[1] — different countries"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages[0].city", "berlin"),
			valueFilter("countries.garages[1].make", "honda"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C2 (mirror of G2): countries.garages.city = berlin AND
	// countries.garages[1].make = honda. Unconstrained city scoped to
	// garages[1] (same-element).
	t.Run("C2_countries.garages.city_AND_countries.garages[1].make", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitGarages := uuid(2)
		idNoMatchSplitCntr := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "munich"}), garage(map[string]any{"city": "berlin", "make": "honda"})),
			)}, note: "g[1] has city=berlin AND make=honda"},
			{id: idNoMatchSplitGarages, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"}), garage(map[string]any{"make": "honda"})),
			)}, note: "city in g[0], make in g[1] within same country — same-element fails"},
			{id: idNoMatchSplitCntr, props: map[string]any{"countries": asArr(
				country(garage(nil), garage(map[string]any{"city": "berlin"})),
				country(garage(nil), garage(map[string]any{"make": "honda"})),
			)}, note: "city in c[0].g[1], make in c[1].g[1] — different countries"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.city", "berlin"),
			valueFilter("countries.garages[1].make", "honda"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C3 (mirror of G3): countries.garages.cars[1].make = honda AND
	// countries.garages.city = berlin. Unconstrained city scoped to same
	// garage as the constrained car.
	t.Run("C3_countries.garages.cars[1].make_AND_countries.garages.city", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitGarages := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"}, car(), car("make", "honda"))),
			)}, note: "same garage has city AND cars[1].make"},
			{id: idNoMatchSplitGarages, props: map[string]any{"countries": asArr(
				country(
					garage(map[string]any{"city": "berlin"}),
					garage(nil, car(), car("make", "honda")),
				),
			)}, note: "city in g[0], cars[1].make in g[1] within same country"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.cars[1].make", "honda"),
			valueFilter("countries.garages.city", "berlin"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C4 (mirror of G4): countries.garages.cars[1].make = honda AND
	// countries.garages.cars.model = civic. Same car required.
	t.Run("C4_countries.garages.cars[1].make_AND_countries.garages.cars.model", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(nil, car(), car("make", "honda", "model", "civic"))),
			)}, note: "cars[1] has both"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(nil, car("model", "civic"), car("make", "honda"))),
			)}, note: "make in cars[1], model in cars[0]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.cars[1].make", "honda"),
			valueFilter("countries.garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C5 (mirror of G5): countries.garages.cars[1].make = honda AND
	// countries.garages.cars[2].color = red. Different car indices, same
	// garage required (root+docID AND enforces same country implicitly).
	t.Run("C5_countries.garages.cars[1].make_AND_countries.garages.cars[2].color", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitGarages := uuid(2)
		idNoMatchSplitCntr := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(nil, car(), car("make", "honda"), car("color", "red"))),
			)}, note: "same garage: cars[1].make AND cars[2].color"},
			{id: idNoMatchSplitGarages, props: map[string]any{"countries": asArr(
				country(
					garage(nil, car(), car("make", "honda")),
					garage(nil, car(), car(), car("color", "red")),
				),
			)}, note: "make in g[0], color in g[1] within same country — different garages"},
			{id: idNoMatchSplitCntr, props: map[string]any{"countries": asArr(
				country(garage(nil, car(), car("make", "honda"))),
				country(garage(nil, car(), car(), car("color", "red"))),
			)}, note: "make in c[0], color in c[1] — different countries"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.cars[1].make", "honda"),
			valueFilter("countries.garages.cars[2].color", "red"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C6 (mirror of G6): countries.garages.city = berlin AND
	// countries.garages.cars.make = honda AND countries.garages.cars.model = civic.
	// Mixed depth, no indices. Same garage AND same car required.
	t.Run("C6_countries.garages.city_AND_countries.garages.cars.{make,model}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		idNoMatchSplitGarages := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"}, car(), car("make", "honda", "model", "civic"))),
			)}, note: "city + same-car make+model in same garage"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"}, car("make", "honda"), car("model", "civic"))),
			)}, note: "make in cars[0], model in cars[1] — different cars same garage"},
			{id: idNoMatchSplitGarages, props: map[string]any{"countries": asArr(
				country(
					garage(map[string]any{"city": "berlin"}),
					garage(nil, car("make", "honda", "model", "civic")),
				),
			)}, note: "city in g[0], same-car make+model in g[1] — different garages"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.city", "berlin"),
			valueFilter("countries.garages.cars.make", "honda"),
			valueFilter("countries.garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C7 (mirror of G7): countries.garages[2].city = berlin AND
	// countries.garages.cars[1].make = honda. City pinned to garages[2];
	// unconstrained make scoped to same garage.
	t.Run("C7_countries.garages[2].city_AND_countries.garages.cars[1].make", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchMakeWrongGarage := uuid(2)
		idNoMatchCityWrongGarage := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(nil), garage(nil), garage(map[string]any{"city": "berlin"}, car(), car("make", "honda"))),
			)}, note: "g[2] has city AND cars[1].make"},
			{id: idNoMatchMakeWrongGarage, props: map[string]any{"countries": asArr(
				country(
					garage(nil),
					garage(nil, car(), car("make", "honda")),
					garage(map[string]any{"city": "berlin"}),
				),
			)}, note: "city in g[2], make in g[1] — different garages"},
			{id: idNoMatchCityWrongGarage, props: map[string]any{"countries": asArr(
				country(
					garage(nil),
					garage(map[string]any{"city": "berlin"}),
					garage(nil, car(), car("make", "honda")),
				),
			)}, note: "city in g[1] (not g[2]); make in g[2]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages[2].city", "berlin"),
			valueFilter("countries.garages.cars[1].make", "honda"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C8 (mirror of G8): countries.garages.cars.make = honda AND
	// countries.garages.cars.model = civic. Pure unconstrained correlation at
	// cars level; same-car required via groupAndAll.
	t.Run("C8_countries.garages.cars.make_AND_countries.garages.cars.model", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		idNoMatchSplitGarages := uuid(3)
		idNoMatchSplitCntr := uuid(4)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(nil, car("make", "honda", "model", "civic"))),
			)}, note: "single car has both"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(nil, car("make", "honda"), car("model", "civic"))),
			)}, note: "make in cars[0], model in cars[1] — different cars"},
			{id: idNoMatchSplitGarages, props: map[string]any{"countries": asArr(
				country(garage(nil, car("make", "honda")), garage(nil, car("model", "civic"))),
			)}, note: "make in g[0].cars, model in g[1].cars within same country"},
			{id: idNoMatchSplitCntr, props: map[string]any{"countries": asArr(
				country(garage(nil, car("make", "honda"))),
				country(garage(nil, car("model", "civic"))),
			)}, note: "make in c[0], model in c[1] — different countries"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.cars.make", "honda"),
			valueFilter("countries.garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C9 (mirror of G9): countries.garages[1].city = berlin AND
	// countries.garages[1].make = honda. Same arr[N] index on both conditions.
	t.Run("C9_countries.garages[1].city_AND_countries.garages[1].make", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchAtIdx0 := uuid(2)
		idNoMatchSplitIndices := uuid(3)
		idNoMatchSplitCntr := uuid(4)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(nil), garage(map[string]any{"city": "berlin", "make": "honda"})),
			)}, note: "c[0].g[1] has both"},
			{id: idNoMatchAtIdx0, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin", "make": "honda"})),
			)}, note: "both at g[0] — wrong index"},
			{id: idNoMatchSplitIndices, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"}), garage(map[string]any{"make": "honda"})),
			)}, note: "city at g[0], make at g[1] — neither index has both"},
			{id: idNoMatchSplitCntr, props: map[string]any{"countries": asArr(
				country(garage(nil), garage(map[string]any{"city": "berlin"})),
				country(garage(nil), garage(map[string]any{"make": "honda"})),
			)}, note: "city in c[0].g[1], make in c[1].g[1] — different countries"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages[1].city", "berlin"),
			valueFilter("countries.garages[1].make", "honda"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// C10 (mirror of G10): countries.garages[0].city = berlin AND
	// countries.garages[1].make = honda AND countries.garages[2].postcode =
	// "10115". Three-way multi-bucket split at garages within a country.
	t.Run("C10_countries.garages[0].city_AND_countries.garages[1].make_AND_countries.garages[2].postcode", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchMissingThird := uuid(2)
		idNoMatchSplitCntr := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(
					garage(map[string]any{"city": "berlin"}),
					garage(map[string]any{"make": "honda"}),
					garage(map[string]any{"postcode": "10115"}),
				),
			)}, note: "all three indices satisfied within same country"},
			{id: idNoMatchMissingThird, props: map[string]any{"countries": asArr(
				country(
					garage(map[string]any{"city": "berlin"}),
					garage(map[string]any{"make": "honda"}),
				),
			)}, note: "g[2] missing"},
			{id: idNoMatchSplitCntr, props: map[string]any{"countries": asArr(
				country(
					garage(map[string]any{"city": "berlin"}),
					garage(map[string]any{"make": "honda"}),
				),
				country(
					garage(nil), garage(nil),
					garage(map[string]any{"postcode": "10115"}),
				),
			)}, note: "city+make in c[0], postcode in c[1] — different countries"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages[0].city", "berlin"),
			valueFilter("countries.garages[1].make", "honda"),
			valueFilter("countries.garages[2].postcode", "10115"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})
}

// TestNestedFilteringCorrelatedAndFilterExamplesIndexed ports the F1-F12 / F14
// indexed filter examples to the DB level. Each is a 4-condition filter with
// arr[N] constraints at one or more depths. Sub-tests are forward and reverse
// ordered to confirm bucketing/dispatch are order-independent.
//
// Schema extends FilterExamplesClass with two object[] sub-arrays inside cars
// (accessories and tires) and a text[] tags field.
func TestNestedFilteringCorrelatedAndFilterExamplesIndexed(t *testing.T) {
	const nestedClass = "FilterExamplesIndexedDB"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	carsProps := []*models.NestedProperty{
		{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "model", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "accessories", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: []*models.NestedProperty{
			{Name: "type", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		}},
		{Name: "tires", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: []*models.NestedProperty{
			{Name: "width", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		}},
	}
	garagesProps := []*models.NestedProperty{
		{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "cars", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: carsProps},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "garages", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: garagesProps},
			{Name: "countries", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: []*models.NestedProperty{
				{Name: "garages", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: garagesProps},
			}},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tire := func(width string) map[string]any { return map[string]any{"width": width} }
	accessory := func(typ string) map[string]any { return map[string]any{"type": typ} }
	garage := func(fields map[string]any, cars ...map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range fields {
			out[k] = v
		}
		if len(cars) > 0 {
			out["cars"] = asArr(cars...)
		}
		return out
	}
	country := func(garages ...map[string]any) map[string]any {
		return map[string]any{"garages": asArr(garages...)}
	}

	valueFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andClauses := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: operands,
		}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}

	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	runOrderings := func(t *testing.T, docs []docDef, parts []*filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
		}
		for _, ordering := range []string{"forward", "reverse"} {
			ordered := make([]*filters.LocalFilter, len(parts))
			if ordering == "forward" {
				copy(ordered, parts)
			} else {
				for i, p := range parts {
					ordered[len(parts)-1-i] = p
				}
			}
			res, err := db.Search(ctx, dto.GetParams{
				ClassName:  nestedClass,
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    andClauses(ordered...),
			})
			require.NoError(t, err, "ordering=%s", ordering)
			got := make([]strfmt.UUID, len(res))
			for i, r := range res {
				got[i] = r.ID
			}
			assert.ElementsMatch(t, want, got, "ordering=%s", ordering)
		}
	}

	carWith := func(fields map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range fields {
			out[k] = v
		}
		return out
	}

	// F1: garages-rooted. garages[0].city AND garages[1].postcode AND
	// garages[1].cars.{make, model}. Split at garages with two buckets;
	// within g[1] same-car required for make+model.
	t.Run("F1_garages[0].city_AND_garages[1].postcode_AND_garages[1].cars.{make,model}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"postcode": "12345"}, carWith(map[string]any{"make": "honda", "model": "civic"})),
			)}, note: "g[0].city + g[1].{postcode,cars[0].make+model}"},
			{id: idNoMatchSplitCars, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"postcode": "12345"}, carWith(map[string]any{"make": "honda"}), carWith(map[string]any{"model": "civic"})),
			)}, note: "g[1] make+model in different cars"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[0].city", "berlin"),
			valueFilter("garages[1].postcode", "12345"),
			valueFilter("garages[1].cars.make", "honda"),
			valueFilter("garages[1].cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F2: garages[0].city AND garages[1].postcode AND garages[2].cars.{make,model}.
	// Three-way split; same-car within g[2].
	t.Run("F2_garages[0].city_AND_garages[1].postcode_AND_garages[2].cars.{make,model}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"postcode": "12345"}),
				garage(nil, carWith(map[string]any{"make": "honda", "model": "civic"})),
			)}, note: "g[0/1/2] each satisfies its part"},
			{id: idNoMatchSplitCars, props: map[string]any{"garages": asArr(
				garage(map[string]any{"city": "berlin"}),
				garage(map[string]any{"postcode": "12345"}),
				garage(nil, carWith(map[string]any{"make": "honda"}), carWith(map[string]any{"model": "civic"})),
			)}, note: "g[2] split cars"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[0].city", "berlin"),
			valueFilter("garages[1].postcode", "12345"),
			valueFilter("garages[2].cars.make", "honda"),
			valueFilter("garages[2].cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F3: countries[0].garages.city AND countries[1].garages.postcode AND
	// countries[1].garages.cars.{make,model}. Split at countries; within c[1]
	// same-garage AND same-car required.
	t.Run("F3_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[1].cars.{make,model}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"})),
				country(garage(map[string]any{"postcode": "12345"}, carWith(map[string]any{"make": "honda", "model": "civic"}))),
			)}, note: "c[0]:city; c[1]: postcode + same-car make+model"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"})),
				country(garage(map[string]any{"postcode": "12345"}, carWith(map[string]any{"make": "honda"}), carWith(map[string]any{"model": "civic"}))),
			)}, note: "c[1] split cars"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries[0].garages.city", "berlin"),
			valueFilter("countries[1].garages.postcode", "12345"),
			valueFilter("countries[1].garages.cars.make", "honda"),
			valueFilter("countries[1].garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F4: countries[0/1/2] three-way split. Within c[2] same-garage AND same-car.
	t.Run("F4_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[2].cars.{make,model}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"})),
				country(garage(map[string]any{"postcode": "12345"})),
				country(garage(nil, carWith(map[string]any{"make": "honda", "model": "civic"}))),
			)}, note: "c[0/1/2] each satisfies its part"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"})),
				country(garage(map[string]any{"postcode": "12345"})),
				country(garage(nil, carWith(map[string]any{"make": "honda"}), carWith(map[string]any{"model": "civic"}))),
			)}, note: "c[2] split cars"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries[0].garages.city", "berlin"),
			valueFilter("countries[1].garages.postcode", "12345"),
			valueFilter("countries[2].garages.cars.make", "honda"),
			valueFilter("countries[2].garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F5: countries[2].garages[3].cars.{make,model} with city+postcode pinned to c[0]/c[1].
	t.Run("F5_countries[0].garages.city_AND_countries[1].garages.postcode_AND_countries[2].garages[3].cars.{make,model}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"})),
				country(garage(map[string]any{"postcode": "12345"})),
				country(
					garage(nil), garage(nil), garage(nil),
					garage(nil, carWith(map[string]any{"make": "honda", "model": "civic"})),
				),
			)}, note: "c[2].g[3] has same-car make+model"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"})),
				country(garage(map[string]any{"postcode": "12345"})),
				country(
					garage(nil), garage(nil), garage(nil),
					garage(nil, carWith(map[string]any{"make": "honda"}), carWith(map[string]any{"model": "civic"})),
				),
			)}, note: "c[2].g[3] split cars"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries[0].garages.city", "berlin"),
			valueFilter("countries[1].garages.postcode", "12345"),
			valueFilter("countries[2].garages[3].cars.make", "honda"),
			valueFilter("countries[2].garages[3].cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F6: countries.garages.{city,postcode} AND cars.{make,model} — no indices.
	// Same garage + same car required.
	t.Run("F6_countries.garages.city_AND_postcode_AND_cars.{make,model}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		idNoMatchSplitGarages := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin", "postcode": "12345"}, carWith(map[string]any{"make": "honda", "model": "civic"}))),
			)}, note: "same garage same car has all four"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin", "postcode": "12345"}, carWith(map[string]any{"make": "honda"}), carWith(map[string]any{"model": "civic"}))),
			)}, note: "same garage but split cars"},
			{id: idNoMatchSplitGarages, props: map[string]any{"countries": asArr(
				country(
					garage(map[string]any{"city": "berlin"}, carWith(map[string]any{"make": "honda", "model": "civic"})),
					garage(map[string]any{"postcode": "12345"}),
				),
			)}, note: "city+cars in g[0]; postcode in g[1]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.city", "berlin"),
			valueFilter("countries.garages.postcode", "12345"),
			valueFilter("countries.garages.cars.make", "honda"),
			valueFilter("countries.garages.cars.model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F7: countries.garages.{city,postcode} AND cars.accessories.type AND cars.tags.
	// Same garage + same car required (accessories and tags both per-car).
	t.Run("F7_countries.garages.city_AND_postcode_AND_cars.accessories.type_AND_cars.tags", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin", "postcode": "12345"},
					map[string]any{"accessories": asArr(accessory("spoiler")), "tags": []any{"electric"}},
				)),
			)}, note: "same car has accessories + tags"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin", "postcode": "12345"},
					map[string]any{"accessories": asArr(accessory("spoiler"))},
					map[string]any{"tags": []any{"electric"}},
				)),
			)}, note: "accessories in cars[0]; tags in cars[1]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.city", "berlin"),
			valueFilter("countries.garages.postcode", "12345"),
			valueFilter("countries.garages.cars.accessories.type", "spoiler"),
			valueFilter("countries.garages.cars.tags", "electric"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F8: countries.garages.{city,postcode} AND cars.accessories.type AND cars.tires.width.
	// Same garage + same car required; the two object[] sub-arrays' positions
	// can differ — only the parent car must match.
	t.Run("F8_countries.garages.city_AND_postcode_AND_cars.accessories.type_AND_cars.tires.width", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitCars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin", "postcode": "12345"},
					map[string]any{"accessories": asArr(accessory("spoiler")), "tires": asArr(tire("225"))},
				)),
			)}, note: "same car has accessories + tires"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin", "postcode": "12345"},
					map[string]any{"accessories": asArr(accessory("spoiler"))},
					map[string]any{"tires": asArr(tire("225"))},
				)),
			)}, note: "accessories in cars[0]; tires in cars[1]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.city", "berlin"),
			valueFilter("countries.garages.postcode", "12345"),
			valueFilter("countries.garages.cars.accessories.type", "spoiler"),
			valueFilter("countries.garages.cars.tires.width", "225"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F9: garages[0].cars.{tires,accessories} AND garages[1].cars.{tires,accessories}.
	// Two compatibility groups (one per garage index); each enforces same-car
	// for tires.width + accessories.type at that garage.
	t.Run("F9_garages[0].cars.{tires,accessories}_AND_garages[1].cars.{tires,accessories}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitG1Cars := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{"tires": asArr(tire("205")), "accessories": asArr(accessory("spoiler"))}),
				garage(nil, map[string]any{"tires": asArr(tire("225")), "accessories": asArr(accessory("sunroof"))}),
			)}, note: "g[0].cars[0] + g[1].cars[0] both same-car"},
			{id: idNoMatchSplitG1Cars, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{"tires": asArr(tire("205")), "accessories": asArr(accessory("spoiler"))}),
				garage(nil, map[string]any{"tires": asArr(tire("225"))}, map[string]any{"accessories": asArr(accessory("sunroof"))}),
			)}, note: "g[1] split: tires in cars[0]; accessories in cars[1]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[0].cars.tires.width", "205"),
			valueFilter("garages[0].cars.accessories.type", "spoiler"),
			valueFilter("garages[1].cars.tires.width", "225"),
			valueFilter("garages[1].cars.accessories.type", "sunroof"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F10: garages.cars[0].{tires,accessories} AND garages.cars[1].{tires,accessories}.
	// Two compatibility groups (one per cars index); same-garage required across
	// groups via root+docID AND.
	t.Run("F10_garages.cars[0].{tires,accessories}_AND_garages.cars[1].{tires,accessories}", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitGarages := uuid(2)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil,
					map[string]any{"tires": asArr(tire("205")), "accessories": asArr(accessory("spoiler"))},
					map[string]any{"tires": asArr(tire("225")), "accessories": asArr(accessory("sunroof"))},
				),
			)}, note: "g[0] has both cars[0] and cars[1] satisfied"},
			{id: idNoMatchSplitGarages, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{"tires": asArr(tire("205")), "accessories": asArr(accessory("spoiler"))}),
				garage(nil, map[string]any{}, map[string]any{"tires": asArr(tire("225")), "accessories": asArr(accessory("sunroof"))}),
			)}, note: "cars[0] in g[0], cars[1] in g[1]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages.cars[0].tires.width", "205"),
			valueFilter("garages.cars[0].accessories.type", "spoiler"),
			valueFilter("garages.cars[1].tires.width", "225"),
			valueFilter("garages.cars[1].accessories.type", "sunroof"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F11: garages[0].cars[1].make AND garages[0].cars[1].model. Single
	// compatibility group with arr[N] at "" and "cars"; nested 1-branch SPLITs.
	t.Run("F11_garages[0].cars[1].make_AND_garages[0].cars[1].model", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchWrongCar := uuid(2)
		idNoMatchWrongGarage := uuid(3)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{}, map[string]any{"make": "honda", "model": "civic"}),
			)}, note: "g[0].cars[1] has both"},
			{id: idNoMatchWrongCar, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{"make": "honda", "model": "civic"}),
			)}, note: "make+model in cars[0] (wrong index)"},
			{id: idNoMatchWrongGarage, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{}, map[string]any{"make": "honda"}),
				garage(nil, map[string]any{}, map[string]any{"model": "civic"}),
			)}, note: "make in g[0].cars[1]; model in g[1].cars[1]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[0].cars[1].make", "honda"),
			valueFilter("garages[0].cars[1].model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F12: garages[0].cars[1].tires[2].width AND garages[0].cars[1].accessories[3].type.
	// Three pinned levels per condition.
	t.Run("F12_garages[0].cars[1].tires[2]_AND_garages[0].cars[1].accessories[3]", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchWrongTire := uuid(2)
		idNoMatchWrongCar := uuid(3)
		idNoMatchWrongGarage := uuid(4)
		// Build cars[1] with the tires/accessories at the right indices.
		matchingCar := map[string]any{
			"tires":       asArr(tire(""), tire(""), tire("205")),                                   // tires[2].width=205
			"accessories": asArr(accessory(""), accessory(""), accessory(""), accessory("spoiler")), // accessories[3].type=spoiler
		}
		docs := []docDef{
			{id: idMatch, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{}, matchingCar),
			)}, note: "g[0].cars[1].tires[2] + accessories[3]"},
			{id: idNoMatchWrongTire, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{}, map[string]any{
					"tires":       asArr(tire(""), tire("205")), // width at tires[1] (wrong)
					"accessories": asArr(accessory(""), accessory(""), accessory(""), accessory("spoiler")),
				}),
			)}, note: "tires[1] not tires[2]"},
			{id: idNoMatchWrongCar, props: map[string]any{"garages": asArr(
				garage(nil,
					map[string]any{},
					map[string]any{"accessories": asArr(accessory(""), accessory(""), accessory(""), accessory("spoiler"))},
					map[string]any{"tires": asArr(tire(""), tire(""), tire("205"))},
				),
			)}, note: "accessories in cars[1], tires in cars[2]"},
			{id: idNoMatchWrongGarage, props: map[string]any{"garages": asArr(
				garage(nil, map[string]any{}, map[string]any{"tires": asArr(tire(""), tire(""), tire("205"))}),
				garage(nil, map[string]any{}, map[string]any{"accessories": asArr(accessory(""), accessory(""), accessory(""), accessory("spoiler"))}),
			)}, note: "tires in g[0].cars[1]; accessories in g[1].cars[1]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("garages[0].cars[1].tires[2].width", "205"),
			valueFilter("garages[0].cars[1].accessories[3].type", "spoiler"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F14: countries.garages[0].city AND garages[1].postcode AND
	// garages[1].cars[2].make AND garages[1].cars[3].model. Multi-group
	// dispatch (compatibility groups always yield ≥2 groups).
	t.Run("F14_garages[0].city_AND_garages[1].postcode_AND_garages[1].cars[2].make_AND_garages[1].cars[3].model", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchCityWrongGarage := uuid(2)
		idNoMatchMakeWrongGarage := uuid(3)
		idNoMatchSplitCntr := uuid(4)
		// g[1] populated with cars at indices [2] and [3]
		g1Cars := []map[string]any{
			{},
			{},
			{"make": "honda"},
			{"model": "civic"},
		}
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(
					garage(map[string]any{"city": "berlin"}),
					garage(map[string]any{"postcode": "12345"}, g1Cars...),
				),
			)}, note: "c[0]: city in g[0]; postcode + cars[2].make + cars[3].model in g[1]"},
			{id: idNoMatchCityWrongGarage, props: map[string]any{"countries": asArr(
				country(
					garage(nil),
					garage(map[string]any{"city": "berlin", "postcode": "12345"}, g1Cars...),
				),
			)}, note: "city in g[1] (must be g[0])"},
			{id: idNoMatchMakeWrongGarage, props: map[string]any{"countries": asArr(
				country(
					garage(map[string]any{"city": "berlin"}),
					garage(map[string]any{"postcode": "12345"}, []map[string]any{{}, {}, {}, {"model": "civic"}}...),
					garage(nil, []map[string]any{{}, {}, {"make": "honda"}}...),
				),
			)}, note: "make in g[2].cars[2] (must be g[1])"},
			{id: idNoMatchSplitCntr, props: map[string]any{"countries": asArr(
				country(garage(map[string]any{"city": "berlin"})),
				country(
					garage(nil),
					garage(map[string]any{"postcode": "12345"}, g1Cars...),
				),
			)}, note: "city in c[0]; rest in c[1]"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages[0].city", "berlin"),
			valueFilter("countries.garages[1].postcode", "12345"),
			valueFilter("countries.garages[1].cars[2].make", "honda"),
			valueFilter("countries.garages[1].cars[3].model", "civic"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// F_tags_double_value: countries.garages.cars.tags = "electric" AND
	// countries.garages.cars.tags = "sport". Two filter conditions on the
	// same multi-value text[] field with different values.
	//
	// Same-element semantics at the LCA (cars): both values must be on the
	// same physical car's tags array.
	//
	// Plan: GROUP@cars { here:[tags=electric, tags=sport], subs:[] }. Has
	// duplicate here paths → groupSubtreeNeedsOuterScope returns true →
	// the wrapping outer GROUP@garages is preserved. Hierarchical
	// runIdxLoopRecursive enforces same-car semantics: outer iterates
	// _idx.garages[K_g], inner iterates _idx.garages.cars[K_c] AND
	// parent_scope, evaluating both tag values within each physical car.
	//
	//   - same car has both tags                         → match
	//   - same country, different garages, one tag each  → reject (different cars)
	//   - same garage, different cars                    → reject (different cars)
	//   - only one tag anywhere                          → reject
	t.Run("F_tags_double_value_cars.tags=electric_AND_cars.tags=sport", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitGarages := uuid(2)
		idNoMatchOnlyOne := uuid(3)
		idNoMatchSplitCars := uuid(4)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(nil, map[string]any{"tags": []any{"electric", "sport"}})),
			)}, note: "single car has both tags"},
			{id: idNoMatchSplitGarages, props: map[string]any{"countries": asArr(
				country(
					garage(nil, map[string]any{"tags": []any{"electric"}}),
					garage(nil, map[string]any{"tags": []any{"sport"}}),
				),
			)}, note: "tags split across g[0].cars[0]/g[1].cars[0] — different physical cars"},
			{id: idNoMatchOnlyOne, props: map[string]any{"countries": asArr(
				country(garage(nil, map[string]any{"tags": []any{"electric"}})),
			)}, note: "only one tag in country"},
			{id: idNoMatchSplitCars, props: map[string]any{"countries": asArr(
				country(garage(nil,
					map[string]any{"tags": []any{"electric"}},
					map[string]any{"tags": []any{"sport"}},
				)),
			)}, note: "tags split across cars[0]/cars[1] of same garage"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.cars.tags", "electric"),
			valueFilter("countries.garages.cars.tags", "sport"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// SameKDifferentParent_with_subs: regression test for the flat-AndAll path
	// in evalGroup. Filter: cars.make = "honda" AND cars.tires.width = "205".
	//
	// Plan: GROUP@cars { here:[make], subs:[GROUP@cars.tires { here:[width] }] }
	// → not canUseRawAndAll (has subs), but the subtree is "flat" (no SPLITs,
	// all here paths globally unique, no scalar-array terminals, ≤1 deeper
	// here-group), so the executor uses evalFlatRawAndAll. Raw AndAll
	// preserves leaf-precise same-element semantics via the analyzer's scalar
	// inheritance — make on cars[K] inherits cars[K].elementPositions, which
	// equals the descendant leaves (incl. tires.width's leaf) when descendants
	// exist. Different physical cars at the same K live at disjoint leaves so
	// the cross-instance false positive is naturally rejected by raw AndAll.
	//
	// Discriminating shapes:
	//   - same physical car has both → match
	//   - split garages, same K=0 → reject (different physical cars under
	//     g[0] vs g[1] have disjoint leaves)
	//   - different K within same garage → reject
	//   - only one condition → reject
	t.Run("SameKDifferentParent_with_subs_make_AND_tires", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitGaragesSameK := uuid(2)
		idNoMatchSplitCarsSameGarage := uuid(3)
		idNoMatchOnlyMake := uuid(4)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(nil, map[string]any{"make": "honda", "tires": asArr(tire("205"))})),
			)}, note: "single car has both"},
			{id: idNoMatchSplitGaragesSameK, props: map[string]any{"countries": asArr(
				country(
					garage(nil, map[string]any{"make": "honda"}),
					garage(nil, map[string]any{"tires": asArr(tire("205"))}),
				),
			)}, note: "make in g[0].cars[0]; tires in g[1].cars[0] — same K=0, different garages"},
			{id: idNoMatchSplitCarsSameGarage, props: map[string]any{"countries": asArr(
				country(garage(nil,
					map[string]any{"make": "honda"},
					map[string]any{"tires": asArr(tire("205"))},
				)),
			)}, note: "different K within same garage"},
			{id: idNoMatchOnlyMake, props: map[string]any{"countries": asArr(
				country(garage(nil, map[string]any{"make": "honda"})),
			)}, note: "only one condition"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.cars.make", "honda"),
			valueFilter("countries.garages.cars.tires.width", "205"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})

	// SameKDifferentParent_with_two_sub_arrays: regression test for the case-3
	// fix. Filter: cars.accessories.type AND cars.tires.width — two
	// sibling sub-arrays under the same cars LCA.
	//
	// Plan: GROUP@garages { here:[], subs:[GROUP@garages.cars { here:[],
	//        subs:[GROUP@accessories here:[type], GROUP@tires here:[width]] }] }
	// canUseRawAndAll false (multi-sub at GROUP@cars), flat-AndAll bails on
	// multi-sub → runIdxLoopRecursive. The wrapping GROUP@garages must be
	// kept (not collapsed away) so its _idx.garages[K_g] iteration provides
	// per-garage parentScope to the inner GROUP@cars' _idx.cars[K_c]
	// iteration, disambiguating same-K-different-parent physical cars.
	//
	// Discriminating shapes:
	//   - same physical car has both → match
	//   - split garages, same K=0 → reject (different physical cars under
	//     g[0] vs g[1] have disjoint leaves; per-garage parentScope rejects)
	t.Run("SameKDifferentParent_with_two_sub_arrays_accessories_AND_tires", func(t *testing.T) {
		idMatch := uuid(1)
		idNoMatchSplitGaragesSameK := uuid(2)
		accessory := func(typ string) map[string]any { return map[string]any{"type": typ} }
		docs := []docDef{
			{id: idMatch, props: map[string]any{"countries": asArr(
				country(garage(nil, map[string]any{
					"accessories": asArr(accessory("spoiler")),
					"tires":       asArr(tire("205")),
				})),
			)}, note: "single car has both accessories.type and tires.width"},
			{id: idNoMatchSplitGaragesSameK, props: map[string]any{"countries": asArr(
				country(
					garage(nil, map[string]any{"accessories": asArr(accessory("spoiler"))}),
					garage(nil, map[string]any{"tires": asArr(tire("205"))}),
				),
			)}, note: "spoiler in g[0].cars[0]; 205 in g[1].cars[0] — same K=0, different garages"},
		}
		parts := []*filters.LocalFilter{
			valueFilter("countries.garages.cars.accessories.type", "spoiler"),
			valueFilter("countries.garages.cars.tires.width", "205"),
		}
		runOrderings(t, docs, parts, []strfmt.UUID{idMatch})
	})
}

// TestNestedFilteringArrayIndexAccess exercises arr[N] positional filters
// end-to-end through the production write+search pipeline, mirroring the
// lower-level TestNestedFilteringArrayIndex coverage:
//
//   - basic positional: addresses[1].city = "berlin" — matches docs whose
//     second address element has the value
//   - out-of-range: addresses[5].city = "berlin" — returns empty when the
//     index is beyond the array length
//   - arr[N] in correlated AND: cars[1].make = "bmw" AND
//     cars[1].tires.width = 205 — both conditions pinned to the same car
//     index; rejects docs that don't have a cars[1]
//   - multi-root (object[] variant only): docs[K].addresses[1].city —
//     verifies arr[N] resolves correctly across multiple root elements
//
// Run under both DataTypeObject (top-level "doc") and DataTypeObjectArray
// (top-level "docs") root properties to cover both encodings.
func TestNestedFilteringArrayIndexAccess(t *testing.T) {
	const nestedClass = "ArrIdxAccess"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "addresses", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
			},
		},
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
			{Name: "docs", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	addr := func(props ...any) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i].(string)] = props[i+1]
		}
		return out
	}
	tire := func(width int) map[string]any { return map[string]any{"width": width} }
	car := func(props ...any) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i].(string)] = props[i+1]
		}
		return out
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorAnd,
			Operands: operands,
		}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ---------- DataTypeObject (top-level "doc") ----------
	t.Run("doc_object", func(t *testing.T) {
		// Sub-test 1: addresses[1].city = "berlin"
		t.Run("addresses[1].city_berlin", func(t *testing.T) {
			idMatch := uuid(1)
			idNoMatchSecondAddrWrongCity := uuid(2)
			idNoMatchSecondAddrAbsent := uuid(3)
			idNoMatchFirstIsBerlin := uuid(4)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"doc": map[string]any{"addresses": asArr(
					addr("city", "paris"), addr("city", "berlin", "postcode", "10115"),
				)}}, note: "addresses[1] = berlin"},
				{id: idNoMatchSecondAddrWrongCity, props: map[string]any{"doc": map[string]any{"addresses": asArr(
					addr("city", "paris"), addr("city", "munich"),
				)}}, note: "addresses[1] = munich"},
				{id: idNoMatchSecondAddrAbsent, props: map[string]any{"doc": map[string]any{"addresses": asArr(
					addr("city", "berlin"),
				)}}, note: "only addresses[0]; no addresses[1]"},
				{id: idNoMatchFirstIsBerlin, props: map[string]any{"doc": map[string]any{"addresses": asArr(
					addr("city", "berlin"), addr("city", "munich"),
				)}}, note: "addresses[0]=berlin but [1]=munich"},
			}
			runScenario(t, docs, textFilter("doc.addresses[1].city", "berlin"), []strfmt.UUID{idMatch})
		})

		// Sub-test 2: addresses[5].city = "berlin" (out of range — no doc has 6+ addresses)
		t.Run("addresses[5].city_out_of_range", func(t *testing.T) {
			idDoc := uuid(1)
			docs := []docDef{
				{id: idDoc, props: map[string]any{"doc": map[string]any{"addresses": asArr(
					addr("city", "berlin"), addr("city", "paris"),
				)}}, note: "only 2 addresses"},
			}
			runScenario(t, docs, textFilter("doc.addresses[5].city", "berlin"), []strfmt.UUID{})
		})

		// Sub-test 3: cars[1].make = "bmw" AND cars[1].tires.width = 205
		t.Run("cars[1]_make_AND_cars[1].tires.width", func(t *testing.T) {
			idMatch := uuid(1)
			idNoMatchSecondCarAbsent := uuid(2)
			idNoMatchWrongMake := uuid(3)
			idNoMatchSplitCars := uuid(4)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("make", "tesla"),
					car("make", "bmw", "tires", asArr(tire(205))),
				)}}, note: "cars[1] has both make=bmw and tires.width=205"},
				{id: idNoMatchSecondCarAbsent, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("make", "bmw", "tires", asArr(tire(205))),
				)}}, note: "only cars[0]; satisfies neither pinned cars[1] condition"},
				{id: idNoMatchWrongMake, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("make", "tesla"),
					car("make", "tesla", "tires", asArr(tire(205))),
				)}}, note: "cars[1] has tires.width=205 but make=tesla, not bmw"},
				{id: idNoMatchSplitCars, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("make", "bmw"),
					car("make", "tesla", "tires", asArr(tire(205))),
				)}}, note: "make=bmw in cars[0]; width=205 in cars[1] — different cars"},
			}
			filter := andFilter(
				textFilter("doc.cars[1].make", "bmw"),
				intFilter("doc.cars[1].tires.width", 205),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})

		// Sub-test 4: arr[N] against an empty intermediate array or absent
		// property — neither should match (no element exists at index N).
		t.Run("cars[0].make_empty_or_absent", func(t *testing.T) {
			idEmptyArr := uuid(1)   // cars: []
			idAbsentCars := uuid(2) // cars key omitted entirely
			idMatch := uuid(3)
			docs := []docDef{
				{id: idEmptyArr, props: map[string]any{"doc": map[string]any{"cars": []any{}}}, note: "cars=[]"},
				{id: idAbsentCars, props: map[string]any{"doc": map[string]any{}}, note: "no cars key"},
				{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "bmw"))}}, note: "cars[0]=bmw"},
			}
			runScenario(t, docs, textFilter("doc.cars[0].make", "bmw"), []strfmt.UUID{idMatch})
		})
	})

	// ---------- DataTypeObjectArray (top-level "docs") ----------
	t.Run("docs_array", func(t *testing.T) {
		// Sub-test 1: docs.addresses[1].city = "berlin" (same shape as object variant,
		// but the root is an object[] so we also test arr[N] across multiple root elements)
		t.Run("docs.addresses[1].city_berlin", func(t *testing.T) {
			idMatchSingleRoot := uuid(1)
			idMatchSecondRoot := uuid(2)
			idNoMatchAbsent := uuid(3)
			docs := []docDef{
				{id: idMatchSingleRoot, props: map[string]any{"docs": asArr(
					map[string]any{"addresses": asArr(addr("city", "paris"), addr("city", "berlin"))},
				)}, note: "single root; addresses[1] = berlin"},
				{id: idMatchSecondRoot, props: map[string]any{"docs": asArr(
					map[string]any{"addresses": asArr(addr("city", "munich"))},
					map[string]any{"addresses": asArr(addr("city", "paris"), addr("city", "berlin"))},
				)}, note: "second root has matching addresses[1]"},
				{id: idNoMatchAbsent, props: map[string]any{"docs": asArr(
					map[string]any{"addresses": asArr(addr("city", "berlin"))},
				)}, note: "no addresses[1] anywhere"},
			}
			runScenario(t, docs, textFilter("docs.addresses[1].city", "berlin"), []strfmt.UUID{idMatchSingleRoot, idMatchSecondRoot})
		})

		// Sub-test 2: out-of-range arr[N]
		t.Run("docs.addresses[5].city_out_of_range", func(t *testing.T) {
			idDoc := uuid(1)
			docs := []docDef{
				{id: idDoc, props: map[string]any{"docs": asArr(
					map[string]any{"addresses": asArr(addr("city", "berlin"), addr("city", "paris"))},
				)}, note: "only 2 addresses"},
			}
			runScenario(t, docs, textFilter("docs.addresses[5].city", "berlin"), []strfmt.UUID{})
		})

		// Sub-test 3: arr[N] in correlated AND, also exercising root_idx disambiguation
		t.Run("docs.cars[1]_make_AND_cars[1].tires.width", func(t *testing.T) {
			idMatch := uuid(1)
			idMatchInSecondRoot := uuid(2)
			idNoMatchSplitRoots := uuid(3)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(
						car("make", "tesla"),
						car("make", "bmw", "tires", asArr(tire(205))),
					)},
				)}, note: "cars[1] in single root has both"},
				{id: idMatchInSecondRoot, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "tesla"))},
					map[string]any{"cars": asArr(
						car("make", "tesla"),
						car("make", "bmw", "tires", asArr(tire(205))),
					)},
				)}, note: "second root's cars[1] has both"},
				{id: idNoMatchSplitRoots, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(
						car("make", "tesla"),
						car("make", "bmw"),
					)},
					map[string]any{"cars": asArr(
						car("make", "tesla"),
						car("make", "tesla", "tires", asArr(tire(205))),
					)},
				)}, note: "make=bmw in root[0].cars[1]; width=205 in root[1].cars[1] — different roots"},
			}
			filter := andFilter(
				textFilter("docs.cars[1].make", "bmw"),
				intFilter("docs.cars[1].tires.width", 205),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchInSecondRoot})
		})
	})
}

// TestNestedFilteringArrayIndexLevels exercises positional arr[N] access at
// different nesting levels (root, mid-1, mid-2) and the dispatch behavior of
// AND / OR over conditions pinned to *different* arr[N] indices at the same
// LCA. Same-element AND on the same index is covered by
// TestNestedFilteringArrayIndexAccess; this test focuses on
//   - levels: root[N], cars[N], cars.tires[N]
//   - combinations: AND with conflicting indices (partitioned via
//     groupChildrenByArrayIndicesKey) and OR (per-clause union).
func TestNestedFilteringArrayIndexLevels(t *testing.T) {
	const nestedClass = "ArrIdxLevels"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
			{Name: "docs", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tire := func(width int) map[string]any { return map[string]any{"width": width} }
	car := func(props ...any) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i].(string)] = props[i+1]
		}
		return out
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}
	orFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorOr, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ---------- DataTypeObject (top-level "doc") ----------
	t.Run("doc_object", func(t *testing.T) {
		// Mid-1 positional: doc.cars[N].make
		t.Run("mid1_cars[N].make_positional", func(t *testing.T) {
			id1 := uuid(1) // cars=[tesla,bmw]
			id2 := uuid(2) // cars=[bmw]
			id3 := uuid(3) // cars=[bmw,tesla]
			docs := []docDef{
				{id: id1, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "tesla"), car("make", "bmw"))}}, note: "[tesla,bmw]"},
				{id: id2, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "bmw"))}}, note: "[bmw]"},
				{id: id3, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "bmw"), car("make", "tesla"))}}, note: "[bmw,tesla]"},
			}
			t.Run("cars[0].make=bmw", func(t *testing.T) {
				runScenario(t, docs, textFilter("doc.cars[0].make", "bmw"), []strfmt.UUID{id2, id3})
			})
			t.Run("cars[1].make=bmw", func(t *testing.T) {
				runScenario(t, docs, textFilter("doc.cars[1].make", "bmw"), []strfmt.UUID{id1})
			})
		})

		// Mid-2 positional: doc.cars.tires[N].width (cars unindexed; only tires pinned)
		t.Run("mid2_tires[N].width_positional", func(t *testing.T) {
			id1 := uuid(1) // tires=[205,305]
			id2 := uuid(2) // tires=[205]
			id3 := uuid(3) // tires=[305,205]
			docs := []docDef{
				{id: id1, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire(205), tire(305))))}}, note: "tires=[205,305]"},
				{id: id2, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire(205))))}}, note: "tires=[205]"},
				{id: id3, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire(305), tire(205))))}}, note: "tires=[305,205]"},
			}
			t.Run("tires[0].width=205", func(t *testing.T) {
				runScenario(t, docs, intFilter("doc.cars.tires[0].width", 205), []strfmt.UUID{id1, id2})
			})
			t.Run("tires[1].width=205", func(t *testing.T) {
				runScenario(t, docs, intFilter("doc.cars.tires[1].width", 205), []strfmt.UUID{id3})
			})
		})

		// AND with conflicting cars[N] indices: groupChildrenByArrayIndicesKey
		// partitions the two clauses into independent groups (each pinned to a
		// different K), then ANDs at docID level.
		t.Run("AND_cars[0]=tesla_AND_cars[1]=bmw", func(t *testing.T) {
			idMatch := uuid(1)            // cars=[tesla,bmw]
			idNoMatchSwapped := uuid(2)   // cars=[bmw,tesla]
			idNoMatchOnlyFirst := uuid(3) // cars=[tesla]
			idNoMatchSecondCarOnly := uuid(4)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "tesla"), car("make", "bmw"))}}, note: "[tesla,bmw]"},
				{id: idNoMatchSwapped, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "bmw"), car("make", "tesla"))}}, note: "[bmw,tesla] swapped"},
				{id: idNoMatchOnlyFirst, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "tesla"))}}, note: "[tesla] only"},
				{id: idNoMatchSecondCarOnly, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "volvo"), car("make", "bmw"))}}, note: "[volvo,bmw] cars[0]≠tesla"},
			}
			filter := andFilter(
				textFilter("doc.cars[0].make", "tesla"),
				textFilter("doc.cars[1].make", "bmw"),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})

		// OR with conflicting cars[N] indices: each clause resolved independently
		// and unioned at docID level (no partitioning needed).
		t.Run("OR_cars[0]=tesla_OR_cars[1]=bmw", func(t *testing.T) {
			id1 := uuid(1) // [tesla,bmw] — matches via either side
			id2 := uuid(2) // [tesla] — matches via cars[0]
			id3 := uuid(3) // [volvo,bmw] — matches via cars[1]
			id4 := uuid(4) // [volvo] — no match
			docs := []docDef{
				{id: id1, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "tesla"), car("make", "bmw"))}}, note: "[tesla,bmw]"},
				{id: id2, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "tesla"))}}, note: "[tesla]"},
				{id: id3, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "volvo"), car("make", "bmw"))}}, note: "[volvo,bmw]"},
				{id: id4, props: map[string]any{"doc": map[string]any{"cars": asArr(car("make", "volvo"))}}, note: "[volvo]"},
			}
			filter := orFilter(
				textFilter("doc.cars[0].make", "tesla"),
				textFilter("doc.cars[1].make", "bmw"),
			)
			runScenario(t, docs, filter, []strfmt.UUID{id1, id2, id3})
		})

		// Multi-level pin in a single clause: doc.cars[0].tires[1].width=205.
		// Both `cars` and `tires` indices are pinned in one path → stacked
		// IdxKey constraints on the same chain. Different from 1a's
		// cars[1].tires.width (only cars pinned) and 1b's tires[N].width
		// (only tires pinned).
		t.Run("multi_level_pin_cars[0].tires[1].width", func(t *testing.T) {
			idMatch := uuid(1)              // cars[0].tires=[305,205]
			idNoMatchTires0Is205 := uuid(2) // cars[0].tires=[205,305] — match in tires[0] not tires[1]
			idNoMatchSingleTire := uuid(3)  // cars[0].tires=[205] — no tires[1]
			idNoMatchSplitCars := uuid(4)   // cars[0].tires=[305], cars[1].tires=[305,205]
			docs := []docDef{
				{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire(305), tire(205))))}}, note: "cars[0].tires[1]=205"},
				{id: idNoMatchTires0Is205, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire(205), tire(305))))}}, note: "cars[0].tires[0]=205"},
				{id: idNoMatchSingleTire, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire(205))))}}, note: "cars[0] only has tires[0]"},
				{id: idNoMatchSplitCars, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("tires", asArr(tire(305))),
					car("tires", asArr(tire(305), tire(205))),
				)}}, note: "match in cars[1].tires[1] not cars[0].tires[1]"},
			}
			runScenario(t, docs, intFilter("doc.cars[0].tires[1].width", 205), []strfmt.UUID{idMatch})
		})

		// Three-clause AND with overlapping indices:
		//   cars[0].make=tesla AND cars[1].make=bmw AND cars[1].tires.width=205
		// Partitioner groups the two cars[1] clauses together (correlated AND
		// over the same physical car at index 1) and ANDs that with the
		// independent cars[0] group at docID level.
		t.Run("AND_three_clause_partitioned_cars[0]_AND_cars[1]_pair", func(t *testing.T) {
			idMatch := uuid(1) // [tesla, bmw+tires205]
			idNoMatchCars1MissingWidth := uuid(2)
			idNoMatchCars0Wrong := uuid(3)
			idNoMatchSplitWidth := uuid(4) // tires.width=205 in cars[0] not cars[1]
			docs := []docDef{
				{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("make", "tesla"),
					car("make", "bmw", "tires", asArr(tire(205))),
				)}}, note: "cars[0]=tesla, cars[1]=bmw+tires205"},
				{id: idNoMatchCars1MissingWidth, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("make", "tesla"),
					car("make", "bmw"),
				)}}, note: "cars[1] missing tires"},
				{id: idNoMatchCars0Wrong, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("make", "volvo"),
					car("make", "bmw", "tires", asArr(tire(205))),
				)}}, note: "cars[0]=volvo not tesla"},
				{id: idNoMatchSplitWidth, props: map[string]any{"doc": map[string]any{"cars": asArr(
					car("make", "tesla", "tires", asArr(tire(205))),
					car("make", "bmw"),
				)}}, note: "tires.width=205 in cars[0] not cars[1]"},
			}
			filter := andFilter(
				textFilter("doc.cars[0].make", "tesla"),
				textFilter("doc.cars[1].make", "bmw"),
				intFilter("doc.cars[1].tires.width", 205),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})
	})

	// ---------- DataTypeObjectArray (top-level "docs") ----------
	t.Run("docs_array", func(t *testing.T) {
		// Root positional: docs[N].cars.make — exercises root_idx encoding.
		t.Run("root_docs[N].cars.make_positional", func(t *testing.T) {
			id1 := uuid(1) // docs=[{tesla},{bmw}]
			id2 := uuid(2) // docs=[{bmw}]
			id3 := uuid(3) // docs=[{bmw},{tesla}]
			docs := []docDef{
				{id: id1, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "tesla"))},
					map[string]any{"cars": asArr(car("make", "bmw"))},
				)}, note: "[{tesla},{bmw}]"},
				{id: id2, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "bmw"))},
				)}, note: "[{bmw}]"},
				{id: id3, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "bmw"))},
					map[string]any{"cars": asArr(car("make", "tesla"))},
				)}, note: "[{bmw},{tesla}]"},
			}
			t.Run("docs[0].cars.make=bmw", func(t *testing.T) {
				runScenario(t, docs, textFilter("docs[0].cars.make", "bmw"), []strfmt.UUID{id2, id3})
			})
			t.Run("docs[1].cars.make=bmw", func(t *testing.T) {
				runScenario(t, docs, textFilter("docs[1].cars.make", "bmw"), []strfmt.UUID{id1})
			})
		})

		// AND with conflicting docs[N] indices (root level partitioning).
		t.Run("AND_docs[0]=tesla_AND_docs[1]=bmw", func(t *testing.T) {
			idMatch := uuid(1)
			idNoMatchSwapped := uuid(2)
			idNoMatchOnlyFirst := uuid(3)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "tesla"))},
					map[string]any{"cars": asArr(car("make", "bmw"))},
				)}, note: "[{tesla},{bmw}]"},
				{id: idNoMatchSwapped, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "bmw"))},
					map[string]any{"cars": asArr(car("make", "tesla"))},
				)}, note: "[{bmw},{tesla}] swapped"},
				{id: idNoMatchOnlyFirst, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "tesla"))},
				)}, note: "[{tesla}] only"},
			}
			filter := andFilter(
				textFilter("docs[0].cars.make", "tesla"),
				textFilter("docs[1].cars.make", "bmw"),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})

		// OR with conflicting docs[N] indices: per-clause union.
		t.Run("OR_docs[0]=tesla_OR_docs[1]=bmw", func(t *testing.T) {
			id1 := uuid(1) // [{tesla},{bmw}] — both
			id2 := uuid(2) // [{tesla}] — docs[0]
			id3 := uuid(3) // [{volvo},{bmw}] — docs[1]
			id4 := uuid(4) // [{volvo}] — no match
			docs := []docDef{
				{id: id1, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "tesla"))},
					map[string]any{"cars": asArr(car("make", "bmw"))},
				)}, note: "[{tesla},{bmw}]"},
				{id: id2, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "tesla"))},
				)}, note: "[{tesla}]"},
				{id: id3, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "volvo"))},
					map[string]any{"cars": asArr(car("make", "bmw"))},
				)}, note: "[{volvo},{bmw}]"},
				{id: id4, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "volvo"))},
				)}, note: "[{volvo}]"},
			}
			filter := orFilter(
				textFilter("docs[0].cars.make", "tesla"),
				textFilter("docs[1].cars.make", "bmw"),
			)
			runScenario(t, docs, filter, []strfmt.UUID{id1, id2, id3})
		})

		// Combined root + intermediate pin in a single clause:
		// docs[1].cars[0].make=bmw — both root_idx and intermediate IdxKey
		// applied to the same path simultaneously.
		t.Run("multi_level_pin_docs[1].cars[0].make", func(t *testing.T) {
			idMatch := uuid(1)             // docs=[{tesla},{bmw}] → docs[1].cars[0]=bmw
			idNoMatchDocs1Tesla := uuid(2) // docs=[{bmw},{tesla}] → docs[1].cars[0]=tesla
			idNoMatchSingleRoot := uuid(3) // docs=[{bmw}] → no docs[1]
			idNoMatchCarsAt1 := uuid(4)    // docs=[{tesla},{tesla,bmw}] → docs[1].cars[0]=tesla, cars[1]=bmw
			docs := []docDef{
				{id: idMatch, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "tesla"))},
					map[string]any{"cars": asArr(car("make", "bmw"))},
				)}, note: "docs[1].cars[0]=bmw"},
				{id: idNoMatchDocs1Tesla, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "bmw"))},
					map[string]any{"cars": asArr(car("make", "tesla"))},
				)}, note: "docs[1].cars[0]=tesla"},
				{id: idNoMatchSingleRoot, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "bmw"))},
				)}, note: "single root, no docs[1]"},
				{id: idNoMatchCarsAt1, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(car("make", "tesla"))},
					map[string]any{"cars": asArr(car("make", "tesla"), car("make", "bmw"))},
				)}, note: "docs[1].cars[1]=bmw but cars[0]=tesla"},
			}
			runScenario(t, docs, textFilter("docs[1].cars[0].make", "bmw"), []strfmt.UUID{idMatch})
		})
	})
}

// TestNestedFilteringMixedArrayIndexConstraints exercises correlated AND when
// clauses at the same LCA have *different* arr[N] constraint sets — one
// unconstrained, the other pinned. Compatibility grouping treats {} and {N}
// as compatible (same compatibility key) → both clauses share the same
// resolveNestedCorrelatedGroup call, and same-element semantics force the
// unconstrained side to match the same physical element pinned by the [N]
// side. This is the cross-cutting case between Levels (1b) and the basic
// arr[N] same-K AND in Access (1a).
func TestNestedFilteringMixedArrayIndexConstraints(t *testing.T) {
	const nestedClass = "ArrIdxMixed"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "addresses", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
			},
		},
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{Name: "color", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
			{Name: "docs", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	obj := func(props ...any) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i].(string)] = props[i+1]
		}
		return out
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ---------- DataTypeObject (top-level "doc") ----------
	t.Run("doc_object", func(t *testing.T) {
		// Intermediate-level mix: doc.cars.color=red (unconstrained) AND
		// doc.cars[1].make=bmw (pinned to cars[1]). Same-car semantics: the
		// unconstrained color must be satisfied at the *same* car element as the
		// pinned make → only docs whose cars[1] has BOTH red and bmw match.
		t.Run("intermediate_color_AND_cars[1].make", func(t *testing.T) {
			idMatch := uuid(1)         // cars=[{green},{red,bmw}] — cars[1] has both
			idNoMatchSplit := uuid(2)  // cars=[{red},{bmw}] — different cars
			idNoMatchNoRed := uuid(3)  // cars=[{green},{green,bmw}] — no red anywhere
			idNoMatchSingle := uuid(4) // cars=[{red,bmw}] — only cars[0]; no cars[1]
			docs := []docDef{
				{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(
					obj("color", "green"),
					obj("color", "red", "make", "bmw"),
				)}}, note: "cars[1] has color=red AND make=bmw"},
				{id: idNoMatchSplit, props: map[string]any{"doc": map[string]any{"cars": asArr(
					obj("color", "red"),
					obj("make", "bmw"),
				)}}, note: "red in cars[0]; bmw in cars[1]"},
				{id: idNoMatchNoRed, props: map[string]any{"doc": map[string]any{"cars": asArr(
					obj("color", "green"),
					obj("color", "green", "make", "bmw"),
				)}}, note: "no red anywhere"},
				{id: idNoMatchSingle, props: map[string]any{"doc": map[string]any{"cars": asArr(
					obj("color", "red", "make", "bmw"),
				)}}, note: "only cars[0]; no cars[1] to satisfy pinned make"},
			}
			filter := andFilter(
				textFilter("doc.cars.color", "red"),
				textFilter("doc.cars[1].make", "bmw"),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})
	})

	// ---------- DataTypeObjectArray (top-level "docs") ----------
	t.Run("docs_array", func(t *testing.T) {
		// Root-level mix: docs.addresses.city=berlin (unconstrained) AND
		// docs[1].cars.make=bmw (pinned to root[1]). Same-root-element semantics:
		// the unconstrained city must be satisfied in the *same* root element as
		// the pinned make → only docs whose root[1] has BOTH berlin and bmw match.
		t.Run("root_addresses.city_AND_docs[1].cars.make", func(t *testing.T) {
			idMatch := uuid(1)        // root[1] has berlin AND bmw
			idNoMatchSplit := uuid(2) // berlin in root[0]; bmw in root[1] — different roots
			idNoMatchNoBerlin := uuid(3)
			idNoMatchSingleRoot := uuid(4) // only root[0]; no root[1]
			docs := []docDef{
				{id: idMatch, props: map[string]any{"docs": asArr(
					obj("addresses", asArr(obj("city", "munich")), "cars", asArr(obj("make", "tesla"))),
					obj("addresses", asArr(obj("city", "berlin")), "cars", asArr(obj("make", "bmw"))),
				)}, note: "root[1] has berlin and bmw"},
				{id: idNoMatchSplit, props: map[string]any{"docs": asArr(
					obj("addresses", asArr(obj("city", "berlin")), "cars", asArr(obj("make", "tesla"))),
					obj("addresses", asArr(obj("city", "munich")), "cars", asArr(obj("make", "bmw"))),
				)}, note: "berlin in root[0]; bmw in root[1]"},
				{id: idNoMatchNoBerlin, props: map[string]any{"docs": asArr(
					obj("addresses", asArr(obj("city", "munich")), "cars", asArr(obj("make", "bmw"))),
					obj("addresses", asArr(obj("city", "paris")), "cars", asArr(obj("make", "bmw"))),
				)}, note: "no berlin anywhere"},
				{id: idNoMatchSingleRoot, props: map[string]any{"docs": asArr(
					obj("addresses", asArr(obj("city", "berlin")), "cars", asArr(obj("make", "bmw"))),
				)}, note: "only root[0]; no root[1] to satisfy pinned make"},
			}
			filter := andFilter(
				textFilter("docs.addresses.city", "berlin"),
				textFilter("docs[1].cars.make", "bmw"),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})

		// Intermediate-level mix: same as doc_object intermediate test but with
		// object[] root. Exercises the same compatibility-grouping logic when the
		// LCA is at cars and root is object[].
		t.Run("intermediate_color_AND_docs.cars[1].make", func(t *testing.T) {
			idMatch := uuid(1)
			idNoMatchSplitCars := uuid(2)
			idNoMatchSplitRoots := uuid(3)
			docs := []docDef{
				{id: idMatch, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(obj("color", "green"), obj("color", "red", "make", "bmw"))},
				)}, note: "single root, cars[1] has both"},
				{id: idNoMatchSplitCars, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(obj("color", "red"), obj("make", "bmw"))},
				)}, note: "single root, red in cars[0], bmw in cars[1]"},
				{id: idNoMatchSplitRoots, props: map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(obj("color", "red"))},
					map[string]any{"cars": asArr(obj("color", "green"), obj("make", "bmw"))},
				)}, note: "red in root[0].cars[0]; bmw in root[1].cars[1] — different cars"},
			}
			filter := andFilter(
				textFilter("docs.cars.color", "red"),
				textFilter("docs.cars[1].make", "bmw"),
			)
			runScenario(t, docs, filter, []strfmt.UUID{idMatch})
		})
	})
}

// TestNestedFilteringIsNullWithArrayIndex covers IsNull / IsNotNull combined
// with arr[N] in *standalone* form (no AND wrapper). Three shapes:
//   - addresses[N] IsNull/IsNotNull  → existence of a Nth element in the array
//   - addresses[N].leaf IsNull/IsNotNull → presence of leaf within the Nth element
//
// IsNotNull (IsNull=false) returns docs where the Nth element exists AND the
// queried path is present at that element. IsNull=true is the complement
// (denylist) under current universal semantics.
//
// The existing TestNestedFilteringIsNullStandalone covers IsNull at all
// nesting levels but never with arr[N]; the existing
// TestNestedFilteringIsNullWithArrNInCorrelatedAnd always combines IsNull with
// a value clause. This test fills the standalone+arr[N] gap.
func TestNestedFilteringIsNullWithArrayIndex(t *testing.T) {
	const nestedClass = "IsNullArrIdx"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "addresses", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
			{Name: "docs", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	addr := func(props ...any) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i].(string)] = props[i+1]
		}
		return out
	}

	isNullFilter := func(path string, isNull bool) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: isNull},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ---------- DataTypeObject (top-level "doc") ----------
	t.Run("doc_object", func(t *testing.T) {
		// docs share across the four sub-tests:
		//   doc1: addresses=[{city:berlin},{city:paris}] — both elements have city
		//   doc2: addresses=[{city:berlin},{}]          — [0] has city, [1] is empty
		//   doc3: addresses=[{city:berlin}]              — only [0]; no [1]
		//   doc4: addresses=[]                           — empty array
		//   doc5: doc has no addresses key               — absent entirely
		idBoth := uuid(1)
		idSecondNoCity := uuid(2)
		idOnlyOne := uuid(3)
		idEmpty := uuid(4)
		idAbsent := uuid(5)
		docs := []docDef{
			{id: idBoth, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "berlin"), addr("city", "paris"))}}, note: "[1] has city"},
			{id: idSecondNoCity, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "berlin"), addr())}}, note: "[1] is empty (no city)"},
			{id: idOnlyOne, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "berlin"))}}, note: "only [0]; no [1]"},
			{id: idEmpty, props: map[string]any{"doc": map[string]any{"addresses": []any{}}}, note: "empty addresses"},
			{id: idAbsent, props: map[string]any{"doc": map[string]any{}}, note: "no addresses key"},
		}

		t.Run("addresses[1]_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.addresses[1]", false), []strfmt.UUID{idBoth, idSecondNoCity})
		})
		t.Run("addresses[1]_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.addresses[1]", true), []strfmt.UUID{idOnlyOne, idEmpty, idAbsent})
		})
		t.Run("addresses[1].city_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.addresses[1].city", false), []strfmt.UUID{idBoth})
		})
		t.Run("addresses[1].city_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.addresses[1].city", true), []strfmt.UUID{idSecondNoCity, idOnlyOne, idEmpty, idAbsent})
		})
	})

	// ---------- DataTypeObjectArray (top-level "docs") ----------
	t.Run("docs_array", func(t *testing.T) {
		// Tests root-level arr[N] applied to IsNull (docs[N]).
		idBoth := uuid(1)         // docs=[{addr=[{berlin}]},{addr=[{paris}]}]
		idSecondNoAddr := uuid(2) // docs=[{addr=[{berlin}]},{}]
		idOnlyOne := uuid(3)      // docs=[{addr=[{berlin}]}]
		idEmpty := uuid(4)        // docs=[]
		idAbsent := uuid(5)       // no docs key
		docs := []docDef{
			{id: idBoth, props: map[string]any{"docs": asArr(
				map[string]any{"addresses": asArr(addr("city", "berlin"))},
				map[string]any{"addresses": asArr(addr("city", "paris"))},
			)}, note: "docs[1] has addresses"},
			{id: idSecondNoAddr, props: map[string]any{"docs": asArr(
				map[string]any{"addresses": asArr(addr("city", "berlin"))},
				map[string]any{},
			)}, note: "docs[1] empty (no addresses)"},
			{id: idOnlyOne, props: map[string]any{"docs": asArr(
				map[string]any{"addresses": asArr(addr("city", "berlin"))},
			)}, note: "only docs[0]"},
			{id: idEmpty, props: map[string]any{"docs": []any{}}, note: "empty docs"},
			{id: idAbsent, props: map[string]any{}, note: "no docs key"},
		}

		t.Run("docs[1]_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("docs[1]", false), []strfmt.UUID{idBoth, idSecondNoAddr})
		})
		t.Run("docs[1]_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("docs[1]", true), []strfmt.UUID{idOnlyOne, idEmpty, idAbsent})
		})
		t.Run("docs[1].addresses.city_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("docs[1].addresses.city", false), []strfmt.UUID{idBoth})
		})
		t.Run("docs[1].addresses.city_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("docs[1].addresses.city", true), []strfmt.UUID{idSecondNoAddr, idOnlyOne, idEmpty, idAbsent})
		})
	})
}

// TestNestedFilteringIsNullArrayIndexFollowups fills four IsNull-cluster
// coverage gaps surfaced by audit:
//
//  1. Multi-level arr[N] + IsNull standalone — `cars[1].tires[0] IsNotNull`,
//     `cars[1].tires[0].width IsNotNull`. Existing IsNull+arr[N] tests use a
//     single arr[N] segment; multi-level pin with IsNull is its own dispatch
//     path.
//  2. Multi-level nested object[] standalone IsNull (no arr[N]) —
//     `cars.tires IsNotNull`, `cars.tires.width IsNotNull`. The existing
//     standalone test uses single-level object[] (addresses); deeper nested
//     paths exercise different lcaPath / restriction logic.
//  3. text[] (scalar array) IsNull combined with arr[N] —
//     `addresses[1].tags IsNotNull`. text[] IsNull is covered standalone in
//     IsNullStandalone, but never with an arr[N] restriction on its parent.
//  4. Mixed constrained/unconstrained IsNull in correlated AND —
//     `addresses.tags IsNotNull AND addresses[1].city = berlin`. The IsNotNull
//     side has empty constraints, the value side pins addresses[1];
//     compatibility grouping puts both in the same group, forcing the
//     IsNotNull to be satisfied at the same address as the value clause.
func TestNestedFilteringIsNullArrayIndexFollowups(t *testing.T) {
	const nestedClass = "IsNullArrFollowups"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "addresses", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
			},
		},
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tire := func(width ...int) map[string]any {
		if len(width) == 0 {
			return map[string]any{}
		}
		return map[string]any{"width": width[0]}
	}
	car := func(props ...any) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i].(string)] = props[i+1]
		}
		return out
	}
	addr := func(props ...any) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i].(string)] = props[i+1]
		}
		return out
	}
	tagsAny := func(tags ...string) []any {
		out := make([]any, len(tags))
		for i, t := range tags {
			out[i] = t
		}
		return out
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	isNullFilter := func(path string, isNull bool) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: isNull},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ----- 1. Multi-level arr[N] + IsNull standalone -----
	t.Run("multi_level_arrN_cars[1].tires[0]_IsNull", func(t *testing.T) {
		idCar1HasTireWithWidth := uuid(1) // cars=[{},{tires:[{305}]}]
		idCar1HasEmptyTire := uuid(2)     // cars=[{},{tires:[{}]}]      (tire exists, no width)
		idCar1NoTires := uuid(3)          // cars=[{},{}]
		idNoCar1 := uuid(4)               // cars=[{tires:[{305}]}]      (only cars[0])
		idAbsent := uuid(5)               // no cars

		docs := []docDef{
			{id: idCar1HasTireWithWidth, props: map[string]any{"doc": map[string]any{"cars": asArr(car(), car("tires", asArr(tire(305))))}}, note: "cars[1].tires[0]={width:305}"},
			{id: idCar1HasEmptyTire, props: map[string]any{"doc": map[string]any{"cars": asArr(car(), car("tires", asArr(tire())))}}, note: "cars[1].tires[0]={} (no width)"},
			{id: idCar1NoTires, props: map[string]any{"doc": map[string]any{"cars": asArr(car(), car())}}, note: "cars[1] has no tires"},
			{id: idNoCar1, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire(305))))}}, note: "only cars[0]; no cars[1]"},
			{id: idAbsent, props: map[string]any{"doc": map[string]any{}}, note: "no cars at all"},
		}

		t.Run("cars[1].tires[0]_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.cars[1].tires[0]", false),
				[]strfmt.UUID{idCar1HasTireWithWidth, idCar1HasEmptyTire})
		})
		t.Run("cars[1].tires[0]_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.cars[1].tires[0]", true),
				[]strfmt.UUID{idCar1NoTires, idNoCar1, idAbsent})
		})
		t.Run("cars[1].tires[0].width_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.cars[1].tires[0].width", false),
				[]strfmt.UUID{idCar1HasTireWithWidth})
		})
		// TODO aliszka:nested_filtering: this asserts CURRENT universal IsNull
		// semantics on the deep path `cars[1].tires[0].width`. The planned
		// existential IsNull rewrite would flip vacuous matches and add
		// cross-element absent matches.
		t.Run("regression_cars[1].tires[0].width_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.cars[1].tires[0].width", true),
				[]strfmt.UUID{idCar1HasEmptyTire, idCar1NoTires, idNoCar1, idAbsent})
		})
	})

	// ----- 2. Multi-level nested object[] standalone IsNull (no arr[N]) -----
	t.Run("multi_level_nested_objArr_cars.tires_IsNull", func(t *testing.T) {
		idAllTires := uuid(1)       // cars=[{tires:[{305}]}]
		idSecondCarTires := uuid(2) // cars=[{},{tires:[{305}]}]
		idTiresNoWidth := uuid(3)   // cars=[{tires:[{}]}]              (tires exist, no width)
		idCarsNoTires := uuid(4)    // cars=[{}]
		idAbsent := uuid(5)         // no cars
		docs := []docDef{
			{id: idAllTires, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire(305))))}}, note: "cars[0].tires[0]={width:305}"},
			{id: idSecondCarTires, props: map[string]any{"doc": map[string]any{"cars": asArr(car(), car("tires", asArr(tire(305))))}}, note: "tires only in cars[1]"},
			{id: idTiresNoWidth, props: map[string]any{"doc": map[string]any{"cars": asArr(car("tires", asArr(tire())))}}, note: "tires exist but no width"},
			{id: idCarsNoTires, props: map[string]any{"doc": map[string]any{"cars": asArr(car())}}, note: "cars[0] has no tires"},
			{id: idAbsent, props: map[string]any{"doc": map[string]any{}}, note: "no cars"},
		}

		// Universal: tires/width IsNull matches docs where the property is absent
		// everywhere within the addressed scope.
		t.Run("cars.tires_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.cars.tires", false),
				[]strfmt.UUID{idAllTires, idSecondCarTires, idTiresNoWidth})
		})
		t.Run("cars.tires_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.cars.tires", true),
				[]strfmt.UUID{idCarsNoTires, idAbsent})
		})
		t.Run("cars.tires.width_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.cars.tires.width", false),
				[]strfmt.UUID{idAllTires, idSecondCarTires})
		})
		// TODO aliszka:nested_filtering: locks in CURRENT universal IsNull
		// on the deep path `cars.tires.width`. Flips when the planned
		// existential IsNull rewrite lands: docs with any tire having a
		// width would no longer match — only docs where at least one tire
		// is missing a width.
		t.Run("regression_cars.tires.width_IsNull_universal", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.cars.tires.width", true),
				[]strfmt.UUID{idTiresNoWidth, idCarsNoTires, idAbsent})
		})
	})

	// ----- 3. text[] IsNull combined with arr[N] standalone -----
	t.Run("text_array_with_arrN_addresses[1].tags_IsNull", func(t *testing.T) {
		idBothHaveTags := uuid(1) // addresses=[{tags:[a]},{tags:[b]}]
		idSecondNoTags := uuid(2) // addresses=[{tags:[a]},{}]
		idOnlyFirst := uuid(3)    // addresses=[{tags:[a]}]
		idAbsent := uuid(4)       // no addresses
		docs := []docDef{
			{id: idBothHaveTags, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("tags", tagsAny("a")), addr("tags", tagsAny("b")))}}, note: "[1] has tags=[b]"},
			{id: idSecondNoTags, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("tags", tagsAny("a")), addr())}}, note: "[1] is empty (no tags)"},
			{id: idOnlyFirst, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("tags", tagsAny("a")))}}, note: "only [0]; no [1]"},
			{id: idAbsent, props: map[string]any{"doc": map[string]any{}}, note: "no addresses"},
		}

		t.Run("addresses[1].tags_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.addresses[1].tags", false), []strfmt.UUID{idBothHaveTags})
		})
		t.Run("addresses[1].tags_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.addresses[1].tags", true),
				[]strfmt.UUID{idSecondNoTags, idOnlyFirst, idAbsent})
		})
	})

	// ----- 5. IsNull on scalar-array positional element (text[] arr[N]) -----
	// `addresses.tags[2] IsNotNull` — does any address have a third tag?
	// IsNull is restricted to the IdxKey on the scalar-array index, exercising
	// the rarely-used path where the IsNull's arrayIndices target the scalar
	// array itself rather than its parent. Different from `addresses[1].tags`
	// (parent pinned, child IsNull) and `addresses.tags` (whole array IsNull).
	t.Run("scalar_array_positional_addresses.tags[2]_IsNull", func(t *testing.T) {
		idHasThirdTag := uuid(1)      // addresses=[{tags:[a,b,c]}]
		idHasThirdInSecond := uuid(2) // addresses=[{tags:[a]},{tags:[a,b,c]}]
		idTwoTagsOnly := uuid(3)      // addresses=[{tags:[a,b]}]
		idEmptyTags := uuid(4)        // addresses=[{tags:[]}]
		idNoTags := uuid(5)           // addresses=[{}]
		idAbsent := uuid(6)           // no addresses
		docs := []docDef{
			{id: idHasThirdTag, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("tags", tagsAny("a", "b", "c")))}}, note: "addresses[0].tags[2]=c"},
			{id: idHasThirdInSecond, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("tags", tagsAny("a")), addr("tags", tagsAny("a", "b", "c")))}}, note: "addresses[1].tags[2]=c"},
			{id: idTwoTagsOnly, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("tags", tagsAny("a", "b")))}}, note: "no tags[2]"},
			{id: idEmptyTags, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("tags", tagsAny()))}}, note: "empty tags"},
			{id: idNoTags, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr())}}, note: "no tags field"},
			{id: idAbsent, props: map[string]any{"doc": map[string]any{}}, note: "no addresses"},
		}

		t.Run("addresses.tags[2]_IsNotNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.addresses.tags[2]", false),
				[]strfmt.UUID{idHasThirdTag, idHasThirdInSecond})
		})
		// TODO aliszka:nested_filtering: locks in CURRENT universal IsNull
		// for scalar-array positional access (`addresses.tags[2]`). Under
		// the planned existential IsNull rewrite, the expected list would
		// shift — empty/absent docs no longer match, docs where ANY
		// address has a missing tags[2] would match.
		t.Run("regression_addresses.tags[2]_IsNull", func(t *testing.T) {
			runScenario(t, docs, isNullFilter("doc.addresses.tags[2]", true),
				[]strfmt.UUID{idTwoTagsOnly, idEmptyTags, idNoTags, idAbsent})
		})
	})

	// ----- 4. Mixed constrained/unconstrained IsNull in correlated AND -----
	// `addresses.tags IsNotNull AND addresses[1].city = berlin`.
	// IsNotNull is unconstrained (empty arrayIndices); value pins addresses[1].
	// Compatibility grouping puts both in the same group → same-element AND
	// forces tags to be present at the same address as city=berlin (i.e., [1]).
	t.Run("mixed_constrained_unconstrained_isNull_AND_value", func(t *testing.T) {
		idMatch1 := uuid(1)                 // [{munich,a},{berlin,b}] — [1] has city+tags
		idMatch2 := uuid(2)                 // [{berlin,a},{berlin,b}] — [1] has city+tags
		idNoMatchSecondNoTags := uuid(3)    // [{berlin,a},{berlin}] — [1] has city but no tags
		idNoMatchOnlyFirst := uuid(4)       // [{berlin,a}] — only [0]
		idNoMatchTagsAtFirstOnly := uuid(5) // [{munich,a},{berlin}] — [1] city ok, no tags; tags only at [0]
		docs := []docDef{
			{id: idMatch1, props: map[string]any{"doc": map[string]any{"addresses": asArr(
				addr("city", "munich", "tags", tagsAny("a")),
				addr("city", "berlin", "tags", tagsAny("b")),
			)}}, note: "[1] city=berlin AND tags present"},
			{id: idMatch2, props: map[string]any{"doc": map[string]any{"addresses": asArr(
				addr("city", "berlin", "tags", tagsAny("a")),
				addr("city", "berlin", "tags", tagsAny("b")),
			)}}, note: "both have berlin and tags"},
			{id: idNoMatchSecondNoTags, props: map[string]any{"doc": map[string]any{"addresses": asArr(
				addr("city", "berlin", "tags", tagsAny("a")),
				addr("city", "berlin"),
			)}}, note: "[1] city=berlin but no tags; tags only at [0]"},
			{id: idNoMatchOnlyFirst, props: map[string]any{"doc": map[string]any{"addresses": asArr(
				addr("city", "berlin", "tags", tagsAny("a")),
			)}}, note: "only [0]"},
			{id: idNoMatchTagsAtFirstOnly, props: map[string]any{"doc": map[string]any{"addresses": asArr(
				addr("city", "munich", "tags", tagsAny("a")),
				addr("city", "berlin"),
			)}}, note: "[1] city=berlin; tags only at [0] (different address)"},
		}

		filter := andFilter(
			isNullFilter("doc.addresses.tags", false),
			textFilter("doc.addresses[1].city", "berlin"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatch1, idMatch2})
	})
}

// TestNestedFilteringScalarArrayIndex covers positional access on scalar
// arrays (text[]) within nested objects. Mirrors lower-level
// TestNestedFilteringIsNullAndMultiLevelArrayIndex Cases 2 and 3:
//
//   - Basic positional: cars.colors[N] = red — IdxKey("cars.colors", N)
//     restriction on a scalar-array element.
//   - Out-of-range: cars.colors[5] = red against docs with shorter arrays.
//   - Multi-level pin: cars[N].colors[M] = red — both object[] and scalar
//     array indices restrict the same path.
//   - Same-parent correlated AND: cars.colors[0]=red AND cars.colors[1]=blue
//     — both clauses share LCA=cars; same-element AND requires the same
//     car's colors to satisfy both positional clauses.
//
// Also covers a single docs_array variant to exercise root_idx in
// combination with scalar-array positional access.
func TestNestedFilteringScalarArrayIndex(t *testing.T) {
	const nestedClass = "ScalarArrIdx"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{Name: "colors", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
			{Name: "docs", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	colorsAny := func(colors ...string) []any {
		out := make([]any, len(colors))
		for i, c := range colors {
			out[i] = c
		}
		return out
	}
	carColors := func(colors ...string) map[string]any {
		return map[string]any{"colors": colorsAny(colors...)}
	}
	carWithMakeAndColors := func(make string, colors ...string) map[string]any {
		out := map[string]any{"make": make}
		if len(colors) > 0 {
			out["colors"] = colorsAny(colors...)
		}
		return out
	}
	tireWithTags := func(tags ...string) map[string]any {
		return map[string]any{"tags": colorsAny(tags...)}
	}
	carWithTires := func(tires ...map[string]any) map[string]any {
		out := make([]any, len(tires))
		for i, t := range tires {
			out[i] = t
		}
		return map[string]any{"tires": out}
	}
	carEmpty := func() map[string]any { return map[string]any{} }

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ----- Sub-test 1: basic scalar-array positional, no parent arr[N] -----
	// `doc.cars.colors[2] = red` — match docs where any car has colors[2]=red.
	t.Run("basic_cars.colors[2]_eq_red", func(t *testing.T) {
		idMatchPosition2 := uuid(1)     // colors=[blue,green,red]
		idNoMatchAtPosition0 := uuid(2) // colors=[red]
		idNoMatchTooShort := uuid(3)    // colors=[red,red]
		idMatchSecondCar := uuid(4)     // first car short, second has [2]=red
		idNoMatchEmptyColors := uuid(5) // colors=[]
		idNoMatchNoColors := uuid(6)    // car with no colors field
		idNoMatchNoCars := uuid(7)      // no cars at all
		docs := []docDef{
			{id: idMatchPosition2, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("blue", "green", "red"))}}, note: "colors[2]=red"},
			{id: idNoMatchAtPosition0, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red"))}}, note: "red at colors[0] only"},
			{id: idNoMatchTooShort, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red", "red"))}}, note: "no colors[2]"},
			{id: idMatchSecondCar, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("blue"), carColors("green", "blue", "red"))}}, note: "cars[1].colors[2]=red"},
			{id: idNoMatchEmptyColors, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors())}}, note: "empty colors"},
			{id: idNoMatchNoColors, props: map[string]any{"doc": map[string]any{"cars": asArr(carEmpty())}}, note: "car with no colors field"},
			{id: idNoMatchNoCars, props: map[string]any{"doc": map[string]any{}}, note: "no cars"},
		}
		runScenario(t, docs, textFilter("doc.cars.colors[2]", "red"), []strfmt.UUID{idMatchPosition2, idMatchSecondCar})
	})

	// ----- Sub-test 2: out-of-range scalar-array positional -----
	t.Run("out_of_range_cars.colors[5]_eq_red", func(t *testing.T) {
		idDoc := uuid(1)
		docs := []docDef{
			{id: idDoc, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red", "red", "red"))}}, note: "only 3 colors"},
		}
		runScenario(t, docs, textFilter("doc.cars.colors[5]", "red"), []strfmt.UUID{})
	})

	// ----- Sub-test 3: multi-level pin (object[] + scalar array) -----
	// `doc.cars[1].colors[2] = red` — two arr[N] segments restrict the same
	// path. Restriction order: cars[1] first, then colors[2] within that car.
	t.Run("multi_level_cars[1].colors[2]_eq_red", func(t *testing.T) {
		idMatch := uuid(1)                 // cars=[{colors:[x]},{colors:[a,b,red]}]
		idNoMatchOnlyFirstCar := uuid(2)   // cars=[{colors:[a,b,red]}] — no cars[1]
		idNoMatchSecondTooShort := uuid(3) // cars=[{colors:[a,b,red]},{colors:[red]}] — cars[1].colors[0]=red, no [2]
		idNoMatchSecondAtZero := uuid(4)   // cars=[{},{colors:[red,blue,green]}] — cars[1].colors[0]=red, [2]=green
		idMatchExtraCars := uuid(5)        // 3 cars, [1] satisfies, [2] is extra
		docs := []docDef{
			{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("x"), carColors("a", "b", "red"))}}, note: "cars[1].colors[2]=red"},
			{id: idNoMatchOnlyFirstCar, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("a", "b", "red"))}}, note: "no cars[1]"},
			{id: idNoMatchSecondTooShort, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("a", "b", "red"), carColors("red"))}}, note: "cars[1] only has colors[0]"},
			{id: idNoMatchSecondAtZero, props: map[string]any{"doc": map[string]any{"cars": asArr(carEmpty(), carColors("red", "blue", "green"))}}, note: "cars[1].colors[0]=red, [2]=green"},
			{id: idMatchExtraCars, props: map[string]any{"doc": map[string]any{"cars": asArr(carEmpty(), carColors("a", "b", "red"), carColors("red"))}}, note: "cars[1].colors[2]=red, extra cars[2]"},
		}
		runScenario(t, docs, textFilter("doc.cars[1].colors[2]", "red"), []strfmt.UUID{idMatch, idMatchExtraCars})
	})

	// ----- Sub-test 4: same-parent correlated AND on scalar-array positional -----
	// `doc.cars.colors[0]=red AND doc.cars.colors[1]=blue` — both clauses share
	// LCA=cars; same-element AND requires the same car's colors[0]=red AND
	// colors[1]=blue.
	t.Run("AND_cars.colors[0]_eq_red_AND_cars.colors[1]_eq_blue", func(t *testing.T) {
		idMatchSingleCar := uuid(1)         // cars=[{colors:[red,blue]}] — same car satisfies
		idMatchSecondCar := uuid(2)         // cars[0] partial; cars[1] satisfies
		idNoMatchSplitAcrossCars := uuid(3) // cars[0].colors[0]=red, cars[1].colors[1]=blue — different cars
		idNoMatchWrongOrder := uuid(4)      // cars=[{colors:[blue,red]}] — wrong positions
		idNoMatchOnlyFirst := uuid(5)       // cars=[{colors:[red]}] — no [1]
		docs := []docDef{
			{id: idMatchSingleCar, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red", "blue"))}}, note: "cars[0].colors=[red,blue]"},
			{id: idMatchSecondCar, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("green"), carColors("red", "blue", "green"))}}, note: "cars[1].colors=[red,blue,...]"},
			{id: idNoMatchSplitAcrossCars, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red", "green"), carColors("green", "blue"))}}, note: "red in cars[0].colors[0]; blue in cars[1].colors[1]"},
			{id: idNoMatchWrongOrder, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("blue", "red"))}}, note: "swapped positions"},
			{id: idNoMatchOnlyFirst, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red"))}}, note: "no colors[1]"},
		}
		filter := andFilter(
			textFilter("doc.cars.colors[0]", "red"),
			textFilter("doc.cars.colors[1]", "blue"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchSingleCar, idMatchSecondCar})
	})

	// ----- Sub-test 5: docs_array root variant -----
	// `docs[1].cars.colors[2] = red` — root_idx pin combined with scalar-array
	// positional. Verifies the dispatch works across DataTypeObjectArray root.
	t.Run("docs_array_docs[1].cars.colors[2]_eq_red", func(t *testing.T) {
		idMatch := uuid(1)              // docs[1].cars[0].colors[2]=red
		idNoMatchInFirstRoot := uuid(2) // docs[0] has it; docs[1] doesn't
		idNoMatchSingleRoot := uuid(3)  // single root only
		docs := []docDef{
			{id: idMatch, props: map[string]any{"docs": asArr(
				map[string]any{"cars": asArr(carColors("blue"))},
				map[string]any{"cars": asArr(carColors("a", "b", "red"))},
			)}, note: "docs[1].cars[0].colors[2]=red"},
			{id: idNoMatchInFirstRoot, props: map[string]any{"docs": asArr(
				map[string]any{"cars": asArr(carColors("a", "b", "red"))},
				map[string]any{"cars": asArr(carColors("blue"))},
			)}, note: "red at docs[0].cars[0].colors[2], not docs[1]"},
			{id: idNoMatchSingleRoot, props: map[string]any{"docs": asArr(
				map[string]any{"cars": asArr(carColors("a", "b", "red"))},
			)}, note: "no docs[1]"},
		}
		runScenario(t, docs, textFilter("docs[1].cars.colors[2]", "red"), []strfmt.UUID{idMatch})
	})

	// ----- Sub-test 6: mixed scalar + scalar-array positional, same-element AND -----
	// `cars.make=tesla AND cars.colors[0]=red` — same-element AND at LCA=cars
	// across two clause types (regular text scalar + text[] positional). Both
	// must hold on the same physical car.
	t.Run("AND_cars.make_AND_cars.colors[0]_same_car", func(t *testing.T) {
		idMatch := uuid(1)               // cars=[{make:tesla,colors:[red]}]
		idMatchSecondCar := uuid(2)      // cars[0] partial, cars[1] satisfies
		idNoMatchSplit := uuid(3)        // tesla in cars[1], red in cars[0]
		idNoMatchTeslaNoColor := uuid(4) // tesla car has no colors
		idNoMatchWrongColor := uuid(5)   // tesla car has colors[0]=blue
		docs := []docDef{
			{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithMakeAndColors("tesla", "red"))}}, note: "single car: tesla, colors[0]=red"},
			{id: idMatchSecondCar, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithMakeAndColors("bmw", "red"), carWithMakeAndColors("tesla", "red", "blue"))}}, note: "cars[1] has tesla and colors[0]=red"},
			{id: idNoMatchSplit, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithMakeAndColors("bmw", "red"), carWithMakeAndColors("tesla"))}}, note: "tesla car has no colors"},
			{id: idNoMatchTeslaNoColor, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithMakeAndColors("tesla"))}}, note: "tesla; no colors"},
			{id: idNoMatchWrongColor, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithMakeAndColors("tesla", "blue", "red"))}}, note: "tesla; colors[0]=blue"},
		}
		filter := andFilter(
			textFilter("doc.cars.make", "tesla"),
			textFilter("doc.cars.colors[0]", "red"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchSecondCar})
	})

	// ----- Sub-test 7: multi-level pin + same-parent correlated AND -----
	// `cars[1].colors[0]=red AND cars[1].colors[1]=blue` — both clauses pinned
	// to cars[1]; same-parent correlated AND requires both color positions to
	// be satisfied within the same cars[1].
	t.Run("AND_cars[1].colors[0]_AND_cars[1].colors[1]_pinned", func(t *testing.T) {
		idMatch := uuid(1)            // cars=[{},{colors:[red,blue]}]
		idNoMatchOnlyFirst := uuid(2) // cars=[{colors:[red,blue]}] — no [1]
		idNoMatchSwapped := uuid(3)   // cars[1].colors=[blue,red]
		idNoMatchTooShort := uuid(4)  // cars[1].colors=[red]
		idMatchExtraColors := uuid(5) // cars[1].colors=[red,blue,green]
		docs := []docDef{
			{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(carEmpty(), carColors("red", "blue"))}}, note: "cars[1].colors=[red,blue]"},
			{id: idNoMatchOnlyFirst, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red", "blue"))}}, note: "no cars[1]"},
			{id: idNoMatchSwapped, props: map[string]any{"doc": map[string]any{"cars": asArr(carEmpty(), carColors("blue", "red"))}}, note: "swapped"},
			{id: idNoMatchTooShort, props: map[string]any{"doc": map[string]any{"cars": asArr(carEmpty(), carColors("red"))}}, note: "no colors[1]"},
			{id: idMatchExtraColors, props: map[string]any{"doc": map[string]any{"cars": asArr(carEmpty(), carColors("red", "blue", "green"))}}, note: "cars[1].colors=[red,blue,green]"},
		}
		filter := andFilter(
			textFilter("doc.cars[1].colors[0]", "red"),
			textFilter("doc.cars[1].colors[1]", "blue"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraColors})
	})

	// ----- Sub-test 8: scalar-array partition with different parents -----
	// `cars[0].colors[0]=red AND cars[1].colors[0]=blue` — different cars,
	// each with its own positional clause. The dispatch's
	// groupChildrenByArrayIndicesKey partitions {cars:0} and {cars:1} into
	// independent groups → ANDed at docID level.
	t.Run("AND_cars[0].colors[0]_red_AND_cars[1].colors[0]_blue_partition", func(t *testing.T) {
		idMatch := uuid(1)              // cars=[{colors:[red]},{colors:[blue]}]
		idMatchExtraColors := uuid(2)   // cars=[{colors:[red,green]},{colors:[blue,red]}]
		idNoMatchSwapped := uuid(3)     // cars=[{colors:[blue]},{colors:[red]}]
		idNoMatchOnlyFirst := uuid(4)   // cars=[{colors:[red]}]
		idNoMatchSecondWrong := uuid(5) // cars[0]=red ok, cars[1].colors[0]=red not blue
		docs := []docDef{
			{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red"), carColors("blue"))}}, note: "cars=[{red},{blue}]"},
			{id: idMatchExtraColors, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red", "green"), carColors("blue", "red"))}}, note: "cars=[{red,green},{blue,red}]"},
			{id: idNoMatchSwapped, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("blue"), carColors("red"))}}, note: "swapped"},
			{id: idNoMatchOnlyFirst, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red"))}}, note: "no cars[1]"},
			{id: idNoMatchSecondWrong, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red"), carColors("red"))}}, note: "cars[1].colors[0]=red not blue"},
		}
		filter := andFilter(
			textFilter("doc.cars[0].colors[0]", "red"),
			textFilter("doc.cars[1].colors[0]", "blue"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraColors})
	})

	// ----- Sub-test 9: deeper-nested scalar array (cars.tires.tags[N]) -----
	// `cars.tires.tags[2] = red` — text[] positional inside a 2-level-deep
	// nested object[]. Exercises lcaPath calculation for scalar arrays at a
	// deeper LCA than the cars-direct case.
	t.Run("deeper_cars.tires.tags[2]_eq_red", func(t *testing.T) {
		idMatch := uuid(1)           // tires=[{tags:[a,b,red]}]
		idMatchSecondTire := uuid(2) // tires=[{},{tags:[a,b,red]}]
		idNoMatchAtZero := uuid(3)   // tires=[{tags:[red]}]
		idNoMatchTooShort := uuid(4) // tires=[{tags:[a,b]}]
		idNoMatchNoTires := uuid(5)  // car with no tires
		docs := []docDef{
			{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireWithTags("a", "b", "red")))}}, note: "tires[0].tags[2]=red"},
			{id: idMatchSecondTire, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(map[string]any{}, tireWithTags("a", "b", "red")))}}, note: "tires[1].tags[2]=red"},
			{id: idNoMatchAtZero, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireWithTags("red")))}}, note: "tags[0]=red only"},
			{id: idNoMatchTooShort, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireWithTags("a", "b")))}}, note: "no tags[2]"},
			{id: idNoMatchNoTires, props: map[string]any{"doc": map[string]any{"cars": asArr(carEmpty())}}, note: "no tires"},
		}
		runScenario(t, docs, textFilter("doc.cars.tires.tags[2]", "red"), []strfmt.UUID{idMatch, idMatchSecondTire})
	})

	// ----- Sub-test 10: same-position contradiction -----
	// `cars.colors[0]=red AND cars.colors[0]=blue` — single position cannot
	// equal two different values; should always return empty.
	t.Run("contradiction_cars.colors[0]_eq_red_AND_eq_blue", func(t *testing.T) {
		idHasRed := uuid(1)
		idHasBlue := uuid(2)
		idHasBoth := uuid(3) // colors=[red,blue] — but [0]=red, [1]=blue, neither position has both
		docs := []docDef{
			{id: idHasRed, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red"))}}, note: "colors[0]=red"},
			{id: idHasBlue, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("blue"))}}, note: "colors[0]=blue"},
			{id: idHasBoth, props: map[string]any{"doc": map[string]any{"cars": asArr(carColors("red", "blue"))}}, note: "colors=[red,blue]"},
		}
		filter := andFilter(
			textFilter("doc.cars.colors[0]", "red"),
			textFilter("doc.cars.colors[0]", "blue"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{})
	})
}

// TestNestedFilteringComprehensive ports the lower-level
// TestNestedFilteringComprehensive smoke test to the DB level. It exercises
// basic filters, simple/complex AND with same-element enforcement, OR
// (no same-element), nested AND/OR mix, and deep nesting (cars.tires.bolts)
// — all driven through the production write+search pipeline. Two root
// variants (DataTypeObject "doc" and DataTypeObjectArray "docs") share
// the same nested shape but differ on doc3, which has data within a
// single root for "doc" and split across two roots for "docs".
//
// Doc setup (per variant):
//   - d1: addresses, tags=premium, cars[0]={bmw, tires=[width:205,bolts.size=10], accessories=[sunroof]}
//   - d2: cars=[{bmw}, {tires:[width:205], accessories:[sunroof]}] — bmw and tires/acc in different cars; no bolts
//   - d3 (doc):  addresses + cars[0]={bmw} — same root, no tires/acc/bolts
//   - d3 (docs): docs[0]={cars:[{bmw}]}, docs[1]={addresses, cars:[{tires:[width:205], accessories:[sunroof]}]} — cross-root split
//   - d4: addresses, cars[0]={honda, tires:[width:205], accessories:[sunroof]} — wrong make, no bolts
func TestNestedFilteringComprehensive(t *testing.T) {
	const nestedClass = "ComprehensiveNested"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "addresses", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
			},
		},
		{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
						{
							Name: "bolts", DataType: schema.DataTypeObjectArray.PropString(),
							NestedProperties: []*models.NestedProperty{
								{Name: "size", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
							},
						},
					},
				},
				{
					Name: "accessories", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "type", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
			{Name: "docs", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tagsAny := func(tags ...string) []any {
		out := make([]any, len(tags))
		for i, t := range tags {
			out[i] = t
		}
		return out
	}
	addr := func(city, postcode string) map[string]any {
		return map[string]any{"city": city, "postcode": postcode}
	}
	bolt := func(size int) map[string]any { return map[string]any{"size": size} }
	tireSimple := func(width int) map[string]any {
		return map[string]any{"width": width}
	}
	tireWithBolts := func(width int, bolts ...map[string]any) map[string]any {
		anyBolts := make([]any, len(bolts))
		for i, b := range bolts {
			anyBolts[i] = b
		}
		return map[string]any{"width": width, "bolts": anyBolts}
	}
	accessory := func(type_ string) map[string]any { return map[string]any{"type": type_} }

	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}
	orFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorOr, Operands: operands}}
	}

	type variantSpec struct {
		name    string
		propKey string
		// d3Props returns doc3's variant-specific props.
		d3Props func() map[string]any
		// wrap takes the body (addresses/tags/cars) and produces top-level Properties.
		wrap func(body map[string]any) map[string]any
	}

	variants := []variantSpec{
		{
			name:    "doc_object",
			propKey: "doc",
			d3Props: func() map[string]any {
				return map[string]any{"doc": map[string]any{
					"addresses": asArr(addr("berlin", "10115")),
					"cars":      asArr(map[string]any{"make": "bmw"}),
				}}
			},
			wrap: func(body map[string]any) map[string]any { return map[string]any{"doc": body} },
		},
		{
			name:    "docs_array",
			propKey: "docs",
			d3Props: func() map[string]any {
				return map[string]any{"docs": asArr(
					map[string]any{"cars": asArr(map[string]any{"make": "bmw"})},
					map[string]any{
						"addresses": asArr(addr("berlin", "10115")),
						"cars":      asArr(map[string]any{"tires": asArr(tireSimple(205)), "accessories": asArr(accessory("sunroof"))}),
					},
				)}
			},
			wrap: func(body map[string]any) map[string]any { return map[string]any{"docs": asArr(body)} },
		},
	}

	for _, v := range variants {
		v := v
		t.Run(v.name, func(t *testing.T) {
			db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
			ctx := context.Background()

			d1 := uuid(1)
			d2 := uuid(2)
			d3 := uuid(3)
			d4 := uuid(4)

			d1Body := map[string]any{
				"addresses": asArr(addr("berlin", "10115")),
				"tags":      tagsAny("premium"),
				"cars": asArr(map[string]any{
					"make":        "bmw",
					"tires":       asArr(tireWithBolts(205, bolt(10))),
					"accessories": asArr(accessory("sunroof")),
				}),
			}
			d2Body := map[string]any{
				"cars": asArr(
					map[string]any{"make": "bmw"},
					map[string]any{"tires": asArr(tireSimple(205)), "accessories": asArr(accessory("sunroof"))},
				),
			}
			d4Body := map[string]any{
				"addresses": asArr(addr("berlin", "10115")),
				"cars": asArr(map[string]any{
					"make":        "honda",
					"tires":       asArr(tireSimple(205)),
					"accessories": asArr(accessory("sunroof")),
				}),
			}

			require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: d1, Properties: v.wrap(d1Body)}, nil, nil, nil, nil, 0), "put d1")
			require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: d2, Properties: v.wrap(d2Body)}, nil, nil, nil, nil, 0), "put d2")
			require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: d3, Properties: v.d3Props()}, nil, nil, nil, nil, 0), "put d3")
			require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: d4, Properties: v.wrap(d4Body)}, nil, nil, nil, nil, 0), "put d4")

			runFilter := func(t *testing.T, filter *filters.LocalFilter, want []strfmt.UUID) {
				t.Helper()
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
			want := func(forDoc, forDocs []strfmt.UUID) []strfmt.UUID {
				if v.name == "doc_object" {
					return forDoc
				}
				return forDocs
			}
			pk := v.propKey

			// ----- Basic single-condition filters -----
			t.Run("basic_cars.make_eq_bmw", func(t *testing.T) {
				runFilter(t, textFilter(pk+".cars.make", "bmw"), []strfmt.UUID{d1, d2, d3})
			})
			t.Run("basic_cars.make_eq_honda", func(t *testing.T) {
				runFilter(t, textFilter(pk+".cars.make", "honda"), []strfmt.UUID{d4})
			})
			t.Run("basic_cars.tires.width_eq_205", func(t *testing.T) {
				// doc:  d3 has no tires → [d1,d2,d4]
				// docs: d3 has tires in docs[1] → [d1,d2,d3,d4]
				runFilter(t, intFilter(pk+".cars.tires.width", 205),
					want([]strfmt.UUID{d1, d2, d4}, []strfmt.UUID{d1, d2, d3, d4}))
			})
			t.Run("basic_addresses.city_eq_berlin", func(t *testing.T) {
				runFilter(t, textFilter(pk+".addresses.city", "berlin"), []strfmt.UUID{d1, d3, d4})
			})
			t.Run("basic_tags_eq_premium", func(t *testing.T) {
				runFilter(t, textFilter(pk+".tags", "premium"), []strfmt.UUID{d1})
			})

			// ----- Simple AND with same-element enforcement -----
			t.Run("AND_make_bmw_AND_tires.width_205_same_car", func(t *testing.T) {
				// d1 only — d2's bmw is in cars[0], tires in cars[1].
				runFilter(t, andFilter(textFilter(pk+".cars.make", "bmw"), intFilter(pk+".cars.tires.width", 205)),
					[]strfmt.UUID{d1})
			})
			t.Run("AND_make_bmw_AND_accessories_sunroof_same_car", func(t *testing.T) {
				runFilter(t, andFilter(textFilter(pk+".cars.make", "bmw"), textFilter(pk+".cars.accessories.type", "sunroof")),
					[]strfmt.UUID{d1})
			})
			t.Run("AND_tires.width_AND_accessories_sunroof_same_car", func(t *testing.T) {
				// doc:  d3 has no tires → [d1,d2,d4]
				// docs: d3 has both in docs[1].cars[0] → [d1,d2,d3,d4]
				runFilter(t, andFilter(intFilter(pk+".cars.tires.width", 205), textFilter(pk+".cars.accessories.type", "sunroof")),
					want([]strfmt.UUID{d1, d2, d4}, []strfmt.UUID{d1, d2, d3, d4}))
			})
			t.Run("AND_3clause_make_tires_accessories_same_car", func(t *testing.T) {
				runFilter(t, andFilter(
					textFilter(pk+".cars.make", "bmw"),
					intFilter(pk+".cars.tires.width", 205),
					textFilter(pk+".cars.accessories.type", "sunroof"),
				), []strfmt.UUID{d1})
			})
			t.Run("AND_make_bmw_AND_addresses.city_berlin_same_root", func(t *testing.T) {
				// doc:  d3 has bmw+berlin in same root → [d1,d3]
				// docs: d3 has bmw in docs[0], berlin in docs[1] (cross-root) → [d1]
				runFilter(t, andFilter(textFilter(pk+".cars.make", "bmw"), textFilter(pk+".addresses.city", "berlin")),
					want([]strfmt.UUID{d1, d3}, []strfmt.UUID{d1}))
			})
			t.Run("AND_addresses.city_AND_postcode_same_address", func(t *testing.T) {
				runFilter(t, andFilter(textFilter(pk+".addresses.city", "berlin"), textFilter(pk+".addresses.postcode", "10115")),
					[]strfmt.UUID{d1, d3, d4})
			})
			t.Run("AND_make_AND_address_pair_same_root", func(t *testing.T) {
				runFilter(t, andFilter(
					textFilter(pk+".cars.make", "bmw"),
					textFilter(pk+".addresses.city", "berlin"),
					textFilter(pk+".addresses.postcode", "10115"),
				), want([]strfmt.UUID{d1, d3}, []strfmt.UUID{d1}))
			})
			t.Run("AND_tags_premium_AND_make_bmw_same_root", func(t *testing.T) {
				runFilter(t, andFilter(textFilter(pk+".tags", "premium"), textFilter(pk+".cars.make", "bmw")),
					[]strfmt.UUID{d1})
			})
			t.Run("AND_tires_AND_accessories_AND_address_pair", func(t *testing.T) {
				// doc:  d3 no tires/acc → [d1,d4]
				// docs: d3 docs[1] has all → [d1,d3,d4]
				runFilter(t, andFilter(
					intFilter(pk+".cars.tires.width", 205),
					textFilter(pk+".cars.accessories.type", "sunroof"),
					textFilter(pk+".addresses.city", "berlin"),
					textFilter(pk+".addresses.postcode", "10115"),
				), want([]strfmt.UUID{d1, d4}, []strfmt.UUID{d1, d3, d4}))
			})

			// ----- Simple OR -----
			t.Run("OR_make_bmw_OR_make_honda", func(t *testing.T) {
				runFilter(t, orFilter(textFilter(pk+".cars.make", "bmw"), textFilter(pk+".cars.make", "honda")),
					[]strfmt.UUID{d1, d2, d3, d4})
			})
			t.Run("OR_make_bmw_OR_tires.width_205", func(t *testing.T) {
				runFilter(t, orFilter(textFilter(pk+".cars.make", "bmw"), intFilter(pk+".cars.tires.width", 205)),
					[]strfmt.UUID{d1, d2, d3, d4})
			})
			t.Run("OR_tags_premium_OR_addresses.city_berlin", func(t *testing.T) {
				runFilter(t, orFilter(textFilter(pk+".tags", "premium"), textFilter(pk+".addresses.city", "berlin")),
					[]strfmt.UUID{d1, d3, d4})
			})
			t.Run("OR_make_bmw_OR_addresses.city_paris_absent", func(t *testing.T) {
				runFilter(t, orFilter(textFilter(pk+".cars.make", "bmw"), textFilter(pk+".addresses.city", "paris")),
					[]strfmt.UUID{d1, d2, d3})
			})

			// ----- Complex multi-condition AND/OR mix -----
			t.Run("complex_AND_make_tires_OR_make_honda", func(t *testing.T) {
				runFilter(t, orFilter(
					andFilter(textFilter(pk+".cars.make", "bmw"), intFilter(pk+".cars.tires.width", 205)),
					textFilter(pk+".cars.make", "honda"),
				), []strfmt.UUID{d1, d4})
			})
			// TODO aliszka:nested_filtering: this sub-test asserts CURRENT
			// docID-level AND-of-OR behavior. The d2 expectation (cars[0]=bmw
			// alone, cars[1]=tires+accessories alone) depends on AND not
			// propagating same-element correlation across the OR boundary.
			// When OR distribution lands (OR-in-AND distribution rewrite),
			// d2 will no longer match — only docs with a single car satisfying
			// (bmw AND 205) or (bmw AND sunroof) would match.
			// See TestNestedFilteringAndOfOrRegression for a dedicated
			// discriminator test.
			t.Run("complex_make_bmw_AND_OR_tires_OR_accessories", func(t *testing.T) {
				runFilter(t, andFilter(
					textFilter(pk+".cars.make", "bmw"),
					orFilter(intFilter(pk+".cars.tires.width", 205), textFilter(pk+".cars.accessories.type", "sunroof")),
				), want([]strfmt.UUID{d1, d2}, []strfmt.UUID{d1, d2, d3}))
			})
			t.Run("complex_make_AND_tires_AND_addresses_all_same_root", func(t *testing.T) {
				runFilter(t, andFilter(
					textFilter(pk+".cars.make", "bmw"),
					intFilter(pk+".cars.tires.width", 205),
					textFilter(pk+".addresses.city", "berlin"),
				), []strfmt.UUID{d1})
			})
			// TODO aliszka:nested_filtering: this sub-test asserts CURRENT
			// docID-level AND-of-OR behavior. d3 in docs_array variant has
			// bmw in docs[0] and berlin in docs[1] (cross-root) — currently
			// matches via docID-level intersection. When OR distribution
			// lands (OR-in-AND distribution rewrite), the
			// rewritten filter `(bmw AND berlin) OR (honda AND berlin)`
			// requires same-root for each branch, so d3 (docs_array) would
			// no longer match. doc_object expectation unchanged (single root).
			t.Run("complex_OR_makes_AND_addresses", func(t *testing.T) {
				runFilter(t, andFilter(
					orFilter(textFilter(pk+".cars.make", "bmw"), textFilter(pk+".cars.make", "honda")),
					textFilter(pk+".addresses.city", "berlin"),
				), []strfmt.UUID{d1, d3, d4})
			})
			t.Run("complex_4clause_tags_make_tires_accessories", func(t *testing.T) {
				runFilter(t, andFilter(
					textFilter(pk+".tags", "premium"),
					textFilter(pk+".cars.make", "bmw"),
					intFilter(pk+".cars.tires.width", 205),
					textFilter(pk+".cars.accessories.type", "sunroof"),
				), []strfmt.UUID{d1})
			})

			// ----- Deep nesting (cars.tires.bolts) — only d1 has bolts -----
			t.Run("deep_bolts.size_alone", func(t *testing.T) {
				runFilter(t, intFilter(pk+".cars.tires.bolts.size", 10), []strfmt.UUID{d1})
			})
			t.Run("deep_bolts.size_AND_tires.width_same_tires", func(t *testing.T) {
				runFilter(t, andFilter(
					intFilter(pk+".cars.tires.bolts.size", 10),
					intFilter(pk+".cars.tires.width", 205),
				), []strfmt.UUID{d1})
			})
			t.Run("deep_bolts.size_AND_tires.width_AND_make_bmw", func(t *testing.T) {
				runFilter(t, andFilter(
					intFilter(pk+".cars.tires.bolts.size", 10),
					intFilter(pk+".cars.tires.width", 205),
					textFilter(pk+".cars.make", "bmw"),
				), []strfmt.UUID{d1})
			})

			// ----- Edge cases -----
			// Same-path AND on different values: a single value at the same path
			// can't equal two distinct constants, so the result must be empty
			// regardless of doc shape.
			t.Run("edge_same_path_contradiction_make_bmw_AND_make_honda", func(t *testing.T) {
				runFilter(t, andFilter(
					textFilter(pk+".cars.make", "bmw"),
					textFilter(pk+".cars.make", "honda"),
				), []strfmt.UUID{})
			})
			// Filter on a value no doc carries — verifies empty-bitmap handling.
			t.Run("edge_empty_result_make_ferrari", func(t *testing.T) {
				runFilter(t, textFilter(pk+".cars.make", "ferrari"), []strfmt.UUID{})
			})
		})
	}
}

// TestNestedFilteringTokenizationFollowups closes three coverage gaps in the
// tokenization cluster left open by TestNestedFilteringTokenizationCorrelatedAnd:
//
//  1. Multi-token text combined with arr[N] —
//     `addresses[0].city = "new york" AND addresses[0].postcode = "10115"`.
//     Verifies that arr[N] restriction applies to all token-leaf positions,
//     not just the wrapper's first leaf.
//  2. Multi-token text at a deeper LCA — `cars.tires.description = "new york"
//     AND cars.tires.brand = "high speed"`. Existing tokenization tests use
//     LCA=addresses (L1); this verifies the same machinery at LCA=cars.tires
//     (L2) where the token wrapper sits at a deeper path.
//  3. Multi-token same-path contradiction — `addresses.city = "new york" AND
//     addresses.city = "munich oslo"`. Both clauses are token wrappers; AndAll
//     collapses to require all four tokens (new, york, munich, oslo) at the
//     same leaf. Documents the actual edge-case behavior — should match only
//     docs whose city literally contains all four tokens at one address.
func TestNestedFilteringTokenizationFollowups(t *testing.T) {
	const nestedClass = "TokenizationFollowups"
	vTrue := true
	word := models.NestedPropertyTokenizationWord

	rootProps := []*models.NestedProperty{
		{
			Name: "addresses", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
			},
		},
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "description", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
						{Name: "brand", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	addr := func(props ...any) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(props); i += 2 {
			out[props[i].(string)] = props[i+1]
		}
		return out
	}
	tireDB := func(desc, brand string) map[string]any {
		out := map[string]any{}
		if desc != "" {
			out["description"] = desc
		}
		if brand != "" {
			out["brand"] = brand
		}
		return out
	}
	carWithTires := func(tires ...map[string]any) map[string]any {
		anyTires := make([]any, len(tires))
		for i, t := range tires {
			anyTires[i] = t
		}
		return map[string]any{"tires": anyTires}
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ----- 1. Multi-token + arr[N] -----
	// `addresses[0].city = "new york" AND addresses[0].postcode = "10115"`
	t.Run("arrN_addresses[0].city_new_york_AND_postcode", func(t *testing.T) {
		idMatch := uuid(1)               // addresses[0] has city="new york" + postcode="10115"
		idMatchExtraToken := uuid(2)     // addresses[0] city="new york city" — extra token at same leaf
		idNoMatchInSecond := uuid(3)     // addresses[0]={}, addresses[1] satisfies — pin to [0] excludes
		idNoMatchSplitTokens := uuid(4)  // addresses[0]={city:"new",postcode:"10115"}, [1]={city:"york"} — tokens split
		idNoMatchOnlyOne := uuid(5)      // addresses[0] has only one of the two clauses
		idNoMatchSplitClauses := uuid(6) // city in [0], postcode in [1] — same-element forces both at [0]
		docs := []docDef{
			{id: idMatch, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "new york", "postcode", "10115"))}}, note: "[0] has both"},
			{id: idMatchExtraToken, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "new york city", "postcode", "10115"))}}, note: "[0].city has extra token"},
			{id: idNoMatchInSecond, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr(), addr("city", "new york", "postcode", "10115"))}}, note: "match only at [1]; pinned to [0]"},
			{id: idNoMatchSplitTokens, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "new", "postcode", "10115"), addr("city", "york"))}}, note: "city tokens split across addresses"},
			{id: idNoMatchOnlyOne, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "new york"))}}, note: "[0] has city but no postcode"},
			{id: idNoMatchSplitClauses, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "new york"), addr("postcode", "10115"))}}, note: "city at [0], postcode at [1]"},
		}
		filter := andFilter(
			textFilter("doc.addresses[0].city", "new york"),
			textFilter("doc.addresses[0].postcode", "10115"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchExtraToken})
	})

	// ----- 2. Multi-token at deeper LCA -----
	// LCA = cars.tires (L2 from root); both clauses are multi-token at the same
	// LCA. Same-tire correlation requires the same physical tire to satisfy
	// both `description` and `brand`.
	t.Run("deeper_LCA_cars.tires.description_AND_brand_multi_token", func(t *testing.T) {
		idMatch := uuid(1)                   // tires=[{description:"new york",brand:"high speed"}]
		idMatchSecondTire := uuid(2)         // tires=[{},{description:"new york",brand:"high speed"}]
		idNoMatchSplitAcrossTires := uuid(3) // description in tires[0], brand in tires[1]
		idNoMatchPartialClause := uuid(4)    // tires[0].description has only "new"
		idNoMatchWrongBrand := uuid(5)       // tires[0] correct desc but brand="low speed"
		idMatchExtraTokens := uuid(6)        // description="new york city", brand="very high speed" — extra tokens
		docs := []docDef{
			{id: idMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireDB("new york", "high speed")))}}, note: "tires[0] has both at same tire"},
			{id: idMatchSecondTire, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireDB("", ""), tireDB("new york", "high speed")))}}, note: "tires[1] has both"},
			{id: idNoMatchSplitAcrossTires, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireDB("new york", ""), tireDB("", "high speed")))}}, note: "different tires"},
			{id: idNoMatchPartialClause, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireDB("new", "high speed")))}}, note: "missing 'york' token"},
			{id: idNoMatchWrongBrand, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireDB("new york", "low speed")))}}, note: "wrong brand"},
			{id: idMatchExtraTokens, props: map[string]any{"doc": map[string]any{"cars": asArr(carWithTires(tireDB("new york city", "very high speed")))}}, note: "extra tokens at same tire"},
		}
		filter := andFilter(
			textFilter("doc.cars.tires.description", "new york"),
			textFilter("doc.cars.tires.brand", "high speed"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatch, idMatchSecondTire, idMatchExtraTokens})
	})

	// ----- 3. Multi-token same-path contradiction -----
	// `addresses.city = "new york" AND addresses.city = "munich oslo"` — both
	// clauses target the same path with disjoint multi-token values. AndAll
	// over the token leaves requires [new, york, munich, oslo] all at the
	// same address element. Edge case: documents the actual behavior, ensures
	// no crash, and verifies a doc that *does* contain all four tokens at one
	// address is matched.
	t.Run("same_path_multi_token_contradiction", func(t *testing.T) {
		idNoMatchOnlyFirst := uuid(1)  // city="new york"
		idNoMatchOnlySecond := uuid(2) // city="munich oslo"
		idNoMatchSplit := uuid(3)      // city="new york" in [0], city="munich oslo" in [1] — different addresses
		idAllFourTokens := uuid(4)     // city="new york munich oslo" — all four tokens at one address
		docs := []docDef{
			{id: idNoMatchOnlyFirst, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "new york"))}}, note: "city has [new,york]; missing munich,oslo"},
			{id: idNoMatchOnlySecond, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "munich oslo"))}}, note: "city has [munich,oslo]; missing new,york"},
			{id: idNoMatchSplit, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "new york"), addr("city", "munich oslo"))}}, note: "split across addresses"},
			{id: idAllFourTokens, props: map[string]any{"doc": map[string]any{"addresses": asArr(addr("city", "new york munich oslo"))}}, note: "all four tokens at same address"},
		}
		filter := andFilter(
			textFilter("doc.addresses.city", "new york"),
			textFilter("doc.addresses.city", "munich oslo"),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idAllFourTokens})
	})
}

// TestNestedFilteringSamePathMultiValueTokenized ports the 4 BUG sub-tests
// of lower-level TestResolveNestedCorrelatedAnd (plain-object LCA and
// object[] LCA × independent=1 / independent>1). These exercise a specific
// pattern not otherwise covered: same path with multiple value clauses where
// at least one is multi-token and one or more are single-token, plus a
// sibling clause on a different path.
//
// At the lower level, the bug was that combinePositions produced empty
// results when mixing MASKED (from token AndAll) and RAW (from independent)
// bitmaps. The DB-level port verifies the user-visible behavior is correct
// regardless of how the internal bitmaps are combined.
func TestNestedFilteringSamePathMultiValueTokenized(t *testing.T) {
	const nestedClass = "MultiValueTokenized"
	vTrue := true
	word := models.NestedPropertyTokenizationWord

	textFilter := func(class, path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: schema.ClassName(class), Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}
	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tagsAny := func(tags ...string) []any {
		out := make([]any, len(tags))
		for i, t := range tags {
			out[i] = t
		}
		return out
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}

	// ----- Variant 1: plain-object LCA, text[] tags, multi-token + 1 single -----
	// Filter: addresses.owner.tags = "new york" AND addresses.owner.tags = "berlin"
	//         AND addresses.owner.name = "alice"
	// LCA = addresses.owner (plain-object inside addresses[]). owner has tags
	// (text[]) and name (text). Single address must satisfy all three clauses.
	t.Run("plain_obj_LCA_tags_multi_token_plus_single_AND_name", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{{
				Name:     "addresses",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{
					Name:     "owner",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: word, IndexFilterable: &vTrue},
						{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
					},
				}},
			}},
		}
		idMatch := uuid(1)            // addresses[0].owner.tags=["new york","berlin"], owner.name="alice"
		idNoMatchSplit := uuid(2)     // tags split: addresses[0]=["new york"], addresses[1]=["berlin"]
		idNoMatchWrongName := uuid(3) // addresses[0] has both tags but owner.name != alice
		idNoMatchPartial := uuid(4)   // addresses[0] has only "new york"; missing "berlin"
		docs := []struct {
			id    strfmt.UUID
			props map[string]any
		}{
			{id: idMatch, props: map[string]any{"addresses": asArr(map[string]any{
				"owner": map[string]any{"tags": tagsAny("new york", "berlin"), "name": "alice"},
			})}},
			{id: idNoMatchSplit, props: map[string]any{"addresses": asArr(
				map[string]any{"owner": map[string]any{"tags": tagsAny("new york"), "name": "alice"}},
				map[string]any{"owner": map[string]any{"tags": tagsAny("berlin"), "name": "alice"}},
			)}},
			{id: idNoMatchWrongName, props: map[string]any{"addresses": asArr(map[string]any{
				"owner": map[string]any{"tags": tagsAny("new york", "berlin"), "name": "bob"},
			})}},
			{id: idNoMatchPartial, props: map[string]any{"addresses": asArr(map[string]any{
				"owner": map[string]any{"tags": tagsAny("new york"), "name": "alice"},
			})}},
		}

		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{Class: nestedClass, ID: d.id, Properties: d.props}, nil, nil, nil, nil, 0))
		}
		filter := andFilter(
			textFilter(nestedClass, "addresses.owner.tags", "new york"),
			textFilter(nestedClass, "addresses.owner.tags", "berlin"),
			textFilter(nestedClass, "addresses.owner.name", "alice"),
		)
		res, err := db.Search(ctx, dto.GetParams{ClassName: nestedClass, Pagination: &filters.Pagination{Limit: 100}, Filters: filter})
		require.NoError(t, err)
		got := make([]strfmt.UUID, len(res))
		for i, r := range res {
			got[i] = r.ID
		}
		assert.ElementsMatch(t, []strfmt.UUID{idMatch}, got)
	})

	// ----- Variant 2: plain-object LCA, text title, multi-token + 2 singles -----
	// Filter: addresses.owner.title = "new york" AND .title = "berlin" AND .title = "tech"
	//         AND addresses.owner.name = "alice"
	// title is text (single-valued); to satisfy three same-path clauses the
	// title's tokenization must contain all of [new, york, berlin, tech] at the
	// same position. With word tokenization, a value like "new york berlin tech"
	// produces all four tokens at the same leaf.
	t.Run("plain_obj_LCA_title_multi_token_plus_2_singles_AND_name", func(t *testing.T) {
		class2 := &models.Class{
			Class:             nestedClass + "2",
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{{
				Name:     "addresses",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{
					Name:     "owner",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
						{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
					},
				}},
			}},
		}
		idMatch := uuid(1)            // title="new york berlin tech", name="alice"
		idNoMatchPartial := uuid(2)   // title="new york berlin" — missing tech
		idNoMatchWrongName := uuid(3) // title has all 4 but name=bob
		idNoMatchSplitAddr := uuid(4) // tokens spread across two addresses
		docs := []struct {
			id    strfmt.UUID
			props map[string]any
		}{
			{id: idMatch, props: map[string]any{"addresses": asArr(map[string]any{
				"owner": map[string]any{"title": "new york berlin tech", "name": "alice"},
			})}},
			{id: idNoMatchPartial, props: map[string]any{"addresses": asArr(map[string]any{
				"owner": map[string]any{"title": "new york berlin", "name": "alice"},
			})}},
			{id: idNoMatchWrongName, props: map[string]any{"addresses": asArr(map[string]any{
				"owner": map[string]any{"title": "new york berlin tech", "name": "bob"},
			})}},
			{id: idNoMatchSplitAddr, props: map[string]any{"addresses": asArr(
				map[string]any{"owner": map[string]any{"title": "new york", "name": "alice"}},
				map[string]any{"owner": map[string]any{"title": "berlin tech", "name": "alice"}},
			)}},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class2)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{Class: class2.Class, ID: d.id, Properties: d.props}, nil, nil, nil, nil, 0))
		}
		filter := andFilter(
			textFilter(class2.Class, "addresses.owner.title", "new york"),
			textFilter(class2.Class, "addresses.owner.title", "berlin"),
			textFilter(class2.Class, "addresses.owner.title", "tech"),
			textFilter(class2.Class, "addresses.owner.name", "alice"),
		)
		res, err := db.Search(ctx, dto.GetParams{ClassName: class2.Class, Pagination: &filters.Pagination{Limit: 100}, Filters: filter})
		require.NoError(t, err)
		got := make([]strfmt.UUID, len(res))
		for i, r := range res {
			got[i] = r.ID
		}
		assert.ElementsMatch(t, []strfmt.UUID{idMatch}, got)
	})

	// ----- Variant 3: object[] LCA (cars), text[] tags, multi-token + 1 single -----
	// Filter: garages.cars.tags = "new york" AND garages.cars.tags = "berlin"
	//         AND garages.cars.make = "tesla"
	// LCA = cars (object[]). Same car must have both tag values (multi-token "new
	// york" at one leaf, "berlin" at another leaf within the same cars element)
	// AND make=tesla.
	t.Run("objArr_LCA_cars.tags_multi_token_plus_single_AND_make", func(t *testing.T) {
		class3 := &models.Class{
			Class:             nestedClass + "3",
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{
					Name:     "cars",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: word, IndexFilterable: &vTrue},
						{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
					},
				}},
			}},
		}
		idMatch := uuid(1)               // cars[0].tags=["new york","berlin"], make="tesla"
		idNoMatchSplitCars := uuid(2)    // tags split across cars; one has make
		idNoMatchPartialTags := uuid(3)  // cars[0].tags=["new york"] only, make="tesla"
		idNoMatchWrongMake := uuid(4)    // cars[0] has both tags but make != tesla
		idNoMatchSplitGarages := uuid(5) // tags+make across separate garages
		docs := []struct {
			id    strfmt.UUID
			props map[string]any
		}{
			{id: idMatch, props: map[string]any{"garages": asArr(map[string]any{
				"cars": asArr(map[string]any{"tags": tagsAny("new york", "berlin"), "make": "tesla"}),
			})}},
			{id: idNoMatchSplitCars, props: map[string]any{"garages": asArr(map[string]any{
				"cars": asArr(
					map[string]any{"tags": tagsAny("new york"), "make": "tesla"},
					map[string]any{"tags": tagsAny("berlin"), "make": "tesla"},
				),
			})}},
			{id: idNoMatchPartialTags, props: map[string]any{"garages": asArr(map[string]any{
				"cars": asArr(map[string]any{"tags": tagsAny("new york"), "make": "tesla"}),
			})}},
			{id: idNoMatchWrongMake, props: map[string]any{"garages": asArr(map[string]any{
				"cars": asArr(map[string]any{"tags": tagsAny("new york", "berlin"), "make": "bmw"}),
			})}},
			{id: idNoMatchSplitGarages, props: map[string]any{"garages": asArr(
				map[string]any{"cars": asArr(map[string]any{"tags": tagsAny("new york", "berlin")})},
				map[string]any{"cars": asArr(map[string]any{"make": "tesla"})},
			)}},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class3)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{Class: class3.Class, ID: d.id, Properties: d.props}, nil, nil, nil, nil, 0))
		}
		filter := andFilter(
			textFilter(class3.Class, "garages.cars.tags", "new york"),
			textFilter(class3.Class, "garages.cars.tags", "berlin"),
			textFilter(class3.Class, "garages.cars.make", "tesla"),
		)
		res, err := db.Search(ctx, dto.GetParams{ClassName: class3.Class, Pagination: &filters.Pagination{Limit: 100}, Filters: filter})
		require.NoError(t, err)
		got := make([]strfmt.UUID, len(res))
		for i, r := range res {
			got[i] = r.ID
		}
		assert.ElementsMatch(t, []strfmt.UUID{idMatch}, got)
	})

	// ----- Variant 4: object[] LCA (cars), text make, multi-token + 2 singles -----
	// Filter: garages.cars.make = "new york" AND .make = "berlin" AND .make = "tech"
	//         AND garages.cars.model = "s"
	// make is text (single-valued per car), so all 4 make-tokens must come from
	// one make value (e.g. "new york berlin tech") on the same car as model="s".
	t.Run("objArr_LCA_cars.make_multi_token_plus_2_singles_AND_model", func(t *testing.T) {
		class4 := &models.Class{
			Class:             nestedClass + "4",
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{{
					Name:     "cars",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
						{Name: "model", DataType: schema.DataTypeText.PropString(), Tokenization: word, IndexFilterable: &vTrue},
					},
				}},
			}},
		}
		idMatch := uuid(1)            // make="new york berlin tech", model="s"
		idNoMatchSplitCars := uuid(2) // make tokens spread across cars
		idNoMatchWrongModel := uuid(3)
		idNoMatchPartial := uuid(4) // make="new york berlin" missing tech
		docs := []struct {
			id    strfmt.UUID
			props map[string]any
		}{
			{id: idMatch, props: map[string]any{"garages": asArr(map[string]any{
				"cars": asArr(map[string]any{"make": "new york berlin tech", "model": "s"}),
			})}},
			{id: idNoMatchSplitCars, props: map[string]any{"garages": asArr(map[string]any{
				"cars": asArr(
					map[string]any{"make": "new york", "model": "s"},
					map[string]any{"make": "berlin tech", "model": "s"},
				),
			})}},
			{id: idNoMatchWrongModel, props: map[string]any{"garages": asArr(map[string]any{
				"cars": asArr(map[string]any{"make": "new york berlin tech", "model": "x"}),
			})}},
			{id: idNoMatchPartial, props: map[string]any{"garages": asArr(map[string]any{
				"cars": asArr(map[string]any{"make": "new york berlin", "model": "s"}),
			})}},
		}
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class4)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{Class: class4.Class, ID: d.id, Properties: d.props}, nil, nil, nil, nil, 0))
		}
		filter := andFilter(
			textFilter(class4.Class, "garages.cars.make", "new york"),
			textFilter(class4.Class, "garages.cars.make", "berlin"),
			textFilter(class4.Class, "garages.cars.make", "tech"),
			textFilter(class4.Class, "garages.cars.model", "s"),
		)
		res, err := db.Search(ctx, dto.GetParams{ClassName: class4.Class, Pagination: &filters.Pagination{Limit: 100}, Filters: filter})
		require.NoError(t, err)
		got := make([]strfmt.UUID, len(res))
		for i, r := range res {
			got[i] = r.ID
		}
		assert.ElementsMatch(t, []strfmt.UUID{idMatch}, got)
	})
}

// TestNestedFilteringSamePathMultiValueNonTokenized closes a gap identified
// in the Tier 2 re-audit: lower-level TestResolveNestedCorrelatedAnd #7,
// #19, #20, #25 cover the non-tokenized variant of "same-path multi-value
// at object[] LCA combined with another path's clause". This shape is
// distinct from:
//
//   - TestNestedFilteringSamePathMultiValueTokenized — tokenized values in
//     the same-path clauses (different combinePositions branch)
//   - F_tags_double_value — same-path multi-value alone, no cross-path
//   - TestNestedFilteringComprehensive — cross-path scalar AND but no
//     same-path multi-value
//
// Both sub-tests use single-token (field-tokenized) text[] values so the
// dispatch goes through the all-independents branch of combinePositions.
func TestNestedFilteringSamePathMultiValueNonTokenized(t *testing.T) {
	const nestedClass = "MultiValueNonTokenized"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tagsAny := func(tags ...string) []any {
		out := make([]any, len(tags))
		for i, t := range tags {
			out[i] = t
		}
		return out
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}
	textFilter := func(class, path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: schema.ClassName(class), Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}
	type docDef struct {
		id    strfmt.UUID
		props map[string]any
	}
	runDocs := func(t *testing.T, class *models.Class, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{Class: class.Class, ID: d.id, Properties: d.props}, nil, nil, nil, nil, 0))
		}
		res, err := db.Search(ctx, dto.GetParams{ClassName: class.Class, Pagination: &filters.Pagination{Limit: 100}, Filters: filter})
		require.NoError(t, err)
		got := make([]strfmt.UUID, len(res))
		for i, r := range res {
			got[i] = r.ID
		}
		assert.ElementsMatch(t, want, got)
	}

	// ----- Sub-test 1: same-path multi-value + cross-path scalar -----
	// Filter: cars.tags = "electric" AND cars.tags = "sport" AND cars.make = "tesla"
	// Same car must have both tags AND the make. Tests combinePositions for
	// cars.tags producing a leaf-masked bitmap (multi-independents) that then
	// ANDs with the raw cars.make bitmap at the same LCA.
	t.Run("same_path_multi_value_AND_cross_path_scalar", func(t *testing.T) {
		class := &models.Class{
			Class:             nestedClass,
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{{
				Name:     "cars",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
					{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				},
			}},
		}
		idMatch := uuid(1)
		idNoMatchSplitTags := uuid(2)
		idNoMatchSplitTagsMake := uuid(3)
		idNoMatchPartialTags := uuid(4)
		idNoMatchWrongMake := uuid(5)
		idMatchSecondCar := uuid(6)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"cars": asArr(map[string]any{"tags": tagsAny("electric", "sport"), "make": "tesla"})}},
			{id: idNoMatchSplitTags, props: map[string]any{"cars": asArr(
				map[string]any{"tags": tagsAny("electric"), "make": "tesla"},
				map[string]any{"tags": tagsAny("sport"), "make": "tesla"},
			)}},
			{id: idNoMatchSplitTagsMake, props: map[string]any{"cars": asArr(
				map[string]any{"tags": tagsAny("electric", "sport")},
				map[string]any{"make": "tesla"},
			)}},
			{id: idNoMatchPartialTags, props: map[string]any{"cars": asArr(map[string]any{"tags": tagsAny("electric"), "make": "tesla"})}},
			{id: idNoMatchWrongMake, props: map[string]any{"cars": asArr(map[string]any{"tags": tagsAny("electric", "sport"), "make": "bmw"})}},
			{id: idMatchSecondCar, props: map[string]any{"cars": asArr(
				map[string]any{"tags": tagsAny("electric"), "make": "bmw"},
				map[string]any{"tags": tagsAny("electric", "sport"), "make": "tesla"},
			)}},
		}
		filter := andFilter(
			textFilter(nestedClass, "cars.tags", "electric"),
			textFilter(nestedClass, "cars.tags", "sport"),
			textFilter(nestedClass, "cars.make", "tesla"),
		)
		runDocs(t, class, docs, filter, []strfmt.UUID{idMatch, idMatchSecondCar})
	})

	// ----- Sub-test 2: two same-path multi-value paths (each text[]) -----
	// Filter: cars.tags = "electric" AND cars.tags = "sport"
	//         AND cars.labels = "red"  AND cars.labels = "blue"
	// Same car must have all four — both pairs of multi-values must coexist
	// at the same physical car. Both path bitmaps are leaf-masked
	// (multi-value), so the AND combines two MASKED bitmaps at the cars LCA.
	t.Run("two_same_path_multi_value_paths", func(t *testing.T) {
		class2 := &models.Class{
			Class:             nestedClass + "2",
			VectorIndexConfig: enthnsw.UserConfig{Skip: true},
			Properties: []*models.Property{{
				Name:     "cars",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
					{Name: "labels", DataType: schema.DataTypeTextArray.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				},
			}},
		}
		idMatch := uuid(1)
		idNoMatchSplitTags := uuid(2)
		idNoMatchSplitLabels := uuid(3)
		idNoMatchSplitBoth := uuid(4)
		idNoMatchPartialTags := uuid(5)
		idNoMatchPartialLabels := uuid(6)
		idMatchSecondCar := uuid(7)
		docs := []docDef{
			{id: idMatch, props: map[string]any{"cars": asArr(map[string]any{
				"tags": tagsAny("electric", "sport"), "labels": tagsAny("red", "blue"),
			})}},
			{id: idNoMatchSplitTags, props: map[string]any{"cars": asArr(
				map[string]any{"tags": tagsAny("electric"), "labels": tagsAny("red", "blue")},
				map[string]any{"tags": tagsAny("sport"), "labels": tagsAny("red", "blue")},
			)}},
			{id: idNoMatchSplitLabels, props: map[string]any{"cars": asArr(
				map[string]any{"tags": tagsAny("electric", "sport"), "labels": tagsAny("red")},
				map[string]any{"labels": tagsAny("blue")},
			)}},
			{id: idNoMatchSplitBoth, props: map[string]any{"cars": asArr(
				map[string]any{"tags": tagsAny("electric", "sport")},
				map[string]any{"labels": tagsAny("red", "blue")},
			)}},
			{id: idNoMatchPartialTags, props: map[string]any{"cars": asArr(map[string]any{
				"tags": tagsAny("electric"), "labels": tagsAny("red", "blue"),
			})}},
			{id: idNoMatchPartialLabels, props: map[string]any{"cars": asArr(map[string]any{
				"tags": tagsAny("electric", "sport"), "labels": tagsAny("red"),
			})}},
			{id: idMatchSecondCar, props: map[string]any{"cars": asArr(
				map[string]any{"tags": tagsAny("electric"), "labels": tagsAny("red")},
				map[string]any{"tags": tagsAny("electric", "sport"), "labels": tagsAny("red", "blue")},
			)}},
		}
		filter := andFilter(
			textFilter(class2.Class, "cars.tags", "electric"),
			textFilter(class2.Class, "cars.tags", "sport"),
			textFilter(class2.Class, "cars.labels", "red"),
			textFilter(class2.Class, "cars.labels", "blue"),
		)
		runDocs(t, class2, docs, filter, []strfmt.UUID{idMatch, idMatchSecondCar})
	})
}

// TestNestedFilteringContextCancellation verifies that a cancelled context
// propagates correctly through db.Search → nested-filter dispatch and is
// returned as context.Canceled. Mirrors the lower-level
// TestRecExecutorContextCancellation but at the production read path.
//
// Each sub-test exercises a different executor entry path:
//   - simple value (canUseRawAndAll)
//   - correlated AND with subs (runIdxLoopRecursive)
//   - IsNull standalone (rootAnchor)
//   - conflicting arr[N] partition (split)
//
// The lower-level test covers per-path internals (top-level + inner
// guards); this DB-level test verifies that whichever path the dispatch
// chooses, db.Search surfaces the cancellation correctly.
func TestNestedFilteringContextCancellation(t *testing.T) {
	const nestedClass = "CtxCancel"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{{
			Name:     "countries",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{{
				Name:     "garages",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
					{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
					{
						Name:     "cars",
						DataType: schema.DataTypeObjectArray.PropString(),
						NestedProperties: []*models.NestedProperty{
							{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
							{
								Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
								NestedProperties: []*models.NestedProperty{
									{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
								},
							},
						},
					},
				},
			}},
		}},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}

	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	bgCtx := context.Background()

	id1 := strfmt.UUID("00000000-0000-0000-0000-000000000001")
	require.NoError(t, db.PutObject(bgCtx, &models.Object{
		Class: nestedClass, ID: id1,
		Properties: map[string]any{"countries": asArr(map[string]any{
			"garages": asArr(map[string]any{
				"city": "berlin", "postcode": "10115",
				"cars": asArr(map[string]any{"make": "honda", "tires": asArr(map[string]any{"width": 205})}),
			}),
		})},
	}, nil, nil, nil, nil, 0))

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	isNullFilter := func(path string, isNull bool) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorIsNull,
			Value:    &filters.Value{Type: schema.DataTypeBoolean, Value: isNull},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}

	cancelled := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	runCancelled := func(t *testing.T, filter *filters.LocalFilter) {
		t.Helper()
		_, err := db.Search(cancelled(), dto.GetParams{
			ClassName:  nestedClass,
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    filter,
		})
		require.Error(t, err, "expected cancellation error")
		require.ErrorIs(t, err, context.Canceled, "expected context.Canceled, got: %v", err)
	}

	// canUseRawAndAll path: a single positive at intermediate scope —
	// GROUP@"garages.cars" with one here, no subs. Cancellation must surface
	// before the AndAll runs.
	t.Run("simple_value_canUseRawAndAll_path", func(t *testing.T) {
		runCancelled(t, textFilter("countries.garages.cars.make", "honda"))
	})

	// runIdxLoopRecursive path: two clauses at different LCAs forces evalGroup
	// into the idx-loop branch (subs reject canUseRawAndAll).
	t.Run("correlated_AND_idxLoopRecursive_path", func(t *testing.T) {
		runCancelled(t, andFilter(
			textFilter("countries.garages.postcode", "10115"),
			intFilter("countries.garages.cars.tires.width", 205),
		))
	})

	// rootAnchor path: standalone IsNull falls through to rootAnchor seeding.
	t.Run("isNull_rootAnchor_path", func(t *testing.T) {
		runCancelled(t, isNullFilter("countries.garages.cars.make", false))
	})

	// SPLIT path: two arr[N] indices on the same LCA → SPLIT@"garages" with
	// two branches. The cancellation must surface before evalSplit's branch
	// loop completes.
	t.Run("conflicting_arrN_split_path", func(t *testing.T) {
		runCancelled(t, andFilter(
			textFilter("countries.garages[0].city", "berlin"),
			textFilter("countries.garages[1].city", "munich"),
		))
	})
}

// TestNestedFilteringOrOfCorrelatedAnds tests `(A AND B) OR (C AND D)` shapes
// where each AND side is a same-element correlated AND. Verifies that the
// OR combinator correctly unions docID-level results from two independent
// correlated AND groups — neither side's positions should bleed into the
// other's same-element evaluation.
//
// Two sub-tests:
//
//   - Same LCA: both AND groups at cars LCA. Each side picks different
//     same-car combinations (tesla+205 vs honda+305). Doc with one car
//     satisfying either side matches.
//   - Different LCAs: left side at cars, right side at addresses. Each
//     AND-group resolves at its own LCA, then the two docID sets are
//     unioned.
func TestNestedFilteringOrOfCorrelatedAnds(t *testing.T) {
	const nestedClass = "OrOfCorrelatedAnds"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "addresses", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
			},
		},
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tire := func(width int) map[string]any { return map[string]any{"width": width} }
	carWith := func(make string, width int) map[string]any {
		out := map[string]any{}
		if make != "" {
			out["make"] = make
		}
		if width > 0 {
			out["tires"] = asArr(tire(width))
		}
		return out
	}
	addr := func(city, postcode string) map[string]any {
		out := map[string]any{}
		if city != "" {
			out["city"] = city
		}
		if postcode != "" {
			out["postcode"] = postcode
		}
		return out
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}
	orFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorOr, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}
	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ----- Sub-test 1: same LCA, both AND groups at cars -----
	// Filter: (cars.make=tesla AND cars.tires.width=205)
	//      OR (cars.make=honda AND cars.tires.width=305)
	// Each side requires a single car element satisfying both clauses on
	// that side. The OR unions docID-level results from two independent
	// correlated AND evaluations.
	t.Run("same_LCA_cars_make_AND_width_OR_make_AND_width", func(t *testing.T) {
		idMatchLeft := uuid(1)              // cars=[{tesla, 205}]
		idMatchRight := uuid(2)             // cars=[{honda, 305}]
		idMatchBoth := uuid(3)              // cars=[{tesla, 205},{honda, 305}]
		idMatchLeftViaSecondCar := uuid(4)  // first car wrong, second satisfies left
		idNoMatchTeslaWrongWidth := uuid(5) // cars=[{tesla, 305}] — left side wrong width
		idNoMatchSplitLeft := uuid(6)       // cars=[{tesla},{tires:[{205}]}] — split across cars
		idNoMatchNeither := uuid(7)         // cars=[{bmw, 225}]
		docs := []docDef{
			{id: idMatchLeft, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 205))}}, note: "tesla+205"},
			{id: idMatchRight, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("honda", 305))}}, note: "honda+305"},
			{id: idMatchBoth, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 205), carWith("honda", 305))}}, note: "both sides"},
			{id: idMatchLeftViaSecondCar, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 225), carWith("tesla", 205))}}, note: "[1] satisfies left"},
			{id: idNoMatchTeslaWrongWidth, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 305))}}, note: "tesla but wrong width; not honda"},
			{id: idNoMatchSplitLeft, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 0), carWith("", 205))}}, note: "tesla and 205 in different cars"},
			{id: idNoMatchNeither, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 225))}}, note: "neither side"},
		}
		filter := orFilter(
			andFilter(textFilter("doc.cars.make", "tesla"), intFilter("doc.cars.tires.width", 205)),
			andFilter(textFilter("doc.cars.make", "honda"), intFilter("doc.cars.tires.width", 305)),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchLeft, idMatchRight, idMatchBoth, idMatchLeftViaSecondCar})
	})

	// ----- Sub-test 2: different LCAs -----
	// Filter: (cars.make=tesla AND cars.tires.width=205)
	//      OR (addresses.city=berlin AND addresses.postcode=10115)
	// Left side at LCA=cars (same-car semantics). Right side at LCA=addresses
	// (same-address semantics). The OR unions docID-level results from two
	// independent correlated AND evaluations at distinct LCAs.
	t.Run("different_LCAs_cars_AND_OR_addresses_AND", func(t *testing.T) {
		idMatchLeft := uuid(1)         // cars=[{tesla,205}], no berlin address
		idMatchRight := uuid(2)        // addresses=[{berlin,10115}], no tesla car
		idMatchBoth := uuid(3)         // satisfies both sides
		idNoMatchSplitLeft := uuid(4)  // tesla and 205 in different cars
		idNoMatchSplitRight := uuid(5) // berlin and 10115 in different addresses
		idNoMatchNeither := uuid(6)
		docs := []docDef{
			{id: idMatchLeft, props: map[string]any{"doc": map[string]any{
				"cars":      asArr(carWith("tesla", 205)),
				"addresses": asArr(addr("munich", "80331")),
			}}, note: "left side only"},
			{id: idMatchRight, props: map[string]any{"doc": map[string]any{
				"cars":      asArr(carWith("bmw", 225)),
				"addresses": asArr(addr("berlin", "10115")),
			}}, note: "right side only"},
			{id: idMatchBoth, props: map[string]any{"doc": map[string]any{
				"cars":      asArr(carWith("tesla", 205)),
				"addresses": asArr(addr("berlin", "10115")),
			}}, note: "both sides"},
			{id: idNoMatchSplitLeft, props: map[string]any{"doc": map[string]any{
				"cars":      asArr(carWith("tesla", 0), carWith("", 205)),
				"addresses": asArr(addr("munich", "80331")),
			}}, note: "left split; right absent"},
			{id: idNoMatchSplitRight, props: map[string]any{"doc": map[string]any{
				"cars":      asArr(carWith("bmw", 225)),
				"addresses": asArr(addr("berlin", "00000"), addr("munich", "10115")),
			}}, note: "right split; left absent"},
			{id: idNoMatchNeither, props: map[string]any{"doc": map[string]any{
				"cars":      asArr(carWith("bmw", 225)),
				"addresses": asArr(addr("munich", "80331")),
			}}, note: "neither side"},
		}
		filter := orFilter(
			andFilter(textFilter("doc.cars.make", "tesla"), intFilter("doc.cars.tires.width", 205)),
			andFilter(textFilter("doc.addresses.city", "berlin"), textFilter("doc.addresses.postcode", "10115")),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idMatchLeft, idMatchRight, idMatchBoth})
	})
}

// TestNestedFilteringNotOfCorrelatedAnd tests `NOT (A AND B)` where the
// inner AND is a same-element correlated AND. Verifies that NOT inverts
// the docID-level result of the AND-group: docs without any car
// satisfying both clauses are returned; docs with at least one car
// satisfying both are excluded.
//
// Two sub-tests:
//   - NOT (cars.make=bmw AND cars.tires.width=205): basic same-element AND
//     wrapped in NOT.
//   - NOT (cars[0].make=tesla AND cars[1].make=bmw): NOT over a partitioned
//     AND (conflicting arr[N] indices). Verifies NOT correctly inverts the
//     partition result.
func TestNestedFilteringNotOfCorrelatedAnd(t *testing.T) {
	const nestedClass = "NotOfCorrelatedAnd"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tire := func(width int) map[string]any { return map[string]any{"width": width} }
	carWith := func(make string, width int) map[string]any {
		out := map[string]any{}
		if make != "" {
			out["make"] = make
		}
		if width > 0 {
			out["tires"] = asArr(tire(width))
		}
		return out
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}
	notFilter := func(inner *filters.LocalFilter) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorNot,
			Operands: []filters.Clause{*inner.Root},
		}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}
	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ----- Sub-test 1: NOT over basic correlated AND -----
	// Filter: NOT (cars.make=bmw AND cars.tires.width=205)
	// Match docs where it is NOT the case that some single car has both
	// make=bmw AND tires.width=205.
	t.Run("NOT_make_bmw_AND_width_205_same_car", func(t *testing.T) {
		idAndMatchSingle := uuid(1)      // single car has both → AND match → NOT excludes
		idAndMatchInSecondCar := uuid(2) // cars[1] has both → AND match → NOT excludes
		idBmwWrongWidth := uuid(3)       // bmw but width=225 → AND no match → NOT match
		idCorrectWidthWrongMake := uuid(4)
		idSplitAcrossCars := uuid(5) // bmw in cars[0], 205 in cars[1] → AND no match → NOT match
		idNoCarsArray := uuid(6)     // empty cars array
		idNoCarsField := uuid(7)     // no cars key
		idMultipleCarsNeitherSatisfies := uuid(8)
		docs := []docDef{
			{id: idAndMatchSingle, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 205))}}, note: "cars[0]={bmw,205}"},
			{id: idAndMatchInSecondCar, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 225), carWith("bmw", 205))}}, note: "cars[1]={bmw,205}"},
			{id: idBmwWrongWidth, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 225))}}, note: "bmw,225"},
			{id: idCorrectWidthWrongMake, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 205))}}, note: "tesla,205"},
			{id: idSplitAcrossCars, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 0), carWith("", 205))}}, note: "bmw and 205 in different cars"},
			{id: idNoCarsArray, props: map[string]any{"doc": map[string]any{"cars": []any{}}}, note: "empty cars"},
			{id: idNoCarsField, props: map[string]any{"doc": map[string]any{}}, note: "no cars field"},
			{id: idMultipleCarsNeitherSatisfies, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 225), carWith("tesla", 205))}}, note: "bmw+225 and tesla+205; no single car has both"},
		}
		filter := notFilter(andFilter(
			textFilter("doc.cars.make", "bmw"),
			intFilter("doc.cars.tires.width", 205),
		))
		runScenario(t, docs, filter, []strfmt.UUID{
			idBmwWrongWidth, idCorrectWidthWrongMake, idSplitAcrossCars,
			idNoCarsArray, idNoCarsField, idMultipleCarsNeitherSatisfies,
		})
	})

	// ----- Sub-test 2: NOT over partitioned AND with conflicting arr[N] -----
	// Filter: NOT (cars[0].make=tesla AND cars[1].make=bmw)
	// Inner AND: cars[0] must be tesla AND cars[1] must be bmw (different
	// physical positions enforced by arr[N] partition). NOT excludes docs
	// satisfying that arrangement.
	t.Run("NOT_cars[0].make_tesla_AND_cars[1].make_bmw_partition", func(t *testing.T) {
		idAndMatch := uuid(1)                // [tesla,bmw] → AND match → NOT excludes
		idSwapped := uuid(2)                 // [bmw,tesla] → AND no match → NOT match
		idOnlyFirst := uuid(3)               // only [0] → no [1] → AND no match → NOT match
		idCorrectFirstWrongSecond := uuid(4) // [tesla,volvo]
		idEmpty := uuid(5)                   // no cars
		idAndMatchExtraCars := uuid(6)       // [tesla,bmw,extra] → AND match → NOT excludes
		docs := []docDef{
			{id: idAndMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 0), carWith("bmw", 0))}}, note: "[tesla,bmw]"},
			{id: idSwapped, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 0), carWith("tesla", 0))}}, note: "[bmw,tesla]"},
			{id: idOnlyFirst, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 0))}}, note: "[tesla] only"},
			{id: idCorrectFirstWrongSecond, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 0), carWith("volvo", 0))}}, note: "[tesla,volvo]"},
			{id: idEmpty, props: map[string]any{"doc": map[string]any{}}, note: "no cars"},
			{id: idAndMatchExtraCars, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 0), carWith("bmw", 0), carWith("volvo", 0))}}, note: "[tesla,bmw,volvo]"},
		}
		filter := notFilter(andFilter(
			textFilter("doc.cars[0].make", "tesla"),
			textFilter("doc.cars[1].make", "bmw"),
		))
		runScenario(t, docs, filter, []strfmt.UUID{
			idSwapped, idOnlyFirst, idCorrectFirstWrongSecond, idEmpty,
		})
	})

	// ----- Sub-test 3 (regression baseline): NOT inside correlated AND -----
	// Filter: cars.make = "tesla" AND NOT cars.tires.width = 205
	//
	// This locks in the *current* docID-level (universal) semantics for NOT.
	// Two docs (idSomeTeslaWithout205 and idTwoTeslasOneHas205) discriminate
	// between universal and per-element semantics — they would flip from "no
	// match" to "match" if NOT inside a correlated AND were rewritten to per-
	// element evaluation. See per-element NOT-in-AND rewrite.
	//
	// Universal interpretation (current):
	//   A = docs with some tesla car
	//   B = docs where NO car has width=205
	//   Result = A ∩ B
	//
	// Per-element interpretation (future, if implemented):
	//   For each car element K: this car is tesla AND this car doesn't have width=205
	//   Result = docs where ANY car satisfies the per-element condition
	//
	// TODO aliszka:nested_filtering: locks in CURRENT universal NOT
	// behavior. Flips when per-element NOT lands
	// (per-element NOT-in-AND rewrite) — discriminator docs
	// idSomeTeslaWithout205 and idTwoTeslasOneHas205 will start matching.
	t.Run("regression_NOT_inside_AND_universal_docID_level", func(t *testing.T) {
		idTeslaNo205 := uuid(1)          // single tesla car w/o 205 — both interpretations: match
		idTeslaWith205 := uuid(2)        // single tesla car with 205 — both: no match
		idSomeTeslaWithout205 := uuid(3) // tesla(225) + bmw(205) — universal: NO; per-element: YES
		idTeslaWith205PlusBmw := uuid(4) // tesla(205) + bmw(225) — both: no match
		idNoTesla := uuid(5)             // no tesla at all — both: no match
		idTwoTeslasOneHas205 := uuid(6)  // tesla(225) + tesla(205) — universal: NO; per-element: YES
		idEmpty := uuid(7)               // no cars — both: no match
		docs := []docDef{
			{id: idTeslaNo205, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 225))}}, note: "tesla,225"},
			{id: idTeslaWith205, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 205))}}, note: "tesla,205"},
			{id: idSomeTeslaWithout205, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 225), carWith("bmw", 205))}}, note: "tesla,225 + bmw,205 — DISCRIMINATOR"},
			{id: idTeslaWith205PlusBmw, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 205), carWith("bmw", 225))}}, note: "tesla,205 + bmw,225"},
			{id: idNoTesla, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 205))}}, note: "bmw,205 — no tesla"},
			{id: idTwoTeslasOneHas205, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 225), carWith("tesla", 205))}}, note: "tesla,225 + tesla,205 — DISCRIMINATOR"},
			{id: idEmpty, props: map[string]any{"doc": map[string]any{}}, note: "no cars"},
		}
		filter := andFilter(
			textFilter("doc.cars.make", "tesla"),
			notFilter(intFilter("doc.cars.tires.width", 205)),
		)
		// Current behavior: only idTeslaNo205 matches. Two discriminator docs
		// (idSomeTeslaWithout205, idTwoTeslasOneHas205) are excluded because
		// some car (any car) has width=205, even though a tesla without 205
		// also exists. If per-element NOT lands, both flip to match.
		runScenario(t, docs, filter, []strfmt.UUID{idTeslaNo205})
	})
}

// TestNestedFilteringNotEqualNestedRegression locks in the current
// docID-level (universal) semantics of NotEqual on nested-leaf paths.
// NotEqual's row-reader implementation produces a denylist: same bitmap
// as Equal, flagged isDenyList=true. The searcher inverts at docID level,
// giving "no element has the value" — universal/NONE-like, NOT consistent
// with Equal's existential semantics on the same path.
//
// Two notable consequences locked in by this test:
//
//  1. A doc with mixed values (`[{bmw}, {tesla}]`) is excluded by
//     NotEqual bmw, even though some element ≠ bmw exists. Existential
//     interpretation would match.
//  2. A doc with NO elements at all (no cars, or empty array) IS matched
//     by NotEqual bmw — vacuously true under universal. Existential would
//     exclude it (no element satisfies the predicate). This is also
//     asymmetric with Equal: `cars.make = bmw` correctly excludes
//     empty-cars docs.
//
// Discriminator docs (idMixed, idEmpty, idEmptyArr) flip from match-or-not
// when the uniform-existential switch in
// explicit ANY/ALL/NONE quantifiers lands.
//
// Sub-test 2 mirrors regression_NOT_inside_AND_universal_docID_level for
// NotEqual inside a correlated AND, showing that NOT and NotEqual share
// the same per-element-vs-docID-level mismatch.
func TestNestedFilteringNotEqualNestedRegression(t *testing.T) {
	const nestedClass = "NotEqualRegression"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tire := func(width int) map[string]any { return map[string]any{"width": width} }
	carWith := func(make string, width int) map[string]any {
		out := map[string]any{}
		if make != "" {
			out["make"] = make
		}
		if width > 0 {
			out["tires"] = asArr(tire(width))
		}
		return out
	}

	textNotEqualFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorNotEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intNotEqualFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorNotEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}
	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// ----- Sub-test 1: basic NotEqual at nested leaf -----
	// Filter: cars.make NotEqual "bmw"
	//
	// Universal (current): match docs where NO car has make=bmw.
	// Existential (proposed): match docs where ANY car has make≠bmw.
	//
	// TODO aliszka:nested_filtering: locks in CURRENT universal NotEqual
	// behavior. Discriminators idMixed/idEmpty/idEmptyArr flip when
	// uniform-existential rewrite lands (explicit ANY/ALL/NONE quantifiers).
	// The recommended NotEqual → NOT(Equal) rewrite would inherit whatever
	// NOT does (currently universal) — this expectation flips together with
	// regression_NOT_inside_AND_universal_docID_level.
	t.Run("regression_basic_NotEqual_universal_docID_level", func(t *testing.T) {
		idOnlyBmw := uuid(1)
		idOnlyTesla := uuid(2)
		idMixed := uuid(3)
		idEmpty := uuid(4)
		idEmptyArr := uuid(5)
		docs := []docDef{
			{id: idOnlyBmw, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 0))}}, note: "bmw"},
			{id: idOnlyTesla, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 0))}}, note: "tesla"},
			{id: idMixed, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 0), carWith("tesla", 0))}}, note: "[bmw,tesla] — DISCRIMINATOR"},
			{id: idEmpty, props: map[string]any{"doc": map[string]any{}}, note: "no cars — DISCRIMINATOR"},
			{id: idEmptyArr, props: map[string]any{"doc": map[string]any{"cars": []any{}}}, note: "cars=[] — DISCRIMINATOR"},
		}
		filter := textNotEqualFilter("doc.cars.make", "bmw")
		// Current universal: idOnlyTesla + idEmpty + idEmptyArr.
		// Existential would: idOnlyTesla + idMixed (and exclude empty docs).
		runScenario(t, docs, filter, []strfmt.UUID{idOnlyTesla, idEmpty, idEmptyArr})
	})

	// ----- Sub-test 2: NotEqual inside correlated AND (BUG REGRESSION) -----
	// Filter: cars.make = "tesla" AND cars.tires.width NotEqual 205
	//
	// **This sub-test locks in BUGGY current behavior, NOT correct behavior.**
	//
	// Verified by experiment: NotEqual's denylist flag is dropped (or
	// inverted) when its bitmap is combined with another nested clause via
	// correlated AND. The filter incorrectly matches docs where
	// `tesla AND width=205 (same car)` — the exact OPPOSITE of what
	// NotEqual should produce.
	//
	// Doc-by-doc analysis with the current bug:
	//   idTeslaNo205    : [{tesla,225}]               → tesla yes, 205 no → bug excludes (correct expectation: include)
	//   idTeslaWith205  : [{tesla,205}]               → same car has tesla AND 205 → bug includes (correct expectation: exclude)
	//   idSomeTeslaWO205: [{tesla,225},{bmw,205}]     → no single car has tesla AND 205 → bug excludes
	//   idTeslaWith205+ : [{tesla,205},{bmw,225}]     → cars[0] has tesla AND 205 → bug includes (correct expectation: exclude)
	//   idNoTesla       : [{bmw,205}]                 → no tesla → bug excludes
	//   idTwoTeslas1@205: [{tesla,225},{tesla,205}]   → cars[1] has tesla AND 205 → bug includes (correct expectation: exclude)
	//   idEmpty         : (no cars)                   → bug excludes (universal-correct expectation: include)
	//
	// When the bug is fixed (whether to universal denylist or the proposed
	// uniform-existential semantics), this test will fail and the expected
	// list must be updated:
	//   - Universal-correct fix: [idTeslaNo205, idEmpty]
	//   - Uniform-existential fix: [idTeslaNo205, idSomeTeslaWO205, idTwoTeslas1@205]
	//
	// Filed alongside explicit ANY/ALL/NONE quantifiers as a separate bug
	// requiring fix regardless of the existential-semantics direction.
	//
	// TODO aliszka:nested_filtering: this is a CORRECTNESS BUG (not a
	// semantics decision). NotEqual returns the OPPOSITE of expected
	// inside a nested correlated AND. Fix path:
	// `NotEqual → NOT(Equal) rewrite` recommends rewriting
	// NotEqual → NOT(Equal) at extractPropValuePair (~0.5 day, independent
	// of any existential-semantics decision). When fixed, expected list
	// flips to [idTeslaNo205, idEmpty] (universal-correct) or
	// [idTeslaNo205, idSomeTeslaWithout205, idTwoTeslasOneHas205]
	// (uniform-existential).
	t.Run("regression_BUG_NotEqual_inside_AND_treated_as_Equal", func(t *testing.T) {
		idTeslaNo205 := uuid(1)
		idTeslaWith205 := uuid(2)
		idSomeTeslaWithout205 := uuid(3)
		idTeslaWith205PlusBmw := uuid(4)
		idNoTesla := uuid(5)
		idTwoTeslasOneHas205 := uuid(6)
		idEmpty := uuid(7)
		docs := []docDef{
			{id: idTeslaNo205, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 225))}}, note: "tesla,225"},
			{id: idTeslaWith205, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 205))}}, note: "tesla,205"},
			{id: idSomeTeslaWithout205, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 225), carWith("bmw", 205))}}, note: "tesla,225 + bmw,205"},
			{id: idTeslaWith205PlusBmw, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 205), carWith("bmw", 225))}}, note: "tesla,205 + bmw,225"},
			{id: idNoTesla, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("bmw", 205))}}, note: "bmw,205"},
			{id: idTwoTeslasOneHas205, props: map[string]any{"doc": map[string]any{"cars": asArr(carWith("tesla", 225), carWith("tesla", 205))}}, note: "tesla,225 + tesla,205"},
			{id: idEmpty, props: map[string]any{"doc": map[string]any{}}, note: "no cars"},
		}
		filter := andFilter(
			textFilter("doc.cars.make", "tesla"),
			intNotEqualFilter("doc.cars.tires.width", 205),
		)
		// BUG: NotEqual is treated as Equal in correlated AND. The matches
		// below are docs where `tesla AND width=205 (same car)` — the
		// opposite of what NotEqual should produce.
		runScenario(t, docs, filter, []strfmt.UUID{
			idTeslaWith205, idTeslaWith205PlusBmw, idTwoTeslasOneHas205,
		})
	})
}

// TestNestedFilteringOrOfCorrelatedAndsWithArrN tests OR-of-correlated-ANDs
// shapes where one or both AND sides are pinned to specific arr[N] indices.
// The combinator boundary tested: groupChildrenByArrayIndicesKey
// partitions/correlates per-K within an AND; the OR combinator must union
// independent partition groups at the docID level (not at per-position
// level).
//
// Coverage matrix: 5 LCA depths × 2 OR shapes = 10 sub-tests.
//
// LCA depths (path: countries.garages.cars):
//   - country (object) root, L1 garages[N]
//   - country (object) root, L2 cars[N]
//   - countries (object[]) root[N]
//   - countries (object[]) root, L1 garages[N]
//   - countries (object[]) root, L2 cars[N]
//
// Shapes:
//   - Shape 1: simple arr[N] clause OR (correlated AND at different K)
//   - Shape 2: (correlated AND at K=0) OR (correlated AND at K=1)
//
// Each sub-test includes a "values at wrong index" partition discriminator
// — a doc where the values that *would* match each side exist, but at the
// wrong arr[N] positions. If the OR combinator incorrectly unioned
// per-position bitmaps across operands, this doc would falsely match.
func TestNestedFilteringOrOfCorrelatedAndsWithArrN(t *testing.T) {
	const nestedClass = "OrCorrelatedAndsArrN"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	garageProps := []*models.NestedProperty{
		{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	rootInner := []*models.NestedProperty{
		{Name: "garages", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: garageProps},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "country", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootInner},
			{Name: "countries", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: rootInner},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tire := func(width int) map[string]any { return map[string]any{"width": width} }
	carM := func(make string) map[string]any { return map[string]any{"make": make} }
	carMW := func(make string, width int) map[string]any {
		return map[string]any{"make": make, "tires": asArr(tire(width))}
	}
	carW := func(width int) map[string]any { return map[string]any{"tires": asArr(tire(width))} }
	garageCity := func(city string) map[string]any { return map[string]any{"city": city} }
	garageCP := func(city, postcode string) map[string]any {
		return map[string]any{"city": city, "postcode": postcode}
	}
	garageP := func(postcode string) map[string]any { return map[string]any{"postcode": postcode} }
	garageWithCars := func(cars ...map[string]any) map[string]any {
		anyCars := make([]any, len(cars))
		for i, c := range cars {
			anyCars[i] = c
		}
		return map[string]any{"cars": anyCars}
	}
	countryWithGarages := func(garages ...map[string]any) map[string]any {
		anyG := make([]any, len(garages))
		for i, g := range garages {
			anyG[i] = g
		}
		return map[string]any{"garages": anyG}
	}

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}
	orFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorOr, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}
	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	withCountry := func(garages ...map[string]any) map[string]any {
		return map[string]any{"country": countryWithGarages(garages...)}
	}
	withCountries := func(countries ...map[string]any) map[string]any {
		anyC := make([]any, len(countries))
		for i, c := range countries {
			anyC[i] = c
		}
		return map[string]any{"countries": anyC}
	}

	// =========================================================================
	// country (object) root, L1 garages[N]
	// =========================================================================

	t.Run("country_object_L1_garages[N]_Shape1_simple_OR_correlated", func(t *testing.T) {
		// Filter: country.garages[0].city="berlin"
		//      OR (country.garages[1].city="munich" AND country.garages[1].postcode="80331")
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idNeitherWrongPostcode := uuid(4)
		idNeitherSplitClauses := uuid(5)
		idValuesAtWrongIndex := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountry(garageCity("berlin")), note: "garages[0]=berlin; no [1]"},
			{id: idRightOnly, props: withCountry(garageCity("paris"), garageCP("munich", "80331")), note: "[0]=paris; [1]=munich+80331"},
			{id: idBoth, props: withCountry(garageCity("berlin"), garageCP("munich", "80331")), note: "both"},
			{id: idNeitherWrongPostcode, props: withCountry(garageCity("paris"), garageCP("munich", "99999")), note: "[1] munich but wrong postcode"},
			{id: idNeitherSplitClauses, props: withCountry(garageCity("paris"), garageCity("munich"), garageP("80331")), note: "[1] munich no postcode; [2] postcode but not pinned"},
			{id: idValuesAtWrongIndex, props: withCountry(garageCP("munich", "80331"), garageCity("berlin")), note: "DISCRIMINATOR: values swapped"},
		}
		filter := orFilter(
			textFilter("country.garages[0].city", "berlin"),
			andFilter(
				textFilter("country.garages[1].city", "munich"),
				textFilter("country.garages[1].postcode", "80331"),
			),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})

	t.Run("country_object_L1_garages[N]_Shape2_correlated_OR_correlated", func(t *testing.T) {
		// Filter: (country.garages[0].city="berlin" AND country.garages[0].postcode="10115")
		//      OR (country.garages[1].city="munich" AND country.garages[1].postcode="80331")
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idLeftWrongPostcode := uuid(4)
		idRightWrongPostcode := uuid(5)
		idValuesAtWrongIndex := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountry(garageCP("berlin", "10115")), note: "[0]=berlin+10115"},
			{id: idRightOnly, props: withCountry(garageCity("paris"), garageCP("munich", "80331")), note: "[1]=munich+80331"},
			{id: idBoth, props: withCountry(garageCP("berlin", "10115"), garageCP("munich", "80331")), note: "both"},
			{id: idLeftWrongPostcode, props: withCountry(garageCP("berlin", "99999")), note: "[0] berlin but wrong postcode"},
			{id: idRightWrongPostcode, props: withCountry(garageCity("paris"), garageCP("munich", "99999")), note: "[1] munich but wrong postcode"},
			{id: idValuesAtWrongIndex, props: withCountry(garageCP("munich", "80331"), garageCP("berlin", "10115")), note: "DISCRIMINATOR: swapped"},
		}
		filter := orFilter(
			andFilter(textFilter("country.garages[0].city", "berlin"), textFilter("country.garages[0].postcode", "10115")),
			andFilter(textFilter("country.garages[1].city", "munich"), textFilter("country.garages[1].postcode", "80331")),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})

	// =========================================================================
	// country (object) root, L2 cars[N]
	// =========================================================================

	t.Run("country_object_L2_cars[N]_Shape1_simple_OR_correlated", func(t *testing.T) {
		// Filter: country.garages.cars[0].make="tesla"
		//      OR (country.garages.cars[1].make="bmw" AND country.garages.cars[1].tires.width=205)
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idNeitherWrongWidth := uuid(4)
		idNeitherSplitClauses := uuid(5)
		idValuesAtWrongIndex := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountry(garageWithCars(carM("tesla"))), note: "cars[0]=tesla; no [1]"},
			{id: idRightOnly, props: withCountry(garageWithCars(carM("volvo"), carMW("bmw", 205))), note: "cars[0]=volvo; cars[1]=bmw+205"},
			{id: idBoth, props: withCountry(garageWithCars(carM("tesla"), carMW("bmw", 205))), note: "both"},
			{id: idNeitherWrongWidth, props: withCountry(garageWithCars(carM("volvo"), carMW("bmw", 225))), note: "cars[1] bmw but width=225"},
			{id: idNeitherSplitClauses, props: withCountry(garageWithCars(carM("volvo"), carM("bmw"), carW(205))), note: "cars[1] bmw no tires; cars[2] tires but not pinned"},
			{id: idValuesAtWrongIndex, props: withCountry(garageWithCars(carMW("bmw", 205), carM("tesla"))), note: "DISCRIMINATOR: cars[0]=bmw+205, cars[1]=tesla"},
		}
		filter := orFilter(
			textFilter("country.garages.cars[0].make", "tesla"),
			andFilter(
				textFilter("country.garages.cars[1].make", "bmw"),
				intFilter("country.garages.cars[1].tires.width", 205),
			),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})

	t.Run("country_object_L2_cars[N]_Shape2_correlated_OR_correlated", func(t *testing.T) {
		// Filter: (country.garages.cars[0].make="tesla" AND .tires.width=205)
		//      OR (country.garages.cars[1].make="bmw"   AND .tires.width=305)
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idLeftWrongWidth := uuid(4)
		idRightWrongWidth := uuid(5)
		idValuesAtWrongIndex := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountry(garageWithCars(carMW("tesla", 205))), note: "cars[0]=tesla+205"},
			{id: idRightOnly, props: withCountry(garageWithCars(carM("volvo"), carMW("bmw", 305))), note: "cars[1]=bmw+305"},
			{id: idBoth, props: withCountry(garageWithCars(carMW("tesla", 205), carMW("bmw", 305))), note: "both"},
			{id: idLeftWrongWidth, props: withCountry(garageWithCars(carMW("tesla", 225))), note: "cars[0] tesla but 225"},
			{id: idRightWrongWidth, props: withCountry(garageWithCars(carM("volvo"), carMW("bmw", 225))), note: "cars[1] bmw but 225"},
			{id: idValuesAtWrongIndex, props: withCountry(garageWithCars(carMW("bmw", 305), carMW("tesla", 205))), note: "DISCRIMINATOR: swapped"},
		}
		filter := orFilter(
			andFilter(textFilter("country.garages.cars[0].make", "tesla"), intFilter("country.garages.cars[0].tires.width", 205)),
			andFilter(textFilter("country.garages.cars[1].make", "bmw"), intFilter("country.garages.cars[1].tires.width", 305)),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})

	// =========================================================================
	// countries (object[]) root[N]
	// =========================================================================

	t.Run("countries_array_root[N]_Shape1_simple_OR_correlated", func(t *testing.T) {
		// Filter: countries[0].garages.cars.make="tesla"
		//      OR (countries[1].garages.cars.make="bmw" AND countries[1].garages.cars.tires.width=205)
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idNeitherWrongWidth := uuid(4)
		idNeitherSplitInsideRight := uuid(5)
		idValuesAtWrongCountry := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountries(countryWithGarages(garageWithCars(carM("tesla")))), note: "countries[0] tesla; no [1]"},
			{id: idRightOnly, props: withCountries(
				countryWithGarages(garageWithCars(carM("volvo"))),
				countryWithGarages(garageWithCars(carMW("bmw", 205))),
			), note: "countries[1]=bmw+205"},
			{id: idBoth, props: withCountries(
				countryWithGarages(garageWithCars(carM("tesla"))),
				countryWithGarages(garageWithCars(carMW("bmw", 205))),
			), note: "both"},
			{id: idNeitherWrongWidth, props: withCountries(
				countryWithGarages(garageWithCars(carM("volvo"))),
				countryWithGarages(garageWithCars(carMW("bmw", 225))),
			), note: "countries[1] bmw but 225"},
			{id: idNeitherSplitInsideRight, props: withCountries(
				countryWithGarages(garageWithCars(carM("volvo"))),
				countryWithGarages(garageWithCars(carM("bmw")), garageWithCars(carW(205))),
			), note: "countries[1] bmw and 205 in different garages"},
			{id: idValuesAtWrongCountry, props: withCountries(
				countryWithGarages(garageWithCars(carMW("bmw", 205))),
				countryWithGarages(garageWithCars(carM("tesla"))),
			), note: "DISCRIMINATOR: countries[0]=bmw+205, countries[1]=tesla"},
		}
		filter := orFilter(
			textFilter("countries[0].garages.cars.make", "tesla"),
			andFilter(
				textFilter("countries[1].garages.cars.make", "bmw"),
				intFilter("countries[1].garages.cars.tires.width", 205),
			),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})

	t.Run("countries_array_root[N]_Shape2_correlated_OR_correlated", func(t *testing.T) {
		// Filter: (countries[0].garages.cars.make="tesla" AND countries[0].garages.cars.tires.width=205)
		//      OR (countries[1].garages.cars.make="bmw"   AND countries[1].garages.cars.tires.width=305)
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idLeftWrongWidth := uuid(4)
		idRightWrongWidth := uuid(5)
		idValuesAtWrongCountry := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountries(countryWithGarages(garageWithCars(carMW("tesla", 205)))), note: "countries[0]=tesla+205"},
			{id: idRightOnly, props: withCountries(
				countryWithGarages(garageWithCars(carM("volvo"))),
				countryWithGarages(garageWithCars(carMW("bmw", 305))),
			), note: "countries[1]=bmw+305"},
			{id: idBoth, props: withCountries(
				countryWithGarages(garageWithCars(carMW("tesla", 205))),
				countryWithGarages(garageWithCars(carMW("bmw", 305))),
			), note: "both"},
			{id: idLeftWrongWidth, props: withCountries(countryWithGarages(garageWithCars(carMW("tesla", 225)))), note: "tesla but 225"},
			{id: idRightWrongWidth, props: withCountries(
				countryWithGarages(garageWithCars(carM("volvo"))),
				countryWithGarages(garageWithCars(carMW("bmw", 225))),
			), note: "bmw but 225"},
			{id: idValuesAtWrongCountry, props: withCountries(
				countryWithGarages(garageWithCars(carMW("bmw", 305))),
				countryWithGarages(garageWithCars(carMW("tesla", 205))),
			), note: "DISCRIMINATOR: swapped"},
		}
		filter := orFilter(
			andFilter(textFilter("countries[0].garages.cars.make", "tesla"), intFilter("countries[0].garages.cars.tires.width", 205)),
			andFilter(textFilter("countries[1].garages.cars.make", "bmw"), intFilter("countries[1].garages.cars.tires.width", 305)),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})

	// =========================================================================
	// countries (object[]) L1 garages[N]
	// =========================================================================

	t.Run("countries_array_L1_garages[N]_Shape1_simple_OR_correlated", func(t *testing.T) {
		// Filter: countries.garages[0].city="berlin"
		//      OR (countries.garages[1].city="munich" AND countries.garages[1].postcode="80331")
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idNeitherWrongPostcode := uuid(4)
		idAcrossCountries := uuid(5)
		idValuesAtWrongIndex := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountries(countryWithGarages(garageCity("berlin"))), note: "garages[0]=berlin"},
			{id: idRightOnly, props: withCountries(countryWithGarages(garageCity("paris"), garageCP("munich", "80331"))), note: "garages[1]=munich+80331"},
			{id: idBoth, props: withCountries(countryWithGarages(garageCity("berlin"), garageCP("munich", "80331"))), note: "both"},
			{id: idNeitherWrongPostcode, props: withCountries(countryWithGarages(garageCity("paris"), garageCP("munich", "99999"))), note: "garages[1] munich wrong postcode"},
			{id: idAcrossCountries, props: withCountries(
				countryWithGarages(garageCity("paris")),
				countryWithGarages(garageCity("paris"), garageCP("munich", "80331")),
			), note: "countries[1].garages[1] satisfies right (existential over countries)"},
			{id: idValuesAtWrongIndex, props: withCountries(countryWithGarages(garageCP("munich", "80331"), garageCity("berlin"))), note: "DISCRIMINATOR: swapped"},
		}
		filter := orFilter(
			textFilter("countries.garages[0].city", "berlin"),
			andFilter(
				textFilter("countries.garages[1].city", "munich"),
				textFilter("countries.garages[1].postcode", "80331"),
			),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth, idAcrossCountries})
	})

	t.Run("countries_array_L1_garages[N]_Shape2_correlated_OR_correlated", func(t *testing.T) {
		// Filter: (countries.garages[0].city="berlin" AND countries.garages[0].postcode="10115")
		//      OR (countries.garages[1].city="munich" AND countries.garages[1].postcode="80331")
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idLeftWrongPostcode := uuid(4)
		idRightWrongPostcode := uuid(5)
		idValuesAtWrongIndex := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountries(countryWithGarages(garageCP("berlin", "10115"))), note: "garages[0]=berlin+10115"},
			{id: idRightOnly, props: withCountries(countryWithGarages(garageCity("paris"), garageCP("munich", "80331"))), note: "garages[1]=munich+80331"},
			{id: idBoth, props: withCountries(countryWithGarages(garageCP("berlin", "10115"), garageCP("munich", "80331"))), note: "both"},
			{id: idLeftWrongPostcode, props: withCountries(countryWithGarages(garageCP("berlin", "99999"))), note: "berlin but wrong postcode"},
			{id: idRightWrongPostcode, props: withCountries(countryWithGarages(garageCity("paris"), garageCP("munich", "99999"))), note: "munich but wrong postcode"},
			{id: idValuesAtWrongIndex, props: withCountries(countryWithGarages(garageCP("munich", "80331"), garageCP("berlin", "10115"))), note: "DISCRIMINATOR: swapped"},
		}
		filter := orFilter(
			andFilter(textFilter("countries.garages[0].city", "berlin"), textFilter("countries.garages[0].postcode", "10115")),
			andFilter(textFilter("countries.garages[1].city", "munich"), textFilter("countries.garages[1].postcode", "80331")),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})

	// =========================================================================
	// countries (object[]) L2 cars[N]
	// =========================================================================

	t.Run("countries_array_L2_cars[N]_Shape1_simple_OR_correlated", func(t *testing.T) {
		// Filter: countries.garages.cars[0].make="tesla"
		//      OR (countries.garages.cars[1].make="bmw" AND countries.garages.cars[1].tires.width=205)
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idNeitherWrongWidth := uuid(4)
		idNeitherSplitClauses := uuid(5)
		idValuesAtWrongIndex := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountries(countryWithGarages(garageWithCars(carM("tesla")))), note: "cars[0]=tesla"},
			{id: idRightOnly, props: withCountries(countryWithGarages(garageWithCars(carM("volvo"), carMW("bmw", 205)))), note: "cars[1]=bmw+205"},
			{id: idBoth, props: withCountries(countryWithGarages(garageWithCars(carM("tesla"), carMW("bmw", 205)))), note: "both"},
			{id: idNeitherWrongWidth, props: withCountries(countryWithGarages(garageWithCars(carM("volvo"), carMW("bmw", 225)))), note: "cars[1] bmw but 225"},
			{id: idNeitherSplitClauses, props: withCountries(countryWithGarages(garageWithCars(carM("volvo"), carM("bmw"), carW(205)))), note: "cars[1] bmw no tires; cars[2] not pinned"},
			{id: idValuesAtWrongIndex, props: withCountries(countryWithGarages(garageWithCars(carMW("bmw", 205), carM("tesla")))), note: "DISCRIMINATOR: swapped"},
		}
		filter := orFilter(
			textFilter("countries.garages.cars[0].make", "tesla"),
			andFilter(
				textFilter("countries.garages.cars[1].make", "bmw"),
				intFilter("countries.garages.cars[1].tires.width", 205),
			),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})

	t.Run("countries_array_L2_cars[N]_Shape2_correlated_OR_correlated", func(t *testing.T) {
		// Filter: (countries.garages.cars[0].make="tesla" AND .tires.width=205)
		//      OR (countries.garages.cars[1].make="bmw"   AND .tires.width=305)
		idLeftOnly := uuid(1)
		idRightOnly := uuid(2)
		idBoth := uuid(3)
		idLeftWrongWidth := uuid(4)
		idRightWrongWidth := uuid(5)
		idValuesAtWrongIndex := uuid(6)
		docs := []docDef{
			{id: idLeftOnly, props: withCountries(countryWithGarages(garageWithCars(carMW("tesla", 205)))), note: "cars[0]=tesla+205"},
			{id: idRightOnly, props: withCountries(countryWithGarages(garageWithCars(carM("volvo"), carMW("bmw", 305)))), note: "cars[1]=bmw+305"},
			{id: idBoth, props: withCountries(countryWithGarages(garageWithCars(carMW("tesla", 205), carMW("bmw", 305)))), note: "both"},
			{id: idLeftWrongWidth, props: withCountries(countryWithGarages(garageWithCars(carMW("tesla", 225)))), note: "tesla but 225"},
			{id: idRightWrongWidth, props: withCountries(countryWithGarages(garageWithCars(carM("volvo"), carMW("bmw", 225)))), note: "bmw but 225"},
			{id: idValuesAtWrongIndex, props: withCountries(countryWithGarages(garageWithCars(carMW("bmw", 305), carMW("tesla", 205)))), note: "DISCRIMINATOR: swapped"},
		}
		filter := orFilter(
			andFilter(textFilter("countries.garages.cars[0].make", "tesla"), intFilter("countries.garages.cars[0].tires.width", 205)),
			andFilter(textFilter("countries.garages.cars[1].make", "bmw"), intFilter("countries.garages.cars[1].tires.width", 305)),
		)
		runScenario(t, docs, filter, []strfmt.UUID{idLeftOnly, idRightOnly, idBoth})
	})
}

// TestNestedFilteringAndOfOrRegression locks in the *current* docID-level
// semantics of `A AND (B OR C)` for nested-leaf paths. Today the OR child
// resolves to a docID set independently and the outer AND intersects at
// docID level — same-element correlation does NOT propagate across the OR
// boundary. See OR-in-AND distribution rewrite for the proposed
// fix (distribute OR over AND at extractPropValuePair time, giving same-
// element semantics per DNF branch).
//
// The discriminator docs (idDiscriminator*) currently MATCH because the
// dispatch only requires "honda exists somewhere AND (205 or 225 exists
// somewhere)". Under per-element AND-of-OR (the proposed direction), they
// would NOT match because no single car satisfies (honda AND 205) or
// (honda AND 225).
//
// When OR distribution lands, this test will fail and the expected list
// must flip from [idSameCarMatch, idSameCarMatchAlt, idDiscriminatorSplit*,
// idDiscriminatorAcrossCars] (current docID-level) to
// [idSameCarMatch, idSameCarMatchAlt] (per-element).
//
// A companion sub-test in TestNestedFilteringComprehensive
// (`complex_make_bmw_AND_OR_tires_OR_accessories`) carries an inline
// cross-reference comment pointing back to this regression test.
func TestNestedFilteringAndOfOrRegression(t *testing.T) {
	const nestedClass = "AndOfOrRegression"
	vTrue := true
	tok := models.NestedPropertyTokenizationField

	rootProps := []*models.NestedProperty{
		{
			Name: "cars", DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: tok, IndexFilterable: &vTrue},
				{
					Name: "tires", DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
					},
				},
			},
		},
	}
	class := &models.Class{
		Class:             nestedClass,
		VectorIndexConfig: enthnsw.UserConfig{Skip: true},
		Properties: []*models.Property{
			{Name: "doc", DataType: schema.DataTypeObject.PropString(), NestedProperties: rootProps},
		},
	}

	asArr := func(items ...map[string]any) []any {
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}
	tire := func(width int) map[string]any { return map[string]any{"width": width} }
	carM := func(make string) map[string]any { return map[string]any{"make": make} }
	carMW := func(make string, width int) map[string]any {
		return map[string]any{"make": make, "tires": asArr(tire(width))}
	}
	carW := func(width int) map[string]any { return map[string]any{"tires": asArr(tire(width))} }

	textFilter := func(path, val string) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeText, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	intFilter := func(path string, val int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			Value:    &filters.Value{Type: schema.DataTypeInt, Value: val},
			On:       &filters.Path{Class: nestedClass, Property: schema.PropertyName(path)},
		}}
	}
	andFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorAnd, Operands: operands}}
	}
	orFilter := func(parts ...*filters.LocalFilter) *filters.LocalFilter {
		operands := make([]filters.Clause, len(parts))
		for i, p := range parts {
			operands[i] = *p.Root
		}
		return &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorOr, Operands: operands}}
	}

	type docDef struct {
		id    strfmt.UUID
		props map[string]any
		note  string
	}
	uuid := func(n int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", n))
	}
	runScenario := func(t *testing.T, docs []docDef, filter *filters.LocalFilter, want []strfmt.UUID) {
		t.Helper()
		db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
		ctx := context.Background()
		for _, d := range docs {
			require.NoError(t, db.PutObject(ctx, &models.Object{
				Class: nestedClass, ID: d.id, Properties: d.props,
			}, nil, nil, nil, nil, 0), "put %s (%s)", d.id, d.note)
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

	// Filter: cars.make = "honda" AND (cars.tires.width = 205 OR cars.tires.width = 225)
	//
	// Current (docID-level): match if SOME car has honda AND SOME car has
	// (205 or 225) — honda and width may be in different cars.
	//
	// Per-element (proposed): match if SOME car has both honda AND (205 or
	// 225) at that same car. Equivalent to distributing the OR:
	//   (cars.make=honda AND cars.tires.width=205)
	//      OR (cars.make=honda AND cars.tires.width=225)
	//
	// TODO aliszka:nested_filtering: locks in CURRENT docID-level AND-of-OR
	// behavior. Flips when OR distribution lands
	// (OR-in-AND distribution rewrite) — discriminator docs
	// (idDiscriminatorSplit205, idDiscriminatorSplit225,
	// idDiscriminatorAcrossCars) stop matching, expected list shrinks to
	// [idSameCarMatch, idSameCarMatchAlt] (per-element same-car).
	t.Run("regression_AND_of_OR_universal_docID_level_simple", func(t *testing.T) {
		idSameCarMatch := uuid(1)            // [{honda,205}] — both interpretations: match
		idSameCarMatchAlt := uuid(2)         // [{honda,225}] — both: match
		idDiscriminatorSplit205 := uuid(3)   // [{honda},{tires:205}] — DISCRIMINATOR
		idDiscriminatorSplit225 := uuid(4)   // [{honda},{tires:225}] — DISCRIMINATOR
		idHondaWrongWidth := uuid(5)         // [{honda,300}] — both: no match
		idDiscriminatorAcrossCars := uuid(6) // [{honda,300},{volvo,205}] — DISCRIMINATOR
		idNoHonda := uuid(7)                 // [{volvo,205}] — both: no match
		idEmpty := uuid(8)                   // no cars — both: no match
		docs := []docDef{
			{id: idSameCarMatch, props: map[string]any{"doc": map[string]any{"cars": asArr(carMW("honda", 205))}}, note: "honda+205"},
			{id: idSameCarMatchAlt, props: map[string]any{"doc": map[string]any{"cars": asArr(carMW("honda", 225))}}, note: "honda+225"},
			{id: idDiscriminatorSplit205, props: map[string]any{"doc": map[string]any{"cars": asArr(carM("honda"), carW(205))}}, note: "DISCRIMINATOR: honda alone + tires:205 in different car"},
			{id: idDiscriminatorSplit225, props: map[string]any{"doc": map[string]any{"cars": asArr(carM("honda"), carW(225))}}, note: "DISCRIMINATOR: honda alone + tires:225 in different car"},
			{id: idHondaWrongWidth, props: map[string]any{"doc": map[string]any{"cars": asArr(carMW("honda", 300))}}, note: "honda but 300"},
			{id: idDiscriminatorAcrossCars, props: map[string]any{"doc": map[string]any{"cars": asArr(carMW("honda", 300), carMW("volvo", 205))}}, note: "DISCRIMINATOR: honda+300 in cars[0]; 205 in cars[1] (volvo)"},
			{id: idNoHonda, props: map[string]any{"doc": map[string]any{"cars": asArr(carMW("volvo", 205))}}, note: "volvo+205, no honda"},
			{id: idEmpty, props: map[string]any{"doc": map[string]any{}}, note: "no cars"},
		}
		filter := andFilter(
			textFilter("doc.cars.make", "honda"),
			orFilter(
				intFilter("doc.cars.tires.width", 205),
				intFilter("doc.cars.tires.width", 225),
			),
		)
		// Current: discriminators match (docID-level intersection succeeds).
		// Per-element (after distribution): only the same-car matches survive.
		// Expected list must flip if OR distribution lands.
		runScenario(t, docs, filter, []strfmt.UUID{
			idSameCarMatch, idSameCarMatchAlt,
			idDiscriminatorSplit205, idDiscriminatorSplit225, idDiscriminatorAcrossCars,
		})
	})

	// Variant: arr[N]-pinned outer AND with OR child also pinned to same K.
	// Filter: cars[1].make = "honda" AND (cars[1].tires.width=205 OR cars[1].tires.width=225)
	//
	// Both clauses are pinned to cars[1], so the same-element question is
	// already constrained — cars[1] is the only candidate. Per-element vs
	// docID-level should produce IDENTICAL results when both clauses pin
	// the same K (they always reference the same physical position).
	//
	// This sub-test verifies that pre-existing arr[N]-pin semantics keep
	// working under the current implementation. After OR distribution
	// lands, results should remain identical (no flip).
	//
	// TODO aliszka:nested_filtering: companion baseline for the AND-of-OR
	// regression — verifies arr[N]-pinned AND-of-OR is unchanged by OR
	// distribution (OR-in-AND distribution rewrite). No
	// expected-list flip when distribution lands.
	t.Run("regression_AND_of_OR_with_same_arrN_pin_unchanged", func(t *testing.T) {
		idCars1Match205 := uuid(1)         // [{volvo},{honda,205}]
		idCars1Match225 := uuid(2)         // [{volvo},{honda,225}]
		idCars1WrongWidth := uuid(3)       // [{volvo},{honda,300}]
		idCars1WrongMake := uuid(4)        // [{volvo},{volvo,205}]
		idValuesAtCars0NotCars1 := uuid(5) // [{honda,205},{volvo,300}]
		idEmpty := uuid(6)
		docs := []docDef{
			{id: idCars1Match205, props: map[string]any{"doc": map[string]any{"cars": asArr(carM("volvo"), carMW("honda", 205))}}, note: "cars[1]=honda+205"},
			{id: idCars1Match225, props: map[string]any{"doc": map[string]any{"cars": asArr(carM("volvo"), carMW("honda", 225))}}, note: "cars[1]=honda+225"},
			{id: idCars1WrongWidth, props: map[string]any{"doc": map[string]any{"cars": asArr(carM("volvo"), carMW("honda", 300))}}, note: "cars[1] honda but 300"},
			{id: idCars1WrongMake, props: map[string]any{"doc": map[string]any{"cars": asArr(carM("volvo"), carMW("volvo", 205))}}, note: "cars[1] volvo not honda"},
			{id: idValuesAtCars0NotCars1, props: map[string]any{"doc": map[string]any{"cars": asArr(carMW("honda", 205), carMW("volvo", 300))}}, note: "honda+205 in cars[0], not cars[1]"},
			{id: idEmpty, props: map[string]any{"doc": map[string]any{}}, note: "no cars"},
		}
		filter := andFilter(
			textFilter("doc.cars[1].make", "honda"),
			orFilter(
				intFilter("doc.cars[1].tires.width", 205),
				intFilter("doc.cars[1].tires.width", 225),
			),
		)
		// Both pinned to cars[1] → same-element implicit. No discriminators
		// expected to flip when distribution lands.
		runScenario(t, docs, filter, []strfmt.UUID{idCars1Match205, idCars1Match225})
	})
}
