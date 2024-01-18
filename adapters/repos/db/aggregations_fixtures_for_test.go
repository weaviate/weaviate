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

//go:build integrationTest
// +build integrationTest

package db

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var productClass = &models.Class{
	Class:               "AggregationsTestProduct",
	VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			Name:         "name",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	},
}

var companyClass = &models.Class{
	Class:               "AggregationsTestCompany",
	VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			Name:         "sector",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			Name:         "location",
			DataType:     []string{"text"},
			Tokenization: "word",
		},
		{
			Name:     "dividendYield",
			DataType: []string{"number"},
		},
		{
			Name:     "price",
			DataType: []string{"int"}, // unrealistic for this to be an int, but
			// we've already tested another number prop ;-)
		},
		{
			Name:     "listedInIndex",
			DataType: []string{"boolean"},
		},
		{
			Name:     "makesProduct",
			DataType: []string{"AggregationsTestProduct"},
		},
	},
}

var arrayTypesClass = &models.Class{
	Class:               "AggregationsTestArrayTypes",
	VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			Name:         "strings",
			DataType:     schema.DataTypeTextArray.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			Name:     "numbers",
			DataType: []string{"number[]"},
		},
	},
}

var customerClass = &models.Class{
	Class:               "AggregationsTestCustomer",
	VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			Name:         "internalId",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
		{
			Name:     "timeArrived",
			DataType: []string{"date"},
		},
		{
			Name:         "countryOfOrigin",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	},
}

var products = []map[string]interface{}{
	{
		"name": "Superbread",
	},
}

var productsIds = []strfmt.UUID{
	"1295c052-263d-4aae-99dd-920c5a370d06",
}

// company objects are imported not just once each, but each is imported
// importFactor times. This should even out shard imbalances a bit better.
var importFactor = 10

var companies = []map[string]interface{}{
	{
		"sector":        "Financials",
		"location":      "New York",
		"dividendYield": 1.3,
		"price":         int64(150),
		"listedInIndex": true,
	},
	{
		"sector":        "Financials",
		"location":      "New York",
		"dividendYield": 4.0,
		"price":         int64(600),
		"listedInIndex": true,
	},
	{
		"sector":        "Financials",
		"location":      "San Francisco",
		"dividendYield": 1.3,
		"price":         int64(47),
		"listedInIndex": true,
	},
	{
		"sector":        "Food",
		"location":      "Atlanta",
		"dividendYield": 1.3,
		"price":         int64(160),
		"listedInIndex": true,
	},
	{
		"sector":        "Food",
		"location":      "Atlanta",
		"dividendYield": 2.0,
		"price":         int64(70),
		"listedInIndex": true,
	},
	{
		"sector":        "Food",
		"location":      "Los Angeles",
		"dividendYield": 0.0,
		"price":         int64(800),
		"listedInIndex": false,
	},
	{
		"sector":        "Food",
		"location":      "Detroit",
		"dividendYield": 8.0,
		"price":         int64(10),
		"listedInIndex": true,
		"makesProduct": models.MultipleRef{
			&models.SingleRef{
				Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", productsIds[0])),
			},
		},
	},
	{
		"sector":        "Food",
		"location":      "San Francisco",
		"dividendYield": 0.0,
		"price":         int64(200),
		"listedInIndex": true,
	},
	{
		"sector":        "Food",
		"location":      "New York",
		"dividendYield": 1.1,
		"price":         int64(70),
		"listedInIndex": true,
	},
}

// Use fixed ids to make the test deterministic. The length of this must match
// the len(companies)*importFactor These are somewhat carefully arranged to
// make sure that we prevent the flakiness that was described in
// https://github.com/weaviate/weaviate/issues/1884
var companyIDs = []strfmt.UUID{
	"9ee7640f-b4fc-45f1-b502-580e79062c99",
	"f745485b-f6ef-4785-bd54-574cc4923899",
	"0109289b-88d5-48d5-9f3e-1e5edb7e56c1",
	"ce7c2930-2af8-44ec-8789-eceee930c37d",
	"3f7b1ea2-e6e8-49d4-b44c-30916dbab43a",
	"ed3b417f-2b7e-456e-9599-744541728950",
	"175c22ea-190a-4b8c-9fd0-fbffa5b1b8be",
	"e0165a52-f504-47d3-b947-b3829eca3563",
	"19ede86d-86d6-4ea8-9ae3-a10a1607b433",
	"034c8d0b-50d7-4753-811c-abbfad44c653",
	"99564c6b-8529-4f56-9c3f-f808385f034a",
	"adc362ea-105d-4544-b981-dedc100d25b9",
	"21e1dc5b-49e1-4d35-acb4-be969e4e3a30",
	"cb278f74-c632-4460-ae78-2c2c140c0e12",
	"4d552f33-552a-41aa-b732-da3d0f708b79",
	"faf509ad-bdb6-4aa1-ae9f-e9f410210419",
	"59992957-65cf-44bd-8894-d8b2364f080f",
	"529217e6-9ec3-41c6-bb6d-13e8f380960a",
	"058ec709-782d-4b58-a38f-01593d97f181",
	"d027204b-805c-46ae-8a61-7d35f9c6eaba",
	"0fce8fea-6ca7-4c80-bc81-55d26e1fd0bd",
	"1f4832c7-d164-441e-b197-aa193b4d128f",
	"15ad080d-49fc-4b8b-b621-d47f98aa5fdb",
	"cb40966d-963d-4283-94be-7da5de70992e",
	"6516f7d9-c505-40b3-94de-a42498eea22d",
	"9dbcbd08-1067-4bec-84a4-4f3ad19286d3",
	"dabd68eb-27b9-462b-a271-300058c5798b",
	"9a33f431-cb28-49ae-b9c9-94e97f752a2a",
	"4aa27f5c-f475-444b-9279-94b8a5da14c9",
	"e71cf490-9a59-4475-80c1-b6c872f3b33c",
	"a8e7e8bf-237b-4d95-99be-6af332bdf942",
	"08c239c3-e19e-4d88-b0fd-861853dd5e36",
	"0209ab7c-2573-4d66-9917-942450e02750",
	"0a71a4da-d9c9-423e-8dd8-ebd5c2a86a2d",
	"f312aa16-992e-4505-82aa-04da30ea5fe3",
	"5b9f9449-1336-44e3-88f9-979f3c752bd7",
	"dcec96ab-9377-4272-8a48-07568c4ed533",
	"3f28352d-9960-4251-aa05-ce4c612e1ab7",
	"4a08101e-f280-41f9-9e7b-fe12d7216c3c",
	"0dd7383f-c71b-483c-9253-e180f8763405",
	"cfb83c85-cf8f-49da-952f-5bd954b7e616",
	"b016bb0f-9e07-4d40-9878-a6bbaa67d866",
	"311d7423-552b-4d4c-b7b6-cdcd15e1009b",
	"895877d6-9cf3-4d79-989e-4d89f6867e09",
	"92bdb79c-6870-44e2-ab71-c3137c78cb2d",
	"b16cb9c4-5a6c-444c-bbf5-7b0ef2c1ac12",
	"efb09d97-09c4-4976-aa14-abfd520c114d",
	"6431f59d-9ed8-4963-9ed7-7336a5795d8b",
	"1ad26844-6f6b-4007-834d-09ec8668fe7b",
	"6d83b349-6ec8-49bb-b438-7f29a16216d3",
	"68156360-1cae-406e-8baf-177178f0815e",
	"3c726a50-ec82-4828-8967-f704477dfef7",
	"46f702b2-e1c3-4e4a-868e-10ec1e260c75",
	"51ff679a-87d8-4bef-830d-327e7b4c8f8e",
	"aea6fc5c-8eb0-4cd7-8cfe-1285239d16bd",
	"b70bbf68-5ebc-4315-9819-deb65b241f3f",
	"6069853f-8822-434f-9d59-c881800e0a27",
	"9a287ef6-6920-4d01-a44a-7561d2fb627d",
	"fa057d95-9ba8-418a-aeb2-fe4b2acd31b0",
	"9b0fb28f-21f1-4df1-a55b-67bf6be79911",
	"044403fb-25f6-461f-ad92-8f9533908070",
	"35d09151-c469-4901-8092-2114060cb86b",
	"85884aa2-5d0b-4dff-80f6-8ca7cbab9fef",
	"bd36a31a-f14e-4040-ad11-0ec5b6217b6a",
	"fe20620f-c612-4712-9475-d4cfc59e8bba",
	"09ba0773-e81d-4cb9-968a-552e1cbaf143",
	"7a7378a5-2d05-4457-b2d4-1fe068535167",
	"6867d1e5-2d30-4f91-90d2-2b453ffd5cd5",
	"2fef1b16-3dd1-4ad5-bc55-cf8f9783b40c",
	"0590f51b-7c9f-41a0-b81e-cdbc205ebdd9",
	"7ed55b94-86d5-440a-9a8a-5f83dabcb69b",
	"2daca92a-c8a6-4ab4-a528-3d99ce6f72f2",
	"24187e67-947d-436d-ae7f-d20b03874b56",
	"864ff42d-00fe-44a8-8163-8af459dc1c0c",
	"0c2cc9a5-089a-4d10-882a-837c154117ea",
	"fb256f18-e812-4355-b41a-c69d933f2a61",
	"b631c4df-8229-43c0-9e5e-189c5d666ac2",
	"8da03018-3272-4bd3-987c-1dd1e807bc1d",
	"bf736b76-fccc-4d1b-8d9f-2e78fdb0d972",
	"1fc9dffc-da23-4b99-8330-44c5598919db",
	"1ed74402-bc81-4245-8275-c2862a4d6a86",
	"6a91adb4-23df-43bd-9564-f97e76382a52",
	"d1661202-c568-4032-8ec6-99fe4238de84",
	"e4b4186d-f02e-47d7-8214-d3854ee475fd",
	"664e6157-bfcc-4513-8f04-c095d3ecb2d5",
	"b3b9951f-0867-453d-bf20-9f22bdb5a38b",
	"52adfeae-ab75-4250-ae45-61af9e231e86",
	"e994c378-ac0b-4f08-bd56-462990be36dd",
	"a012f65a-28a7-4005-95bb-f55783bcdda0",
	"0cbc7fb6-843c-4aad-b540-e37ddb7c84c6",
}

var arrayTypes = []map[string]interface{}{
	{
		"strings": []string{"a", "b", "c"},
		"numbers": []float64{1.0, 2.0, 2.0, 3.0, 3.0},
	},
	{
		"strings": []string{"a"},
		"numbers": []float64{1.0, 2.0},
	},
}

var customers = []map[string]interface{}{
	{
		"internalId":      "customer 1",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:17.231346Z"),
		"isNewCustomer":   false,
	},
	{
		"internalId":      "customer 2",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:17.231346Z"),
		"isNewCustomer":   false,
	},
	{
		"internalId":      "customer 3",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:17.231346Z"),
		"isNewCustomer":   false,
	},
	{
		"internalId":      "customer 4",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:20.123546Z"),
		"isNewCustomer":   true,
	},
	{
		"internalId":      "customer 5",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:20.123546Z"),
		"isNewCustomer":   true,
	},
	{
		"internalId":      "customer 6",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:22.112435Z"),
		"isNewCustomer":   false,
	},
	{
		"internalId":      "customer 7",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:23.754272Z"),
		"isNewCustomer":   false,
	},
	{
		"internalId":      "customer 8",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:24.325698Z"),
		"isNewCustomer":   true,
	},
	{
		"internalId":      "customer 9",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:25.524536Z"),
		"isNewCustomer":   false,
	},
	{
		"internalId":      "customer 10",
		"countryOfOrigin": "US",
		"timeArrived":     mustStringToTime("2022-06-16T17:30:26.451235Z"),
		"isNewCustomer":   true,
	},
}
