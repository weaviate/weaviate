//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package main

import "github.com/go-openapi/strfmt"

type mainCategory struct {
	ID   strfmt.UUID
	Name string
}

var mainCategories = []mainCategory{
	mainCategory{
		ID:   "51dd8b95-9e80-4824-9229-21f40e9b4e85",
		Name: "Computers",
	},
	mainCategory{
		ID:   "e546bbab-fcc2-4688-8803-b75b061cc349",
		Name: "Recreation",
	},
	mainCategory{
		ID:   "c2d5a423-5e5a-41df-b7b0-fd7e159482a3",
		Name: "Science",
	},
	mainCategory{
		ID:   "ed0bab28-1479-4970-ad4f-c07ee6502da8",
		Name: "For Sale",
	},
	mainCategory{
		ID:   "d0ddbcc2-964a-4211-9ddc-d5d366e0dc14",
		Name: "Politics",
	},
	mainCategory{
		ID:   "74e76a5b-8b4c-46b5-9898-e6b569c18a00",
		Name: "Religion",
	},
}

func mainCategoryFromCategoryID(id string) mainCategory {
	switch id {
	case "f0102af9-c45a-447a-8c06-2bec2f96a5c6",
		"5d854455-8ab4-4bd1-b4c6-19cb18c5fd05",
		"ea15eb4a-b145-49fe-9cac-4d688a9cbf1f",
		"d08afdf7-2e5e-44cc-be0d-926752084efc",
		"5b608ab2-b8bc-4a57-8eb6-8c7cb6d773ea":
		return mainCategoryById("51dd8b95-9e80-4824-9229-21f40e9b4e85")

	case "b68a4691-b83c-4c60-8db8-5acd91851617":
		return mainCategoryById("ed0bab28-1479-4970-ad4f-c07ee6502da8")

	case "19fb7dd6-b775-4421-a055-a368145144cc",
		"5257952b-74c5-4697-9c65-9240e189c771",
		"a3f618c7-6224-4427-9eb1-6ea820ecfc34",
		"140f3b5a-a78c-4ac8-aea3-4ee9025b2758":
		return mainCategoryById("e546bbab-fcc2-4688-8803-b75b061cc349")

	case "b03ac29f-d0e0-4b52-818e-30ff46b0e718",
		"fe29ccd7-de6d-4f12-b910-b28fed4fe09a",
		"e36ffc0a-7dee-4241-ab1f-7b52b36001c2",
		"0a57cefb-dbb1-420a-b921-64c6d7b5559a":
		return mainCategoryById("c2d5a423-5e5a-41df-b7b0-fd7e159482a3")

	case "6fa7bf33-0e50-400d-a0d8-48cabf19bdcf",
		"9ce20123-16ea-41e2-b509-808c09426bbb":
		return mainCategoryById("74e76a5b-8b4c-46b5-9898-e6b569c18a00")

	case "fbca4c94-3fc4-4312-b832-d0ae61475d43",
		"2609f1bc-7693-48f3-b531-6ddc52cd2501",
		"64b5cb24-53f7-4468-a07b-50ed68c0642f":
		return mainCategoryById("d0ddbcc2-964a-4211-9ddc-d5d366e0dc14")

	default:
		panic("wrong category")
	}
}

func mainCategoryById(id strfmt.UUID) mainCategory {
	for _, cat := range mainCategories {
		if cat.ID == id {
			return cat
		}
	}

	panic("main cat not found by id")
}
