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

package classification

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func testSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "ExactCategory",
				},
				{
					Class: "MainCategory",
				},
				{
					Class: "Article",
					Properties: []*models.Property{
						{
							Name:     "description",
							DataType: []string{string(schema.DataTypeText)},
						},
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "exactCategory",
							DataType: []string{"ExactCategory"},
						},
						{
							Name:     "mainCategory",
							DataType: []string{"MainCategory"},
						},
						{
							Name:     "categories",
							DataType: []string{"ExactCategory"},
						},
						{
							Name:     "anyCategory",
							DataType: []string{"MainCategory", "ExactCategory"},
						},
					},
				},
			},
		},
	}
}

// vector position close to [1,0,0] means -> politics, [0,1,0] means -> society, [0, 0, 1] -> food&drink
func testDataToBeClassified() search.Results {
	return search.Results{
		search.Result{
			ID:        "75ba35af-6a08-40ae-b442-3bec69b355f9",
			ClassName: "Article",
			Vector:    []float32{0.78, 0, 0},
			Schema: map[string]interface{}{
				"description": "Barack Obama is a former US president",
			},
		},
		search.Result{
			ID:        "f850439a-d3cd-4f17-8fbf-5a64405645cd",
			ClassName: "Article",
			Vector:    []float32{0.90, 0, 0},
			Schema: map[string]interface{}{
				"description": "Michelle Obama is Barack Obamas wife",
			},
		},
		search.Result{
			ID:        "a2bbcbdc-76e1-477d-9e72-a6d2cfb50109",
			ClassName: "Article",
			Vector:    []float32{0, 0.78, 0},
			Schema: map[string]interface{}{
				"description": "Johnny Depp is an actor",
			},
		},
		search.Result{
			ID:        "069410c3-4b9e-4f68-8034-32a066cb7997",
			ClassName: "Article",
			Vector:    []float32{0, 0.90, 0},
			Schema: map[string]interface{}{
				"description": "Brad Pitt starred in a Quentin Tarantino movie",
			},
		},
		search.Result{
			ID:        "06a1e824-889c-4649-97f9-1ed3fa401d8e",
			ClassName: "Article",
			Vector:    []float32{0, 0, 0.78},
			Schema: map[string]interface{}{
				"description": "Ice Cream often contains a lot of sugar",
			},
		},
		search.Result{
			ID:        "6402e649-b1e0-40ea-b192-a64eab0d5e56",
			ClassName: "Article",
			Vector:    []float32{0, 0, 0.90},
			Schema: map[string]interface{}{
				"description": "French Fries are more common in Belgium and the US than in France",
			},
		},
	}
}

func testDataVectors() map[string][]float32 {
	return map[string][]float32{
		"barack":   {0.7, 0, 0},
		"michelle": {0.7, 0, 0},
		"obama":    {1.0, 0, 0},
		"us":       {0.6, 0.5, 0.4},
		"depp":     {0.1, 0.8, 0.2},
		"actor":    {0.1, 0.9, 0.0},
		"brad":     {0.1, 0.8, 0.2},
		"starred":  {0.1, 0.9, 0.0},
		"ice":      {0, 0.1, 0.9},
		"cream":    {0, 0.1, 0.8},
		"sugar":    {0.3, 0.2, 0.9},
		"french":   {0.5, 0.5, 0.4},
		"fries":    {0, 0.1, 0.95},
		"belgium":  {0.3, 0.3, 0.2},
	}
}

const (
	idMainCategoryPoliticsAndSociety = "39c6abe3-4bbe-4c4e-9e60-ca5e99ec6b4e"
	idMainCategoryFoodAndDrink       = "5a3d909a-4f0d-4168-8f5c-cd3074d1e79a"
	idCategoryPolitics               = "1b204f16-7da6-44fd-bbd2-8cc4a7414bc3"
	idCategorySociety                = "ec500f39-1dc9-4580-9bd1-55a8ea8e37a2"
	idCategoryFoodAndDrink           = "027b708a-31ca-43ea-9001-88bec864c79c"
)

// only used for contextual type classification
func testDataPossibleTargets() search.Results {
	return search.Results{
		search.Result{
			ID:        idMainCategoryPoliticsAndSociety,
			ClassName: "MainCategory",
			Vector:    []float32{1.01, 1.01, 0},
			Schema: map[string]interface{}{
				"name": "Politics and Society",
			},
		},
		search.Result{
			ID:        idMainCategoryFoodAndDrink,
			ClassName: "MainCategory",
			Vector:    []float32{0, 0, 0.99},
			Schema: map[string]interface{}{
				"name": "Food and Drinks",
			},
		},
		search.Result{
			ID:        idCategoryPolitics,
			ClassName: "ExactCategory",
			Vector:    []float32{0.99, 0, 0},
			Schema: map[string]interface{}{
				"name": "Politics",
			},
		},
		search.Result{
			ID:        idCategorySociety,
			ClassName: "ExactCategory",
			Vector:    []float32{0, 0.90, 0},
			Schema: map[string]interface{}{
				"name": "Society",
			},
		},
		search.Result{
			ID:        idCategoryFoodAndDrink,
			ClassName: "ExactCategory",
			Vector:    []float32{0, 0, 0.99},
			Schema: map[string]interface{}{
				"name": "Food and Drink",
			},
		},
	}
}

func beaconRef(target string) *models.SingleRef {
	beacon := fmt.Sprintf("weaviate://localhost/%s", target)
	return &models.SingleRef{Beacon: strfmt.URI(beacon)}
}

// only used for knn-type
func testDataAlreadyClassified() search.Results {
	return search.Results{
		search.Result{
			ID:        "8aeecd06-55a0-462c-9853-81b31a284d80",
			ClassName: "Article",
			Vector:    []float32{1, 0, 0},
			Schema: map[string]interface{}{
				"description":   "This article talks about politics",
				"exactCategory": models.MultipleRef{beaconRef(idCategoryPolitics)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryPoliticsAndSociety)},
			},
		},
		search.Result{
			ID:        "9f4c1847-2567-4de7-8861-34cf47a071ae",
			ClassName: "Article",
			Vector:    []float32{0, 1, 0},
			Schema: map[string]interface{}{
				"description":   "This articles talks about society",
				"exactCategory": models.MultipleRef{beaconRef(idCategorySociety)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryPoliticsAndSociety)},
			},
		},
		search.Result{
			ID:        "926416ec-8fb1-4e40-ab8c-37b226b3d68e",
			ClassName: "Article",
			Vector:    []float32{0, 0, 1},
			Schema: map[string]interface{}{
				"description":   "This article talks about food",
				"exactCategory": models.MultipleRef{beaconRef(idCategoryFoodAndDrink)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryFoodAndDrink)},
			},
		},
	}
}
