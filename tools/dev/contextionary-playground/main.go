/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package main

import (
	"fmt"
	"os"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	schemaContextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
)

func fatal(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

var sampleSchema = schema.Schema{
	Things: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{{
			Class: "City",
			Keywords: models.SemanticSchemaKeywords{{
				Keyword: "city",
				Weight:  1.0,
			}, {
				Keyword: "town",
				Weight:  0.8,
			}, {
				Keyword: "urban",
				Weight:  0.9,
			}},
		}, {
			Class: "Town",
			Keywords: models.SemanticSchemaKeywords{{
				Keyword: "city",
				Weight:  0.8,
			}, {
				Keyword: "town",
				Weight:  1,
			}, {
				Keyword: "urban",
				Weight:  0.3,
			}, {
				Keyword: "village",
				Weight:  0.8,
			}},
		}},
	},
	Actions: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{},
	},
}

func main() {
	c13y, err := contextionary.LoadVectorFromDisk("./tools/dev/contextionary-playground/contextionary.knn", "./tools/dev/contextionary-playground/contextionary.idx")
	fatal(err)

	fmt.Println("results before building centroid based on keywords: ")
	kNN("city", c13y)

	inMemoryC13y, err := schemaContextionary.BuildInMemoryContextionaryFromSchema(sampleSchema, &c13y)
	fatal(err)

	// Combine contextionaries
	contextionaries := []contextionary.Contextionary{*inMemoryC13y, c13y}
	combined, err := contextionary.CombineVectorIndices(contextionaries)
	fatal(err)

	fmt.Println("results after building centroid based on keywords: ")
	kNN("ocean", combined)
}

func kNN(name string, contextionary contextionary.Contextionary) {
	itemIndex := contextionary.WordToItemIndex(name)
	if ok := itemIndex.IsPresent(); !ok {
		fatal(fmt.Errorf("item index for %s is not present", name))
	}

	list, distances, err := contextionary.GetNnsByItem(itemIndex, 1000000, 3)
	if err != nil {
		fatal(fmt.Errorf("get nns errored: %s", err))
	}

	for i := range list {
		w, err := contextionary.ItemIndexToWord(list[i])
		if err != nil {
			fmt.Printf("error: %s", err)
		}
		fmt.Printf("\n%d %f %s\n", list[i], distances[i], w)
	}

}
