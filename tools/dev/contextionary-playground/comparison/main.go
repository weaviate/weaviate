/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package main

import (
	"fmt"
	"os"

	"github.com/semi-technologies/weaviate/contextionary"
)

func fatal(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func main() {
	root := os.Args[1]
	c1Path := root + "/filter-after-glove"
	c2Path := root + "/preprocessing"
	c3Path := root + "/stopword-removal"

	c1, err := contextionary.LoadVectorFromDisk(c1Path+"/contextionary-en.knn", c1Path+"/contextionary-en.idx")
	fatal(err)

	c2, err := contextionary.LoadVectorFromDisk(c2Path+"/contextionary-en.knn", c2Path+"/contextionary-en.idx")
	fatal(err)

	c3, err := contextionary.LoadVectorFromDisk(c3Path+"/contextionary-en.knn", c3Path+"/contextionary-en.idx")
	fatal(err)

	word := os.Args[2]
	c1Dist, c1Words := kNN(word, c1)
	c2Dist, c2Words := kNN(word, c2)
	c3Dist, c3Words := kNN(word, c3)

	for i := range c1Dist {
		fmt.Printf("%f  %-15s\t\t\t%f  %-15s\t\t\t%f  %-15s\n", c1Dist[i], c1Words[i], c2Dist[i], c2Words[i], c3Dist[i], c3Words[i])
	}
}

func kNN(name string, contextionary contextionary.Contextionary) ([]float32, []string) {
	itemIndex := contextionary.WordToItemIndex(name)
	if ok := itemIndex.IsPresent(); !ok {
		fatal(fmt.Errorf("item index for %s is not present", name))
	}

	list, distances, err := contextionary.GetNnsByItem(itemIndex, 20, 3)
	if err != nil {
		fatal(fmt.Errorf("get nns errored: %s", err))
	}

	words := make([]string, len(list), len(list))
	for i := range list {
		w, err := contextionary.ItemIndexToWord(list[i])
		if err != nil {
			fmt.Printf("error: %s", err)
		}
		words[i] = w
	}

	return distances, words
}
