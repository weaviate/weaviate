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
 */
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/contextionary/generator"
	flags "github.com/jessevdk/go-flags"
)

func validate(fileName string) bool {

	c13y, err := contextionary.LoadVectorFromDisk(fileName+".knn", fileName+".idx")
	if err != nil {
		log.Fatal(err)
	}

	// load the vocab file line by line
	vocabFile, err := os.Open(fileName + ".vocab")
	if err != nil {
		log.Fatal(err)
	}
	defer vocabFile.Close()
	scanner := bufio.NewScanner(vocabFile)

	for scanner.Scan() {
		wordToCheck := scanner.Text()
		itemIndex := c13y.WordToItemIndex(wordToCheck)
		if ok := itemIndex.IsPresent(); !ok {
			log.Fatal(fmt.Errorf("item index for %s is not present", wordToCheck))
			return false
		}
	}

	return true
}

func main() {
	var options generator.Options
	var parser = flags.NewParser(&options, flags.Default)

	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}

	// generate the file
	generator.Generate(options)

	// validate
	if validate(options.OutputPrefix) == true {
		os.Exit(0)
	} else {
		os.Exit(1)
	}

}
