//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/TylerBrock/colorjson"
	"github.com/sirupsen/logrus"
)

func check(err error) {
	if err != nil {
		logrus.WithError(err).Errorf("an error occurred")
		os.Exit(1)
	}
}

type sendGremlin struct {
	Gremlin string `json:"gremlin"`
}

func main() {
	query, err := ioutil.ReadFile("./query.groovy")
	check(err)

	payload := sendGremlin{
		Gremlin: string(query),
	}

	payloadBytes, err := json.Marshal(payload)
	check(err)

	res, err := http.Post("http://localhost:8182", "application/json", bytes.NewReader(payloadBytes))
	check(err)

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	check(err)

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	check(err)

	f := colorjson.NewFormatter()
	f.Indent = 2
	s, err := f.Marshal(result)
	check(err)

	fmt.Printf("\n\nSource Query: %s\n\n", string(query))
	fmt.Println(string(s))
}
