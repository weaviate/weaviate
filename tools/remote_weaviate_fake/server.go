/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

var thingID = "711da979-4b0b-41e2-bcb8-fcc03554c7c8"

func main() {
	http.HandleFunc("/weaviate/v1/graphql", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			w.WriteHeader(405)
			w.Write([]byte("only POST allowed"))
			return
		}

		defer req.Body.Close()
		bodyBytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(422)
			w.Write([]byte(fmt.Sprintf("could not read body: %s", err)))
			return
		}

		var body map[string]string
		err = json.Unmarshal(bodyBytes, &body)
		if err != nil {
			w.WriteHeader(422)
			w.Write([]byte(fmt.Sprintf("could not parse query: %s", err)))
			return
		}

		parsed := removeAllWhiteSpace(body["query"])

		expectedQuery := fmt.Sprintf("%s", `{ Local { Get { Things { Instruments { name } } } } }`)
		if removeAllWhiteSpace(parsed) != removeAllWhiteSpace(expectedQuery) {
			w.WriteHeader(422)
			w.Write([]byte(fmt.Sprintf("wrong body, got \n%#v\nwanted\n%#v\n", parsed, expectedQuery)))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "%s", graphQLhappyPathResponse)
	})

	http.HandleFunc("/weaviate/v1/schema", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			w.WriteHeader(405)
			w.Write([]byte("only GET allowed"))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "%s", schemaResponse)
	})

	http.HandleFunc(fmt.Sprintf("/weaviate/v1/things/%s", thingID), func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			w.WriteHeader(405)
			w.Write([]byte("only GET allowed"))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "%s", restThingHappyPathResponse)
	})

	log.Fatal(http.ListenAndServe(":8081", nil))
}

var graphQLhappyPathResponse = `{
  "data": {
    "Local": {
      "Get": {
        "Things": {
          "Instruments": [
            {
              "name": "Piano"
            },
            {
              "name": "Guitar"
            },
            {
              "name": "Bass Guitar"
            },
            {
              "name": "Talkbox"
            }
          ]
        }
      }
    }
  }
}`

var restThingHappyPathResponse = fmt.Sprintf(`{
  "@class": "Instruments",
	"schema": {
		"name": "Talkbox"
	},
  "@context": "string",
  "thingId": "%s"
}`, thingID)

var schemaResponse = `{
  "actions": {
    "@context": "",
    "version": "0.0.1",
    "type": "action",
    "name": "weaviate demo actions schema",
    "maintainer": "yourfriends@weaviate.com",
    "classes": []
  },
  "things": {
    "@context": "",
    "version": "0.0.1",
    "type": "thing",
    "name": "weaviate demo things schema",
    "maintainer": "yourfriends@weaviate.com",
    "classes": [
      {
        "class": "Instruments",
        "description": "Musical instruments",
        "properties": [
          {
            "name": "name",
            "@dataType": [
              "string"
            ],
            "description": "The name of the instrument",
            "keywords": [
              {
                "keyword": "name",
                "weight": 1
              }
            ]
          }
        ],
        "keywords": [
          {
            "keyword": "instrument",
            "weight": 1
          },
          {
            "keyword": "music",
            "weight": 0.25
          }
        ]
      }
    ]
  }
}`

func removeAllWhiteSpace(input string) string {
	noWS := strings.Replace(input, " ", "", -1)
	noTabs := strings.Replace(noWS, "\t", "", -1)
	return strings.Replace(noTabs, "\n", "", -1)
}
