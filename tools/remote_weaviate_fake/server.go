/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

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
		expectedBody := fmt.Sprintf("%s\n", `{"query":"{ Local { Get { Things { Instruments { name } } } } }"}`)

		if string(bodyBytes) != expectedBody {
			w.WriteHeader(422)
			w.Write([]byte(fmt.Sprintf("wrong body, got \n%#v\nwanted\n%#v\n", string(bodyBytes), expectedBody)))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "%s", happyPathResponse)
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

	log.Fatal(http.ListenAndServe(":8081", nil))
}

var happyPathResponse = `{
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
