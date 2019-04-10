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

		getQuery := fmt.Sprintf("%s", `{ Local { Get { Things { Instruments { name } } } } }`)
		getMetaQuery := fmt.Sprintf("%s", `{ Local { GetMeta { Things { Instruments { volume { maximum minimum mean } } } } } }`)
		aggregateQuery := fmt.Sprintf("%s", ` { Local { Aggregate { Things { Instruments(groupBy:["name"]) { volume { count } } } } } }`)
		fetchQuery := fmt.Sprintf("%s", ` { Local { Fetch { Things(where: { class: { name: "bestclass" certainty: 0.8 keywords: [{value: "foo", weight: 0.9}] }, properties: { name: "bestproperty" certainty: 0.8 keywords: [{value: "bar", weight: 0.9}] operator: Equal valueString: "some-value" }, }) { beacon certainty } } } }`)
		switch parsed {
		case removeAllWhiteSpace(getQuery):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "%s", graphQLGetResponse)
			return
		case removeAllWhiteSpace(getMetaQuery):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "%s", graphQLGetMetaResponse)
			return
		case removeAllWhiteSpace(aggregateQuery):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "%s", graphQLAggregateResponse)
			return
		case removeAllWhiteSpace(fetchQuery):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, "%s", graphQLFetchResponse)
			return
		default:
			w.WriteHeader(422)
			w.Write([]byte(fmt.Sprintf("unrecognized body, got \n%#v\nwanted\n%#v\nor\n%#v\nor\n%#v\nor\n%#v",
				parsed, removeAllWhiteSpace(fetchQuery), removeAllWhiteSpace(aggregateQuery),
				removeAllWhiteSpace(getQuery), removeAllWhiteSpace(getMetaQuery))))
			return
		}
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

var graphQLGetMetaResponse = `{
  "data": {
    "Local": {
      "GetMeta": {
        "Things": {
          "Instruments": {
            "volume": {
              "maximum": 110,
              "minimum": 65,
              "mean": 82
            }
          }
        }
      }
    }
  }
}`

var graphQLAggregateResponse = `{
  "data": {
    "Local": {
      "Aggregate": {
        "Things": {
          "Instruments": [{
            "volume": {
              "count": 82
            }
          }]
        }
      }
    }
  }
}`

var graphQLFetchResponse = `{
  "data": {
    "Local": {
      "Fetch": {
        "Things": [
					{
						"beacon": "weaviate://RemoteWeaviateForAcceptanceTest/things/c2b94c9a-fea2-4f9a-ae40-6d63534633f7",
						"certainty": 0.5
					},
					{
						"beacon": "weaviate://RemoteWeaviateForAcceptanceTest/things/32fc9b12-00b8-46b2-962d-63c1f352e090",
						"certainty": 0.7
					}
				]
      }
    }
  }
}`

var graphQLGetResponse = `{
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
  "class": "Instruments",
	"schema": {
		"name": "Talkbox"
	},
  "thingId": "%s"
}`, thingID)

var schemaResponse = `{
  "actions": {
    "version": "0.0.1",
    "type": "action",
    "name": "weaviate demo actions schema",
    "maintainer": "yourfriends@weaviate.com",
    "classes": []
  },
  "things": {
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
          }, {
            "name": "volume",
            "@dataType": [
              "number"
            ],
            "description": "The volume the instrument can achieve",
            "keywords": [
              {
                "keyword": "volume",
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
