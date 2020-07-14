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

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strings"
)

type category struct {
	ID          string
	DisplayName string
	Code        string
}

func categoryLookup(id string) string {
	var includedCategories = []category{
		category{
			"f0102af9-c45a-447a-8c06-2bec2f96a5c6",
			"comp.graphics",
			"comp.graphics",
		},
		category{
			"5d854455-8ab4-4bd1-b4c6-19cb18c5fd05",
			"comp.os.ms-windows.misc",
			"comp.os.ms-windows.misc",
		},
		category{
			"ea15eb4a-b145-49fe-9cac-4d688a9cbf1f",
			"comp.sys.ibm.pc.hardware",
			"comp.sys.ibm.pc.hardware",
		},
		category{
			"d08afdf7-2e5e-44cc-be0d-926752084efc",
			"comp.sys.mac.hardware",
			"comp.sys.mac.hardware",
		},
		category{
			"5b608ab2-b8bc-4a57-8eb6-8c7cb6d773ea",
			"comp.windows.x",
			"comp.windows.x",
		},
		category{
			"b68a4691-b83c-4c60-8db8-5acd91851617",
			"misc.forsale",
			"misc.forsale",
		},
		category{
			"19fb7dd6-b775-4421-a055-a368145144cc",
			"rec.autos",
			"rec.autos",
		},
		category{
			"5257952b-74c5-4697-9c65-9240e189c771",
			"rec.motorcycles",
			"rec.motorcycles",
		},
		category{
			"a3f618c7-6224-4427-9eb1-6ea820ecfc34",
			"rec.sport.baseball",
			"rec.sport.baseball",
		},
		category{
			"140f3b5a-a78c-4ac8-aea3-4ee9025b2758",
			"rec.sport.hockey",
			"rec.sport.hockey",
		},
		category{
			"b03ac29f-d0e0-4b52-818e-30ff46b0e718",
			"sci.crypt",
			"sci.crypt",
		},
		category{
			"fe29ccd7-de6d-4f12-b910-b28fed4fe09a",
			"sci.electronics",
			"sci.electronics",
		},
		category{
			"e36ffc0a-7dee-4241-ab1f-7b52b36001c2",
			"sci.med",
			"sci.med",
		},
		category{
			"0a57cefb-dbb1-420a-b921-64c6d7b5559a",
			"sci.space",
			"sci.space",
		},
		category{
			"6fa7bf33-0e50-400d-a0d8-48cabf19bdcf",
			"soc.religion.christian",
			"soc.religion.christian",
		},
		category{
			"fbca4c94-3fc4-4312-b832-d0ae61475d43",
			"talk.politics.guns",
			"talk.politics.guns",
		},
		category{
			"2609f1bc-7693-48f3-b531-6ddc52cd2501",
			"talk.politics.mideast",
			"talk.politics.mideast",
		},
		category{
			"64b5cb24-53f7-4468-a07b-50ed68c0642f",
			"talk.politics.misc",
			"talk.politics.misc",
		},
		category{
			"9ce20123-16ea-41e2-b509-808c09426bbb",
			"talk.religion.misc",
			"talk.religion.misc",
		},
	}

	for _, cat := range includedCategories {
		if cat.ID == id {
			return cat.DisplayName
		}
	}

	panic("no cat found")
}

func convertBase64ToArray(base64Str string) ([]float32, error) {
	decoded, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return nil, err
	}

	length := len(decoded)
	array := make([]float32, 0, length/4)

	for i := 0; i < len(decoded); i += 4 {
		bits := binary.BigEndian.Uint32(decoded[i : i+4])
		f := math.Float32frombits(bits)
		array = append(array, f)
	}
	return array, nil
}

func main() {

	unclassified := getUnclassified(9999)

	var countFineCorrect int
	var countFineWrong int
	var countCoarseCorrect int
	var countCoarseWrong int

	for _, item := range unclassified {
		v, err := convertBase64ToArray(item.EmbeddingVector)
		if err != nil {
			log.Fatal(err)
		}

		_, fineId, _, coarseId := classify(v, 5)
		correct := item.ControlCategoryID == fineId
		if correct {
			countFineCorrect++
		} else {
			countFineWrong++
		}

		if mainCategoryFromCategoryID(item.ControlCategoryID).ID.String() == coarseId {
			countCoarseCorrect++
		} else {
			countCoarseWrong++
		}

		fmt.Printf("%v\t%s\t%s\t%v\n", item.ID, categoryLookup(item.ControlCategoryID), categoryLookup(fineId), correct)
	}
	percentageFine := float64(countFineCorrect) / float64(countFineCorrect+countFineWrong)
	percentageCoarse := float64(countCoarseCorrect) / float64(countCoarseCorrect+countCoarseWrong)
	fmt.Printf("Exact Category:\nCorrect: %d\tWrong: %d\tPercentage Correct: %f\n", countFineCorrect, countFineWrong, percentageFine)
	fmt.Printf("Broad Category:\nCorrect: %d\tWrong: %d\tPercentage Correct: %f\n", countCoarseCorrect, countCoarseWrong, percentageCoarse)
}

type post struct {
	Content           string `json:"content"`
	EmbeddingVector   string `json:"_embedding_vector"`
	ControlCategoryID string `json:"controlCategoryId"`
	ID                string `json:"_uuid"`
}

func getUnclassified(size int) []post {
	body := fmt.Sprintf(`{
  "query": {
		"bool": {
			"must_not": {
				"exists": {
					"field": "ofCategory"
				}
			}
		}
	},
	"size": %d
} `, size)

	req, _ := http.NewRequest("GET", "http://localhost:9201/class_thing_post/_search", bytes.NewReader([]byte(body)))
	req.Header.Add("Content-Type", "application/json")
	res, err := (&http.Client{}).Do(req)
	if err != nil {
		panic(err)
	}

	if res.StatusCode != 200 {
		bb, _ := ioutil.ReadAll(res.Body)
		panic(fmt.Errorf("status is %d: %s", res.StatusCode, bb))
	}

	defer res.Body.Close()
	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	type searchRes struct {
		Hits struct {
			Hits []struct {
				Source post `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	var parsed searchRes
	err = json.Unmarshal(bytes, &parsed)
	if err != nil {
		panic(err)
	}

	out := make([]post, len(parsed.Hits.Hits), len(parsed.Hits.Hits))
	for i, hit := range parsed.Hits.Hits {
		out[i] = hit.Source
	}

	return out
}

func classify(v []float32, k int) (string, string, string, string) {
	body := fmt.Sprintf(`{
  "query": {
    "function_score": {
		  "query": {
				"bool": {
					"filter": {
						"exists": {
							"field": "ofCategory"
						}
					}
				}
			},
      "boost_mode": "replace",
      "script_score": {
        "script": {
          "inline": "binary_vector_score",
          "lang": "knn",
          "params": {
            "cosine": true,
            "field": "_embedding_vector",
            "vector": [
						%s
             ]
          }
        }
      }
    }
  },
	"aggregations": {
	  "sample": {
		 "sampler": {
				"shard_size": %d
			},
		  "aggregations": {
				"ofCategory": {
					"terms": {
						"size":1,
						"field": "ofCategory.beacon"
					}
				},
				"ofMainCategory": {
					"terms": {
						"size":1,
						"field": "ofMainCategory.beacon"
					}
				}

			}
		}
	},
	"size": 3
} `, printVector(v), k)

	req, _ := http.NewRequest("GET", "http://localhost:9201/class_thing_post/_search", bytes.NewReader([]byte(body)))
	req.Header.Add("Content-Type", "application/json")
	res, err := (&http.Client{}).Do(req)
	if err != nil {
		panic(err)
	}

	if res.StatusCode != 200 {
		bb, _ := ioutil.ReadAll(res.Body)
		panic(fmt.Errorf("status is %d: %s", res.StatusCode, bb))
	}

	defer res.Body.Close()
	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	var parsed searchRes
	err = json.Unmarshal(bytes, &parsed)
	if err != nil {
		panic(err)
	}

	// spew.Dump(parsed.Aggregations)
	// spew.Dump(parsed.Hits.Hits)

	fineBeacon := parsed.Aggregations["sample"].(map[string]interface{})["ofCategory"].(map[string]interface{})["buckets"].([]interface{})[0].(map[string]interface{})["key"]
	fineSplit := strings.Split(fineBeacon.(string), "/")

	coarseBeacon := parsed.Aggregations["sample"].(map[string]interface{})["ofMainCategory"].(map[string]interface{})["buckets"].([]interface{})[0].(map[string]interface{})["key"]
	coarseSplit := strings.Split(coarseBeacon.(string), "/")

	return fineSplit[3], fineSplit[4], coarseSplit[3], coarseSplit[4]
}

type searchRes struct {
	Hits struct {
		Hits []hit `json:"hits"`
	} `json:"hits"`
	Aggregations map[string]interface{} `json:"aggregations"`
}

type hit map[string]interface{}

func printVector(v []float32) string {
	var asStrings = make([]string, len(v), len(v))
	for i, number := range v {
		asStrings[i] = fmt.Sprintf("%f", number)
	}

	return strings.Join(asStrings, ", ")
}
