//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

var id = "d2a9b5be-4cfc-4929-963c-185c7f9c8697"
var weaviateFakeID = "e90effd8-dac7-40af-9a15-6eb8f2f7bcab"

func getEnvOrDefault(envName string, defaultValue string) string {
	value := os.Getenv(envName)
	if value == "" {
		return defaultValue
	}

	return value
}

func updatePeerWithList() {
	payload := []map[string]string{{
		"id":   weaviateFakeID,
		"name": getEnvOrDefault("REMOTE_PEER_NAME", "WeaviateB"),
		"uri":  getEnvOrDefault("REMOTE_PEER_URI", "http://localhost:8081"),
	}}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("could not marshal payload to json: %s", err)
		return
	}

	// TODO: don't rely on macOS specific docker-features
	// has to be fixed before merging the branch
	// most likely this will become irrelevant as
	// the p2p feature will only run in docker-compose
	localPeerOrigin := getEnvOrDefault("LOCAL_PEER_URI", "http://docker.for.mac.localhost:8080")
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/weaviate/v1/p2p/genesis", localPeerOrigin), bytes.NewReader(payloadBytes))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		log.Printf("could create put request: %s", err)
		return
	}

	res, err := (&http.Client{}).Do(req)
	if err != nil {
		log.Printf("could send put request: %s", err)
		return
	}

	if res.StatusCode != http.StatusOK {
		log.Printf("could not send peer list: response status was %s", res.Status)
		return
	}

	log.Print("successfully sent peer list to weaviate")
}

func main() {
	http.HandleFunc("/peers/register", func(w http.ResponseWriter, req *http.Request) {
		response := map[string]interface{}{
			"peer": map[string]interface{}{
				"id":              id,
				"peerName":        "bestWeaviate",
				"last_contact_at": 1234,
				"peerUri":         "http://localhost:8080",
			},
			"contextionary": map[string]interface{}{
				"hash": map[string]string{
					"algorithm": "foo",
					"value":     "bar",
				},
				"url": "http://contextionary.com",
			},
		}
		responseBytes, err := json.Marshal(response)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "%s", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(responseBytes)
	})

	go func() {
		var backOff time.Duration = 1
		for {
			if backOff < 30 {
				backOff++
			}
			updatePeerWithList()
			time.Sleep(backOff * time.Second)
		}

	}()

	http.HandleFunc("/peers/"+id+"/ping", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Fatal(http.ListenAndServe(":8090", nil))
}
