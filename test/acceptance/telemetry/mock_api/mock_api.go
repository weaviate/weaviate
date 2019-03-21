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

// Acceptance tests for logging. Sets up a small fake endpoint that logs are sent to.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	fmt.Println("Hello world")

	router := mux.NewRouter()

	firstLog := TestJSON{"henk", 1, "Human", 12345, "5"}
	receivedLogs = append(receivedLogs, firstLog)

	router.HandleFunc("/mock", PostLog).Methods("POST")
	router.HandleFunc("/mock", AccessLogs).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))
}

// TestJSON ...
type TestJSON struct {
	Name      string `json:"n,omitempty"`
	Amount    int    `json:"a,omitempty"`
	Type      string `json:"t,omitempty"`
	Timestamp int    `json:"w,omitempty"`
	Id        string `json:"i,omitempty"`
}

var receivedLogs []TestJSON

// PostLog ...
func PostLog(w http.ResponseWriter, r *http.Request) {
	fmt.Println("received POST")
	var log TestJSON
	_ = json.NewDecoder(r.Body).Decode(&log)
	receivedLogs = append(receivedLogs, log)
	json.NewEncoder(w).Encode(receivedLogs)
}

// AccessLogs ...
func AccessLogs(w http.ResponseWriter, r *http.Request) {
	fmt.Println("received GET")
	json.NewEncoder(w).Encode(receivedLogs)
}
