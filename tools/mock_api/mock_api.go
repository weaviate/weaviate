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

// A mock endpoint used to test telemetry request logging.

import (
	"bytes"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	fmt.Println("Hello world")

	router := mux.NewRouter()

	router.HandleFunc("/mock/new", AddLog).Methods("POST")
	router.HandleFunc("/mock/last", GetMostRecentLog).Methods("GET")
	router.HandleFunc("/mock/count", GetMostRecentLog).Methods("GET")
	log.Fatal(http.ListenAndServe(":8087", router))
}

var receivedLogs [][]byte
var mostRecentLog []byte
var counter int

// AddLog stores a POSTed log.
func AddLog(w http.ResponseWriter, r *http.Request) {
	fmt.Println("received POST")
	buf := new(bytes.Buffer)
	buf.ReadFrom(r.Body)
	content := buf.Bytes()

	if len(content) > 0 {
		counter++
		receivedLogs = append(receivedLogs, content)
		mostRecentLog = content
	}
}

// GetMostRecentLog returns the most recently received log.
func GetMostRecentLog(w http.ResponseWriter, r *http.Request) {
	fmt.Println("received GET")
	w.Write(mostRecentLog)
}
