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

// A mock endpoint used to test telemetry request logging.

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

func main() {
	fmt.Println("Listening on Port 8087")

	router := mux.NewRouter()

	router.HandleFunc("/mock/new", AddLog).Methods("POST")
	router.HandleFunc("/mock/last", GetMostRecentLog).Methods("GET")
	log.Fatal(http.ListenAndServe(":8087", router))
}

var receivedLogs [][]byte
var mostRecentLog []byte

// EmptyRequestSize is the length of an empty log converted to a byte array (cbor)
const EmptyRequestSize int = 3

// AddLog stores the most recently received non-empty POSTed log.
func AddLog(w http.ResponseWriter, r *http.Request) {
	fmt.Println(fmt.Sprintf("received POST at %s", time.Now()))

	buf := new(bytes.Buffer)
	buf.ReadFrom(r.Body)
	content := buf.Bytes()

	if len(content) > EmptyRequestSize {
		fmt.Println(fmt.Sprintf("%x", content))
		receivedLogs = append(receivedLogs, content)
		mostRecentLog = content
	}
}

// GetMostRecentLog returns the most recently received log.
func GetMostRecentLog(w http.ResponseWriter, r *http.Request) {
	fmt.Println("received GET")
	fmt.Println(mostRecentLog)
	w.Write(mostRecentLog)
}
