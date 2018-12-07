package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

var id = "d2a9b5be-4cfc-4929-963c-185c7f9c8697"

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

	http.HandleFunc("/peers/"+id+"/ping", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Fatal(http.ListenAndServe(":8090", nil))
}
