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
			w.WriteHeader(422)
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
		expectedBody := fmt.Sprintf("%s\n", `{"query":"{ Local { Get { Things { City { name } } } } }"}`)

		if string(bodyBytes) != expectedBody {
			w.WriteHeader(422)
			w.Write([]byte(fmt.Sprintf("wrong body, got \n%#v\nwanted\n%#v\n", string(bodyBytes), expectedBody)))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "%s", happyPathResponse)
	})

	log.Fatal(http.ListenAndServe(":8081", nil))
}

var happyPathResponse = `{
  "data": {
    "Local": {
      "Get": {
        "Things": {
          "City": [
            {
              "name": "Hamburg"
            },
            {
              "name": "New York"
            },
            {
              "name": "Neustadt an der Weinstraße"
            },
            {
              "name": "Tokyo"
            }
          ]
        }
      }
    }
  }
}`
