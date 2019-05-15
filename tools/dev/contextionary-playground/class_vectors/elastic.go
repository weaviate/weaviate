package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strings"
)

func convertArrayToBase64(array []float32) string {
	bytes := make([]byte, 0, 4*len(array))
	for _, a := range array {
		bits := math.Float32bits(a)
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, bits)
		bytes = append(bytes, b...)
	}

	encoded := base64.StdEncoding.EncodeToString(bytes)
	return encoded
}

type document struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Content        string `json:"content"`
	SampleBoolProp bool   `json:"sampleBoolProp"`
}

type vectorDocument struct {
	document
	EmbeddingVector string `json:"embedding_vector"`
}

func asJSON(u vectorDocument) []byte {
	b, _ := json.Marshal(u)
	return b
}

func put(item vectorDocument) error {
	json := asJSON(item)
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://localhost:9900/documents/text/%d", item.ID), bytes.NewReader(json))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{}).Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 && res.StatusCode != 201 {
		b := res.Body
		defer b.Close()
		body, _ := ioutil.ReadAll(b)
		return errors.New("failed: " + string(body))
	}
	return nil
}

func setMapping() error {
	payload := []byte(`{
  "mappings": {
    "text": {
      "properties": {
        "embedding_vector": {
					"type": "binary",
					"doc_values": true
        },
				"sampleBoolProp": {
					"type": "boolean"
				}
      }
    }
  }
}`)

	req, err := http.NewRequest("PUT", "http://localhost:9900/documents/", bytes.NewReader(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{}).Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 && res.StatusCode != 201 {
		b := res.Body
		defer b.Close()
		body, _ := ioutil.ReadAll(b)
		return errors.New("failed: " + string(body))
	}
	return nil
}

func printVector(v []float32) string {
	var asStrings = make([]string, len(v), len(v))
	for i, number := range v {
		asStrings[i] = fmt.Sprintf("%f", number)
	}

	return strings.Join(asStrings, ", ")
}
