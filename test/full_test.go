/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https:///blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package main

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"
)

/*
 * Request function
 */
func doRequest(endpoint string, method string, accept string, body io.Reader) *http.Response {
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}

	req, _ := http.NewRequest(method, "http://localhost:8080/weaviate/v1-alpha"+endpoint, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)

	response, err := client.Do(req)

	if err != nil {
		panic(err)
	}

	return response

}

/******************
 * START TESTS
 ******************/

// weaviate.adapters.list
func Test__weaviate_adapters_list_JSON(t *testing.T) {
	result := doRequest("/adapters", "GET", "application/json", nil)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_list_XML(t *testing.T) {
	result := doRequest("/adapters", "GET", "application/xml", nil)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.insert
func Test__weaviate_adapters_insert_JSON(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{}`))
	result := doRequest("/adapters", "POST", "application/json", jsonStr)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_insert_XML(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{}`))
	result := doRequest("/adapters", "POST", "application/xml", jsonStr)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.delete
func Test__weaviate_adapters_delete_JSON(t *testing.T) {
	result := doRequest("/adapters/1", "DELETE", "application/json", nil)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_delete_XML(t *testing.T) {
	result := doRequest("/adapters/1", "DELETE", "application/xml", nil)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.get
func Test__weaviate_adapters_get_JSON(t *testing.T) {
	result := doRequest("/adapters/1", "GET", "application/json", nil)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_get_XML(t *testing.T) {
	result := doRequest("/adapters/1", "GET", "application/xml", nil)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.update
func Test__weaviate_adapters_update_JSON(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{ "activateUrl": "string", "activated": true, "deactivateUrl": "string", "displayName": "string", "iconUrl": "string", "id": "string", "manageUrl": "string" }`))
	result := doRequest("/adapters/1", "PUT", "application/json", jsonStr)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_update_XML(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{"activateUrl": "string"}`))
	result := doRequest("/adapters/1", "PUT", "application/xml", jsonStr)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

// weaviate.adapters.patch
func Test__weaviate_adapters_patch_JSON(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{}`))
	result := doRequest("/adapters/1", "PATCH", "application/json", jsonStr)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}

func Test__weaviate_adapters_patch_XML(t *testing.T) {
	jsonStr := bytes.NewBuffer([]byte(`{}`))
	result := doRequest("/adapters/1", "PATCH", "application/xml", jsonStr)
	if result.StatusCode != http.StatusNotImplemented {
		t.Errorf("Expected response code %d. Got %d\n", http.StatusNotImplemented, result.StatusCode)
	}
}
