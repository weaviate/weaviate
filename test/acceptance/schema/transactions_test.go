//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test makes sure that a malformed tx payload (possibly from a bad actor)
// can't crash - or possibly worse - deadlock Weaviate
//
// See https://github.com/weaviate/weaviate/issues/3401 for details
func TestCrashServerThroughInvalidTxPayloads(t *testing.T) {
	tests := []struct {
		name           string
		txId           string
		payload        string
		errMustContain string
	}{
		{
			name:           "add_class: empty class payload",
			txId:           "1",
			payload:        `{"id":"1", "type": "add_class", "payload":{}}`,
			errMustContain: "class is nil",
		},
		{
			name:           "add_class: class set, but empty, no state",
			txId:           "2",
			payload:        `{"id":"2", "type": "add_class", "payload":{"class":{}}}`,
			errMustContain: "state is nil",
		},
		{
			name:           "add_class: class set and valid, no state",
			txId:           "3",
			payload:        `{"id":"3", "type": "add_class", "payload":{"class":{"vectorIndexType":"hnsw","class":"Foo"}}}`,
			errMustContain: "state is nil",
		},
		{
			name:           "add_class: class set, but empty, with state",
			txId:           "4",
			payload:        `{"id":"4", "type": "add_class", "payload":{"state":{},"class":{}}}`,
			errMustContain: "unsupported vector index",
		},
		{
			name:           "update_class: empty class payload",
			txId:           "1",
			payload:        `{"id":"1", "type": "update_class", "payload":{}}`,
			errMustContain: "class is nil",
		},
		{
			name:    "update_class: class set and valid, no state",
			txId:    "3",
			payload: `{"id":"3", "type": "update_class", "payload":{"class":{"vectorIndexType":"hnsw","class":"FoobarBazzzar"}}}`,
		},
		{
			name:           "update_class: class set, but empty, with state",
			txId:           "4",
			payload:        `{"id":"4", "type": "update_class", "payload":{"state":{},"class":{}}}`,
			errMustContain: "unsupported vector index",
		},
		{
			name:           "add_tenants: empty payload",
			txId:           "1",
			payload:        `{"id":"1", "type": "add_tenants", "payload":{}}`,
			errMustContain: "not found",
		},
		{
			name:           "delete_tenants: malformed payload",
			txId:           "1",
			payload:        `{"id":"1", "type": "delete_tenants", "payload":7}`,
			errMustContain: "invalid",
		},
		{
			name:           "delete_class: malformed payload",
			txId:           "2",
			payload:        `{"id":"2", "type": "delete_class", "payload":7}`,
			errMustContain: "invalid",
		},
		{
			name:           "add_property: missing prop",
			txId:           "1",
			payload:        `{"id":"1", "type": "add_property", "payload":{"class":"Foo"}}`,
			errMustContain: "property is nil",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := http.Client{}

			// open tx
			payload := []byte(test.payload)
			req, err := http.NewRequest(http.MethodPost, "http://localhost:7101/schema/transactions/", bytes.NewReader(payload))
			require.NoError(t, err)

			req.Header.Add("Content-Type", "application/json")

			res, err := client.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			// try to commit tx
			req, err = http.NewRequest(http.MethodPut,
				fmt.Sprintf("http://localhost:7101/schema/transactions/%s/commit", test.txId), nil)
			require.NoError(t, err)

			res, err = client.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Greater(t, res.StatusCode, 399)
			resBytes, _ := io.ReadAll(res.Body)
			assert.Contains(t, string(resBytes), test.errMustContain)

			// clean up tx (so next test doesn't have concurrent tx error)
			req, err = http.NewRequest(http.MethodDelete,
				fmt.Sprintf("http://localhost:7101/schema/transactions/%s", test.txId), nil)
			require.NoError(t, err)

			res, err = client.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()
		})
	}
}
