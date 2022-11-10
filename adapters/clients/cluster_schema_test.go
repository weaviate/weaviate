//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenTransactionNoReturnPayload(t *testing.T) {
	// The No-Return-Payload is the situation that existed prior to v1.17 where
	// the only option for transactions was to broadcast updates. By keeping this
	// test around, we can make sure that we are not breaking backward
	// compatibility.

	handler := func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := ioutil.ReadAll(r.Body)
		require.Nil(t, err)

		var pl txPayload
		err = json.Unmarshal(body, &pl)
		require.Nil(t, err)

		assert.Equal(t, "mamma-mia-paylodia-belissima", pl.Payload.(string))
		w.WriteHeader(201)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	u, _ := url.Parse(server.URL)

	c := NewClusterSchema(&http.Client{})

	tx := &cluster.Transaction{
		ID:      "12345",
		Type:    "the best",
		Payload: "mamma-mia-paylodia-belissima",
	}

	err := c.OpenTransaction(context.Background(), u.Host, tx)
	assert.Nil(t, err)
}

func TestOpenTransactionWithReturnPayload(t *testing.T) {
	// Newly added as part of v1.17 where read-transactions were introduced which
	// are used to sync up cluster schema state when nodes join
	handler := func(w http.ResponseWriter, r *http.Request) {
		resTx := txPayload{
			Type:    "my-tx",
			ID:      "987",
			Payload: "gracie-mille-per-payload",
		}
		resBody, err := json.Marshal(resTx)
		require.Nil(t, err)

		w.WriteHeader(201)
		w.Write(resBody)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	u, _ := url.Parse(server.URL)

	c := NewClusterSchema(&http.Client{})

	txIn := &cluster.Transaction{
		ID:   "987",
		Type: "my-tx",
	}

	err := c.OpenTransaction(context.Background(), u.Host, txIn)
	assert.Nil(t, err)

	expectedTxOut := &cluster.Transaction{
		ID:      "987",
		Type:    "my-tx",
		Payload: json.RawMessage("\"gracie-mille-per-payload\""),
	}

	assert.Equal(t, expectedTxOut, txIn)
}
