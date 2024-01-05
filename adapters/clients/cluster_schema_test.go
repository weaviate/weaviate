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

package clients

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/cluster"
)

func TestOpenTransactionNoReturnPayload(t *testing.T) {
	// The No-Return-Payload is the situation that existed prior to v1.17 where
	// the only option for transactions was to broadcast updates. By keeping this
	// test around, we can make sure that we are not breaking backward
	// compatibility.

	handler := func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		require.Nil(t, err)

		var pl txPayload
		err = json.Unmarshal(body, &pl)
		require.Nil(t, err)

		assert.Equal(t, "mamma-mia-paylodia-belissima", pl.Payload.(string))
		w.WriteHeader(http.StatusCreated)
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

		w.WriteHeader(http.StatusCreated)
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

func TestOpenTransactionWithTTL(t *testing.T) {
	deadline, err := time.Parse(time.RFC3339Nano, "2040-01-02T15:04:05.00Z")
	require.Nil(t, err)

	handler := func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		require.Nil(t, err)

		var pl txPayload
		err = json.Unmarshal(body, &pl)
		require.Nil(t, err)

		parsedDL := time.UnixMilli(pl.DeadlineMilli)
		assert.Equal(t, deadline.UnixNano(), parsedDL.UnixNano())
		w.WriteHeader(http.StatusCreated)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	u, _ := url.Parse(server.URL)

	c := NewClusterSchema(&http.Client{})

	tx := &cluster.Transaction{
		ID:       "12345",
		Type:     "the best",
		Payload:  "mamma-mia-paylodia-belissima",
		Deadline: deadline,
	}

	err = c.OpenTransaction(context.Background(), u.Host, tx)
	assert.Nil(t, err)
}

func TestOpenTransactionUnhappyPaths(t *testing.T) {
	type test struct {
		name                string
		handler             http.HandlerFunc
		expectedErr         error
		expectedErrContains string
		ctx                 context.Context
		shutdownPrematurely bool
	}

	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []test{
		{
			name: "concurrent transaction",
			handler: func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusConflict)
			},
			expectedErr: cluster.ErrConcurrentTransaction,
		},
		{
			name: "arbitrary 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("nope!"))
			},
			expectedErrContains: "nope!",
		},
		{
			name: "invalid json",
			handler: func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte("<<<!@#*)!@#****@!''"))
			},
			expectedErrContains: "error unmarshalling",
		},
		{
			name: "expired ctx",
			ctx:  expiredCtx,
			handler: func(w http.ResponseWriter, r *http.Request) {
			},
			expectedErrContains: "context",
		},
		{
			name:                "remote server shut down",
			shutdownPrematurely: true,
			handler: func(w http.ResponseWriter, r *http.Request) {
			},
			expectedErrContains: "refused",
		},
		{
			name: "tx id mismatch",
			handler: func(w http.ResponseWriter, r *http.Request) {
				resTx := txPayload{
					Type:    "wrong-tx-id",
					ID:      "987",
					Payload: "gracie-mille-per-payload",
				}
				resBody, err := json.Marshal(resTx)
				require.Nil(t, err)

				w.WriteHeader(http.StatusCreated)
				w.Write(resBody)
			},
			expectedErrContains: "mismatch between outgoing and incoming tx ids",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(test.handler))
			if test.shutdownPrematurely {
				server.Close()
			} else {
				defer server.Close()
			}

			u, _ := url.Parse(server.URL)

			c := NewClusterSchema(&http.Client{})

			tx := &cluster.Transaction{
				ID:      "12345",
				Type:    "the best",
				Payload: "mamma-mia-paylodia-belissima",
			}

			if test.ctx == nil {
				test.ctx = context.Background()
			}

			err := c.OpenTransaction(test.ctx, u.Host, tx)
			assert.NotNil(t, err)

			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
			}

			if test.expectedErrContains != "" {
				assert.Contains(t, err.Error(), test.expectedErrContains)
			}
		})
	}
}

func TestAbortTransaction(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		w.WriteHeader(http.StatusNoContent)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	u, _ := url.Parse(server.URL)

	c := NewClusterSchema(&http.Client{})

	tx := &cluster.Transaction{
		ID:      "am-i-going-to-be-cancelled",
		Type:    "the worst",
		Payload: "",
	}

	err := c.AbortTransaction(context.Background(), u.Host, tx)
	assert.Nil(t, err)
}

func TestAbortTransactionUnhappyPaths(t *testing.T) {
	type test struct {
		name                string
		handler             http.HandlerFunc
		expectedErr         error
		expectedErrContains string
		ctx                 context.Context
		shutdownPrematurely bool
	}

	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []test{
		{
			name: "arbitrary 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("nope!"))
			},
			expectedErrContains: "nope!",
		},
		{
			name: "expired ctx",
			ctx:  expiredCtx,
			handler: func(w http.ResponseWriter, r *http.Request) {
			},
			expectedErrContains: "context",
		},
		{
			name:                "remote server shut down",
			shutdownPrematurely: true,
			handler: func(w http.ResponseWriter, r *http.Request) {
			},
			expectedErrContains: "refused",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(test.handler))
			if test.shutdownPrematurely {
				server.Close()
			} else {
				defer server.Close()
			}

			u, _ := url.Parse(server.URL)

			c := NewClusterSchema(&http.Client{})

			tx := &cluster.Transaction{
				ID:      "12345",
				Type:    "the best",
				Payload: "mamma-mia-paylodia-belissima",
			}

			if test.ctx == nil {
				test.ctx = context.Background()
			}

			err := c.AbortTransaction(test.ctx, u.Host, tx)
			assert.NotNil(t, err)

			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
			}

			if test.expectedErrContains != "" {
				assert.Contains(t, err.Error(), test.expectedErrContains)
			}
		})
	}
}

func TestCommitTransaction(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		w.WriteHeader(http.StatusNoContent)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	u, _ := url.Parse(server.URL)

	c := NewClusterSchema(&http.Client{})

	tx := &cluster.Transaction{
		ID:      "am-i-going-to-be-cancelled",
		Type:    "the worst",
		Payload: "",
	}

	err := c.CommitTransaction(context.Background(), u.Host, tx)
	assert.Nil(t, err)
}

func TestCommitTransactionUnhappyPaths(t *testing.T) {
	type test struct {
		name                string
		handler             http.HandlerFunc
		expectedErr         error
		expectedErrContains string
		ctx                 context.Context
		shutdownPrematurely bool
	}

	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []test{
		{
			name: "arbitrary 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("nope!"))
			},
			expectedErrContains: "nope!",
		},
		{
			name: "expired ctx",
			ctx:  expiredCtx,
			handler: func(w http.ResponseWriter, r *http.Request) {
			},
			expectedErrContains: "context",
		},
		{
			name:                "remote server shut down",
			shutdownPrematurely: true,
			handler: func(w http.ResponseWriter, r *http.Request) {
			},
			expectedErrContains: "refused",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(test.handler))
			if test.shutdownPrematurely {
				server.Close()
			} else {
				defer server.Close()
			}

			u, _ := url.Parse(server.URL)

			c := NewClusterSchema(&http.Client{})

			tx := &cluster.Transaction{
				ID:      "12345",
				Type:    "the best",
				Payload: "mamma-mia-paylodia-belissima",
			}

			if test.ctx == nil {
				test.ctx = context.Background()
			}

			err := c.CommitTransaction(test.ctx, u.Host, tx)
			assert.NotNil(t, err)

			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
			}

			if test.expectedErrContains != "" {
				assert.Contains(t, err.Error(), test.expectedErrContains)
			}
		})
	}
}
