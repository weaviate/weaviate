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

package clusterapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
	ucs "github.com/weaviate/weaviate/usecases/schema"
)

type txManager interface {
	IncomingBeginTransaction(ctx context.Context, tx *cluster.Transaction) ([]byte, error)
	IncomingCommitTransaction(ctx context.Context, tx *cluster.Transaction) error
	IncomingAbortTransaction(ctx context.Context, tx *cluster.Transaction)
}

type txPayload struct {
	ID            string                  `json:"id"`
	Type          cluster.TransactionType `json:"type"`
	Payload       json.RawMessage         `json:"payload"`
	DeadlineMilli int64                   `json:"deadlineMilli"`
}

type txHandler struct {
	manager txManager
	auth    auth
}

func (h *txHandler) Transactions() http.Handler {
	return h.auth.handleFunc(h.transactionsHandler())
}

func (h *txHandler) transactionsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "":
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			h.incomingTransaction().ServeHTTP(w, r)
			return

		case strings.HasSuffix(path, "/commit"):
			if r.Method != http.MethodPut {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			h.incomingCommitTransaction().ServeHTTP(w, r)
			return
		default:
			if r.Method != http.MethodDelete {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			h.incomingAbortTransaction().ServeHTTP(w, r)
			return
		}
	}
}

func (h *txHandler) incomingTransaction() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.Header.Get("content-type") != "application/json" {
			http.Error(w, "415 Unsupported Media Type", http.StatusUnsupportedMediaType)
			return
		}

		var payload txPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, errors.Wrap(err, "decode body").Error(),
				http.StatusInternalServerError)
			return
		}

		if len(payload.ID) == 0 {
			http.Error(w, "id must be set", http.StatusBadRequest)
			return
		}

		if len(payload.Type) == 0 {
			http.Error(w, "type must be set", http.StatusBadRequest)
			return
		}

		txPayload, err := ucs.UnmarshalTransaction(payload.Type, payload.Payload)
		if err != nil {
			http.Error(w, errors.Wrap(err, "decode tx payload").Error(),
				http.StatusInternalServerError)
			return
		}
		txType := payload.Type
		tx := &cluster.Transaction{
			ID:       payload.ID,
			Type:     txType,
			Payload:  txPayload,
			Deadline: time.UnixMilli(payload.DeadlineMilli),
		}

		data, err := h.manager.IncomingBeginTransaction(r.Context(), tx)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, cluster.ErrConcurrentTransaction) {
				status = http.StatusConflict
			}

			http.Error(w, errors.Wrap(err, "open transaction").Error(), status)
			return
		}
		if txType != ucs.ReadSchema {
			w.WriteHeader(http.StatusCreated)
			return
		}

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
		w.WriteHeader(http.StatusCreated)
		w.Write(data)
	})
}

func (h *txHandler) incomingAbortTransaction() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		path := r.URL.String()
		tx := &cluster.Transaction{
			ID: path,
		}

		h.manager.IncomingAbortTransaction(r.Context(), tx)
		w.WriteHeader(http.StatusNoContent)
	})
}

func (h *txHandler) incomingCommitTransaction() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 2 {
			http.NotFound(w, r)
			return
		}

		tx := &cluster.Transaction{
			ID: parts[0],
		}

		if err := h.manager.IncomingCommitTransaction(r.Context(), tx); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, cluster.ErrConcurrentTransaction) {
				status = http.StatusConflict
			}

			http.Error(w, errors.Wrap(err, "open transaction").Error(), status)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
