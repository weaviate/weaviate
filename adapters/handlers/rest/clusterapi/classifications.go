//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clusterapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/cluster"
)

type classifications struct {
	txManager txManager
}

func NewClassifications(manager txManager) *classifications {
	return &classifications{txManager: manager}
}

func (s *classifications) Transactions() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "":
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			s.incomingTransaction().ServeHTTP(w, r)
			return

		case strings.HasSuffix(path, "/commit"):
			if r.Method != http.MethodPut {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			s.incomingCommitTransaction().ServeHTTP(w, r)
			return
		default:
			if r.Method != http.MethodDelete {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			s.incomingAbortTransaction().ServeHTTP(w, r)
			return
		}
	})
}

func (s *classifications) incomingTransaction() http.Handler {
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

		txPayload, err := classification.UnmarshalTransaction(payload.Type,
			payload.Payload)
		if err != nil {
			http.Error(w, errors.Wrap(err, "decode tx payload").Error(),
				http.StatusInternalServerError)
			return
		}

		tx := &cluster.Transaction{
			ID:      payload.ID,
			Type:    payload.Type,
			Payload: txPayload,
		}

		if err := s.txManager.IncomingBeginTransaction(r.Context(), tx); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, cluster.ErrConcurrentTransaction) {
				status = http.StatusConflict
			}

			http.Error(w, errors.Wrap(err, "open transaction").Error(), status)
			return
		}

		w.WriteHeader(http.StatusCreated)
	})
}

func (s *classifications) incomingAbortTransaction() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		path := r.URL.String()
		tx := &cluster.Transaction{
			ID: path,
		}

		s.txManager.IncomingAbortTransaction(r.Context(), tx)
		w.WriteHeader(http.StatusNoContent)
	})
}

func (s *classifications) incomingCommitTransaction() http.Handler {
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

		if err := s.txManager.IncomingCommitTransaction(r.Context(), tx); err != nil {
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
