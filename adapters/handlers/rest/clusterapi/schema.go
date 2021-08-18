package clusterapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	schemauc "github.com/semi-technologies/weaviate/usecases/schema"
)

type txManager interface {
	IncomingBeginTransaction(ctx context.Context, tx *cluster.Transaction) error
	IncomingCommitTransaction(ctx context.Context, tx *cluster.Transaction) error
	IncomingAbortTransaction(ctx context.Context, tx *cluster.Transaction)
}

type schema struct {
	txManager txManager
}

func newSchema(manager txManager) *schema {
	return &schema{txManager: manager}
}

// TODO: move out of schema, this is not schema specific
func (s *schema) index() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "" && r.URL.String() != "/" {
			http.NotFound(w, r)
			return
		}

		payload := map[string]string{
			"description": "Weaviate's cluster-internal API for cross-node communication",
		}

		json.NewEncoder(w).Encode(payload)
	})
}

func (s *schema) transactions() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.incomingTransaction().ServeHTTP(w, r)
			return
		default:
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		}
	})
}

func (s *schema) incomingTransaction() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.URL.String() != "" {
			// path prefix is already stripped and we do not want any sub-paths or
			// url-level arguments present
			http.NotFound(w, r)
			return
		}

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

		txPayload, err := schemauc.UnmarshalTransaction(payload.Type, payload.Payload)
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

type txPayload struct {
	ID      string
	Type    cluster.TransactionType
	Payload json.RawMessage
}
