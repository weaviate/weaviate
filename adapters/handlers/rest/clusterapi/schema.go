package clusterapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/semi-technologies/weaviate/usecases/cluster"
)

type txManager interface {
	IncomingBeginTransaction(ctx context.Context, tx *cluster.Transaction) error
	IncomingCommitTransaction(ctx context.Context, tx *cluster.Transaction) error
	IncomingAbortTransaction(ctx context.Context, tx *cluster.Transaction) error
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
