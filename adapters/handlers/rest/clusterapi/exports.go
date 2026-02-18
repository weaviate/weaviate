//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clusterapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/weaviate/weaviate/usecases/export"
)

type exports struct {
	participant *export.Participant
	auth        auth
}

// NewExports creates a new exports cluster API handler
func NewExports(participant *export.Participant, auth auth) *exports {
	return &exports{participant: participant, auth: auth}
}

// Execute handles POST /exports/execute
func (e *exports) Execute() http.Handler {
	return e.auth.handleFunc(e.executeHandler())
}

func (e *exports) executeHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Errorf("read request body: %w", err).Error(), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		var req export.ExportRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, fmt.Errorf("unmarshal request: %w", err).Error(), http.StatusBadRequest)
			return
		}

		if err := e.participant.OnExecute(r.Context(), &req); err != nil {
			http.Error(w, fmt.Errorf("execute export: %w", err).Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}
}
