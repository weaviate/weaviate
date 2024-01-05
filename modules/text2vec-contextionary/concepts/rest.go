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

package concepts

import (
	"context"
	"net/http"

	"github.com/weaviate/weaviate/entities/models"
)

type RESTHandlers struct {
	inspector Inspector
}

func NewRESTHandlers(inspector Inspector) *RESTHandlers {
	return &RESTHandlers{
		inspector: inspector,
	}
}

func (h *RESTHandlers) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.get(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func (h *RESTHandlers) get(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.String()) == 0 || h.extractConcept(r) == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	h.getOne(w, r)
}

func (h *RESTHandlers) getOne(w http.ResponseWriter, r *http.Request) {
	concept := h.extractConcept(r)

	res, err := h.inspector.GetWords(r.Context(), concept)
	if err != nil {
		h.writeError(w, err, http.StatusBadRequest)
		return
	}

	json, err := res.MarshalBinary()
	if err != nil {
		h.writeError(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(json)
}

func (h *RESTHandlers) writeError(w http.ResponseWriter, err error, code int) {
	res := &models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{{
		Message: err.Error(),
	}}}

	json, mErr := res.MarshalBinary()
	if mErr != nil {
		// fallback to text
		w.Header().Add("content-type", "text/plain")
		w.WriteHeader(code)
		w.Write([]byte(err.Error()))
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(code)
	w.Write(json)
}

func (h *RESTHandlers) extractConcept(r *http.Request) string {
	// cutoff leading slash, consider the rest the concept
	return r.URL.String()[1:]
}

type Inspector interface {
	GetWords(ctx context.Context, words string) (*models.C11yWordsResponse, error)
}
