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

package extensions

import (
	"context"
	"io"
	"net/http"

	"github.com/weaviate/weaviate/entities/models"
)

type RESTUserFacingHandlers struct {
	proxy Proxy
}

func newRESTUserFacingHandlers(proxy Proxy) *RESTUserFacingHandlers {
	return &RESTUserFacingHandlers{
		proxy: proxy,
	}
}

func (h *RESTHandlers) UserFacingHandler() http.Handler {
	return newRESTUserFacingHandlers(h.proxy).Handler()
}

func (h *RESTUserFacingHandlers) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			h.post(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func (h *RESTUserFacingHandlers) post(w http.ResponseWriter, r *http.Request) {
	ct := r.Header.Get("content-type")
	if ct != "application/json" {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}

	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, err, http.StatusInternalServerError)
		return
	}

	var ext models.C11yExtension
	if err := (&ext).UnmarshalBinary(body); err != nil {
		h.writeError(w, err, http.StatusUnprocessableEntity)
		return
	}

	if err := h.proxy.AddExtension(r.Context(), &ext); err != nil {
		h.writeError(w, err, http.StatusBadRequest)
		return
	}

	resBody, err := ext.MarshalBinary()
	if err != nil {
		h.writeError(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(resBody)
}

// C11yProxy proxies the request through the separate container, only for it to
// come back here for the storage. This is legacy from the pre-module times.
// TODO: cleanup, there does not need to be a separation between user-facing
// and internal storage endpoint in the long-term
type Proxy interface {
	AddExtension(ctx context.Context, extension *models.C11yExtension) error
}

func (h *RESTUserFacingHandlers) writeError(w http.ResponseWriter, err error, code int) {
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
