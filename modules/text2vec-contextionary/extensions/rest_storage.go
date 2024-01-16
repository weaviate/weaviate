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
	"io"
	"net/http"
)

type RESTHandlers struct {
	ls    LoaderStorer
	proxy Proxy
}

func NewRESTHandlers(ls LoaderStorer, proxy Proxy) *RESTHandlers {
	return &RESTHandlers{
		ls:    ls,
		proxy: proxy,
	}
}

type RESTStorageHandlers struct {
	ls LoaderStorer
}

func newRESTStorageHandlers(ls LoaderStorer) *RESTStorageHandlers {
	return &RESTStorageHandlers{
		ls: ls,
	}
}

func (h *RESTHandlers) StorageHandler() http.Handler {
	return newRESTStorageHandlers(h.ls).Handler()
}

func (h *RESTStorageHandlers) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			h.get(w, r)
		case http.MethodPut:
			h.put(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func (h *RESTStorageHandlers) get(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.String()) == 0 || h.extractConcept(r) == "" {
		h.getAll(w, r)
		return
	}

	h.getOne(w, r)
}

func (h *RESTStorageHandlers) getOne(w http.ResponseWriter, r *http.Request) {
	concept := h.extractConcept(r)
	if concept == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	res, err := h.ls.Load(concept)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	if res == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Write(res)
}

func (h *RESTStorageHandlers) getAll(w http.ResponseWriter, r *http.Request) {
	res, err := h.ls.LoadAll()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Write(res)
}

func (h *RESTStorageHandlers) put(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	concept := h.extractConcept(r)
	if len(concept) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

	err = h.ls.Store(concept, body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func (h *RESTStorageHandlers) extractConcept(r *http.Request) string {
	// cutoff leading slash, consider the rest the concept
	return r.URL.String()[1:]
}

type Storer interface {
	Store(concept string, value []byte) error
}

type Loader interface {
	Load(concept string) ([]byte, error)
}

type LoaderAller interface {
	LoadAll() ([]byte, error)
}

type LoaderStorer interface {
	Storer
	Loader
	LoaderAller
}
