package extensions

import (
	"io/ioutil"
	"net/http"
)

type RESTHandlers struct {
	Mux *http.ServeMux
	ls  LoaderStorer
}

func NewRESTHandlers(ls LoaderStorer) *RESTHandlers {
	return &RESTHandlers{
		ls: ls,
	}
}

func (h *RESTHandlers) Handler() http.Handler {
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

func (h *RESTHandlers) get(w http.ResponseWriter, r *http.Request) {
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

func (h *RESTHandlers) put(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	concept := h.extractConcept(r)
	if concept == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
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

func (h *RESTHandlers) extractConcept(r *http.Request) string {
	// cutoff leading slash, consider the rest the concept
	return r.URL.String()[1:]
}

type Storer interface {
	Store(concept string, value []byte) error
}

type Loader interface {
	Load(concept string) ([]byte, error)
}

type LoaderStorer interface {
	Storer
	Loader
}
