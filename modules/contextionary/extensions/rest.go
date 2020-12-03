package extensions

import "net/http"

type RESTHandlers struct {
	Mux *http.ServeMux
}

func NewRESTHandlers() *RESTHandlers {
	return &RESTHandlers{}
}

func (h *RESTHandlers) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`hello world from the contextionary extensions`))
	})
}
