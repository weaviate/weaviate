package clusterapi

import (
	"context"
	"io"
	"net/http"
	"regexp"

	"github.com/semi-technologies/weaviate/entities/storobj"
)

type indices struct {
	shards           shards
	regexpPostObject *regexp.Regexp
}

const (
	urlPatternObjects = `\/indices\/([A-Za-z0-9_+-]+)\/shards\/([A-Za-z0-9]+)\/objects`
)

type shards interface {
	PutObject(ctx context.Context, indexName, shardName string,
		obj *storobj.Object) error
}

func newIndices(shards shards) *indices {
	return &indices{
		regexpPostObject: regexp.MustCompile(urlPatternObjects),
		shards:           shards,
	}
}

func (i *indices) indices() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case i.regexpPostObject.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postObject().ServeHTTP(w, r)
			return
		default:
			http.NotFound(w, r)
			return
		}
	})
}

func (i *indices) postObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpPostObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		if r.Header.Get("content-type") != "application/vnd.weaviate.storobj+octet-stream" {
			http.Error(w, "415 Unsupported Media Type", http.StatusUnsupportedMediaType)
			return
		}

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		obj, err := storobj.FromBinary(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := i.shards.PutObject(r.Context(), index, shard, obj); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}
