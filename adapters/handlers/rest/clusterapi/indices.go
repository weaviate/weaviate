package clusterapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"regexp"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type indices struct {
	shards              shards
	regexpObjects       *regexp.Regexp
	regexpObjectsSearch *regexp.Regexp
	regexpObject        *regexp.Regexp
}

const (
	urlPatternObjects = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects`
	urlPatternObjectsSearch = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects\/_search`
	urlPatternObject = `\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects\/([A-Za-z0-9_+-]+)`
)

type shards interface {
	PutObject(ctx context.Context, indexName, shardName string,
		obj *storobj.Object) error
	GetObject(ctx context.Context, indexName, shardName string,
		id strfmt.UUID, selectProperties search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	Search(ctx context.Context, indexName, shardName string,
		vector []float32, limit int, filters *filters.LocalFilter,
		additional additional.Properties) ([]*storobj.Object, []float32, error)
}

func newIndices(shards shards) *indices {
	return &indices{
		regexpObjects:       regexp.MustCompile(urlPatternObjects),
		regexpObjectsSearch: regexp.MustCompile(urlPatternObjectsSearch),
		regexpObject:        regexp.MustCompile(urlPatternObject),
		shards:              shards,
	}
}

func (i *indices) indices() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case i.regexpObjectsSearch.MatchString(path):
			if r.Method != http.MethodPost {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.postSearchObjects().ServeHTTP(w, r)
			return
		case i.regexpObject.MatchString(path):
			if r.Method != http.MethodGet {
				http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
				return
			}

			i.getObject().ServeHTTP(w, r)
			return

		case i.regexpObjects.MatchString(path):
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
		args := i.regexpObjects.FindStringSubmatch(r.URL.Path)
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

func (i *indices) getObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		defer r.Body.Close()

		additionalEncoded := r.URL.Query().Get("additional")
		if additionalEncoded == "" {
			http.Error(w, "missing required url param 'additional'",
				http.StatusBadRequest)
			return
		}

		additionalBytes, err := base64.StdEncoding.DecodeString(additionalEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'additional' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		selectPropertiesEncoded := r.URL.Query().Get("selectProperties")
		if selectPropertiesEncoded == "" {
			http.Error(w, "missing required url param 'selectProperties'",
				http.StatusBadRequest)
			return
		}

		selectPropertiesBytes, err := base64.StdEncoding.
			DecodeString(selectPropertiesEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'selectProperties' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var additional additional.Properties
		if err := json.Unmarshal(additionalBytes, &additional); err != nil {
			http.Error(w, "unmarshal 'additional' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var selectProperties search.SelectProperties
		if err := json.Unmarshal(selectPropertiesBytes, &selectProperties); err != nil {
			http.Error(w, "unmarshal 'selectProperties' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		obj, err := i.shards.GetObject(r.Context(), index, shard, strfmt.UUID(id),
			selectProperties, additional)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		objBytes, err := json.Marshal(obj)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		r.Header.Set("content-type", "application/vnd.weaviate.storobj+octet-stream")
		w.Write(objBytes)
	})
}

type searchParametersPayload struct {
	SearchVector []float32             `json:"searchVector"`
	Limit        int                   `json:"limit"`
	Filters      *filters.LocalFilter  `json:"filters"`
	Additional   additional.Properties `json:"additional"`
}

type searchResultsPayload struct {
	Results   []*storobj.Object `json:"results"`
	Distances []float32         `json:"distances"`
}

func (i *indices) postSearchObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjectsSearch.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var params searchParametersPayload
		if err := json.Unmarshal(reqPayload, &params); err != nil {
			http.Error(w, "unmarshal search params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results, dists, err := i.shards.Search(r.Context(), index, shard,
			params.SearchVector, params.Limit, params.Filters, params.Additional)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resPayload := searchResultsPayload{
			Results:   results,
			Distances: dists,
		}

		resBytes, err := json.Marshal(resPayload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		r.Header.Set("content-type", "application/vnd.weaviate.shardsearchresults+octet-stream")
		w.Write(resBytes)
	})
}
