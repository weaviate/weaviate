package clusterapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
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
	BatchPutObjects(ctx context.Context, indexName, shardName string,
		objs []*storobj.Object) []error
	GetObject(ctx context.Context, indexName, shardName string,
		id strfmt.UUID, selectProperties search.SelectProperties,
		additional additional.Properties) (*storobj.Object, error)
	MultiGetObjects(ctx context.Context, indexName, shardName string,
		id []strfmt.UUID) ([]*storobj.Object, error)
	Search(ctx context.Context, indexName, shardName string,
		vector []float32, limit int, filters *filters.LocalFilter,
		additional additional.Properties) ([]*storobj.Object, []float32, error)
}

func NewIndices(shards shards) *indices {
	return &indices{
		regexpObjects:       regexp.MustCompile(urlPatternObjects),
		regexpObjectsSearch: regexp.MustCompile(urlPatternObjectsSearch),
		regexpObject:        regexp.MustCompile(urlPatternObject),
		shards:              shards,
	}
}

func (i *indices) Indices() http.Handler {
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
			if r.Method == http.MethodGet {
				i.getObjectsMulti().ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodPost {
				i.postObject().ServeHTTP(w, r)
				return
			}
			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
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

		ct := r.Header.Get("content-type")

		switch ct {
		case "application/vnd.weaviate.storobj.list+octet-stream":
			i.postObjectBatch(w, r, index, shard)
			return

		case "application/vnd.weaviate.storobj+octet-stream":
			i.postObjectSingle(w, r, index, shard)
			return

		default:
			http.Error(w, "415 Unsupported Media Type", http.StatusUnsupportedMediaType)
			return
		}
	})
}

func (i *indices) postObjectSingle(w http.ResponseWriter, r *http.Request,
	index, shard string) {
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
}

func unmarshalObjsList(in []byte) ([]*storobj.Object, error) {
	// NOTE: This implementation is not optimized for allocation efficiency
	var out []*storobj.Object

	reusableLengthBuf := make([]byte, 8)
	r := bytes.NewReader(in)

	for {
		_, err := r.Read(reusableLengthBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		payloadBytes := make([]byte, binary.LittleEndian.Uint64(reusableLengthBuf))
		_, err = r.Read(payloadBytes)
		if err != nil {
			return nil, err
		}

		obj, err := storobj.FromBinary(payloadBytes)
		if err != nil {
			return nil, err
		}

		out = append(out, obj)
	}

	return out, nil
}

func marshalErrList(in []error) ([]byte, error) {
	converted := make([]interface{}, len(in))
	for i, err := range in {
		if err == nil {
			continue
		}

		converted[i] = err.Error()
	}

	return json.Marshal(converted)
}

func (i *indices) postObjectBatch(w http.ResponseWriter, r *http.Request,
	index, shard string) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	objs, err := unmarshalObjsList(bodyBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	errs := i.shards.BatchPutObjects(r.Context(), index, shard, objs)
	errsJSON, err := marshalErrList(errs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("content-type", "application/vnd.weaviate.error.list+json")
	w.Write(errsJSON)
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

		if obj == nil {
			// this is a legitimate case - the requested ID doesn't exist, don't try
			// to marshal anything
			w.WriteHeader(http.StatusNotFound)
			return
		}

		objBytes, err := obj.MarshalBinary()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("content-type", "application/vnd.weaviate.storobj+octet-stream")
		w.Write(objBytes)
	})
}

func (i *indices) getObjectsMulti() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := i.regexpObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, fmt.Sprintf("invalid URI: %s", r.URL.Path),
				http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		idsEncoded := r.URL.Query().Get("ids")
		if idsEncoded == "" {
			http.Error(w, "missing required url param 'ids'",
				http.StatusBadRequest)
			return
		}

		idsBytes, err := base64.StdEncoding.DecodeString(idsEncoded)
		if err != nil {
			http.Error(w, "base64 decode 'ids' param: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		var ids []strfmt.UUID
		if err := json.Unmarshal(idsBytes, &ids); err != nil {
			http.Error(w, "unmarshal 'ids' param from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		objs, err := i.shards.MultiGetObjects(r.Context(), index, shard, ids)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		objsBytes, err := marshalObjsList(objs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().
			Set("content-type", "application/vnd.weaviate.storobj.list+octet-stream")
		w.Write(objsBytes)
	})
}

type searchParametersPayload struct {
	SearchVector []float32             `json:"searchVector"`
	Limit        int                   `json:"limit"`
	Filters      *filters.LocalFilter  `json:"filters"`
	Additional   additional.Properties `json:"additional"`
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

		resBytes, err := marshalSearchResultsPayload(results, dists)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		r.Header.Set("content-type", "application/vnd.weaviate.shardsearchresults+octet-stream")
		w.Write(resBytes)
	})
}

func marshalSearchResultsPayload(objs []*storobj.Object,
	dists []float32) ([]byte, error) {
	reusableLengthBuf := make([]byte, 8)
	var out []byte
	objsBytes, err := marshalObjsList(objs)
	if err != nil {
		return nil, err
	}

	objsLength := uint64(len(objsBytes))
	binary.LittleEndian.PutUint64(reusableLengthBuf, objsLength)

	out = append(out, reusableLengthBuf...)
	out = append(out, objsBytes...)

	distsLength := uint64(len(dists))
	binary.LittleEndian.PutUint64(reusableLengthBuf, distsLength)
	out = append(out, reusableLengthBuf...)

	distsBuf := make([]byte, distsLength*4)
	for i, dist := range dists {
		distUint32 := math.Float32bits(dist)
		binary.LittleEndian.PutUint32(distsBuf[(i*4):((i+1)*4)], distUint32)
	}
	out = append(out, distsBuf...)

	return out, nil
}

func marshalObjsList(in []*storobj.Object) ([]byte, error) {
	// NOTE: This implementation is not optimized for allocation efficiency,
	// reserve 1024 byte per object which is rather arbitrary
	out := make([]byte, 0, 1024*len(in))

	reusableLengthBuf := make([]byte, 8)
	for _, ind := range in {
		bytes, err := ind.MarshalBinary()
		if err != nil {
			return nil, err
		}

		length := uint64(len(bytes))
		binary.LittleEndian.PutUint64(reusableLengthBuf, length)

		out = append(out, reusableLengthBuf...)
		out = append(out, bytes...)
	}

	return out, nil
}
