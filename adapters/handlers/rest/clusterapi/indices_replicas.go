//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clusterapi

import (
	"context"
	"io"
	"net/http"
	"regexp"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type replicator interface {
	PutObject(ctx context.Context, index, shardName string,
		obj *storobj.Object) error
	DeleteObject(ctx context.Context, index, shardName string,
		id strfmt.UUID) error
}

type replicatedIndices struct {
	shards replicator
}

var (
	regxObject = regexp.MustCompile(`\/replica\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects\/([A-Za-z0-9_+-]+)`)
	regxObjects = regexp.MustCompile(`\/replica\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects`)
)

func NewReplicatedIndices(shards replicator) *replicatedIndices {
	return &replicatedIndices{
		shards: shards,
	}
}

func (i *replicatedIndices) Indices() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {

		case regxObject.MatchString(path):
			if r.Method == http.MethodDelete {
				i.deleteObject().ServeHTTP(w, r)
				return
			}

			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case regxObjects.MatchString(path):

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

func (i *replicatedIndices) postObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		ct := r.Header.Get("content-type")

		switch ct {

		case IndicesPayloads.SingleObject.MIME():
			i.postObjectSingle(w, r, index, shard)
			return

		default:
			http.Error(w, "415 Unsupported Media Type", http.StatusUnsupportedMediaType)
			return
		}
	})
}

func (i *replicatedIndices) deleteObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		defer r.Body.Close()

		err := i.shards.DeleteObject(r.Context(), index, shard, strfmt.UUID(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *replicatedIndices) postObjectSingle(w http.ResponseWriter, r *http.Request,
	index, shard string,
) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	obj, err := IndicesPayloads.SingleObject.Unmarshal(bodyBytes)
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
