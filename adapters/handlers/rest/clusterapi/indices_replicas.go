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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/replica"
	"github.com/semi-technologies/weaviate/usecases/scaler"
)

type replicator interface {
	ReplicateObject(ctx context.Context, indexName, shardName,
		requestID string, object *storobj.Object) replica.SimpleResponse
	ReplicateObjects(ctx context.Context, indexName, shardName,
		requestID string, objects []*storobj.Object) replica.SimpleResponse
	ReplicateUpdate(ctx context.Context, indexName, shardName,
		requestID string, mergeDoc *objects.MergeDocument) replica.SimpleResponse
	ReplicateDeletion(ctx context.Context, indexName, shardName,
		requestID string, uuid strfmt.UUID) replica.SimpleResponse
	ReplicateDeletions(ctx context.Context, indexName, shardName,
		requestID string, docIDs []uint64, dryRun bool) replica.SimpleResponse
	ReplicateReferences(ctx context.Context, indexName, shardName,
		requestID string, refs []objects.BatchReference) replica.SimpleResponse
	CommitReplication(indexName,
		shardName, requestID string) interface{}
	AbortReplication(indexName,
		shardName, requestID string) interface{}
}

type localScaler interface {
	LocalScaleOut(ctx context.Context, className string,
		dist scaler.ShardDist) error
}

type replicatedIndices struct {
	shards replicator
	scaler localScaler
}

var (
	regxObject = regexp.MustCompile(`\/replicas\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects\/([A-Za-z0-9_+-]+)`)
	regxObjects = regexp.MustCompile(`\/replicas\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects`)
	regxReferences = regexp.MustCompile(`\/replicas\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+)\/objects/references`)
	regxIncreaseRepFactor = regexp.MustCompile(`\/replicas\/indices\/([A-Za-z0-9_+-]+)` +
		`\/replication-factor:increase`)
	regxCommitPhase = regexp.MustCompile(`\/replicas\/indices\/([A-Za-z0-9_+-]+)` +
		`\/shards\/([A-Za-z0-9]+):(commit|abort)`)
)

func NewReplicatedIndices(shards replicator, scaler localScaler) *replicatedIndices {
	return &replicatedIndices{
		shards: shards,
		scaler: scaler,
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

			if r.Method == http.MethodPatch {
				i.patchObject().ServeHTTP(w, r)
				return
			}

			if regxReferences.MatchString(path) {
				if r.Method == http.MethodPost {
					i.postRefs().ServeHTTP(w, r)
					return
				}
			}

			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case regxObjects.MatchString(path):
			if r.Method == http.MethodPost {
				i.postObject().ServeHTTP(w, r)
				return
			}

			if r.Method == http.MethodDelete {
				i.deleteObjects().ServeHTTP(w, r)
				return
			}

			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case regxIncreaseRepFactor.MatchString(path):
			if r.Method == http.MethodPut {
				i.increaseReplicationFactor().ServeHTTP(w, r)
				return
			}

			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return

		case regxCommitPhase.MatchString(path):
			if r.Method == http.MethodPost {
				i.executeCommitPhase().ServeHTTP(w, r)
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

func (i *replicatedIndices) executeCommitPhase() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxCommitPhase.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		requestID := r.URL.Query().Get(replica.RequestKey)
		if requestID == "" {
			http.Error(w, "request_id not provided", http.StatusBadRequest)
			return
		}

		index, shard, cmd := args[1], args[2], args[3]

		var resp interface{}

		switch cmd {
		case "commit":
			resp = i.shards.CommitReplication(index, shard, requestID)
		case "abort":
			resp = i.shards.AbortReplication(index, shard, requestID)
		default:
			http.Error(w, fmt.Sprintf("unrecognized command: %s", cmd), http.StatusNotImplemented)
			return
		}
		if resp == nil { // could not find request with specified id
			http.Error(w, "request not found", http.StatusNotFound)
			return
		}
		b, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to marshal response: %+v, error: %v", resp, err),
				http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})
}

func (i *replicatedIndices) increaseReplicationFactor() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxIncreaseRepFactor.FindStringSubmatch(r.URL.Path)
		fmt.Printf("path: %v, args: %+v", r.URL.Path, args)
		if len(args) != 2 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index := args[1]

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		dist, err := IndicesPayloads.IncreaseReplicationFactor.Unmarshal(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := i.scaler.LocalScaleOut(r.Context(), index, dist); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})
}

func (i *replicatedIndices) postObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		requestID := r.URL.Query().Get(replica.RequestKey)
		if requestID == "" {
			http.Error(w, "request_id not provided", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		ct := r.Header.Get("content-type")

		switch ct {

		case IndicesPayloads.SingleObject.MIME():
			i.postObjectSingle(w, r, index, shard, requestID)
			return
		case IndicesPayloads.ObjectList.MIME():
			i.postObjectBatch(w, r, index, shard, requestID)
			return
		default:
			http.Error(w, "415 Unsupported Media Type", http.StatusUnsupportedMediaType)
			return
		}
	})
}

func (i *replicatedIndices) patchObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		requestID := r.URL.Query().Get(replica.RequestKey)
		if requestID == "" {
			http.Error(w, "request_id not provided", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		mergeDoc, err := IndicesPayloads.MergeDoc.Unmarshal(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := i.shards.ReplicateUpdate(r.Context(), index, shard, requestID, &mergeDoc)
		if localIndexNotReady(resp) {
			http.Error(w, resp.FirstError().Error(), http.StatusServiceUnavailable)
			return
		}

		b, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to marshal response: %+v, error: %v", resp, err),
				http.StatusInternalServerError)
			return
		}

		w.Write(b)
	})
}

func (i *replicatedIndices) deleteObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		requestID := r.URL.Query().Get(replica.RequestKey)
		if requestID == "" {
			http.Error(w, "request_id not provided", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		defer r.Body.Close()

		resp := i.shards.ReplicateDeletion(r.Context(), index, shard, requestID, strfmt.UUID(id))
		if localIndexNotReady(resp) {
			http.Error(w, resp.FirstError().Error(), http.StatusServiceUnavailable)
			return
		}

		b, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to marshal response: %+v, error: %v", resp, err),
				http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})
}

func (i *replicatedIndices) deleteObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		requestID := r.URL.Query().Get(replica.RequestKey)
		if requestID == "" {
			http.Error(w, "request_id not provided", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		docIDs, dryRun, err := IndicesPayloads.BatchDeleteParams.Unmarshal(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := i.shards.ReplicateDeletions(r.Context(), index, shard, requestID, docIDs, dryRun)
		if localIndexNotReady(resp) {
			http.Error(w, resp.FirstError().Error(), http.StatusServiceUnavailable)
			return
		}

		b, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to marshal response: %+v, error: %v", resp, err),
				http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})
}

func (i *replicatedIndices) postObjectSingle(w http.ResponseWriter, r *http.Request,
	index, shard, requestID string,
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

	resp := i.shards.ReplicateObject(r.Context(), index, shard, requestID, obj)
	if localIndexNotReady(resp) {
		http.Error(w, resp.FirstError().Error(), http.StatusServiceUnavailable)
		return
	}

	b, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshal response: %+v, error: %v", resp, err),
			http.StatusInternalServerError)
		return
	}

	w.Write(b)
}

func (i *replicatedIndices) postObjectBatch(w http.ResponseWriter, r *http.Request,
	index, shard, requestID string,
) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	objs, err := IndicesPayloads.ObjectList.Unmarshal(bodyBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := i.shards.ReplicateObjects(r.Context(), index, shard, requestID, objs)
	if localIndexNotReady(resp) {
		http.Error(w, resp.FirstError().Error(), http.StatusServiceUnavailable)
		return
	}

	b, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, fmt.Sprintf("unmarshal resp: %+v, error: %v", resp, err),
			http.StatusInternalServerError)
		return
	}

	w.Write(b)
}

func (i *replicatedIndices) postRefs() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObjects.FindStringSubmatch(r.URL.Path)
		if len(args) != 3 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		requestID := r.URL.Query().Get(replica.RequestKey)
		if requestID == "" {
			http.Error(w, "request_id not provided", http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		refs, err := IndicesPayloads.ReferenceList.Unmarshal(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := i.shards.ReplicateReferences(r.Context(), index, shard, requestID, refs)
		if localIndexNotReady(resp) {
			http.Error(w, resp.FirstError().Error(), http.StatusServiceUnavailable)
			return
		}

		b, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, fmt.Sprintf("unmarshal resp: %+v, error: %v", resp, err),
				http.StatusInternalServerError)
			return
		}

		w.Write(b)
	})
}

func localIndexNotReady(resp replica.SimpleResponse) bool {
	if err := resp.FirstError(); err != nil {
		re, ok := err.(*replica.Error)
		if ok && re.IsStatusCode(replica.StatusNotReady) {
			return true
		}
	}
	return false
}
