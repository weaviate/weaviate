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

package clusterapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/scaler"
)

type replicator interface {
	// Write endpoints
	ReplicateObject(ctx context.Context, indexName, shardName,
		requestID string, object *storobj.Object) replica.SimpleResponse
	ReplicateObjects(ctx context.Context, indexName, shardName,
		requestID string, objects []*storobj.Object) replica.SimpleResponse
	ReplicateUpdate(ctx context.Context, indexName, shardName,
		requestID string, mergeDoc *objects.MergeDocument) replica.SimpleResponse
	ReplicateDeletion(ctx context.Context, indexName, shardName,
		requestID string, uuid strfmt.UUID) replica.SimpleResponse
	ReplicateDeletions(ctx context.Context, indexName, shardName,
		requestID string, uuids []strfmt.UUID, dryRun bool) replica.SimpleResponse
	ReplicateReferences(ctx context.Context, indexName, shardName,
		requestID string, refs []objects.BatchReference) replica.SimpleResponse
	CommitReplication(indexName,
		shardName, requestID string) interface{}
	AbortReplication(indexName,
		shardName, requestID string) interface{}
	OverwriteObjects(ctx context.Context, index, shard string,
		vobjects []*objects.VObject) ([]replica.RepairResponse, error)
	// Read endpoints
	FetchObject(ctx context.Context, indexName,
		shardName string, id strfmt.UUID) (objects.Replica, error)
	FetchObjects(ctx context.Context, class,
		shardName string, ids []strfmt.UUID) ([]objects.Replica, error)
	DigestObjects(ctx context.Context, class, shardName string,
		ids []strfmt.UUID) (result []replica.RepairResponse, err error)
}

type localScaler interface {
	LocalScaleOut(ctx context.Context, className string,
		dist scaler.ShardDist) error
}

type replicatedIndices struct {
	shards replicator
	scaler localScaler
	auth   auth
}

var (
	regxObject = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects\/(` + ob + `)`)
	regxOverwriteObjects = regexp.MustCompile(`\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects/_overwrite`)
	regxObjectsDigest = regexp.MustCompile(`\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects/_digest`)
	regxObjects = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects`)
	regxReferences = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects/references`)
	regxIncreaseRepFactor = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/replication-factor:increase`)
	regxCommitPhase = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `):(commit|abort)`)
)

func NewReplicatedIndices(shards replicator, scaler localScaler, auth auth) *replicatedIndices {
	return &replicatedIndices{
		shards: shards,
		scaler: scaler,
		auth:   auth,
	}
}

func (i *replicatedIndices) Indices() http.Handler {
	return i.auth.handleFunc(i.indicesHandler())
}

func (i *replicatedIndices) indicesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case regxObjectsDigest.MatchString(path):
			if r.Method == http.MethodGet {
				i.getObjectsDigest().ServeHTTP(w, r)
				return
			}

			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case regxOverwriteObjects.MatchString(path):
			if r.Method == http.MethodPut {
				i.putOverwriteObjects().ServeHTTP(w, r)
				return
			}

			http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
			return
		case regxObject.MatchString(path):
			if r.Method == http.MethodDelete {
				i.deleteObject().ServeHTTP(w, r)
				return
			}

			if r.Method == http.MethodPatch {
				i.patchObject().ServeHTTP(w, r)
				return
			}

			if r.Method == http.MethodGet {
				i.getObject().ServeHTTP(w, r)
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
			if r.Method == http.MethodGet {
				i.getObjectsMulti().ServeHTTP(w, r)
				return
			}

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
	}
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

func (i *replicatedIndices) getObjectsDigest() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObjectsDigest.FindStringSubmatch(r.URL.Path)
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

		var ids []strfmt.UUID
		if err := json.Unmarshal(reqPayload, &ids); err != nil {
			http.Error(w, "unmarshal digest objects params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results, err := i.shards.DigestObjects(r.Context(), index, shard, ids)
		if err != nil {
			http.Error(w, "digest objects: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(resBytes)
	})
}

func (i *replicatedIndices) putOverwriteObjects() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxOverwriteObjects.FindStringSubmatch(r.URL.Path)
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

		vobjs, err := IndicesPayloads.VersionedObjectList.Unmarshal(reqPayload)
		if err != nil {
			http.Error(w, "unmarshal overwrite objects params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results, err := i.shards.OverwriteObjects(r.Context(), index, shard, vobjs)
		if err != nil {
			http.Error(w, "overwrite objects: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(results)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(resBytes)
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

		uuids, dryRun, err := IndicesPayloads.BatchDeleteParams.Unmarshal(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := i.shards.ReplicateDeletions(r.Context(), index, shard, requestID, uuids, dryRun)
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

func (i *replicatedIndices) getObject() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObject.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		defer r.Body.Close()

		var (
			resp objects.Replica
			err  error
		)

		resp, err = i.shards.FetchObject(r.Context(), index, shard, strfmt.UUID(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		b, err := resp.MarshalBinary()
		if err != nil {
			http.Error(w, fmt.Sprintf("unmarshal resp: %+v, error: %v", resp, err),
				http.StatusInternalServerError)
			return
		}

		w.Write(b)
	})
}

func (i *replicatedIndices) getObjectsMulti() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxObjects.FindStringSubmatch(r.URL.Path)
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

		resp, err := i.shards.FetchObjects(r.Context(), index, shard, ids)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		b, err := objects.Replicas(resp).MarshalBinary()
		if err != nil {
			http.Error(w, fmt.Sprintf("unmarshal resp: %+v, error: %v", resp, err),
				http.StatusInternalServerError)
			return
		}

		w.Write(b)
	})
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
