//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clusterapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

type replicatedIndices struct {
	replicator replicaTypes.Replicator
	auth       auth
	// maintenanceModeEnabled is an experimental feature to allow the system to be
	// put into a maintenance mode where all replicatedIndices requests just return a 418
	maintenanceModeEnabled func() bool

	requestQueueConfig cluster.RequestQueueConfig
	// requestQueue buffers requests until they're picked up by a worker, goal is to avoid
	// overwhelming the system with requests during spikes (also allows for backpressure)
	requestQueue chan queuedRequest
	// requestQueueMu guards access to the queue during shutdown
	requestQueueMu sync.RWMutex
	// startWorkersOnce ensures that the workers are started only once
	startWorkersOnce sync.Once
	// set to true when shutting down
	isShutdown atomic.Bool
	// workerWg waits for all workers to finish
	workerWg sync.WaitGroup
	logger   logrus.FieldLogger
	// nodeReady reports whether the node is ready to accept requests
	nodeReady func() bool
}

var (
	errReplicatedIndicesShutdown  = errors.New("replicated indices shutting down")
	errReplicatedIndicesQueueFull = errors.New("replicated indices request queue full")
)

const (
	responseShuttingDown = "503 Service Unavailable - shutting down"
	responseQueueFull    = "too many buffered requests"
)

var (
	regxObject = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects\/(` + ob + `)(\/[0-9]{1,64})?`)
	regxOverwriteObjects = regexp.MustCompile(`\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects/_overwrite`)
	regxObjectsDigest = regexp.MustCompile(`\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects/_digest`)
	regexObjectsDigestsInRange = regexp.MustCompile(`\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects/digestsInRange`)
	regxHashTreeLevel = regexp.MustCompile(`\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects\/hashtree\/(` + l + `)`)
	regxObjects = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects`)
	regxReferences = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `)\/objects/references`)
	regxCommitPhase = regexp.MustCompile(`\/replicas\/indices\/(` + cl + `)` +
		`\/shards\/(` + sh + `):(commit|abort)`)
)

func NewReplicatedIndices(
	indexer replicaTypes.Replicator,
	auth auth,
	maintenanceModeEnabled func() bool,
	requestQueueConfig cluster.RequestQueueConfig,
	logger logrus.FieldLogger,
	nodeReady func() bool,
) *replicatedIndices {
	// validate the requestQueueConfig
	if requestQueueConfig.QueueFullHttpStatus == 0 {
		logger.WithField("default_status", cluster.DefaultRequestQueueFullHttpStatus).Debug("no replicated indices buffer full http status provided, using default")
		requestQueueConfig.QueueFullHttpStatus = cluster.DefaultRequestQueueFullHttpStatus
	}
	if requestQueueConfig.QueueFullHttpStatus != http.StatusTooManyRequests && requestQueueConfig.QueueFullHttpStatus != http.StatusGatewayTimeout {
		logger.WithField("status", requestQueueConfig.QueueFullHttpStatus).Warn("unexpected replicated indices buffer full http status")
	}

	i := &replicatedIndices{
		replicator:             indexer,
		auth:                   auth,
		maintenanceModeEnabled: maintenanceModeEnabled,
		requestQueue:           make(chan queuedRequest, requestQueueConfig.QueueSize),
		requestQueueConfig:     requestQueueConfig,
		logger:                 logger,
		nodeReady:              nodeReady,
	}
	if requestQueueConfig.IsEnabled != nil && requestQueueConfig.IsEnabled.Get() {
		i.startWorkersOnce.Do(i.startWorkers)
	}
	return i
}

func (i *replicatedIndices) queueEnabled() bool {
	return i.requestQueueConfig.IsEnabled != nil && i.requestQueueConfig.IsEnabled.Get()
}

func (i *replicatedIndices) writeResponse(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errReplicatedIndicesShutdown):
		http.Error(w, responseShuttingDown, http.StatusServiceUnavailable)
	case errors.Is(err, errReplicatedIndicesQueueFull):
		http.Error(w, responseQueueFull, i.requestQueueConfig.QueueFullHttpStatus)
	default:
		i.logger.WithError(err).Error("unhandled error in replicated indices handler")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (i *replicatedIndices) enqueueRequest(r *http.Request, w http.ResponseWriter, wg *sync.WaitGroup) error {
	i.requestQueueMu.RLock()
	defer i.requestQueueMu.RUnlock()

	if i.isShutdown.Load() {
		return errReplicatedIndicesShutdown
	}

	select {
	case i.requestQueue <- queuedRequest{r: r, w: w, wg: wg}:
		return nil
	default:
		return errReplicatedIndicesQueueFull
	}
}

type queuedRequest struct {
	r *http.Request
	w http.ResponseWriter
	// when the request is done being handled, the waitgroup is done
	wg *sync.WaitGroup
}

func (i *replicatedIndices) startWorkers() {
	for j := 0; j < max(1, i.requestQueueConfig.NumWorkers); j++ {
		i.workerWg.Add(1)
		enterrors.GoWrapper(func() {
			defer i.workerWg.Done()
			for rq := range i.requestQueue {
				if rq.r.Context().Err() != nil {
					if rq.wg != nil {
						rq.wg.Done() //nolint:SA2000
					}
					rq.w.WriteHeader(http.StatusRequestTimeout)
					continue
				}
				i.handleRequest(rq)
			}
		}, i.logger)
	}
}

func (i *replicatedIndices) Indices() http.Handler {
	return i.auth.handleFunc(i.indicesHandler())
}

func (i *replicatedIndices) indicesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if i.maintenanceModeEnabled() {
			http.Error(w, "418 Maintenance mode", http.StatusTeapot)
			return
		}

		if i.nodeReady != nil && !i.nodeReady() {
			http.Error(w, "503 Node not ready", http.StatusServiceUnavailable)
			return
		}

		if i.isShutdown.Load() {
			i.writeResponse(w, errReplicatedIndicesShutdown)
			return
		}

		if i.queueEnabled() {
			i.startWorkersOnce.Do(i.startWorkers)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			err := i.enqueueRequest(r, w, wg)
			if err != nil {
				wg.Done() //nolint:SA2000
				i.writeResponse(w, err)
				return
			}
			wg.Wait() // worker calls Done() after handling request
			return
		}

		i.handleRequest(queuedRequest{r: r, w: w, wg: nil})
	}
}

func (i *replicatedIndices) handleRequest(qr queuedRequest) {
	if qr.wg != nil {
		defer qr.wg.Done()
	}
	r := qr.r
	w := qr.w
	path := r.URL.Path

	// NOTE if you update any of these handler methods/paths, also update the indices_replicas_test.go
	// TestMaintenanceModeReplicatedIndices test to include the new methods/paths.
	switch {
	case regxObjectsDigest.MatchString(path):
		if r.Method == http.MethodGet {
			i.getObjectsDigest().ServeHTTP(w, r)
			return
		}

		http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
		return
	case regexObjectsDigestsInRange.MatchString(path):
		if r.Method == http.MethodPost {
			i.getObjectsDigestsInRange().ServeHTTP(w, r)
			return
		}

		http.Error(w, "405 Method not Allowed", http.StatusMethodNotAllowed)
		return
	case regxHashTreeLevel.MatchString(path):
		if r.Method == http.MethodPost {
			i.getHashTreeLevel().ServeHTTP(w, r)
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
			resp = i.replicator.CommitReplication(index, shard, requestID)
		case "abort":
			resp = i.replicator.AbortReplication(index, shard, requestID)
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

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		index, shard := args[1], args[2]

		defer r.Body.Close()

		ct := r.Header.Get("content-type")

		switch ct {

		case IndicesPayloads.SingleObject.MIME():
			i.postObjectSingle(w, r, index, shard, requestID, schemaVersion)
			return
		case IndicesPayloads.ObjectList.MIME():
			i.postObjectBatch(w, r, index, shard, requestID, schemaVersion)
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

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := i.replicator.ReplicateUpdate(r.Context(), index, shard, requestID, &mergeDoc, schemaVersion)
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

		results, err := i.replicator.DigestObjects(r.Context(), index, shard, ids)
		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, "digest objects: "+err.Error(),
				http.StatusUnprocessableEntity)
			return
		}

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

func (i *replicatedIndices) getObjectsDigestsInRange() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regexObjectsDigestsInRange.FindStringSubmatch(r.URL.Path)
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

		var rangeReq replica.DigestObjectsInRangeReq
		if err := json.Unmarshal(reqPayload, &rangeReq); err != nil {
			http.Error(w, "unmarshal digest objects in token range params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		digests, err := i.replicator.DigestObjectsInRange(r.Context(), index, shard, rangeReq.InitialUUID, rangeReq.FinalUUID, rangeReq.Limit)
		if err != nil {
			http.Error(w, "digest objects in range: "+err.Error(),
				http.StatusInternalServerError)
			return
		}

		resBytes, err := json.Marshal(replica.DigestObjectsInRangeResp{
			Digests: digests,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(resBytes)
	})
}

func (i *replicatedIndices) getHashTreeLevel() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		args := regxHashTreeLevel.FindStringSubmatch(r.URL.Path)
		if len(args) != 4 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, level := args[1], args[2], args[3]

		l, err := strconv.Atoi(level)
		if err != nil {
			http.Error(w, "unmarshal hashtree level params: "+err.Error(), http.StatusInternalServerError)
			return
		}

		defer r.Body.Close()
		reqPayload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var discriminant hashtree.Bitset
		if err := discriminant.Unmarshal(reqPayload); err != nil {
			http.Error(w, "unmarshal hashtree level params from json: "+err.Error(),
				http.StatusBadRequest)
			return
		}

		results, err := i.replicator.HashTreeLevel(r.Context(), index, shard, l, &discriminant)
		if err != nil {
			http.Error(w, "hashtree level: "+err.Error(),
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

		results, err := i.replicator.OverwriteObjects(r.Context(), index, shard, vobjs)
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
		if len(args) != 5 {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		requestID := r.URL.Query().Get(replica.RequestKey)
		if requestID == "" {
			http.Error(w, "request_id not provided", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		var deletionTime time.Time

		if args[4] != "" {
			deletionTimeUnixMilli, err := strconv.ParseInt(args[4][1:], 10, 64)
			if err != nil {
				http.Error(w, "invalid URI", http.StatusBadRequest)
			}
			deletionTime = time.UnixMilli(deletionTimeUnixMilli)
		}

		defer r.Body.Close()

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := i.replicator.ReplicateDeletion(r.Context(), index, shard, requestID, strfmt.UUID(id), deletionTime, schemaVersion)
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

		uuids, deletionTimeUnix, dryRun, err := IndicesPayloads.BatchDeleteParams.Unmarshal(bodyBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp := i.replicator.ReplicateDeletions(r.Context(), index, shard, requestID, uuids, deletionTimeUnix, dryRun, schemaVersion)
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
	index, shard, requestID string, schemaVersion uint64,
) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	obj, err := IndicesPayloads.SingleObject.Unmarshal(bodyBytes, MethodPut)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := i.replicator.ReplicateObject(r.Context(), index, shard, requestID, obj, schemaVersion)
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
	index, shard, requestID string, schemaVersion uint64,
) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	objs, err := IndicesPayloads.ObjectList.Unmarshal(bodyBytes, MethodPut)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := i.replicator.ReplicateObjects(r.Context(), index, shard, requestID, objs, schemaVersion)
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
		if len(args) != 5 || args[4] != "" {
			http.Error(w, "invalid URI", http.StatusBadRequest)
			return
		}

		index, shard, id := args[1], args[2], args[3]

		defer r.Body.Close()

		resp, err := i.replicator.FetchObject(r.Context(), index, shard, strfmt.UUID(id))
		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, "digest objects: "+err.Error(),
				http.StatusUnprocessableEntity)
			return
		}

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

		resp, err := i.replicator.FetchObjects(r.Context(), index, shard, ids)
		if err != nil && errors.As(err, &enterrors.ErrUnprocessable{}) {
			http.Error(w, "digest objects: "+err.Error(),
				http.StatusUnprocessableEntity)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		b, err := replica.Replicas(resp).MarshalBinary()
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

		schemaVersion, err := extractSchemaVersionFromUrlQuery(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp := i.replicator.ReplicateReferences(r.Context(), index, shard, requestID, refs, schemaVersion)
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
		var replicaErr *replica.Error
		if errors.As(err, &replicaErr) && replicaErr.IsStatusCode(replica.StatusNotReady) {
			return true
		}
	}
	return false
}

// Close gracefully shuts down the replicatedIndices by draining the queue and waiting for workers to finish
func (i *replicatedIndices) Close(ctx context.Context) error {
	i.logger.WithField("action", "close_replicated_indices").Debug("attempting to shut down replicated indices")
	if i.isShutdown.CompareAndSwap(false, true) {
		i.logger.WithField("action", "close_replicated_indices").Debug("shutting down replicated indices")
		// Set a timeout for graceful shutdown
		shutdownTimeoutSeconds := i.requestQueueConfig.QueueShutdownTimeoutSeconds
		if shutdownTimeoutSeconds == 0 {
			shutdownTimeoutSeconds = cluster.DefaultRequestQueueShutdownTimeoutSeconds
		}

		// Create a context with timeout for shutdown
		shutdownCtx, cancel := context.WithTimeout(ctx, time.Duration(shutdownTimeoutSeconds)*time.Second)
		defer cancel()

		// Wait for workers to finish with timeout
		done := make(chan struct{})
		enterrors.GoWrapper(func() {
			i.workerWg.Wait()
			close(done)
		}, i.logger)

		i.requestQueueMu.Lock()
		close(i.requestQueue)
		i.requestQueueMu.Unlock()
		select {
		case <-done:
			// Workers finished gracefully
			i.logger.WithField("action", "close_replicated_indices").Debug("workers finished gracefully")
		case <-shutdownCtx.Done():
			// Timeout reached, workers are still running
			err := fmt.Errorf("shutdown timeout reached, some workers may still be running")
			i.logger.WithField("action", "close_replicated_indices").WithError(err).Warn("shutdown timeout reached, attempting to drain queue")
			for {
				select {
				case rq, ok := <-i.requestQueue:
					if !ok {
						i.logger.WithField("action", "close_replicated_indices").Debug("queue closed")
						return err
					}
					func() {
						defer rq.wg.Done()
						rq.w.WriteHeader(http.StatusRequestTimeout)
					}()
				default:
					i.logger.WithField("action", "close_replicated_indices").Debug("no more requests to drain")
					return err
				}
			}
		}
		i.logger.WithField("action", "close_replicated_indices").Debug("replicated indices shutdown complete")
	}
	return nil
}
