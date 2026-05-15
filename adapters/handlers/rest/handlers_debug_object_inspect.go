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

package rest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

// /debug/objects/inspect — for a set of UUIDs, fetches the object from the
// objects bucket and runs behavioural index-consistency checks against the
// derived inverted indexes (filterable AND-of-terms, searchable AND of per-term
// presence — equivalent to BlockMax-AND result membership without scoring,
// rangeable equality). Optionally dumps full bucket entries that reference the
// resolved docID(s).
//
// Default mode (no buckets/all): cheap. One Get per UUID + a handful of direct
// per-term lookups per indexed property. No full bucket scans.
//
// Extended mode (buckets= or all=true): walks every requested bucket cursor
// once and emits matching entries up to ?limit per bucket.
//
// If the resolved shard is owned by another node, the request is forwarded via
// HTTP to that node's debug port (default 6060). The forwarded request carries
// _forwarded=1 to prevent loops.
//
// On POST, the handler will additionally re-put any UUID it reports as
// unhealthy (consistency.ok=false or, in all_replicas mode, consistent/match
// false) and poll the inspection until every targeted UUID becomes healthy or
// the timeout elapses. The response contains the initial (broken) result and
// the final result.
func setupDebugObjectInspectHandler(appState *state.State) {
	logger := appState.Logger.WithField("handler", "debug_objects_inspect")

	http.HandleFunc("/debug/objects/inspect", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		params, err := parseInspectParams(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}

		idx := appState.DB.GetIndex(schema.ClassName(params.Collection))
		if idx == nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "collection not found or not ready"})
			return
		}
		canonicalClass := idx.Config.ClassName.String()

		classInfo := appState.SchemaManager.ClassInfo(canonicalClass)
		mtEnabled := classInfo.MultiTenancy.Enabled
		if mtEnabled && params.Tenant == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "tenant is required for multi-tenant collections",
			})
			return
		}

		// POST forces verbose internally so we can re-put the object using the
		// decoded body. We strip verbose-only fields again before responding
		// unless the caller actually asked for them.
		stripAfter := r.Method == http.MethodPost && !params.Verbose
		inspectParamsForRun := params
		if r.Method == http.MethodPost {
			inspectParamsForRun.Verbose = true
		}

		before := runInspection(r, appState, idx, canonicalClass, mtEnabled, inspectParamsForRun, logger)

		// GET — return inspection as-is.
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusOK, before)
			return
		}

		// POST — try to fix every unhealthy UUID, then poll until healthy or
		// the timeout elapses.
		post := reindexAndPoll(r, appState, idx, canonicalClass, mtEnabled, inspectParamsForRun, before, logger)
		if stripAfter {
			stripVerboseFromResponse(&post.Before)
			stripVerboseFromResponse(&post.After)
		}
		writeJSON(w, http.StatusOK, post)
	})
}

// runInspection executes a full inspection cycle against the given params and
// returns the aggregated response. Used by both GET and POST paths.
func runInspection(r *http.Request, appState *state.State, idx *db.Index,
	canonicalClass string, mtEnabled bool, params inspectParams, logger *logrus.Entry,
) inspectResponse {
	// Group UUIDs by shard name. Schema layer returns the shard for non-MT;
	// for MT the shard equals the tenant.
	bucketsByUUID := make(map[string]string)
	for _, idStr := range params.IDs {
		parsed, err := uuid.Parse(idStr)
		if err != nil {
			bucketsByUUID[idStr] = ""
			continue
		}
		if mtEnabled {
			bucketsByUUID[idStr] = params.Tenant
		} else {
			uuidBytes, _ := parsed.MarshalBinary()
			bucketsByUUID[idStr] = appState.SchemaManager.
				ShardFromUUID(canonicalClass, uuidBytes)
		}
	}

	grouped := make(map[string][]string)
	for id, shardName := range bucketsByUUID {
		grouped[shardName] = append(grouped[shardName], id)
	}

	response := inspectResponse{
		Collection: params.Collection,
		Results:    map[string]*inspectUUIDResult{},
	}

	for shardName, ids := range grouped {
		if shardName == "" {
			for _, id := range ids {
				response.Results[id] = &inspectUUIDResult{
					Found: false,
					Error: "could not resolve shard for uuid",
				}
			}
			continue
		}

		localName := appState.Cluster.LocalName()

		if params.AllReplicas && !params.Forwarded {
			replicas, _ := appState.SchemaManager.ShardReplicas(canonicalClass, shardName)
			if len(replicas) > 0 {
				perReplica := fanOutReplicas(r, appState, idx, replicas, shardName, localName, params, ids, logger)
				for _, id := range ids {
					response.Results[id] = mergeReplicaResults(shardName, id, perReplica)
				}
				continue
			}
		}

		owner, _ := appState.SchemaManager.ShardOwner(canonicalClass, shardName)
		isLocal := owner == "" || owner == localName

		if !isLocal && !params.Forwarded {
			if remoteResults, err := forwardObjectInspect(r, appState, owner, ids, params); err == nil {
				for id, res := range remoteResults {
					if res != nil {
						res.Forwarded = true
						res.Shard = shardName
						res.Node = owner
					}
					response.Results[id] = res
				}
				continue
			} else {
				logger.WithField("node", owner).WithField("shard", shardName).Error(err)
				for _, id := range ids {
					response.Results[id] = &inspectUUIDResult{
						Shard:     shardName,
						Node:      owner,
						Forwarded: false,
						Found:     false,
						Error:     fmt.Sprintf("failed to forward to owning node %q: %v", owner, err),
					}
				}
				continue
			}
		}

		shard, release, err := idx.GetShard(r.Context(), shardName)
		if err != nil || shard == nil {
			msg := "tenant not loaded or shard missing"
			if err != nil {
				msg = err.Error()
			}
			for _, id := range ids {
				response.Results[id] = &inspectUUIDResult{
					Shard: shardName,
					Node:  localName,
					Found: false,
					Error: msg,
				}
			}
			if release != nil {
				release()
			}
			continue
		}

		func() {
			defer release()
			inspectShard(r.Context(), shard, params, ids, shardName, localName, response.Results, logger)
		}()
	}

	return response
}

// inspectParams captures the parsed query string.
type inspectParams struct {
	Collection      string
	Tenant          string
	IDs             []string
	BucketAllowlist []string // explicit per-bucket dump scope
	DumpAll         bool     // shorthand for "every bucket in the shard"
	Limit           int
	Verbose         bool          // include the full storobj.Object and the detailed consistency breakdown
	AllReplicas     bool          // fan out to every replica of the resolved shard and return per-replica results
	Forwarded       bool          // _forwarded=1 — already proxied, do not forward again
	ReindexTimeout  time.Duration // POST: max wall-clock time to wait for healthy after re-put
	ReindexInterval time.Duration // POST: poll interval between consecutive inspections
}

func parseInspectParams(r *http.Request) (inspectParams, error) {
	q := r.URL.Query()
	p := inspectParams{
		Collection:      strings.TrimSpace(q.Get("collection")),
		Tenant:          strings.TrimSpace(q.Get("tenant")),
		Limit:           1000,
		Forwarded:       q.Get("_forwarded") == "1",
		DumpAll:         q.Get("all") == "true",
		Verbose:         q.Get("verbose") == "true",
		AllReplicas:     q.Get("all_replicas") == "true",
		ReindexTimeout:  30 * time.Second,
		ReindexInterval: time.Second,
	}
	if v := strings.TrimSpace(q.Get("reindex_timeout")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return p, fmt.Errorf("invalid reindex_timeout: %q", v)
		}
		p.ReindexTimeout = d
	}
	if v := strings.TrimSpace(q.Get("reindex_interval")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return p, fmt.Errorf("invalid reindex_interval: %q", v)
		}
		p.ReindexInterval = d
	}
	if p.Collection == "" {
		return p, fmt.Errorf("collection is required")
	}
	idsStr := strings.TrimSpace(q.Get("ids"))
	if idsStr == "" {
		return p, fmt.Errorf("ids is required (comma-separated UUIDs)")
	}
	for _, s := range strings.Split(idsStr, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			p.IDs = append(p.IDs, s)
		}
	}
	if buckets := strings.TrimSpace(q.Get("buckets")); buckets != "" {
		for _, b := range strings.Split(buckets, ",") {
			b = strings.TrimSpace(b)
			if b != "" {
				p.BucketAllowlist = append(p.BucketAllowlist, b)
			}
		}
	}
	if l := strings.TrimSpace(q.Get("limit")); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 0 {
			return p, fmt.Errorf("invalid limit: %q", l)
		}
		p.Limit = n
	}
	return p, nil
}

// forwardObjectInspect proxies the request to the node that owns the shard
// using the assumed debug port (6060). Returns a per-uuid map of results.
func forwardObjectInspect(r *http.Request, appState *state.State, ownerNode string,
	ids []string, params inspectParams,
) (map[string]*inspectUUIDResult, error) {
	addr := appState.Cluster.NodeAddress(ownerNode)
	if addr == "" {
		return nil, fmt.Errorf("no address known for node %q", ownerNode)
	}

	q := r.URL.Query()
	q.Set("ids", strings.Join(ids, ","))
	q.Set("_forwarded", "1")
	q.Del("collection")
	q.Set("collection", params.Collection)
	// Reflect parsed `params` onto the forwarded URL so flags we set
	// internally (POST forces Verbose, the outer caller's choice of
	// AllReplicas / DumpAll / Limit / Tenant) reach the remote node even when
	// they weren't on the original request line.
	if params.Verbose {
		q.Set("verbose", "true")
	} else {
		q.Del("verbose")
	}
	// Forwarded requests must never recursively fan out — the outer caller
	// already orchestrates per-replica dispatch.
	q.Del("all_replicas")

	url := fmt.Sprintf("http://%s:6060/debug/objects/inspect?%s", addr, q.Encode())
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote returned %d: %s", resp.StatusCode, string(body))
	}
	var remote inspectResponse
	if err := json.Unmarshal(body, &remote); err != nil {
		return nil, fmt.Errorf("unmarshal remote response: %w", err)
	}
	return remote.Results, nil
}

// fanOutReplicas queries every replica of `shardName` in parallel and returns
// a map keyed by node name. Each node-keyed map holds the per-UUID result for
// that replica. Local replica is served from this process; remote replicas
// are reached via HTTP forwarding with `_forwarded=1`.
func fanOutReplicas(r *http.Request, appState *state.State, idx *db.Index,
	replicas []string, shardName, localName string, params inspectParams,
	ids []string, logger *logrus.Entry,
) map[string]map[string]*inspectUUIDResult {
	out := make(map[string]map[string]*inspectUUIDResult, len(replicas))
	var mu sync.Mutex

	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(-1)

	for _, node := range replicas {
		node := node
		eg.Go(func() error {
			var idMap map[string]*inspectUUIDResult

			if node == localName {
				idMap = make(map[string]*inspectUUIDResult, len(ids))
				shard, release, err := idx.GetShard(r.Context(), shardName)
				if err != nil || shard == nil {
					msg := "shard not loaded"
					if err != nil {
						msg = err.Error()
					}
					for _, id := range ids {
						idMap[id] = &inspectUUIDResult{
							Shard: shardName, Node: node, Found: false, Error: msg,
						}
					}
				} else {
					func() {
						defer release()
						inspectShard(r.Context(), shard, params, ids, shardName, node, idMap, logger)
					}()
				}
			} else {
				remote, err := forwardObjectInspect(r, appState, node, ids, params)
				if err != nil {
					logger.WithField("node", node).WithField("shard", shardName).
						Error(err)
					idMap = make(map[string]*inspectUUIDResult, len(ids))
					for _, id := range ids {
						idMap[id] = &inspectUUIDResult{
							Shard: shardName, Node: node, Forwarded: false, Found: false,
							Error: fmt.Sprintf("failed to forward to replica %q: %v", node, err),
						}
					}
				} else {
					idMap = remote
					for _, res := range idMap {
						if res == nil {
							continue
						}
						res.Forwarded = true
						res.Shard = shardName
						res.Node = node
					}
				}
			}

			mu.Lock()
			out[node] = idMap
			mu.Unlock()
			return nil
		})
	}
	_ = eg.Wait()
	return out
}

// mergeReplicaResults builds the per-UUID aggregate from the per-replica
// outcomes. The top-level summary fields are populated from the first replica
// that found the object; every replica's view appears under .Replicas. Two
// cluster-level flags are computed:
//   - Consistent: every replica's local consistency.ok is true.
//   - Match: every replica agrees on whether the object exists and, if it
//     does, on its LastUpdateTimeUnix. Catches "one replica is on a stale
//     version" divergence.
func mergeReplicaResults(shardName, id string,
	perReplica map[string]map[string]*inspectUUIDResult,
) *inspectUUIDResult {
	agg := &inspectUUIDResult{
		Shard:    shardName,
		Replicas: make(map[string]*inspectUUIDResult, len(perReplica)),
	}
	for node, idMap := range perReplica {
		res, ok := idMap[id]
		if !ok || res == nil {
			continue
		}
		agg.Replicas[node] = res
	}
	// Pick the replica with the highest UpdateTime as the source of truth
	// for the aggregate. Matters for divergence cases (match=false) — POST
	// reads .Object from this aggregate when deciding what body to re-put,
	// so we want the most recent version to win, not whichever replica Go's
	// map iteration happened to visit first.
	var newest *inspectUUIDResult
	var newestNode string
	for node, res := range agg.Replicas {
		if !res.Found {
			continue
		}
		if newest == nil || res.UpdateTime > newest.UpdateTime {
			newest = res
			newestNode = node
		}
	}
	if newest != nil {
		agg.Found = true
		agg.DocID = newest.DocID
		agg.UpdateTime = newest.UpdateTime
		agg.Node = newestNode
		agg.Consistency = newest.Consistency
		agg.Details = newest.Details
		agg.Object = newest.Object
		agg.Buckets = newest.Buckets
	}

	// Compute cluster-wide flags. With zero replicas captured we leave them
	// unset (nil) so the JSON omits them — the question is meaningless.
	if len(agg.Replicas) == 0 {
		return agg
	}

	allConsistent := true
	allMatch := true
	var refFound bool
	var refUpdate int64
	first := true
	for _, res := range agg.Replicas {
		// Consistent: per-replica consistency.ok must be true. A replica that
		// errored out or didn't find the object cannot count as consistent.
		if res.Consistency == nil || !res.Consistency.Ok {
			allConsistent = false
		}
		// Match: every replica must agree on Found and (when Found) UpdateTime.
		if first {
			refFound = res.Found
			refUpdate = res.UpdateTime
			first = false
			continue
		}
		if res.Found != refFound {
			allMatch = false
			continue
		}
		if res.Found && res.UpdateTime != refUpdate {
			allMatch = false
		}
	}
	agg.Consistent = &allConsistent
	agg.Match = &allMatch
	agg.Healthy = agg.Found && allConsistent && allMatch
	return agg
}

// inspectShard handles one shard's worth of UUIDs entirely against local data.
func inspectShard(ctx context.Context, shard db.ShardLike,
	params inspectParams, ids []string, shardName, localName string,
	out map[string]*inspectUUIDResult, logger interface{ Error(args ...interface{}) },
) {
	store := shard.Store()
	objectsBucket := store.Bucket(helpers.ObjectsBucketLSM)
	if objectsBucket == nil {
		for _, id := range ids {
			out[id] = &inspectUUIDResult{
				Shard: shardName,
				Node:  localName,
				Found: false,
				Error: "objects bucket not found",
			}
		}
		return
	}

	// Collect docIDs for the bucket-dump pass.
	docIDsForDump := make(map[uint64]string)

	for _, id := range ids {
		res := &inspectUUIDResult{Shard: shardName, Node: localName}
		out[id] = res

		parsed, err := uuid.Parse(id)
		if err != nil {
			res.Found = false
			res.Error = "invalid uuid"
			continue
		}
		uuidBytes, _ := parsed.MarshalBinary()

		objBytes, err := objectsBucket.Get(uuidBytes)
		if err != nil || len(objBytes) == 0 {
			res.Found = false
			if err != nil {
				res.Error = err.Error()
			}
			continue
		}
		obj, err := storobj.FromBinary(objBytes)
		if err != nil {
			res.Found = true
			res.Error = "failed to decode object: " + err.Error()
			continue
		}
		res.Found = true
		res.DocID = obj.DocID
		res.UpdateTime = obj.LastUpdateTimeUnix()
		docIDsForDump[obj.DocID] = id

		// Behavioural consistency checks. The full report is always computed;
		// the default response only carries the summary (per-property ok/not-ok)
		// — pass ?verbose=true for the detailed breakdown and the full object.
		full := checkObjectConsistency(ctx, shard, objectsBucket, obj, uuidBytes, logger)
		res.Consistency = summarize(full)
		res.Healthy = res.Found && res.Consistency != nil && res.Consistency.Ok
		if params.Verbose {
			res.Object = obj
			res.Details = full
		}
	}

	// Optional extended mode: dump matching entries from the requested buckets.
	wantDump := params.DumpAll || len(params.BucketAllowlist) > 0
	if !wantDump || len(docIDsForDump) == 0 {
		return
	}

	bucketsToScan := params.BucketAllowlist
	if params.DumpAll {
		bucketsToScan = bucketsToScan[:0]
		for name := range store.GetBucketsByName() {
			if name == helpers.ObjectsBucketLSM {
				continue
			}
			bucketsToScan = append(bucketsToScan, name)
		}
	}

	dumps := make(map[string]*bucketDump)
	for _, name := range bucketsToScan {
		b := store.Bucket(name)
		if b == nil {
			dumps[name] = &bucketDump{Error: "bucket not found"}
			continue
		}
		dumps[name] = dumpBucket(ctx, b, docIDsForDump, params.Limit)
	}

	for docID, id := range docIDsForDump {
		res := out[id]
		if res == nil {
			continue
		}
		res.Buckets = make(map[string]*bucketDump, len(dumps))
		for name, full := range dumps {
			res.Buckets[name] = filterDumpForDocID(full, docID)
		}
	}
}

// checkObjectConsistency runs the behavioural checks for one decoded object.
func checkObjectConsistency(ctx context.Context, shard db.ShardLike,
	objectsBucket *lsmkv.Bucket, obj *storobj.Object, uuidBytes []byte,
	logger interface{ Error(args ...interface{}) },
) *consistencyReport {
	rep := &consistencyReport{Ok: true}
	docID := obj.DocID
	store := shard.Store()

	// (a) Objects-bucket duplicate check via property___id and the docID
	// secondary index.
	rep.Duplicates = checkDuplicates(ctx, store, objectsBucket, uuidBytes, docID)
	if rep.Duplicates != nil && (len(rep.Duplicates.Extras) > 0 || rep.Duplicates.LiveCollision) {
		rep.Ok = false
	}

	// (b) Property analysis — exactly what the writer would have indexed.
	props, _, err := shard.AnalyzeObject(obj)
	if err != nil {
		logger.Error(fmt.Errorf("AnalyzeObject for docID %d: %w", docID, err))
		rep.Error = err.Error()
		rep.Ok = false
		return rep
	}

	for _, prop := range props {
		if prop.HasFilterableIndex {
			c := checkFilterable(prop, store, docID)
			if c != nil {
				rep.Filterable = append(rep.Filterable, *c)
				if !c.AndAll.Ok {
					rep.Ok = false
				}
			}
		}
		if prop.HasSearchableIndex {
			c := checkSearchable(ctx, prop, store, docID)
			if c != nil {
				rep.Searchable = append(rep.Searchable, *c)
				if !c.AndAll.Ok {
					rep.Ok = false
				}
			}
		}
		if prop.HasRangeableIndex {
			c := checkRangeable(ctx, prop, store, docID)
			if c != nil {
				rep.Rangeable = append(rep.Rangeable, *c)
				for _, v := range c.PerValue {
					if !v.Ok {
						rep.Ok = false
					}
				}
			}
		}
	}

	// (c) Vector behavioural check — issue a near-object query (top-1 on the
	// object's own vector) per target. The top hit should be the object itself
	// with a near-zero distance. If not, the vector index is missing or stale
	// for this docID.
	rep.Vectors = checkVectors(ctx, shard, obj)
	for _, v := range rep.Vectors {
		if !v.Ok {
			rep.Ok = false
		}
	}
	return rep
}

// checkVectors runs a top-1 near-object search on every target vector the
// object carries. The result is the live HNSW/flat/dynamic read path, so it
// detects missing tombstones, un-indexed docIDs, and stale shards regardless
// of how vectors are persisted (commit log vs. LSM bucket).
func checkVectors(ctx context.Context, shard db.ShardLike, obj *storobj.Object) []vectorCheck {
	docID := obj.DocID
	var out []vectorCheck

	doSingle := func(target string, vec []float32) {
		idx, ok := shard.GetVectorIndex(target)
		if !ok {
			out = append(out, vectorCheck{
				Target: target, Multivector: false,
				Error: "vector index not found",
			})
			return
		}
		check := vectorCheck{Target: target, Dimensions: len(vec)}
		check.ContainsDoc = idx.ContainsDoc(docID)
		if len(vec) == 0 {
			check.Error = "object has no vector for this target"
			out = append(out, check)
			return
		}
		ids, dists, err := idx.SearchByVector(ctx, vec, 10, nil)
		if err != nil {
			check.Error = err.Error()
			out = append(out, check)
			return
		}
		for i, id := range ids {
			if id == docID {
				check.TopDocID = id
				check.TopDistance = dists[i]
				check.Ok = check.ContainsDoc && math.Abs(float64(dists[i])) < 1e-6
				break
			}
		}
		out = append(out, check)
	}

	doMulti := func(target string, multivec [][]float32) {
		idx, ok := shard.GetVectorIndex(target)
		if !ok {
			out = append(out, vectorCheck{
				Target: target, Multivector: true,
				Error: "vector index not found",
			})
			return
		}
		mIdx, ok := idx.(db.VectorIndexMulti)
		if !ok {
			out = append(out, vectorCheck{
				Target: target, Multivector: true,
				Error: "vector index does not support multi-vector queries",
			})
			return
		}
		check := vectorCheck{Target: target, Multivector: true, Dimensions: len(multivec)}
		check.ContainsDoc = idx.ContainsDoc(docID)
		if len(multivec) == 0 {
			check.Error = "object has no multi-vector for this target"
			out = append(out, check)
			return
		}
		ids, dists, err := mIdx.SearchByMultiVector(ctx, multivec, 1, nil)
		if err != nil {
			check.Error = err.Error()
			out = append(out, check)
			return
		}
		if len(ids) > 0 {
			check.TopDocID = ids[0]
			check.TopDistance = dists[0]
		}
		check.Ok = check.ContainsDoc && len(ids) > 0 && ids[0] == docID
		out = append(out, check)
	}

	if len(obj.Vector) > 0 {
		doSingle("", obj.Vector)
	}
	for target, vec := range obj.Vectors {
		doSingle(target, vec)
	}
	for target, vec := range obj.MultiVectors {
		doMulti(target, vec)
	}
	return out
}

func checkDuplicates(ctx context.Context, store *lsmkv.Store, objectsBucket *lsmkv.Bucket,
	uuidBytes []byte, liveDocID uint64,
) *duplicatesReport {
	idBucket := store.Bucket(helpers.BucketFromPropNameLSM(filters.InternalPropID))
	if idBucket == nil {
		return nil
	}
	rep := &duplicatesReport{}

	// property___id may be SetCollection (legacy) or RoaringSet — try both.
	if idBucket.Strategy() == lsmkv.StrategySetCollection {
		values, err := idBucket.SetList(uuidBytes)
		if err == nil {
			for _, v := range values {
				if len(v) == 8 {
					// SetCollection stores docIDs as LittleEndian uint64.
					rep.IDBucketDocIDs = append(rep.IDBucketDocIDs, binary.LittleEndian.Uint64(v))
				}
			}
		}
	} else if idBucket.Strategy() == lsmkv.StrategyRoaringSet {
		bm, release, err := idBucket.RoaringSetGet(uuidBytes)
		if err == nil && bm != nil {
			rep.IDBucketDocIDs = append(rep.IDBucketDocIDs, bm.ToArray()...)
		}
		if release != nil {
			release()
		}
	}

	// Verify the live docID's slot in the secondary index matches our UUID.
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, liveDocID)
	if obj := lookupBySecondaryDocID(ctx, objectsBucket, docIDBytes); obj != nil {
		if !bytes.Equal(obj.IDBytes, uuidBytes) {
			rep.LiveCollision = true
			rep.LiveCollisionUUID = obj.IDStr
		}
	}

	// Any docID besides the live one is a stale duplicate. Resolve via the
	// secondary index to surface what's still on disk.
	for _, did := range rep.IDBucketDocIDs {
		if did == liveDocID {
			continue
		}
		didBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(didBytes, did)
		extra := duplicateEntry{DocID: did, Found: false}
		if obj := lookupBySecondaryDocID(ctx, objectsBucket, didBytes); obj != nil {
			extra.Found = true
			extra.UUID = obj.IDStr
			extra.CreationTime = obj.CreationTime
			extra.UpdateTime = obj.UpdateTime
		}
		rep.Extras = append(rep.Extras, extra)
	}
	return rep
}

type secondaryHit struct {
	IDBytes      []byte
	IDStr        string
	CreationTime int64
	UpdateTime   int64
}

func lookupBySecondaryDocID(ctx context.Context, b *lsmkv.Bucket, docIDBytes []byte) *secondaryHit {
	objBytes, err := b.GetBySecondary(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)
	if err != nil || len(objBytes) == 0 {
		return nil
	}
	obj, err := storobj.FromBinary(objBytes)
	if err != nil {
		return nil
	}
	idStr := obj.ID().String()
	parsed, err := uuid.Parse(idStr)
	if err != nil {
		return nil
	}
	idBytes, _ := parsed.MarshalBinary()
	return &secondaryHit{
		IDBytes:      idBytes,
		IDStr:        idStr,
		CreationTime: obj.CreationTimeUnix(),
		UpdateTime:   obj.LastUpdateTimeUnix(),
	}
}

func checkFilterable(prop inverted.Property, store *lsmkv.Store, docID uint64) *filterableCheck {
	bucketName := helpers.BucketFromPropNameLSM(prop.Name)
	b := store.Bucket(bucketName)
	if b == nil {
		return &filterableCheck{Property: prop.Name, Bucket: bucketName, Error: "bucket not found"}
	}

	c := filterableCheck{Property: prop.Name, Bucket: bucketName}
	bitmaps := make([]*sroar.Bitmap, 0, len(prop.Items))
	releases := make([]func(), 0, len(prop.Items))
	defer func() {
		for _, rel := range releases {
			if rel != nil {
				rel()
			}
		}
	}()

	for _, item := range prop.Items {
		t := termCheck{KeyHex: hex.EncodeToString(item.Data)}
		switch b.Strategy() {
		case lsmkv.StrategyRoaringSet:
			bm, release, err := b.RoaringSetGet(item.Data)
			if err != nil {
				t.Error = err.Error()
			} else if bm != nil {
				t.Ok = bm.Contains(docID)
				bitmaps = append(bitmaps, bm)
				releases = append(releases, release)
			}
		case lsmkv.StrategySetCollection:
			values, err := b.SetList(item.Data)
			if err != nil {
				t.Error = err.Error()
			} else {
				t.Ok = setContainsDocID(values, docID)
			}
		default:
			t.Error = "unsupported filterable strategy: " + b.Strategy()
		}
		c.PerTerm = append(c.PerTerm, t)
	}

	// AND-of-all-terms: equivalent to "WHERE prop = v1 AND prop = v2 ..." —
	// the docID must survive the bitmap intersection.
	if len(bitmaps) > 0 {
		anded := bitmaps[0].Clone()
		for i := 1; i < len(bitmaps); i++ {
			anded.And(bitmaps[i])
		}
		c.AndAll.Ok = anded.Contains(docID)
	} else {
		// Either no bitmaps (SetCollection path) or empty: derive from per-term.
		c.AndAll.Ok = true
		for _, t := range c.PerTerm {
			if !t.Ok {
				c.AndAll.Ok = false
				break
			}
		}
	}
	return &c
}

func setContainsDocID(values [][]byte, docID uint64) bool {
	for _, v := range values {
		if len(v) != 8 {
			continue
		}
		if binary.LittleEndian.Uint64(v) == docID {
			return true
		}
	}
	return false
}

func checkSearchable(ctx context.Context, prop inverted.Property, store *lsmkv.Store, docID uint64) *searchableCheck {
	bucketName := helpers.BucketSearchableFromPropNameLSM(prop.Name)
	b := store.Bucket(bucketName)
	if b == nil {
		return &searchableCheck{Property: prop.Name, Bucket: bucketName, Error: "bucket not found"}
	}

	c := searchableCheck{Property: prop.Name, Bucket: bucketName, Strategy: b.Strategy()}
	allOk := true
	for _, item := range prop.Items {
		c.Terms = append(c.Terms, string(item.Data))
		t := searchableTermCheck{Term: string(item.Data), KeyHex: hex.EncodeToString(item.Data)}

		pairs, err := b.MapList(ctx, item.Data)
		if err != nil {
			t.Error = err.Error()
			allOk = false
			c.PerTerm = append(c.PerTerm, t)
			continue
		}
		// First 8 bytes of pair.Key is docID; encoding is BE for shard
		// version >= 2, LE for older shards. Try both.
		docIDBE := make([]byte, 8)
		binary.BigEndian.PutUint64(docIDBE, docID)
		docIDLE := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDLE, docID)

		for _, p := range pairs {
			if len(p.Key) < 8 {
				continue
			}
			if !bytes.Equal(p.Key[:8], docIDBE) && !bytes.Equal(p.Key[:8], docIDLE) {
				continue
			}
			t.Ok = true
			if len(p.Value) == 8 {
				t.TermFrequency = math.Float32frombits(binary.LittleEndian.Uint32(p.Value[0:4]))
				t.PropLength = math.Float32frombits(binary.LittleEndian.Uint32(p.Value[4:8]))
			}
			break
		}
		if !t.Ok {
			allOk = false
		}
		c.PerTerm = append(c.PerTerm, t)
	}

	// Logical AND of per-term presence — correctness-equivalent to BlockMax-AND
	// result membership without scoring. We avoid invoking the full BlockMax
	// pipeline (CreateDiskTerm + DoBlockMaxAnd) here since it requires
	// corpus stats and segment plumbing that don't add diagnostic value over
	// per-term presence for a debug endpoint.
	c.AndAll.Ok = allOk
	return &c
}

func checkRangeable(ctx context.Context, prop inverted.Property, store *lsmkv.Store, docID uint64) *rangeableCheck {
	bucketName := helpers.BucketRangeableFromPropNameLSM(prop.Name)
	b := store.Bucket(bucketName)
	if b == nil {
		return &rangeableCheck{Property: prop.Name, Bucket: bucketName, Error: "bucket not found"}
	}
	c := rangeableCheck{Property: prop.Name, Bucket: bucketName}
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()

	for _, item := range prop.Items {
		if len(item.Data) != 8 {
			c.PerValue = append(c.PerValue, rangeableValueCheck{
				KeyHex: hex.EncodeToString(item.Data),
				Error:  fmt.Sprintf("expected 8-byte key, got %d", len(item.Data)),
			})
			continue
		}
		value := binary.BigEndian.Uint64(item.Data)
		bm, release, err := reader.Read(ctx, value, filters.OperatorEqual)
		v := rangeableValueCheck{Value: value, KeyHex: hex.EncodeToString(item.Data)}
		if err != nil {
			v.Error = err.Error()
		} else if bm != nil {
			v.Ok = bm.Contains(docID)
		}
		if release != nil {
			release()
		}
		c.PerValue = append(c.PerValue, v)
	}
	return &c
}

// dumpBucket walks every key in the bucket and emits entries that reference
// any of the target docIDs. Truncates after `limit` matching entries.
func dumpBucket(ctx context.Context, b *lsmkv.Bucket, targets map[uint64]string, limit int) *bucketDump {
	out := &bucketDump{Strategy: b.Strategy()}
	switch b.Strategy() {
	case lsmkv.StrategyReplace:
		// Skip — only objects bucket uses Replace, and that's already handled.
		return out

	case lsmkv.StrategyRoaringSet:
		c := b.CursorRoaringSet()
		defer c.Close()
		for k, bm := c.First(); k != nil; k, bm = c.Next() {
			matches := matchingDocIDsInBitmap(bm, targets)
			if len(matches) == 0 {
				continue
			}
			out.Entries = append(out.Entries, bucketEntry{
				Key:     printableKey(k),
				KeyHex:  hex.EncodeToString(k),
				Matches: matches,
			})
			if limit > 0 && len(out.Entries) >= limit {
				out.Truncated = true
				break
			}
		}

	case lsmkv.StrategyRoaringSetRange:
		// Rangeable buckets share the roaring-set on-disk layout (key = 8-byte
		// BE uint64 value, value = roaring set of docIDs). The
		// CursorRoaringSet path works against that representation directly.
		c := b.CursorRoaringSet()
		defer c.Close()
		for k, bm := c.First(); k != nil; k, bm = c.Next() {
			matches := matchingDocIDsInBitmap(bm, targets)
			if len(matches) == 0 {
				continue
			}
			entry := bucketEntry{
				Key:     printableKey(k),
				KeyHex:  hex.EncodeToString(k),
				Matches: matches,
			}
			if len(k) == 8 {
				v := binary.BigEndian.Uint64(k)
				entry.NumericKey = &v
			}
			out.Entries = append(out.Entries, entry)
			if limit > 0 && len(out.Entries) >= limit {
				out.Truncated = true
				break
			}
		}

	case lsmkv.StrategySetCollection:
		c := b.SetCursor()
		defer c.Close()
		for k, vals := c.First(); k != nil; k, vals = c.Next() {
			var matches []dumpMatch
			for _, v := range vals {
				if len(v) != 8 {
					continue
				}
				if id, ok := decodeDocIDAny(v, targets); ok {
					matches = append(matches, dumpMatch{DocID: id, UUID: targets[id]})
				}
			}
			if len(matches) == 0 {
				continue
			}
			out.Entries = append(out.Entries, bucketEntry{
				Key:     printableKey(k),
				KeyHex:  hex.EncodeToString(k),
				Matches: matches,
			})
			if limit > 0 && len(out.Entries) >= limit {
				out.Truncated = true
				break
			}
		}

	case lsmkv.StrategyMapCollection, lsmkv.StrategyInverted:
		c, err := b.MapCursor()
		if err != nil {
			out.Error = err.Error()
			return out
		}
		defer c.Close()
		for k, pairs := c.First(ctx); k != nil; k, pairs = c.Next(ctx) {
			var matches []dumpMatch
			for _, p := range pairs {
				if len(p.Key) < 8 {
					continue
				}
				id, ok := decodeDocIDAny(p.Key[:8], targets)
				if !ok {
					continue
				}
				m := dumpMatch{DocID: id, UUID: targets[id]}
				if len(p.Value) == 8 {
					m.TermFrequency = math.Float32frombits(binary.LittleEndian.Uint32(p.Value[0:4]))
					m.PropLength = math.Float32frombits(binary.LittleEndian.Uint32(p.Value[4:8]))
				}
				matches = append(matches, m)
			}
			if len(matches) == 0 {
				continue
			}
			out.Entries = append(out.Entries, bucketEntry{
				Key:     printableKey(k),
				KeyHex:  hex.EncodeToString(k),
				Matches: matches,
			})
			if limit > 0 && len(out.Entries) >= limit {
				out.Truncated = true
				break
			}
		}

	default:
		out.Error = "unsupported strategy: " + b.Strategy()
	}
	return out
}

// filterDumpForDocID returns a copy of the dump containing only entries that
// match the given docID. Lets us share one bucket scan across all UUIDs.
func filterDumpForDocID(full *bucketDump, docID uint64) *bucketDump {
	if full == nil {
		return nil
	}
	out := &bucketDump{Strategy: full.Strategy, Truncated: full.Truncated, Error: full.Error}
	for _, e := range full.Entries {
		var kept []dumpMatch
		for _, m := range e.Matches {
			if m.DocID == docID {
				kept = append(kept, m)
			}
		}
		if len(kept) == 0 {
			continue
		}
		copyEntry := e
		copyEntry.Matches = kept
		out.Entries = append(out.Entries, copyEntry)
	}
	return out
}

func matchingDocIDsInBitmap(bm *sroar.Bitmap, targets map[uint64]string) []dumpMatch {
	if bm == nil {
		return nil
	}
	var out []dumpMatch
	for id, uuidStr := range targets {
		if bm.Contains(id) {
			out = append(out, dumpMatch{DocID: id, UUID: uuidStr})
		}
	}
	return out
}

func decodeDocIDAny(b []byte, targets map[uint64]string) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	be := binary.BigEndian.Uint64(b)
	if _, ok := targets[be]; ok {
		return be, true
	}
	le := binary.LittleEndian.Uint64(b)
	if _, ok := targets[le]; ok {
		return le, true
	}
	return 0, false
}

func printableKey(k []byte) string {
	for _, c := range k {
		if c < 0x20 || c > 0x7e {
			return ""
		}
	}
	return string(k)
}

// Response types.

type inspectResponse struct {
	Collection string                        `json:"collection"`
	Results    map[string]*inspectUUIDResult `json:"results"`
}

type postInspectResponse struct {
	Collection string                     `json:"collection"`
	Before     inspectResponse            `json:"before"`             // initial broken state (before reindex)
	After      inspectResponse            `json:"after"`              // state after reindex + polling
	Reindex    map[string]*reindexOutcome `json:"reindex"`            // per-uuid summary of the reindex attempt
	TimedOut   bool                       `json:"timedOut,omitempty"` // true if the poll loop hit the timeout
}

type reindexOutcome struct {
	WasHealthy   bool   `json:"wasHealthy"`         // result of the initial inspection
	Attempted    bool   `json:"attempted"`          // true if we actually called DB.PutObject
	HealthyAtEnd bool   `json:"healthyAtEnd"`       // result after the final inspection
	Iterations   int    `json:"iterations"`         // number of inspections performed in the polling loop
	ElapsedMs    int64  `json:"elapsedMs"`          // wall-clock time spent in the poll loop
	Error        string `json:"error,omitempty"`    // reason the re-put or fetch failed, if any
	NewDocID     uint64 `json:"newDocID,omitempty"` // docID observed after reindex
}

// reindexAndPoll handles the POST flow: re-put every UUID that the initial
// inspection reported as unhealthy, then poll until they all report healthy or
// the timeout elapses. The decoded object body lives on `before.Results[id].Object`
// because the caller forced Verbose=true for the initial run.
func reindexAndPoll(r *http.Request, appState *state.State, idx *db.Index,
	canonicalClass string, mtEnabled bool, params inspectParams,
	before inspectResponse, logger *logrus.Entry,
) postInspectResponse {
	out := postInspectResponse{
		Collection: params.Collection,
		Before:     before,
		Reindex:    map[string]*reindexOutcome{},
	}

	// Pass 1: identify and re-put.
	var targets []string
	for _, id := range params.IDs {
		res := before.Results[id]
		oc := &reindexOutcome{}
		if res == nil {
			oc.Error = "no result"
			out.Reindex[id] = oc
			continue
		}
		oc.WasHealthy = res.Healthy
		out.Reindex[id] = oc

		if res.Healthy {
			oc.HealthyAtEnd = true
			continue
		}

		obj := res.Object
		// In all_replicas mode the body is only attached to the top-level
		// result; if it's missing, walk the replicas for the first body.
		if obj == nil && res.Replicas != nil {
			for _, rr := range res.Replicas {
				if rr != nil && rr.Object != nil {
					obj = rr.Object
					break
				}
			}
		}
		if obj == nil {
			oc.Error = "no decoded object body available — cannot re-put"
			continue
		}

		err := appState.DB.PutObject(
			r.Context(), &obj.Object,
			obj.Vector, obj.Vectors, obj.MultiVectors,
			nil, 0,
		)
		if err != nil {
			oc.Error = err.Error()
			logger.WithField("uuid", id).Error(err)
			continue
		}
		oc.Attempted = true
		targets = append(targets, id)
	}

	if len(targets) == 0 {
		out.After = before
		return out
	}

	// Pass 2: poll until every target is healthy or we time out.
	deadline := time.Now().Add(params.ReindexTimeout)
	var after inspectResponse
	iterations := 0
	loopStart := time.Now()
	for {
		iterations++
		after = runInspection(r, appState, idx, canonicalClass, mtEnabled, params, logger)

		allHealthy := true
		for _, id := range targets {
			res := after.Results[id]
			if res == nil || !res.Healthy {
				allHealthy = false
				break
			}
		}
		if allHealthy {
			break
		}
		if time.Now().After(deadline) {
			out.TimedOut = true
			break
		}
		// Respect ctx cancellation; otherwise sleep and re-poll.
		select {
		case <-r.Context().Done():
			out.TimedOut = true
		case <-time.After(params.ReindexInterval):
		}
		if r.Context().Err() != nil {
			break
		}
	}
	elapsed := time.Since(loopStart).Milliseconds()
	out.After = after

	for _, id := range targets {
		oc := out.Reindex[id]
		if oc == nil {
			continue
		}
		oc.Iterations = iterations
		oc.ElapsedMs = elapsed
		if res := after.Results[id]; res != nil {
			oc.HealthyAtEnd = res.Healthy
			oc.NewDocID = res.DocID
		}
	}
	return out
}

// stripVerboseFromResponse trims fields that are only meant for verbose mode
// (the decoded object body, the detailed consistency report) from every
// per-uuid and per-replica result.
func stripVerboseFromResponse(resp *inspectResponse) {
	for _, res := range resp.Results {
		stripVerboseFromResult(res)
	}
}

func stripVerboseFromResult(res *inspectUUIDResult) {
	if res == nil {
		return
	}
	res.Object = nil
	res.Details = nil
	for _, r := range res.Replicas {
		stripVerboseFromResult(r)
	}
}

type inspectUUIDResult struct {
	Shard      string `json:"shard,omitempty"`
	Node       string `json:"node,omitempty"`
	Forwarded  bool   `json:"forwarded,omitempty"`
	Found      bool   `json:"found"`
	Error      string `json:"error,omitempty"`
	DocID      uint64 `json:"docID,omitempty"`
	UpdateTime int64  `json:"updateTime,omitempty"` // LastUpdateTimeUnix; used to detect divergence across replicas
	// only set in verbose mode
	Object *storobj.Object `json:"object,omitempty"`
	// always set when found
	Consistency *consistencySummary `json:"consistency,omitempty"`
	// only set in verbose mode
	Details *consistencyReport     `json:"details,omitempty"`
	Buckets map[string]*bucketDump `json:"buckets,omitempty"`
	// Replicas is set when ?all_replicas=true. Each entry holds the per-replica
	// view of the same UUID; the top-level Found/DocID/Consistency/etc. fields
	// reflect the first replica that successfully found the object (or stay
	// zero-valued if none did).
	Replicas map[string]*inspectUUIDResult `json:"replicas,omitempty"`
	// Consistent is true iff every replica's local consistency.ok is true.
	// Only set when ?all_replicas=true.
	Consistent *bool `json:"consistent,omitempty"`
	// Match is true iff every replica agrees on the object's existence and,
	// when found, its LastUpdateTimeUnix. Detects "one node is on an older
	// version of the object" divergence. Only set when ?all_replicas=true.
	Match *bool `json:"match,omitempty"`
	// Healthy is the one-glance summary: the object was found, every replica
	// (in all_replicas mode) is internally consistent, and every replica
	// agrees on the object's version. False under any failure mode.
	Healthy bool `json:"healthy"`
}

// consistencySummary is the terse default view: just ok/not-ok per property,
// plus a duplicate count. Full per-term errors live in `details` when
// ?verbose=true is set.
type consistencySummary struct {
	Ok         bool            `json:"ok"`
	Error      string          `json:"error,omitempty"`
	Duplicates int             `json:"duplicates"`
	Filterable map[string]bool `json:"filterable,omitempty"`
	Searchable map[string]bool `json:"searchable,omitempty"`
	Rangeable  map[string]bool `json:"rangeable,omitempty"`
	Vectors    map[string]bool `json:"vectors,omitempty"`
}

func summarize(full *consistencyReport) *consistencySummary {
	if full == nil {
		return nil
	}
	s := &consistencySummary{Ok: full.Ok, Error: full.Error}
	if full.Duplicates != nil {
		s.Duplicates = len(full.Duplicates.Extras)
		if full.Duplicates.LiveCollision {
			s.Duplicates++ // surface the collision in the count too
		}
	}
	if len(full.Filterable) > 0 {
		s.Filterable = make(map[string]bool, len(full.Filterable))
		for _, c := range full.Filterable {
			s.Filterable[c.Property] = c.Error == "" && c.AndAll.Ok
		}
	}
	if len(full.Searchable) > 0 {
		s.Searchable = make(map[string]bool, len(full.Searchable))
		for _, c := range full.Searchable {
			s.Searchable[c.Property] = c.Error == "" && c.AndAll.Ok
		}
	}
	if len(full.Rangeable) > 0 {
		s.Rangeable = make(map[string]bool, len(full.Rangeable))
		for _, c := range full.Rangeable {
			ok := c.Error == ""
			for _, v := range c.PerValue {
				if !v.Ok || v.Error != "" {
					ok = false
					break
				}
			}
			s.Rangeable[c.Property] = ok
		}
	}
	if len(full.Vectors) > 0 {
		s.Vectors = make(map[string]bool, len(full.Vectors))
		for _, v := range full.Vectors {
			key := v.Target
			if key == "" {
				key = "default"
			}
			s.Vectors[key] = v.Ok && v.Error == ""
		}
	}
	return s
}

type consistencyReport struct {
	Ok         bool              `json:"ok"`
	Error      string            `json:"error,omitempty"`
	Duplicates *duplicatesReport `json:"duplicates,omitempty"`
	Filterable []filterableCheck `json:"filterable,omitempty"`
	Searchable []searchableCheck `json:"searchable,omitempty"`
	Rangeable  []rangeableCheck  `json:"rangeable,omitempty"`
	Vectors    []vectorCheck     `json:"vectors,omitempty"`
}

type vectorCheck struct {
	Target      string  `json:"target"`
	Multivector bool    `json:"multivector,omitempty"`
	Dimensions  int     `json:"dimensions,omitempty"`
	ContainsDoc bool    `json:"containsDoc"`
	TopDocID    uint64  `json:"topDocID,omitempty"`
	TopDistance float32 `json:"topDistance,omitempty"`
	Ok          bool    `json:"ok"`
	Error       string  `json:"error,omitempty"`
}

type duplicatesReport struct {
	IDBucketDocIDs    []uint64         `json:"idBucketDocIDs"`
	LiveCollision     bool             `json:"liveCollision,omitempty"`
	LiveCollisionUUID string           `json:"liveCollisionUUID,omitempty"`
	Extras            []duplicateEntry `json:"extras"`
}

type duplicateEntry struct {
	DocID        uint64 `json:"docID"`
	UUID         string `json:"uuid,omitempty"`
	Found        bool   `json:"found"`
	CreationTime int64  `json:"creationTime,omitempty"`
	UpdateTime   int64  `json:"updateTime,omitempty"`
}

type filterableCheck struct {
	Property string       `json:"property"`
	Bucket   string       `json:"bucket"`
	Error    string       `json:"error,omitempty"`
	PerTerm  []termCheck  `json:"perTerm,omitempty"`
	AndAll   andAllReport `json:"andAll"`
}

type termCheck struct {
	KeyHex string `json:"keyHex"`
	Ok     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
}

type andAllReport struct {
	Ok bool `json:"ok"`
}

type searchableCheck struct {
	Property string                `json:"property"`
	Bucket   string                `json:"bucket"`
	Strategy string                `json:"strategy,omitempty"`
	Error    string                `json:"error,omitempty"`
	Terms    []string              `json:"terms,omitempty"`
	PerTerm  []searchableTermCheck `json:"perTerm,omitempty"`
	AndAll   andAllReport          `json:"andAll"`
}

type searchableTermCheck struct {
	Term          string  `json:"term"`
	KeyHex        string  `json:"keyHex"`
	Ok            bool    `json:"ok"`
	Error         string  `json:"error,omitempty"`
	TermFrequency float32 `json:"tf,omitempty"`
	PropLength    float32 `json:"propLen,omitempty"`
}

type rangeableCheck struct {
	Property string                `json:"property"`
	Bucket   string                `json:"bucket"`
	Error    string                `json:"error,omitempty"`
	PerValue []rangeableValueCheck `json:"perValue,omitempty"`
}

type rangeableValueCheck struct {
	Value  uint64 `json:"value,omitempty"`
	KeyHex string `json:"keyHex"`
	Ok     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
}

type bucketDump struct {
	Strategy  string        `json:"strategy"`
	Entries   []bucketEntry `json:"entries,omitempty"`
	Truncated bool          `json:"truncated,omitempty"`
	Error     string        `json:"error,omitempty"`
}

type bucketEntry struct {
	Key        string      `json:"key,omitempty"`
	KeyHex     string      `json:"keyHex"`
	NumericKey *uint64     `json:"numericKey,omitempty"`
	Matches    []dumpMatch `json:"matches"`
}

type dumpMatch struct {
	DocID         uint64  `json:"docID"`
	UUID          string  `json:"uuid,omitempty"`
	TermFrequency float32 `json:"tf,omitempty"`
	PropLength    float32 `json:"propLen,omitempty"`
}
