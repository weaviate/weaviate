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
	"time"

	"github.com/google/uuid"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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
		// DB.GetIndex lowercases the lookup key, but SchemaReader.ShardFromUUID
		// /ShardOwner use exact-case map keys. Use the canonical name stored
		// on the index to avoid spurious "could not resolve shard" errors when
		// the caller passes a different case.
		canonicalClass := idx.Config.ClassName.String()

		classInfo := appState.SchemaManager.ClassInfo(canonicalClass)
		mtEnabled := classInfo.MultiTenancy.Enabled
		if mtEnabled && params.Tenant == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "tenant is required for multi-tenant collections",
			})
			return
		}

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
				bucketsByUUID[idStr] = appState.SchemaManager.
					ShardFromUUID(canonicalClass, parsed[:])
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

			owner, _ := appState.SchemaManager.SchemaReader.ShardOwner(canonicalClass, shardName)
			localName := appState.Cluster.LocalName()
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
					logger.WithField("node", owner).WithField("shard", shardName).
						Error(err)
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

		writeJSON(w, http.StatusOK, response)
	})
}

// inspectParams captures the parsed query string.
type inspectParams struct {
	Collection      string
	Tenant          string
	IDs             []string
	BucketAllowlist []string // explicit per-bucket dump scope
	DumpAll         bool     // shorthand for "every bucket in the shard"
	Limit           int
	Forwarded       bool // _forwarded=1 — already proxied, do not forward again
}

func parseInspectParams(r *http.Request) (inspectParams, error) {
	q := r.URL.Query()
	p := inspectParams{
		Collection: strings.TrimSpace(q.Get("collection")),
		Tenant:     strings.TrimSpace(q.Get("tenant")),
		Limit:      1000,
		Forwarded:  q.Get("_forwarded") == "1",
		DumpAll:    q.Get("all") == "true",
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
		res.Object = obj
		docIDsForDump[obj.DocID] = id

		// Behavioural consistency checks.
		res.Consistency = checkObjectConsistency(ctx, shard, objectsBucket, obj, uuidBytes, logger)
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

type inspectUUIDResult struct {
	Shard       string                 `json:"shard,omitempty"`
	Node        string                 `json:"node,omitempty"`
	Forwarded   bool                   `json:"forwarded,omitempty"`
	Found       bool                   `json:"found"`
	Error       string                 `json:"error,omitempty"`
	DocID       uint64                 `json:"docID,omitempty"`
	Object      *storobj.Object        `json:"object,omitempty"`
	Consistency *consistencyReport     `json:"consistency,omitempty"`
	Buckets     map[string]*bucketDump `json:"buckets,omitempty"`
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
