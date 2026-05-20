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

package vector_index_restrictions

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutWithoutCompressionBlock_PreservesStoredCompression pins the
// invariant: a PUT on a class whose body omits the compression block
// must NOT silently change the class's stored compression.
//
// Origin: investigated Dirk's gist finding #3
// (https://gist.github.com/dirkkul/b1edb2cc777d2b8a695a74b7b24c4691):
//
//	"When clients PUT without a compression block, defaults are
//	 applied before validation, potentially converting legacy
//	 compression types (e.g., pq → rq-8)."
//
// The probe-run empirical result on this PR's branch
// (vector-index-usage-restrictions @ 53d549a9d6):
//
//	after create:                        pq.enabled=true  rq.enabled=false
//	after PUT-without-compression-block: pq.enabled=false rq.enabled=false
//
// So the gist's specific claim ("pq → rq-8") does NOT fire — rq
// stays disabled — but a related and equally-real bug DOES fire:
// compression is silently DROPPED. The class had pq before the PUT
// and has no compression after. The PR's restriction validation
// passes (empty compression slice → no allow-list rejection), so
// the silent drop slips through.
//
// Root cause traced (not introduced by this PR; pre-existing on
// stable/v1.37 and main):
//
//   - Handler.UpdateClass → UpdateClassInternal calls setClassDefaults
//     (sets VectorIndexType / Vectorizer / distance defaults but NOT
//     compression), then h.parser.ParseClass, then validation, then
//     schemaManager.UpdateClass(updated) which REPLACES the stored
//     VectorIndexConfig with the (no-compression) body.
//   - parseGivenVectorIndexConfig receives DefaultQuantization but
//     never passes it to the configParser; the default is only
//     applied by enableQuantization, which is called from AddClass
//     and not from UpdateClass / UpdateClassInternal.
//   - Net: no default injection, but no preservation either — the
//     stored compression is dropped by the REPLACE-semantics PUT.
//
// This test is committed RED to pin the bug. The fix is non-trivial
// (changes UpdateClass merge semantics, which has BC implications
// beyond the restrictions PR's scope) and intentionally NOT shipped
// in this commit set — per repo policy, a failing test that pins a
// real bug is preferred over silently deferring the finding.
//
// Allow-list is permissive (pq AND rq-8 both allowed) so the
// restriction guard is taken out of the picture; this isolates the
// underlying UpdateClass merge behaviour.
func TestPutWithoutCompressionBlock_PreservesStoredCompression(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	compose, terminate := startContainer(t, ctx, map[string]string{
		"ALLOWED_VECTOR_INDEX_TYPES": "hnsw",
		"ALLOWED_COMPRESSION_TYPES":  "pq,rq-8",
		"DEFAULT_VECTOR_INDEX":       "hnsw",
		"DEFAULT_QUANTIZATION":       "rq-8",
	})
	defer terminate()
	httpURI := "http://" + compose.GetWeaviate().URI()

	className := "ProbeCompressionMutation"
	classURL := httpURI + "/v1/schema/" + className

	// Step 1: create a class WITH explicit pq compression.
	createBody := []byte(`{
		"class":"` + className + `",
		"vectorizer":"none",
		"vectorIndexType":"hnsw",
		"vectorIndexConfig":{
			"pq":{"enabled":true}
		}
	}`)
	assertCreateOK(t, ctx, httpURI+"/v1/schema", createBody)

	// Sanity: GET the class and confirm pq is enabled before we PUT.
	pqEnabledBefore, rqEnabledBefore := readCompressionState(t, ctx, classURL)
	require.True(t, pqEnabledBefore, "precondition: class should have pq enabled after create")
	require.False(t, rqEnabledBefore, "precondition: class should not have rq enabled after create")
	t.Logf("after create: pq.enabled=%v rq.enabled=%v", pqEnabledBefore, rqEnabledBefore)

	// Step 2: PUT the class with NO compression block. The class body
	// keeps the same class name and vectorIndexType but omits the
	// `vectorIndexConfig.pq` and `vectorIndexConfig.rq` blocks.
	putBody := []byte(`{
		"class":"` + className + `",
		"vectorizer":"none",
		"vectorIndexType":"hnsw",
		"vectorIndexConfig":{}
	}`)
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, classURL, bytes.NewReader(putBody))
	require.NoError(t, err)
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(putReq)
	require.NoError(t, err, "PUT %s", classURL)
	defer putResp.Body.Close()
	putRaw, _ := io.ReadAll(putResp.Body)
	// PUT must succeed (the permissive allow-list includes whatever
	// state the class ends up in). A 4xx here would indicate a
	// different bug — validation rejected an unchanged class.
	if putResp.StatusCode >= 400 {
		t.Fatalf("PUT failed with %d: %s", putResp.StatusCode, putRaw)
	}

	// Step 3: GET the class again and observe the compression state.
	pqEnabledAfter, rqEnabledAfter := readCompressionState(t, ctx, classURL)
	t.Logf("after PUT-without-compression: pq.enabled=%v rq.enabled=%v", pqEnabledAfter, rqEnabledAfter)

	// Invariant 1 (currently HOLDS): rq is not silently enabled. The
	// gist's specific "pq → rq-8" claim does not fire because the
	// parser doesn't inject DEFAULT_QUANTIZATION and enableQuantization
	// is not called from UpdateClass.
	assert.False(t, rqEnabledAfter,
		"rq must NOT be silently enabled by a PUT that omitted the compression block")

	// Invariant 2 (currently FAILS — pinned as red): pq must remain
	// enabled. A PUT that doesn't mention compression should preserve
	// the stored compression rather than wipe it. This is the actual
	// silent-mutation bug surfaced by the probe.
	assert.True(t, pqEnabledAfter,
		"pq compression must be preserved across a PUT that omitted the compression block — "+
			"the class had pq before the PUT and must still have pq after. "+
			"FAILURE here = silent compression DROP bug, traced to UpdateClass REPLACE "+
			"semantics + no compression-preservation in setClassDefaults / parser. "+
			"Fix candidates: (a) merge initial.VectorIndexConfig compression block into "+
			"updated when updated's compression block is absent (in UpdateClassInternal "+
			"before ParseClass), (b) change schemaManager.UpdateClass to MERGE on "+
			"sub-fields instead of REPLACE. (a) is the smaller blast radius.")
}

// readCompressionState GETs the class and returns (pqEnabled, rqEnabled)
// from the vectorIndexConfig. Other compression knobs (sq, bq) are
// ignored — the test cares about the specific pq→rq mutation the gist
// names.
func readCompressionState(t *testing.T, ctx context.Context, url string) (pqEnabled, rqEnabled bool) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s expected 200", url)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var parsed struct {
		VectorIndexConfig struct {
			PQ struct {
				Enabled bool `json:"enabled"`
			} `json:"pq"`
			RQ struct {
				Enabled bool `json:"enabled"`
			} `json:"rq"`
		} `json:"vectorIndexConfig"`
	}
	require.NoError(t, json.Unmarshal(body, &parsed),
		"could not parse class body: %s", body)
	return parsed.VectorIndexConfig.PQ.Enabled, parsed.VectorIndexConfig.RQ.Enabled
}
