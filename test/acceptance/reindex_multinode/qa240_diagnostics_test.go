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

package reindex_multinode

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/weaviate/weaviate/test/docker"
)

// dumpQA240Diagnostics is the on-failure diagnostic dumper for
// TestMultiNode_RollingRestartMidMigration and any sibling test
// chasing weaviate/0-weaviate-issues#240. Wire via:
//
//	t.Cleanup(func() {
//		if t.Failed() {
//			dumpQA240Diagnostics(t, ctx, compose, className, propName, taskID)
//		}
//	})
//
// Captured per node (1, 2, 3):
//   - schema view: what tokenization THIS node thinks the prop has
//   - task view: /v1/tasks/{taskID} as observed locally
//   - data view: per-test-token BM25 hit counts AND result UUIDs
//   - object count: total object count via Aggregate
//   - disk dump: ls of the class LSM tree + cat of every .migrations/*.mig
//     sentinel file. Done via `docker exec` against the container's name.
//
// Everything goes through t.Logf so it surfaces in CI artifacts. The
// dumper does NOT call t.Fail() itself — it only describes state.
func dumpQA240Diagnostics(t *testing.T, ctx context.Context, compose *docker.DockerCompose, className, propName, taskID string) {
	t.Helper()
	t.Logf("=========================================================================")
	t.Logf("QA #240 DIAGNOSTICS — class=%q prop=%q taskID=%q at %s",
		className, propName, taskID, time.Now().UTC().Format(time.RFC3339Nano))
	t.Logf("=========================================================================")

	for i := 1; i <= 3; i++ {
		uri := compose.GetWeaviateNode(i).URI()
		nodeName := compose.GetWeaviateNode(i).Name()
		t.Logf("---- node %d (%s, http=%s) ----", i, nodeName, uri)

		// 1. Schema view per node.
		tokenization := tryGetPropertyTokenization(uri, className, propName)
		t.Logf("  schema: prop %q tokenization = %q", propName, tokenization)

		// 2. Task view per node (this node's RAFT-observed task state).
		if taskID != "" {
			taskState := fetchTaskState(uri, taskID)
			t.Logf("  task %q state = %s", taskID, taskState)
		}

		// 3. Aggregate count (sanity check: did the objects replicate at all?).
		count := fetchAggregateCount(uri, className)
		t.Logf("  Aggregate { count } = %d", count)

		// 4. Per-query BM25 results from this node, including the UUIDs
		// returned. Knowing WHICH docs each replica returned (not just
		// how many) is what lets us distinguish "wrong tokenization in
		// the bucket" from "missing rows in the bucket".
		for _, q := range testBM25Queries {
			ids, err := runBM25QueryOnNode(t, uri, className, q)
			if err != nil {
				t.Logf("  BM25 %q: ERROR %v", q, err)
				continue
			}
			t.Logf("  BM25 %q: %d hits", q, len(ids))
			for j, id := range ids {
				if j >= 6 {
					t.Logf("    ... (%d more)", len(ids)-6)
					break
				}
				t.Logf("    - %s", id)
			}
		}

		// 5. On-disk state via docker exec. The data path inside the
		// container is /var/lib/weaviate (the standard image default).
		// Best-effort; failures just log and move on.
		t.Logf("  on-disk LSM tree (top-level):")
		if out := dockerExec(nodeName, "sh", "-c",
			"ls -la /var/lib/weaviate/"+lowerClassName(className)+"/ 2>&1 | head -40"); out != "" {
			for _, ln := range strings.Split(strings.TrimSpace(out), "\n") {
				t.Logf("    %s", ln)
			}
		}

		t.Logf("  per-shard searchable bucket + .migrations dirs:")
		if out := dockerExec(nodeName, "sh", "-c",
			"find /var/lib/weaviate/"+lowerClassName(className)+"/ -maxdepth 4 \\( -name 'property_"+
				propName+"_searchable*' -o -name '.migrations' -o -path '*/.migrations/*' \\) -printf '%p\\t%s\\n' 2>&1 | head -80"); out != "" {
			for _, ln := range strings.Split(strings.TrimSpace(out), "\n") {
				t.Logf("    %s", ln)
			}
		}

		t.Logf("  .migrations/*.mig sentinel contents:")
		if out := dockerExec(nodeName, "sh", "-c",
			"for f in $(find /var/lib/weaviate/"+lowerClassName(className)+"/ -maxdepth 5 -path '*/.migrations/*' -name '*.mig' 2>/dev/null); do echo \"--- $f ---\"; cat \"$f\" 2>&1; done | head -200"); out != "" {
			for _, ln := range strings.Split(strings.TrimSpace(out), "\n") {
				t.Logf("    %s", ln)
			}
		}
	}

	t.Logf("=========================================================================")
	t.Logf("END QA #240 DIAGNOSTICS")
	t.Logf("=========================================================================")
}

// fetchTaskState returns the status string for the task as observed
// LOCALLY on this node. Format: "STATUS (n_units_done/total)". On
// network or parse error returns "(error: ...)".
func fetchTaskState(restURI, taskID string) string {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks/%s", restURI, taskID))
	if err != nil {
		return fmt.Sprintf("(http error: %v)", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("(HTTP %d)", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Sprintf("(read error: %v)", err)
	}
	// Caller will see status + progress in the raw JSON; minimize parsing.
	s := string(body)
	if len(s) > 600 {
		s = s[:600] + "...(truncated)"
	}
	return s
}

// fetchAggregateCount returns Aggregate { count } for the class on a
// specific node. -1 on error.
func fetchAggregateCount(restURI, className string) int {
	gql := fmt.Sprintf(`{"query":"{ Aggregate { %s { meta { count } } } }"}`, className)
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json", strings.NewReader(gql))
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	// crude regex-free extraction of "count": NNN
	const marker = `"count":`
	idx := strings.Index(string(body), marker)
	if idx < 0 {
		return -1
	}
	rest := string(body)[idx+len(marker):]
	end := strings.IndexAny(rest, ",}")
	if end <= 0 {
		return -1
	}
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(rest[:end]), "%d", &n); err != nil {
		return -1
	}
	return n
}

// dockerExec runs `docker exec <container> <args>` and returns combined
// stdout+stderr. Best-effort; returns "" on any error. We assume the
// CI runner has a `docker` binary on PATH (testcontainers requires it).
func dockerExec(container string, args ...string) string {
	cmd := exec.Command("docker", append([]string{"exec", container}, args...)...)
	out, _ := cmd.CombinedOutput()
	return string(out)
}

// lowerClassName lowercases the class for filesystem lookup. Weaviate's
// LSM tree puts shards under a lowercased class dir (the on-disk
// canonical form). If this assumption changes, only the path strings
// in dumpQA240Diagnostics need updating.
func lowerClassName(s string) string {
	return strings.ToLower(s)
}
