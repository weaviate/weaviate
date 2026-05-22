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
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
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

	// "Stale tokenization residue" probes — query each node with terms
	// that should return ZERO hits under FIELD tokenization but MULTIPLE
	// hits under WORD tokenization. If any node returns >0 on one of
	// these, its bucket still carries word-tokenized residue (which is
	// exactly the per-replica divergence shape we're chasing).
	staleResidueProbes := []string{"alpha", "bravo", "charlie", "echo"}

	for i := 1; i <= 3; i++ {
		uri := compose.GetWeaviateNode(i).URI()
		nodeName := compose.GetWeaviateNode(i).Name()
		container := compose.GetWeaviateNode(i).Container()
		t.Logf("---- node %d (%s, http=%s) ----", i, nodeName, uri)

		tokenization := tryGetPropertyTokenization(uri, className, propName)
		t.Logf("  schema: prop %q tokenization = %q", propName, tokenization)

		if taskID != "" {
			t.Logf("  task %q state = %s", taskID, fetchTaskState(uri, taskID))
		}

		count := fetchAggregateCount(uri, className)
		t.Logf("  Aggregate { count } = %d", count)

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

		// Single-token probes — under FIELD they should yield 0 unless
		// the bucket has stale word-tokenized residue.
		t.Logf("  stale-residue probes (each should be 0 under FIELD; non-zero = bucket has word-tokenized leftovers):")
		for _, q := range staleResidueProbes {
			ids, err := runBM25QueryOnNode(t, uri, className, q)
			if err != nil {
				t.Logf("    %q: ERROR %v", q, err)
				continue
			}
			marker := ""
			if len(ids) > 0 {
				marker = " ** STALE-WORD RESIDUE **"
			}
			t.Logf("    %q: %d hits%s", q, len(ids), marker)
			for j, id := range ids {
				if j >= 3 {
					t.Logf("      ... (%d more)", len(ids)-3)
					break
				}
				t.Logf("      - %s", id)
			}
		}

		// Disk dump via testcontainers Container.Exec (Docker container
		// names from testcontainers are random; the Container interface
		// hides that and goes via the Docker socket directly). Data
		// path is /data per the test image's PERSISTENCE_DATA_PATH=./data
		// with WORKDIR=/. find uses BusyBox flags (no -printf).
		classDir := "/data/" + lowerClassName(className)
		t.Logf("  on-disk LSM tree (top-level at %s):", classDir)
		dumpContainerExec(t, ctx, container,
			"ls -la "+classDir+"/ 2>&1 | head -40")

		t.Logf("  per-shard searchable bucket + .migrations dirs (path + ls):")
		dumpContainerExec(t, ctx, container,
			"find "+classDir+"/ -maxdepth 5 \\( -name 'property_"+
				propName+"_searchable*' -o -name '.migrations' -o -path '*/.migrations/*' \\) 2>/dev/null | "+
				"while read p; do echo \"$p\"; done | head -120")

		t.Logf("  .migrations/*.mig sentinel contents:")
		dumpContainerExec(t, ctx, container,
			"find "+classDir+"/ -maxdepth 6 -path '*/.migrations/*' -name '*.mig' 2>/dev/null | "+
				"while read f; do echo \"--- $f ($(wc -c < \"$f\") bytes) ---\"; cat \"$f\" 2>&1; done | head -200")

		t.Logf("  segment files under searchable bucket dirs:")
		dumpContainerExec(t, ctx, container,
			"find "+classDir+"/ -maxdepth 6 -path '*/property_"+propName+"_searchable*/*' "+
				"\\( -name '*.db' -o -name '*.wal' -o -name '*.tmp' \\) 2>/dev/null | "+
				"while read s; do echo \"$s ($(wc -c < \"$s\" 2>/dev/null) bytes)\"; done | head -80")
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

// dumpContainerExec runs the given shell command inside the container
// via the testcontainers Container.Exec API (which talks to the Docker
// daemon directly using the container ID — testcontainers gives
// containers random names, so `docker exec <logical-name>` from the
// host doesn't work). Logs each line with `    ` indent for diagnostics
// readability. Best-effort: any error is logged and the function returns.
func dumpContainerExec(t *testing.T, ctx context.Context, container interface {
	Exec(ctx context.Context, cmd []string, options ...tcexec.ProcessOption) (int, io.Reader, error)
}, shellCmd string,
) {
	t.Helper()
	code, reader, err := container.Exec(ctx, []string{"sh", "-c", shellCmd})
	if err != nil {
		t.Logf("    (exec error: %v)", err)
		return
	}
	out, _ := io.ReadAll(reader)
	if code != 0 {
		t.Logf("    (exit code %d)", code)
	}
	for _, ln := range strings.Split(strings.TrimRight(string(out), "\n"), "\n") {
		if ln == "" {
			continue
		}
		t.Logf("    %s", ln)
	}
}

// lowerClassName lowercases the class for filesystem lookup. Weaviate's
// LSM tree puts shards under a lowercased class dir (the on-disk
// canonical form). If this assumption changes, only the path strings
// in dumpQA240Diagnostics need updating.
func lowerClassName(s string) string {
	return strings.ToLower(s)
}
