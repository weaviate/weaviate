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

// Package ci_shard_audit is a unit-test-time CI invariant that catches
// "added a test but forgot to wire it into any CI shard" — the silent-
// false-green failure mode QA Claude documented on weaviate/weaviate#11370
// where `TestMultiNode_GracefulLeaderRestartDuringReindex` and two
// further `RollingRestart*` tests were never matched by any shard's
// `-run` regex and ran exclusively as zero-output coverage for weeks.
//
// The audit runs against every reindex acceptance package
// (`test/acceptance/reindex_*`) and asserts that every top-level
// `Test*` function is claimed by at least one shard in `test/run.sh`.
// "Claimed" means: a shard runs the test's package AND either matches
// the test name with its `AOF_GROUP_RUN` regex OR doesn't exclude the
// test via `AOF_GROUP_SKIP` (catch-all shards).
//
// Cost: ~5 s wall-clock (4 `go test -list` calls + run.sh parse +
// regex eval). Runs as part of unit-test CI, so a forgotten shard
// wiring fails the PR before the acceptance tier even spins up.
package ci_shard_audit

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// reindexPackages enumerates the acceptance packages whose tests must
// be wired into a CI shard. Adding a new reindex_* package? Add it
// here and ensure at least one shard in test/run.sh runs it.
var reindexPackages = []string{
	"test/acceptance/reindex_multinode",
	"test/acceptance/reindex_singlenode",
	"test/acceptance/reindex_concurrent",
	"test/acceptance/reindex_mt",
}

// shardConfig captures one CI shard's effective filter against a
// package: which packages it runs, optional `-run` regex, optional
// `-skip` regex. Empty regex == match-everything / skip-nothing.
type shardConfig struct {
	function string
	name     string
	run      *regexp.Regexp
	skip     *regexp.Regexp
	packages []string
}

// claims reports whether shardConfig covers a (package, testName).
// A shard claims a test iff:
//   - The shard's packages list includes this test's package, AND
//   - The shard's `-run` matches the test name (if set), AND
//   - The shard's `-skip` does NOT match the test name (if set).
func (s shardConfig) claims(pkg, testName string) bool {
	pkgMatch := false
	for _, p := range s.packages {
		if p == pkg {
			pkgMatch = true
			break
		}
	}
	if !pkgMatch {
		return false
	}
	if s.run != nil && !s.run.MatchString(testName) {
		return false
	}
	if s.skip != nil && s.skip.MatchString(testName) {
		return false
	}
	return true
}

// TestReindexShardCoverage asserts every top-level test in every
// reindex_* acceptance package is claimed by at least one CI shard
// in test/run.sh. Fails loudly with the unclaimed test + a list of
// nearby-shaped shards if you forgot to wire a new test into the
// alternation.
func TestReindexShardCoverage(t *testing.T) {
	repoRoot := repoRootFromTestBinary(t)
	runSh := filepath.Join(repoRoot, "test", "run.sh")
	require.FileExists(t, runSh, "expected test/run.sh at %s", runSh)

	shards := parseShards(t, runSh)
	require.NotEmpty(t, shards, "no shard configs parsed from test/run.sh; parser regression?")

	for _, pkg := range reindexPackages {
		tests := listTestsInPackage(t, repoRoot, pkg)
		require.NotEmpty(t, tests,
			"go test -list returned 0 tests for %s; the audit needs at least one test name to validate against", pkg)

		var unclaimed []string
		for _, name := range tests {
			ok := false
			for _, shard := range shards {
				if shard.claims(pkg, name) {
					ok = true
					break
				}
			}
			if !ok {
				unclaimed = append(unclaimed, name)
			}
		}
		if len(unclaimed) > 0 {
			t.Errorf(
				"package %s has %d test(s) not claimed by any CI shard in test/run.sh:\n  - %s\n\n"+
					"Each shown test runs in zero CI sub-shards, producing a silent false-green for its coverage.\n"+
					"Fix: extend an existing `AOF_GROUP_RUN='...'` alternation in test/run.sh to include the test name "+
					"(exact prefix, NOT substring), or add a new shard function. See "+
					"https://github.com/weaviate/weaviate/pull/11370 for the canonical case study.",
				pkg, len(unclaimed), strings.Join(unclaimed, "\n  - "),
			)
		}
	}
}

// repoRootFromTestBinary walks up from `pwd` until it finds a go.mod,
// returning the path. The audit needs the repo root to locate
// test/run.sh and to invoke `go test -list .` from the tree top.
func repoRootFromTestBinary(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("walked up from %q to root without finding go.mod", dir)
		}
		dir = parent
	}
}

// listTestsInPackage runs `go test -list .` against the given relative
// package path and parses the output. The list command compiles the
// test binary but does not run it; it's the cheapest way to enumerate
// top-level Test* functions without source-parsing every _test.go.
func listTestsInPackage(t *testing.T, repoRoot, relPkg string) []string {
	t.Helper()
	cmd := exec.Command("go", "test", "-list", ".", "./"+relPkg+"/...")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOFLAGS=") // strip any caller-imposed -run/-tags
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("go test -list . ./%s/... failed: %v\nstderr:\n%s", relPkg, err, stderr.String())
	}
	var tests []string
	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// go test -list output: one test name per line plus a final
		// "ok <path> <duration>" footer. We only want the test names.
		if line == "" || strings.HasPrefix(line, "ok ") || strings.HasPrefix(line, "?") {
			continue
		}
		if strings.HasPrefix(line, "Test") || strings.HasPrefix(line, "Benchmark") || strings.HasPrefix(line, "Fuzz") {
			tests = append(tests, line)
		}
	}
	sort.Strings(tests)
	return tests
}

// aofGroupRunRe matches a line like `AOF_GROUP_RUN='<regex>'` or
// `AOF_GROUP_RUN="<regex>"`. The value is captured.
var aofGroupRunRe = regexp.MustCompile(`AOF_GROUP_RUN=['"]([^'"]+)['"]`)

// aofGroupSkipRe matches a line like `AOF_GROUP_SKIP='<regex>'`.
var aofGroupSkipRe = regexp.MustCompile(`AOF_GROUP_SKIP=['"]([^'"]+)['"]`)

// runAofGroupRe matches a `run_aof_group "<name>" <pkg> [<pkg>...]`
// invocation. Captures the shard name and the remainder (packages).
var runAofGroupRe = regexp.MustCompile(`run_aof_group\s+"([^"]+)"\s+(.+)`)

// shardFunctionRe matches `function run_acceptance_<...>()`.
var shardFunctionRe = regexp.MustCompile(`^function\s+(run_acceptance_\w+)\s*\(\s*\)`)

// parseShards walks test/run.sh function-by-function and returns one
// shardConfig per shard that actually invokes `run_aof_group` against
// a reindex_* package. Functions that don't invoke `run_aof_group`
// (build helpers, dispatchers) are skipped.
//
// Pre-processes line-continuations (`<line>\\\n<next line>`) into
// single logical lines so shards like
//
//	run_aof_group "reindex-concurrent" \
//	  test/acceptance/reindex_concurrent
//
// expose their package path on the same line as `run_aof_group`.
func parseShards(t *testing.T, runSh string) []shardConfig {
	t.Helper()
	src, err := os.ReadFile(runSh)
	require.NoError(t, err)
	lines := joinBackslashContinuations(strings.Split(string(src), "\n"))

	var shards []shardConfig
	var current *shardConfig
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if m := shardFunctionRe.FindStringSubmatch(line); m != nil {
			// Flush the previous shard (if any) and start a new one.
			if current != nil && len(current.packages) > 0 {
				shards = append(shards, *current)
			}
			current = &shardConfig{function: m[1]}
			continue
		}
		if current == nil {
			continue
		}
		// All three regexes are evaluated on the same logical line:
		// after joinBackslashContinuations, `AOF_GROUP_RUN='...' \\\n
		// run_aof_group "...".` collapses into one line and the run
		// regex + the package capture both fire.
		if m := aofGroupRunRe.FindStringSubmatch(line); m != nil {
			re, err := regexp.Compile(m[1])
			require.NoErrorf(t, err, "AOF_GROUP_RUN regex in %s is invalid: %s", current.function, m[1])
			current.run = re
		}
		if m := aofGroupSkipRe.FindStringSubmatch(line); m != nil {
			re, err := regexp.Compile(m[1])
			require.NoErrorf(t, err, "AOF_GROUP_SKIP regex in %s is invalid: %s", current.function, m[1])
			current.skip = re
		}
		if m := runAofGroupRe.FindStringSubmatch(line); m != nil {
			current.name = m[1]
			for _, tok := range strings.Fields(m[2]) {
				tok = strings.Trim(tok, `"`)
				if strings.HasPrefix(tok, "test/") || strings.HasPrefix(tok, "./test/") {
					current.packages = append(current.packages, strings.TrimPrefix(tok, "./"))
				}
			}
		}
		if line == "}" && current != nil {
			if len(current.packages) > 0 {
				shards = append(shards, *current)
			}
			current = nil
		}
	}
	if current != nil && len(current.packages) > 0 {
		shards = append(shards, *current)
	}
	return shards
}

// joinBackslashContinuations collapses bash line-continuations into
// single logical lines so the regexes that find `AOF_GROUP_RUN` /
// `run_aof_group` can capture arguments that span the natural shell
// continuation. The trailing `\` is dropped; whitespace between
// continued tokens collapses to a single space.
func joinBackslashContinuations(lines []string) []string {
	var out []string
	var pending string
	for _, l := range lines {
		// Strip a single trailing `\` (with optional trailing spaces
		// before/after it on the next physical line).
		trimmed := strings.TrimRight(l, " \t")
		if strings.HasSuffix(trimmed, `\`) {
			pending += strings.TrimSuffix(trimmed, `\`) + " "
			continue
		}
		out = append(out, pending+l)
		pending = ""
	}
	if pending != "" {
		out = append(out, pending)
	}
	return out
}

// TestRunShParserSanity is a guard against the parser silently
// drifting. It asserts known facts about test/run.sh: every reindex
// package is referenced by at least one shard, and every shard's
// regex compiles. A future test/run.sh refactor that breaks the
// invariant will fail this loud before TestReindexShardCoverage.
func TestRunShParserSanity(t *testing.T) {
	repoRoot := repoRootFromTestBinary(t)
	runSh := filepath.Join(repoRoot, "test", "run.sh")
	shards := parseShards(t, runSh)
	require.NotEmpty(t, shards)

	covered := map[string]bool{}
	for _, s := range shards {
		for _, p := range s.packages {
			covered[p] = true
		}
	}
	for _, pkg := range reindexPackages {
		require.Truef(t, covered[pkg],
			"package %s is not referenced by any shard in test/run.sh — every reindex_* package needs at least one shard. Did you add a new package without wiring its run function?",
			pkg)
	}

	// Print the parsed shard table in `-v` mode so a failing PR's
	// log shows which shards were considered. Cheap, useful for
	// triage.
	if testing.Verbose() {
		fmt.Println("parsed CI shards:")
		for _, s := range shards {
			runStr, skipStr := "<none>", "<none>"
			if s.run != nil {
				runStr = s.run.String()
			}
			if s.skip != nil {
				skipStr = s.skip.String()
			}
			fmt.Printf("  %s  packages=%v  run=%q  skip=%q\n", s.function, s.packages, runStr, skipStr)
		}
	}
}
