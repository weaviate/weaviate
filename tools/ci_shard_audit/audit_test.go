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

// Package ci_shard_audit asserts every Go test in every reindex_*
// acceptance package is claimed by at least one CI shard in
// test/run.sh. Closes the silent-false-green class documented on
// weaviate/weaviate#11370 where `Graceful` in a shard alternation
// did NOT match `GracefulLeaderRestart` — alternation is exact name,
// not prefix.
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

var reindexPackages = []string{
	"test/acceptance/reindex_multinode",
	"test/acceptance/reindex_singlenode",
	"test/acceptance/reindex_concurrent",
	"test/acceptance/reindex_mt",
}

type shardConfig struct {
	function string
	name     string
	run      *regexp.Regexp
	skip     *regexp.Regexp
	packages []string
}

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

func TestReindexShardCoverage(t *testing.T) {
	repoRoot := repoRootFromTestBinary(t)
	runSh := filepath.Join(repoRoot, "test", "run.sh")
	require.FileExists(t, runSh)

	shards := parseShards(t, runSh)
	require.NotEmpty(t, shards, "no shard configs parsed from test/run.sh")

	for _, pkg := range reindexPackages {
		tests := listTestsInPackage(t, repoRoot, pkg)
		require.NotEmpty(t, tests,
			"go test -list returned 0 tests for %s", pkg)

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
				"%s has %d test(s) not claimed by any CI shard in test/run.sh:\n  - %s\n\n"+
					"Extend an `AOF_GROUP_RUN='...'` alternation in test/run.sh to include each name "+
					"(exact prefix, NOT substring). See weaviate/weaviate#11370.",
				pkg, len(unclaimed), strings.Join(unclaimed, "\n  - "),
			)
		}
	}
}

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

// listTestsInPackage enumerates top-level Test* names via `go test
// -list .`. Benchmark*/Fuzz* are skipped — CI shards invoke `go test`
// without `-bench`/`-fuzz`, so those never run in shards and the
// audit must not flag them as unclaimed (per Copilot review on #11376).
func listTestsInPackage(t *testing.T, repoRoot, relPkg string) []string {
	t.Helper()
	cmd := exec.Command("go", "test", "-list", ".", "./"+relPkg+"/...")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOFLAGS=")
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
		if line == "" || strings.HasPrefix(line, "ok ") || strings.HasPrefix(line, "?") {
			continue
		}
		if strings.HasPrefix(line, "Test") {
			tests = append(tests, line)
		}
	}
	// Per Copilot review on #11376: bufio.Scanner can drop output on
	// long lines or IO error without returning an error from Scan(),
	// silently under-reporting coverage gaps. Fail loudly instead.
	require.NoError(t, scanner.Err(),
		"scanner.Err on go test -list output for %s", relPkg)
	sort.Strings(tests)
	return tests
}

var (
	aofGroupRunRe   = regexp.MustCompile(`AOF_GROUP_RUN=['"]([^'"]+)['"]`)
	aofGroupSkipRe  = regexp.MustCompile(`AOF_GROUP_SKIP=['"]([^'"]+)['"]`)
	runAofGroupRe   = regexp.MustCompile(`run_aof_group\s+"([^"]+)"\s+(.+)`)
	shardFunctionRe = regexp.MustCompile(`^function\s+(run_acceptance_\w+)\s*\(\s*\)`)
)

// parseShards walks test/run.sh and returns one shardConfig per
// function that invokes run_aof_group. Bash `\` continuations are
// joined first so `AOF_GROUP_RUN='...' \\\n  run_aof_group "..."`
// exposes both halves on a single logical line.
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
			if current != nil && len(current.packages) > 0 {
				shards = append(shards, *current)
			}
			current = &shardConfig{function: m[1]}
			continue
		}
		if current == nil {
			continue
		}
		if m := aofGroupRunRe.FindStringSubmatch(line); m != nil {
			re, err := regexp.Compile(m[1])
			require.NoErrorf(t, err, "AOF_GROUP_RUN regex in %s: %s", current.function, m[1])
			current.run = re
		}
		if m := aofGroupSkipRe.FindStringSubmatch(line); m != nil {
			re, err := regexp.Compile(m[1])
			require.NoErrorf(t, err, "AOF_GROUP_SKIP regex in %s: %s", current.function, m[1])
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

func joinBackslashContinuations(lines []string) []string {
	var out []string
	var pending string
	for _, l := range lines {
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

// TestRunShParserSanity guards against parser drift: every reindex_*
// package must be referenced by at least one shard, and the parsed
// table renders on `-v` for triage.
func TestRunShParserSanity(t *testing.T) {
	repoRoot := repoRootFromTestBinary(t)
	shards := parseShards(t, filepath.Join(repoRoot, "test", "run.sh"))
	require.NotEmpty(t, shards)

	covered := map[string]bool{}
	for _, s := range shards {
		for _, p := range s.packages {
			covered[p] = true
		}
	}
	for _, pkg := range reindexPackages {
		require.Truef(t, covered[pkg],
			"package %s is not referenced by any shard in test/run.sh", pkg)
	}

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
