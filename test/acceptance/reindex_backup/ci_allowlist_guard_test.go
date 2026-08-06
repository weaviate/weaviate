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

package reindex_backup_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This package is split across two CI matrix entries, each passing an exact-name
// -run allowlist. A test added here but to neither list never runs, and the job
// still reports green — the failure mode this guard exists to make loud.
const runShPackagePath = "test/acceptance/reindex_backup"

var (
	aofGroupRunRe   = regexp.MustCompile(`AOF_GROUP_RUN='([^']*)'`)
	testNameRe      = regexp.MustCompile(`^Test[A-Za-z0-9_]*$`)
	runShFunctionRe = regexp.MustCompile(`^function (run_acceptance_[A-Za-z0-9_]+)\(\)`)
	runShFlagRe     = regexp.MustCompile(`^\s*(--[a-z0-9-]+)[|)]`)
)

// repoRoot walks up from the working directory to the checkout root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "test", "run.sh")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "walked to the filesystem root without finding test/run.sh")
		dir = parent
	}
}

// allowlistedTests collects the exact test names from every AOF_GROUP_RUN in
// run.sh whose group runs this package.
func allowlistedTests(t *testing.T, runSh string) map[string]string {
	t.Helper()

	lines := strings.Split(runSh, "\n")
	allowed := map[string]string{}
	for i, line := range lines {
		m := aofGroupRunRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		// The package the filter applies to is named on the same line or the
		// next few, as the argument to run_aof_group.
		scope := strings.Join(lines[i:min(i+4, len(lines))], "\n")
		if !strings.Contains(scope, runShPackagePath) {
			continue
		}
		for _, name := range parseExactNameAlternation(t, m[1]) {
			allowed[name] = m[1]
		}
	}
	require.NotEmpty(t, allowed,
		"found no AOF_GROUP_RUN filter for %s in test/run.sh — either the group was renamed "+
			"or this guard's parsing broke; a silent pass here would defeat the guard", runShPackagePath)
	return allowed
}

// parseExactNameAlternation turns `^(A|B)$` or `^A$` into its names, and fails
// on anything else: a pattern this cannot read exactly is one whose coverage
// this guard cannot vouch for.
func parseExactNameAlternation(t *testing.T, pattern string) []string {
	t.Helper()

	body := pattern
	require.True(t, strings.HasPrefix(body, "^") && strings.HasSuffix(body, "$"),
		"AOF_GROUP_RUN %q is not anchored; this guard only understands exact-name allowlists", pattern)
	body = strings.TrimSuffix(strings.TrimPrefix(body, "^"), "$")
	if strings.HasPrefix(body, "(") && strings.HasSuffix(body, ")") {
		body = strings.TrimSuffix(strings.TrimPrefix(body, "("), ")")
	}

	names := strings.Split(body, "|")
	for _, name := range names {
		require.Regexp(t, testNameRe, name,
			"AOF_GROUP_RUN %q contains %q, which is not a plain test name; this guard only "+
				"understands exact-name allowlists", pattern, name)
	}
	return names
}

// declaredTests asks the toolchain which top-level tests this package has, so a
// test file added without touching this guard is still seen.
func declaredTests(t *testing.T) []string {
	t.Helper()

	out, err := exec.Command("go", "test", "-list", ".*", ".").CombinedOutput()
	require.NoErrorf(t, err, "go test -list failed: %s", out)

	var names []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if testNameRe.MatchString(line) {
			names = append(names, line)
		}
	}
	require.NotEmpty(t, names, "go test -list reported no tests; output was:\n%s", out)
	return names
}

// TestCIAllowlistCoversEveryTestInThisPackage fails when a test in this package
// is missing from run.sh's -run allowlists, which would leave it never executed
// while CI stays green. It is itself in the allowlist; if it were not, it could
// not report on anything.
func TestCIAllowlistCoversEveryTestInThisPackage(t *testing.T) {
	root := repoRoot(t)
	runSh, err := os.ReadFile(filepath.Join(root, "test", "run.sh"))
	require.NoError(t, err)

	allowed := allowlistedTests(t, string(runSh))
	declared := declaredTests(t)

	var missing []string
	for _, name := range declared {
		if _, ok := allowed[name]; !ok {
			missing = append(missing, name)
		}
	}
	require.Emptyf(t, missing,
		"these tests in %s are in NO run.sh allowlist, so CI never runs them while reporting green: %s\n"+
			"Add each to an AOF_GROUP_RUN filter in test/run.sh (run_acceptance_reindex_backup for "+
			"single-node, run_acceptance_reindex_backup_cluster for multi-node).",
		runShPackagePath, strings.Join(missing, ", "))

	declaredSet := map[string]struct{}{}
	for _, name := range declared {
		declaredSet[name] = struct{}{}
	}
	var stale []string
	for name, pattern := range allowed {
		if _, ok := declaredSet[name]; !ok {
			stale = append(stale, name+" (in "+pattern+")")
		}
	}
	require.Emptyf(t, stale,
		"these names in run.sh's allowlists match no test in %s, so that filter silently runs "+
			"fewer tests than it reads as covering: %s",
		runShPackagePath, strings.Join(stale, ", "))
}

// The guard above proves every test here is named in some run.sh filter. It
// says nothing about whether CI ever invokes the group holding that filter. A
// matrix entry renamed or deleted in .github/workflows takes its whole group
// out of CI, and both the allowlist guard and the job stay green.
//
// The chain that has to hold, end to end:
//
//	AOF_GROUP_RUN filter  ->  the run_acceptance_* function it lives in
//	                      ->  run.sh's dispatcher, which calls that function
//	                      ->  run.sh's --flag that arms the dispatcher
//	                      ->  a workflow matrix entry passing that --flag
//
// Every hop is a literal string match, so this needs no YAML parsing: the
// matrix entries pass run.sh flags verbatim, and that is the whole contract
// between the two files.
func TestCIWorkflowInvokesEveryGroupThatRunsThisPackage(t *testing.T) {
	root := repoRoot(t)
	runShBytes, err := os.ReadFile(filepath.Join(root, "test", "run.sh"))
	require.NoError(t, err)
	runSh := string(runShBytes)

	groups := groupsRunningThisPackage(t, runSh)
	require.NotEmptyf(t, groups,
		"found no run_acceptance_* function holding an AOF_GROUP_RUN filter for %s; "+
			"either the groups were renamed or this guard's parsing broke", runShPackagePath)

	workflows := workflowSources(t, root)

	for _, group := range groups {
		t.Run(group, func(t *testing.T) {
			requireDispatched(t, runSh, group)
			flag := flagArming(t, runSh, group)

			var found string
			for name, body := range workflows {
				if strings.Contains(body, `"`+flag+`"`) {
					found = name
					break
				}
			}
			require.NotEmptyf(t, found,
				"no workflow in .github/workflows passes %q, so %s never runs in CI while "+
					"the tests it owns still read as covered by the run.sh allowlist guard",
				flag, group)
		})
	}
}

// groupsRunningThisPackage returns the run_acceptance_* functions that carry an
// AOF_GROUP_RUN filter for this package.
func groupsRunningThisPackage(t *testing.T, runSh string) []string {
	t.Helper()

	lines := strings.Split(runSh, "\n")
	var (
		current string
		groups  []string
		seen    = map[string]struct{}{}
	)
	for i, line := range lines {
		if m := runShFunctionRe.FindStringSubmatch(line); m != nil {
			current = m[1]
			continue
		}
		if !aofGroupRunRe.MatchString(line) {
			continue
		}
		scope := strings.Join(lines[i:min(i+4, len(lines))], "\n")
		if !strings.Contains(scope, runShPackagePath) {
			continue
		}
		require.NotEmptyf(t, current,
			"an AOF_GROUP_RUN filter for %s sits outside any run_acceptance_* function "+
				"(test/run.sh line %d); this guard cannot trace it to a CI flag",
			runShPackagePath, i+1)
		if _, ok := seen[current]; !ok {
			seen[current] = struct{}{}
			groups = append(groups, current)
		}
	}
	return groups
}

// requireDispatched pins the hop between run.sh's flag variable and the call:
// a flag that arms a variable nobody reads runs nothing.
func requireDispatched(t *testing.T, runSh, group string) {
	t.Helper()

	lines := strings.Split(runSh, "\n")
	for i, line := range lines {
		if !strings.Contains(line, "if $"+group+"; then") {
			continue
		}
		scope := strings.Join(lines[i:min(i+4, len(lines))], "\n")
		require.Containsf(t, scope, "\n    "+group+"\n",
			"run.sh guards on $%s but does not call %s underneath it", group, group)
		return
	}
	t.Fatalf("run.sh never reads $%s, so no flag can reach %s", group, group)
}

// flagArming returns the long CLI flag whose arg-parse arm sets this group's
// variable.
func flagArming(t *testing.T, runSh, group string) string {
	t.Helper()

	for _, line := range strings.Split(runSh, "\n") {
		if !strings.Contains(line, group+"=true") {
			continue
		}
		m := runShFlagRe.FindStringSubmatch(line)
		require.NotNilf(t, m, "run.sh line %q sets %s=true but starts with no --flag", line, group)
		return m[1]
	}
	t.Fatalf("no run.sh argument sets %s=true, so nothing on a CI command line can select it", group)
	return ""
}

// workflowSources reads every workflow file as text. The matrix entries pass
// run.sh flags verbatim, so a literal search over the file is the whole check
// and a YAML parser would only add ways to be wrong.
func workflowSources(t *testing.T, root string) map[string]string {
	t.Helper()

	dir := filepath.Join(root, ".github", "workflows")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	sources := map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "ml") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		sources[entry.Name()] = string(body)
	}
	require.NotEmpty(t, sources, "no workflow files found under %s", dir)
	return sources
}
