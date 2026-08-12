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

// Package ciguard pins the full chain that runs test/acceptance/reindex_backup
// in CI (test -> allowlist -> run.sh function -> workflow matrix entry). It
// lives outside the guarded package so a matrix edit that disables the shard
// cannot also disable its own guard.
package ciguard

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const guardedPackagePath = "test/acceptance/reindex_backup"

// imageBuildAllowanceMinutes is how much of the job window this guard reserves
// for building the test image before go test starts, so the build and the
// go-test budget together still fit inside the workflow's timeout_minutes.
//
// An allowance, not a measurement: nothing in the repo reports how long the
// build actually takes. A build that overruns it eats the same number of
// minutes off the end of the run instead, and the group is runner-killed at
// timeout_minutes rather than panicking with stacks — the outcome this
// arithmetic exists to prevent. Raise it if that starts happening.
const imageBuildAllowanceMinutes = 5

var (
	aofGroupRunRe = regexp.MustCompile(`AOF_GROUP_RUN='([^']*)'`)
	// Captures whatever a group sets its budget to, not only the readable
	// shapes, so an unreadable one is rejected rather than skipped.
	aofGroupTimeoutRe        = regexp.MustCompile(`AOF_GROUP_TIMEOUT=([^\s\\]*)`)
	aofGroupTimeoutDefaultRe = regexp.MustCompile(`AOF_GROUP_TIMEOUT:-([^\s}]*)`)
	aofTestBudgetRe          = regexp.MustCompile(`AOF_TEST_BUDGET (Test[A-Za-z0-9_]+) ([0-9]+)m`)
	// Fuzz targets are declared like tests but no group's exact-name -run
	// alternation can select one, so they must be visible to the guards rather
	// than filtered out before they are counted.
	declaredTestRe     = regexp.MustCompile(`^func ((?:Test|Fuzz)[A-Za-z0-9_]*)\(`)
	testNameRe         = regexp.MustCompile(`^(?:Test|Fuzz)[A-Za-z0-9_]*$`)
	runShFunctionRe    = regexp.MustCompile(`^function (run_acceptance_[A-Za-z0-9_]+)\(\)`)
	runShAnyFunctionRe = regexp.MustCompile(`^function ([A-Za-z0-9_]+)\(\)`)
	runShFlagRe        = regexp.MustCompile(`^\s*(--[a-z0-9-]+)[|)]`)
	grepExcludeRe      = regexp.MustCompile(`grep -v[A-Za-z]* '([^']*)'`)
	wholeMinutesRe     = regexp.MustCompile(`^([0-9]+)m$`)
	// Matches the package path only as a whole path segment, so a future
	// sibling such as test/acceptance/reindex_backup_mt is not read as this
	// package and does not silently widen what these guards claim to cover.
	guardedPackageRe = regexp.MustCompile(regexp.QuoteMeta(guardedPackagePath) + `(?:[^\w.-]|$)`)
)

// TestCIAllowlistCoversEveryTestInThisPackage fails when a test in the guarded
// package is missing from run.sh's -run allowlists, which would leave it never
// executed while CI stays green. It also fails the other way round, on a
// filter or a budget line naming a test that no longer exists.
func TestCIAllowlistCoversEveryTestInThisPackage(t *testing.T) {
	root := repoRoot(t)
	runSh := readRunSh(t, root)

	allowed := allowlistedTests(t, runSh)
	declared := declaredTests(t, root)

	var missing []string
	for _, name := range declared {
		if _, ok := allowed[name]; !ok {
			missing = append(missing, name)
		}
	}
	require.Emptyf(t, missing,
		"these tests in %s are in NO run.sh allowlist, so CI never runs them while reporting green: %s\n"+
			"Add each to an AOF_GROUP_RUN filter in test/run.sh (run_acceptance_reindex_backup_suite, "+
			"_a or _b for single-node, run_acceptance_reindex_backup_cluster for multi-node), "+
			"picking the group whose budget still covers its worst case.",
		guardedPackagePath, strings.Join(missing, ", "))

	declaredSet := map[string]struct{}{}
	for _, name := range declared {
		declaredSet[name] = struct{}{}
	}
	var stale []string
	for name, group := range allowed {
		if _, ok := declaredSet[name]; !ok {
			stale = append(stale, name+" (in "+group+")")
		}
	}
	require.Emptyf(t, stale,
		"these names in run.sh's allowlists match no test in %s, so that filter silently runs "+
			"fewer tests than it reads as covering: %s",
		guardedPackagePath, strings.Join(stale, ", "))

	var staleBudgets []string
	for name := range testWorstCases(t, runSh) {
		if _, ok := declaredSet[name]; !ok {
			staleBudgets = append(staleBudgets, name)
		}
	}
	require.Emptyf(t, staleBudgets,
		"these AOF_TEST_BUDGET lines in run.sh name no test in %s, so a group budget is being "+
			"read against a worst case nothing spends: %s",
		guardedPackagePath, strings.Join(staleBudgets, ", "))
}

// TestCIGroupTimeoutFitsTheJobWindow checks each group's go-test budget against
// the deadlines its own tests wait on and the timeout_minutes the workflow
// gives the job, so a hang panics with stacks instead of being runner-killed.
func TestCIGroupTimeoutFitsTheJobWindow(t *testing.T) {
	root := repoRoot(t)
	runSh := readRunSh(t, root)

	groups := groupsRunningThisPackage(t, runSh)
	windows := workflowRunShWindows(t, root)
	worstCases := testWorstCases(t, runSh)

	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			budget := groupTimeoutMinutes(t, runSh, group.name)
			requireDispatched(t, runSh, group.name)
			flag := flagArming(t, runSh, group.name)

			// Zero covers both ways the window goes missing: no PR job passes
			// this flag, or one does with an unreadable timeout_minutes.
			window := windows[flag]
			require.NotZerof(t, window,
				"no pull-request-triggered workflow job in .github/workflows passes %q to "+
					"test/run.sh under a readable timeout_minutes, so nothing bounds the job "+
					"%s runs in — and if no job passes it at all, that group and every test it "+
					"owns silently left PR CI",
				flag, group.name)

			require.LessOrEqualf(t, budget+imageBuildAllowanceMinutes, window,
				"%s gets a %dm go-test budget and the image build takes about %dm, which "+
					"together exceed the %dm the workflow step passing %q allows. The runner "+
					"kills the job before go test can panic with stacks. Lower the budget, "+
					"raise timeout_minutes, or split the group.",
				group.name, budget, imageBuildAllowanceMinutes, window, flag)

			var floor int
			for _, name := range group.tests {
				minutes, declared := worstCases[name]
				require.Truef(t, declared,
					"%s runs %s but run.sh declares no AOF_TEST_BUDGET line for it, so nothing says "+
						"how much of the %dm budget it needs; add one next to the others",
					group.name, name, budget)
				floor += minutes
			}
			require.GreaterOrEqualf(t, budget, floor,
				"%s gets a %dm go-test budget but its tests wait on %dm of deadlines (%s). go test "+
					"is killed before the slowest one reaches its own assertion, and the failure "+
					"reads as a product hang rather than a budget that is too small. Raise the "+
					"budget, or move a test to another group.",
				group.name, budget, floor, strings.Join(group.tests, ", "))
		})
	}
}

// TestCIGuardRunsInTheUnitJob pins the placement every other guard here rests
// on. They only bind because the unit job runs this package, and it only runs it
// because the package sits outside the trees run_unit_tests filters out. Move it
// into one of those and it leaves CI entirely, while every assertion in this
// file still reads as passing.
func TestCIGuardRunsInTheUnitJob(t *testing.T) {
	root := repoRoot(t)
	self := selfPackagePath(t, root)

	for _, pattern := range unitJobExclusions(t, readRunSh(t, root)) {
		require.NotRegexpf(t, pattern, self,
			"run_unit_tests in test/run.sh drops every package matching %q, and this guard's own "+
				"package %s now matches it. Nothing then runs this file, and the shard split it "+
				"guards goes unchecked while CI reports green. Move the guard back out of that tree.",
			pattern, self)
	}
	// Below that filter run.sh splits what is left over two shards with a grep
	// and its own -v over one shared expression, so a package that survives
	// here is run by exactly one of them.
}

// selfPackagePath is this guard's own directory relative to the checkout root.
// Read off the working directory rather than written down, so it follows the
// package instead of having to be kept in step with it.
func selfPackagePath(t *testing.T, root string) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)
	rel, err := filepath.Rel(root, dir)
	require.NoError(t, err)
	return filepath.ToSlash(rel)
}

// unitJobExclusions returns the patterns run_unit_tests drops from the package
// list both unit shards start from.
func unitJobExclusions(t *testing.T, runSh string) []string {
	t.Helper()

	var inUnitTests bool
	for _, line := range strings.Split(runSh, "\n") {
		if m := runShAnyFunctionRe.FindStringSubmatch(line); m != nil {
			inUnitTests = m[1] == "run_unit_tests"
			continue
		}
		if !inUnitTests || !strings.Contains(line, "go list ./...") {
			continue
		}
		var patterns []string
		for _, m := range grepExcludeRe.FindAllStringSubmatch(line, -1) {
			patterns = append(patterns, m[1])
		}
		require.NotEmptyf(t, patterns,
			"run_unit_tests builds its package list with %q, which this guard cannot read any "+
				"exclusion out of; either the pipeline changed shape or this guard's parsing "+
				"broke, and reading zero exclusions would pass every placement", strings.TrimSpace(line))
		return patterns
	}
	t.Fatal("test/run.sh has no `go list ./...` line inside run_unit_tests, so this guard cannot " +
		"tell which packages the unit job runs")
	return nil
}

// TestCIGuardParsers pins the two readers whose failure mode is a silent pass:
// a budget shape this guard rounds instead of rejecting would validate a group
// against a number the group does not use, and a package match on a substring
// would read a future sibling package as this one.
func TestCIGuardParsers(t *testing.T) {
	t.Run("only whole minutes are readable", func(t *testing.T) {
		// The rejected shapes are all shapes go test itself accepts.
		tests := []struct {
			value string
			want  int
			ok    bool
		}{
			{value: "20m", want: 20, ok: true},
			{value: "90s"},
			{value: "18m30s"},
			{value: "20"},
			{value: ""},
		}
		for _, tc := range tests {
			t.Run(tc.value, func(t *testing.T) {
				minutes, ok := wholeMinutes(tc.value)
				require.Equal(t, tc.ok, ok)
				require.Equal(t, tc.want, minutes)
			})
		}
	})

	t.Run("the package path matches whole segments only", func(t *testing.T) {
		tests := []struct {
			scope string
			want  bool
		}{
			{scope: `run_aof_group "reindex-backup-a" test/acceptance/reindex_backup`, want: true},
			{scope: `run_aof_group "x" test/acceptance/reindex_backup someArg`, want: true},
			{scope: `run_aof_group "x" test/acceptance/reindex_backup_mt`},
			{scope: `run_aof_group "x" test/acceptance/reindex_backup-legacy`},
		}
		for _, tc := range tests {
			t.Run(tc.scope, func(t *testing.T) {
				require.Equal(t, tc.want, guardedPackageRe.MatchString(tc.scope))
			})
		}
	})
}

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

func readRunSh(t *testing.T, root string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, "test", "run.sh"))
	require.NoError(t, err)
	return string(body)
}

// declaredTests reads the guarded package's sources for top-level test
// functions. A source scan, not `go test -list`: it needs no toolchain and no
// compile of an acceptance package, and it still sees a test hidden behind a
// build tag, which `go test -list` without -tags does not.
//
// The one escape left open: the read is non-recursive, so tests in a
// subpackage of the guarded package are not covered by these guards.
func declaredTests(t *testing.T, root string) []string {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join(root, guardedPackagePath))
	require.NoError(t, err)

	var names []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(root, guardedPackagePath, entry.Name()))
		require.NoError(t, err)
		for _, line := range strings.Split(string(body), "\n") {
			m := declaredTestRe.FindStringSubmatch(line)
			// TestMain is the package's own entry point, never selectable by a
			// -run filter, so it is not something an allowlist can cover.
			if m == nil || m[1] == "TestMain" {
				continue
			}
			names = append(names, m[1])
		}
	}
	require.NotEmptyf(t, names,
		"%s declares no tests; either the package moved or this guard's parsing broke, and a "+
			"silent pass here would defeat every guard in this file", guardedPackagePath)
	return names
}

// allowlistedTests maps each allowlisted test name to the one group that runs
// it. Being in two groups is rejected, not merged: each group's budget is
// derived on the assumption a test runs once.
func allowlistedTests(t *testing.T, runSh string) map[string]string {
	t.Helper()

	allowed := map[string]string{}
	for _, group := range groupsRunningThisPackage(t, runSh) {
		for _, name := range group.tests {
			other, duplicate := allowed[name]
			require.Falsef(t, duplicate,
				"%s is in the AOF_GROUP_RUN filter of both %s and %s, so it runs twice while each "+
					"group's budget is derived on the assumption it runs once; delete the stale entry",
				name, other, group.name)
			allowed[name] = group.name
		}
	}
	return allowed
}

// testWorstCases reads run.sh's per-test AOF_TEST_BUDGET lines, which are the
// sums the group budgets are derived from. They live in run.sh so the
// derivation and the budget it produces cannot drift apart in separate files.
func testWorstCases(t *testing.T, runSh string) map[string]int {
	t.Helper()

	worst := map[string]int{}
	for _, m := range aofTestBudgetRe.FindAllStringSubmatch(runSh, -1) {
		_, duplicate := worst[m[1]]
		require.Falsef(t, duplicate, "run.sh declares two AOF_TEST_BUDGET lines for %s", m[1])
		minutes, err := strconv.Atoi(m[2])
		require.NoError(t, err)
		worst[m[1]] = minutes
	}
	require.NotEmpty(t, worst,
		"run.sh declares no AOF_TEST_BUDGET lines, so nothing says what each group's budget "+
			"has to cover; either they were removed or this guard's parsing broke")
	return worst
}

// packageGroup is one run_acceptance_* function running the guarded package,
// with the exact test names its AOF_GROUP_RUN filter selects.
type packageGroup struct {
	name  string
	tests []string
}

// groupsRunningThisPackage returns, in run.sh order, the run_acceptance_*
// functions that carry an AOF_GROUP_RUN filter for the guarded package.
func groupsRunningThisPackage(t *testing.T, runSh string) []packageGroup {
	t.Helper()

	lines := strings.Split(runSh, "\n")
	var (
		current string
		groups  []packageGroup
		at      = map[string]int{}
	)
	for i, line := range lines {
		if m := runShFunctionRe.FindStringSubmatch(line); m != nil {
			current = m[1]
			continue
		}
		m := aofGroupRunRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		scope := strings.Join(lines[i:min(i+4, len(lines))], "\n")
		if !guardedPackageRe.MatchString(scope) {
			continue
		}
		require.NotEmptyf(t, current,
			"an AOF_GROUP_RUN filter for %s sits outside any run_acceptance_* function "+
				"(test/run.sh line %d); this guard cannot trace it to a CI flag",
			guardedPackagePath, i+1)
		idx, seen := at[current]
		if !seen {
			idx = len(groups)
			at[current] = idx
			groups = append(groups, packageGroup{name: current})
		}
		groups[idx].tests = append(groups[idx].tests, parseExactNameAlternation(t, m[1])...)
	}
	require.NotEmptyf(t, groups,
		"test/run.sh holds no AOF_GROUP_RUN filter for %s; the whole shard split is gone "+
			"or this guard's parsing broke", guardedPackagePath)
	return groups
}

// parseExactNameAlternation turns `^(A|B)$` or `^A$` into its names, and fails
// on anything else: a pattern this cannot read exactly is one whose coverage
// this guard cannot vouch for.
func parseExactNameAlternation(t *testing.T, pattern string) []string {
	t.Helper()

	require.True(t, strings.HasPrefix(pattern, "^") && strings.HasSuffix(pattern, "$"),
		"AOF_GROUP_RUN %q is not anchored; this guard only understands exact-name allowlists", pattern)
	body := strings.TrimSuffix(strings.TrimPrefix(pattern, "^"), "$")
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

// groupTimeoutMinutes reads the go-test budget a group sets, falling back to
// run_aof_group's default when it sets none. A budget in any other shape than
// whole minutes fails here rather than falling through to the default.
func groupTimeoutMinutes(t *testing.T, runSh, group string) int {
	t.Helper()

	var inGroup bool
	for _, line := range strings.Split(runSh, "\n") {
		if m := runShFunctionRe.FindStringSubmatch(line); m != nil {
			inGroup = m[1] == group
			continue
		}
		if !inGroup {
			continue
		}
		if m := aofGroupTimeoutRe.FindStringSubmatch(line); m != nil {
			return requireWholeMinutes(t, m[1], group+" sets AOF_GROUP_TIMEOUT")
		}
	}

	m := aofGroupTimeoutDefaultRe.FindStringSubmatch(runSh)
	require.NotNilf(t, m, "%s sets no AOF_GROUP_TIMEOUT and run_aof_group's default "+
		"is not in the shape this guard reads", group)
	return requireWholeMinutes(t, m[1], "run_aof_group's default AOF_GROUP_TIMEOUT")
}

// wholeMinutes reads an Nm duration. Every other shape go test accepts —
// 1h, 90s, 18m30s — reports false rather than a number, because rounding one
// silently is the same defect as reading the wrong value.
func wholeMinutes(value string) (int, bool) {
	m := wholeMinutesRe.FindStringSubmatch(value)
	if m == nil {
		return 0, false
	}
	minutes, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, false
	}
	return minutes, true
}

// requireWholeMinutes is wholeMinutes with the failure spelled out for whoever
// wrote the unreadable value.
func requireWholeMinutes(t *testing.T, value, what string) int {
	t.Helper()

	minutes, ok := wholeMinutes(value)
	require.Truef(t, ok,
		"%s to %q, which is not the whole-minutes shape this guard reads, so it cannot say "+
			"whether the group's tests fit inside it; write the budget as Nm",
		what, value)
	return minutes
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
			"test/run.sh guards on $%s but does not call %s underneath it", group, group)
		return
	}
	t.Fatalf("test/run.sh never reads $%s, so no flag can reach %s", group, group)
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
		require.NotNilf(t, m, "test/run.sh line %q sets %s=true but starts with no --flag", line, group)
		return m[1]
	}
	t.Fatalf("no test/run.sh argument sets %s=true, so nothing on a CI command line can select it", group)
	return ""
}

// workflowFile is the slice of a workflow this guard reads: what triggers it,
// and which jobs it has.
type workflowFile struct {
	On   yaml.Node              `yaml:"on"`
	Jobs map[string]workflowJob `yaml:"jobs"`
}

type workflowJob struct {
	If       string `yaml:"if"`
	Strategy struct {
		Matrix struct {
			Include []map[string]yaml.Node `yaml:"include"`
		} `yaml:"matrix"`
	} `yaml:"strategy"`
	Steps []workflowStep `yaml:"steps"`
}

type workflowStep struct {
	If   string               `yaml:"if"`
	Run  string               `yaml:"run"`
	With map[string]yaml.Node `yaml:"with"`
}

// workflowRunShWindows maps every test/run.sh flag a pull request can reach to
// the smallest timeout_minutes bounding a step that runs it; an absent key
// means the group left PR CI, a zero one means its budget can't be checked.
func workflowRunShWindows(t *testing.T, root string) map[string]int {
	t.Helper()

	dir := filepath.Join(root, ".github", "workflows")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	windows := map[string]int{}
	var parsed int
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "ml") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)

		var wf workflowFile
		require.NoErrorf(t, yaml.Unmarshal(body, &wf),
			"%s does not parse as YAML; this guard cannot tell what it runs", entry.Name())
		parsed++

		if !triggersOnPullRequest(&wf.On) {
			continue
		}
		for _, job := range wf.Jobs {
			if notProvenTrue(job.If) {
				continue
			}
			collectRunShWindows(job, windows)
		}
	}
	require.NotZero(t, parsed, "no workflow files found under %s", dir)
	return windows
}

// collectRunShWindows records the window bounding every flag one job's run.sh
// steps can be handed, including the ones its matrix interpolates in.
func collectRunShWindows(job workflowJob, windows map[string]int) {
	var matrixFlags []string
	for _, entry := range job.Strategy.Matrix.Include {
		for _, v := range entry {
			matrixFlags = append(matrixFlags, flagsIn(scalar(&v))...)
		}
	}

	for _, step := range job.Steps {
		if notProvenTrue(step.If) {
			continue
		}
		commands := []string{step.Run}
		for _, v := range step.With {
			commands = append(commands, scalar(&v))
		}

		var runsRunSh bool
		// Cloned: appending to the matrix flags in place would leak this step's
		// flags into every later step's list.
		stepFlags := append([]string(nil), matrixFlags...)
		for _, c := range commands {
			if strings.Contains(c, "run.sh") {
				runsRunSh = true
			}
			stepFlags = append(stepFlags, flagsIn(c)...)
		}
		if !runsRunSh {
			continue
		}

		// A timeout_minutes written as a `${{ }}` expression is a window this
		// guard cannot read. Recording zero makes the group it covers fail as
		// "no window found" rather than pass on an unread number.
		var minutes int
		if raw, ok := step.With["timeout_minutes"]; ok {
			minutes, _ = strconv.Atoi(scalar(&raw))
		}
		for _, flag := range stepFlags {
			existing, seen := windows[flag]
			if !seen {
				windows[flag] = minutes
				continue
			}
			if minutes > 0 && (existing == 0 || minutes < existing) {
				windows[flag] = minutes
			}
		}
	}
}

func flagsIn(command string) []string {
	var flags []string
	for _, w := range strings.Fields(command) {
		if strings.HasPrefix(w, "--") {
			flags = append(flags, w)
		}
	}
	return flags
}

// triggersOnPullRequest reads the `on:` key in each of its three legal shapes.
func triggersOnPullRequest(on *yaml.Node) bool {
	switch on.Kind {
	case yaml.ScalarNode:
		return on.Value == "pull_request"
	case yaml.SequenceNode:
		for _, item := range on.Content {
			if item.Value == "pull_request" {
				return true
			}
		}
	case yaml.MappingNode:
		for i := 0; i < len(on.Content); i += 2 {
			if on.Content[i].Value == "pull_request" {
				return true
			}
		}
	case yaml.DocumentNode, yaml.AliasNode:
		// not a legal shape for an `on:` value; treat as not triggered
	}
	return false
}

// notProvenTrue reports whether an if: might not hold on a pull request.
// Anything other than an absent or literally-true condition counts, since e.g.
// github.event_name == 'schedule' takes the group out of every PR run too.
func notProvenTrue(expr string) bool {
	e := strings.TrimSpace(expr)
	if e == "" {
		return false
	}
	e = strings.TrimSuffix(strings.TrimPrefix(e, "${{"), "}}")
	return !strings.EqualFold(strings.TrimSpace(e), "true")
}

func scalar(n *yaml.Node) string {
	if n.Kind != yaml.ScalarNode {
		return ""
	}
	return n.Value
}
