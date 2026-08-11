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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// This package is split across four CI matrix entries, each passing an exact-name
// -run allowlist. A test added here but to no list never runs, and the job
// still reports green — the failure mode this guard exists to make loud.
//
// Two ways a test still escapes both guards, left open deliberately. A test
// behind a build tag is invisible to the `go test -list` below, which runs
// without -tags; and a test in a SUBpackage of this one is invisible to
// run.sh's `go list "./$path"`, which does not recurse. Adding /... there
// would change every acceptance group's package set and the budgets derived
// from it, which is a bigger change than the escape is worth. Neither hatch is
// in use today; if one is ever needed, close it here first.
const runShPackagePath = "test/acceptance/reindex_backup"

var (
	aofGroupRunRe = regexp.MustCompile(`AOF_GROUP_RUN='([^']*)'`)
	// Captures whatever a group sets its budget to, not only the readable
	// shapes, so an unreadable one is rejected rather than skipped.
	aofGroupTimeoutRe        = regexp.MustCompile(`AOF_GROUP_TIMEOUT=([^\s\\]*)`)
	aofGroupTimeoutDefaultRe = regexp.MustCompile(`AOF_GROUP_TIMEOUT:-([^\s}]*)`)
	aofTestBudgetRe          = regexp.MustCompile(`AOF_TEST_BUDGET (Test[A-Za-z0-9_]+) ([0-9]+)m`)
	// Fuzz targets list alongside tests but no group's exact-name -run
	// alternation can select one, so they must be visible to the guards
	// rather than filtered out before they are counted.
	testNameRe      = regexp.MustCompile(`^(Test|Fuzz)[A-Za-z0-9_]*$`)
	runShFunctionRe = regexp.MustCompile(`^function (run_acceptance_[A-Za-z0-9_]+)\(\)`)
	runShFlagRe     = regexp.MustCompile(`^\s*(--[a-z0-9-]+)[|)]`)
	wholeMinutesRe  = regexp.MustCompile(`^([0-9]+)m$`)
	// Matches the package path only as a whole path segment, so a future
	// sibling such as test/acceptance/reindex_backup_mt is not read as this
	// package and does not silently widen what these guards claim to cover.
	runShPackageRe = regexp.MustCompile(regexp.QuoteMeta(runShPackagePath) + `(?:[^\w.-]|$)`)
)

// imageBuildAllowanceMinutes is the slice of the job window spent building the
// weaviate test image before go test starts. The go-test budget is what makes
// a hang panic with stacks, so it only does that if the runner has not killed
// the job first — which means the budget and the build have to share the
// window.
//
// An observed average, not an enforced cap: nothing measures the build, so a
// build that slowly grows past 5 minutes eats into the budget this guard
// believes is available. It fails as a runner-killed job, which reads as a
// hang without stacks.
const imageBuildAllowanceMinutes = 5

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

// allowlistedTests maps each allowlisted test name to the one group that runs
// it. Being in two groups is rejected, not merged: the second group runs the
// test again against a budget derived on the assumption it runs once, which is
// what moving a test between groups and leaving the old entry behind looks
// like.
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
	require.NotEmpty(t, allowed,
		"found no AOF_GROUP_RUN filter for %s in test/run.sh — either the group was renamed "+
			"or this guard's parsing broke; a silent pass here would defeat the guard", runShPackagePath)
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
			"Add each to an AOF_GROUP_RUN filter in test/run.sh (run_acceptance_reindex_backup_suite, "+
			"_a or _b for single-node, run_acceptance_reindex_backup_cluster for multi-node), "+
			"picking the group whose budget still covers its worst case.",
		runShPackagePath, strings.Join(missing, ", "))

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
		runShPackagePath, strings.Join(stale, ", "))

	var staleBudgets []string
	for name := range testWorstCases(t, string(runSh)) {
		if _, ok := declaredSet[name]; !ok {
			staleBudgets = append(staleBudgets, name)
		}
	}
	require.Emptyf(t, staleBudgets,
		"these AOF_TEST_BUDGET lines in run.sh name no test in %s, so a group budget is being "+
			"read against a worst case nothing spends: %s",
		runShPackagePath, strings.Join(staleBudgets, ", "))
}

// Pins the chain the guard above doesn't check: filter -> run.sh function ->
// dispatcher --flag -> a workflow matrix entry actually passing that flag. A
// matrix entry renamed, deleted, commented out, or put behind any if: that is
// not literally true takes the whole group out of PR CI while both the
// allowlist guard and the job stay green, so the last hop is read from parsed
// YAML, not the file's bytes.
func TestCIWorkflowInvokesEveryGroupThatRunsThisPackage(t *testing.T) {
	root := repoRoot(t)
	runShBytes, err := os.ReadFile(filepath.Join(root, "test", "run.sh"))
	require.NoError(t, err)
	runSh := string(runShBytes)

	groups := groupsRunningThisPackage(t, runSh)
	require.NotEmptyf(t, groups,
		"found no run_acceptance_* function holding an AOF_GROUP_RUN filter for %s; "+
			"either the groups were renamed or this guard's parsing broke", runShPackagePath)

	invocations := workflowRunShInvocations(t, root)

	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			requireDispatched(t, runSh, group.name)
			flag := flagArming(t, runSh, group.name)

			_, invoked := invocations[flag]
			require.Truef(t, invoked,
				"no pull-request-triggered workflow job in .github/workflows passes %q to test/run.sh, "+
					"so %s never runs in CI while the tests it owns still read as covered by the "+
					"run.sh allowlist guard",
				flag, group.name)
		})
	}
}

// TestCIGroupTimeoutFitsTheJobWindow checks each budget against the two numbers
// that bound it: the deadlines its own tests wait on, and the window the
// workflow gives the job. Go's -timeout is what turns a hang into a panic with
// stacks, and it produces that only if it fires before the runner kills the
// job and after the tests have had the time they ask for.
//
// The floor comes from hand-written AOF_TEST_BUDGET lines, not from the test
// source, so raising a helper.WithDeadline without editing that test's line
// moves the real floor above the budget and leaves this guard green. That rot
// is accepted: it costs CI triage, not correctness. go test still panics with
// stacks inside the job window; the failure just reads as a product hang
// rather than as a budget too small.
func TestCIGroupTimeoutFitsTheJobWindow(t *testing.T) {
	root := repoRoot(t)
	runShBytes, err := os.ReadFile(filepath.Join(root, "test", "run.sh"))
	require.NoError(t, err)
	runSh := string(runShBytes)

	groups := groupsRunningThisPackage(t, runSh)
	require.NotEmpty(t, groups, "found no run_acceptance_* function running "+runShPackagePath)

	jobWindows := workflowRunShTimeouts(t, root)
	worstCases := testWorstCases(t, runSh)

	// A budget the reader cannot understand used to fall through to
	// run_aof_group's default and have the group validated against that number
	// instead. These are the shapes go test accepts that this reader does not.
	t.Run("budgets not written in whole minutes are unreadable", func(t *testing.T) {
		tests := []struct {
			value string
			want  int
			ok    bool
		}{
			{value: "20m", want: 20, ok: true},
			{value: "0m", want: 0, ok: true},
			{value: "90s"},
			{value: "1h"},
			{value: "18m30s"},
			{value: "1h30m"},
			{value: "20"},
			{value: "m"},
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

	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			budget := groupTimeoutMinutes(t, runSh, group.name)
			flag := flagArming(t, runSh, group.name)

			window, found := jobWindows[flag]
			require.Truef(t, found,
				"no workflow step passing %q declares a timeout_minutes, so nothing bounds "+
					"the job %s runs in", flag, group.name)

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

// groupTimeoutMinutes reads the go-test budget a group sets, falling back to
// run_aof_group's own default when the group does not set one. A budget written
// in any other shape than whole minutes fails here rather than falling through
// to the default, which would have this guard validate the group against a
// number the group does not use.
func groupTimeoutMinutes(t *testing.T, runSh, group string) int {
	t.Helper()

	lines := strings.Split(runSh, "\n")
	var inGroup bool
	for _, line := range lines {
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

// workflowRunShTimeouts maps each run.sh flag a pull request can reach to the
// smallest timeout_minutes bounding a step that runs it.
func workflowRunShTimeouts(t *testing.T, root string) map[string]int {
	t.Helper()

	dir := filepath.Join(root, ".github", "workflows")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	windows := map[string]int{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "ml") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)

		var wf workflowFile
		require.NoError(t, yaml.Unmarshal(body, &wf))
		if !triggersOnPullRequest(&wf.On) {
			continue
		}
		for _, job := range wf.Jobs {
			if notProvenTrue(job.If) {
				continue
			}
			collectRunShTimeouts(t, job, windows)
		}
	}
	return windows
}

// collectRunShTimeouts records the window bounding every flag a job's run.sh
// steps can be handed, including the ones its matrix interpolates in.
func collectRunShTimeouts(t *testing.T, job workflowJob, windows map[string]int) {
	t.Helper()

	var flags []string
	for _, entry := range job.Strategy.Matrix.Include {
		for _, v := range entry {
			for _, w := range strings.Fields(scalar(&v)) {
				if strings.HasPrefix(w, "--") {
					flags = append(flags, w)
				}
			}
		}
	}

	for _, step := range job.Steps {
		if notProvenTrue(step.If) {
			continue
		}
		var runsRunSh bool
		// Cloned: appending to the matrix flags in place would leak this step's
		// flags into every later step's list.
		stepFlags := append([]string(nil), flags...)
		for _, v := range step.With {
			for _, w := range strings.Fields(scalar(&v)) {
				if strings.Contains(w, "run.sh") {
					runsRunSh = true
				}
				if strings.HasPrefix(w, "--") {
					stepFlags = append(stepFlags, w)
				}
			}
		}
		for _, w := range strings.Fields(step.Run) {
			if strings.Contains(w, "run.sh") {
				runsRunSh = true
			}
			if strings.HasPrefix(w, "--") {
				stepFlags = append(stepFlags, w)
			}
		}
		if !runsRunSh {
			continue
		}

		raw, ok := step.With["timeout_minutes"]
		if !ok {
			continue
		}
		// A timeout_minutes written as a `${{ }}` expression is a window this
		// guard cannot read. Recording nothing makes the group it covers fail
		// as "no window found" rather than pass on an unread number.
		minutes, err := strconv.Atoi(scalar(&raw))
		if err != nil {
			continue
		}
		for _, flag := range stepFlags {
			if existing, seen := windows[flag]; !seen || minutes < existing {
				windows[flag] = minutes
			}
		}
	}
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

// workflowRunShInvocations returns every test/run.sh flag a pull request can
// actually reach: the flag has to be a word in a live step's command or in a
// matrix entry feeding a job whose steps call run.sh.
func workflowRunShInvocations(t *testing.T, root string) map[string]struct{} {
	t.Helper()

	dir := filepath.Join(root, ".github", "workflows")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	flags := map[string]struct{}{}
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
			collectRunShFlags(job, flags)
		}
	}
	require.NotZero(t, parsed, "no workflow files found under %s", dir)
	return flags
}

// collectRunShFlags adds this job's run.sh flags, from the step commands
// themselves and from the matrix entries those commands interpolate.
func collectRunShFlags(job workflowJob, flags map[string]struct{}) {
	var invokesRunSh bool
	var stepWords []string
	for _, step := range job.Steps {
		if notProvenTrue(step.If) {
			continue
		}
		commands := []string{step.Run}
		for _, v := range step.With {
			commands = append(commands, scalar(&v))
		}
		for _, c := range commands {
			if !strings.Contains(c, "run.sh") {
				continue
			}
			invokesRunSh = true
			stepWords = append(stepWords, strings.Fields(c)...)
		}
	}
	if !invokesRunSh {
		return
	}

	for _, w := range stepWords {
		if strings.HasPrefix(w, "--") {
			flags[w] = struct{}{}
		}
	}
	for _, entry := range job.Strategy.Matrix.Include {
		for _, v := range entry {
			for _, w := range strings.Fields(scalar(&v)) {
				if strings.HasPrefix(w, "--") {
					flags[w] = struct{}{}
				}
			}
		}
	}
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

// notProvenTrue reports whether an if: might not hold on a pull request, which
// is how a job or step is kept in the file while being taken out of PR CI.
// Anything other than an absent or literally-true condition counts, because a
// condition like github.event_name == 'schedule' takes the group out of every
// PR run just as effectively as a literal false.
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

// packageGroup is one run_acceptance_* function running this package, with the
// exact test names its AOF_GROUP_RUN filter selects.
type packageGroup struct {
	name  string
	tests []string
}

// groupsRunningThisPackage returns, in run.sh order, the run_acceptance_*
// functions that carry an AOF_GROUP_RUN filter for this package.
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
		if !runShPackageRe.MatchString(scope) {
			continue
		}
		require.NotEmptyf(t, current,
			"an AOF_GROUP_RUN filter for %s sits outside any run_acceptance_* function "+
				"(test/run.sh line %d); this guard cannot trace it to a CI flag",
			runShPackagePath, i+1)
		idx, seen := at[current]
		if !seen {
			idx = len(groups)
			at[current] = idx
			groups = append(groups, packageGroup{name: current})
		}
		groups[idx].tests = append(groups[idx].tests, parseExactNameAlternation(t, m[1])...)
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

// TestCIPackagePathMatchesWholeSegments pins that the guards above find their
// own package and only their own. A substring match would read a future
// sibling package as this one and claim to cover tests it never sees.
func TestCIPackagePathMatchesWholeSegments(t *testing.T) {
	tests := []struct {
		scope string
		want  bool
	}{
		{scope: `run_aof_group "reindex-backup-a" test/acceptance/reindex_backup`, want: true},
		{scope: `run_aof_group "x" test/acceptance/reindex_backup someArg`, want: true},
		{scope: `go list ./test/acceptance/reindex_backup/...`, want: true},
		{scope: `run_aof_group "x" test/acceptance/reindex_backup_mt`},
		{scope: `run_aof_group "x" test/acceptance/reindex_backup-legacy`},
		{scope: `run_aof_group "x" test/acceptance/reindex_mt`},
	}

	for _, tc := range tests {
		t.Run(tc.scope, func(t *testing.T) {
			require.Equal(t, tc.want, runShPackageRe.MatchString(tc.scope))
		})
	}
}
