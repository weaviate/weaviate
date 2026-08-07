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

// This package is split across two CI matrix entries, each passing an exact-name
// -run allowlist. A test added here but to neither list never runs, and the job
// still reports green — the failure mode this guard exists to make loud.
const runShPackagePath = "test/acceptance/reindex_backup"

var (
	aofGroupRunRe     = regexp.MustCompile(`AOF_GROUP_RUN='([^']*)'`)
	aofGroupTimeoutRe = regexp.MustCompile(`AOF_GROUP_TIMEOUT=([0-9]+)m`)
	testNameRe        = regexp.MustCompile(`^Test[A-Za-z0-9_]*$`)
	runShFunctionRe   = regexp.MustCompile(`^function (run_acceptance_[A-Za-z0-9_]+)\(\)`)
	runShFlagRe       = regexp.MustCompile(`^\s*(--[a-z0-9-]+)[|)]`)
)

// imageBuildAllowanceMinutes is the slice of the job window spent building the
// weaviate test image before go test starts. The go-test budget is what makes
// a hang panic with stacks, so it only does that if the runner has not killed
// the job first — which means the budget and the build have to share the
// window.
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

// Pins the chain the guard above doesn't check: filter -> run.sh function ->
// dispatcher --flag -> a workflow matrix entry actually passing that flag. A
// matrix entry renamed/deleted/commented-out/dead-behind-an-if takes the
// whole group out of CI while both the allowlist guard and the job stay
// green, so the last hop is read from parsed YAML, not the file's bytes.
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
		t.Run(group, func(t *testing.T) {
			requireDispatched(t, runSh, group)
			flag := flagArming(t, runSh, group)

			_, invoked := invocations[flag]
			require.Truef(t, invoked,
				"no pull-request-triggered workflow job in .github/workflows passes %q to test/run.sh, "+
					"so %s never runs in CI while the tests it owns still read as covered by the "+
					"run.sh allowlist guard",
				flag, group)
		})
	}
}

// TestCIGroupTimeoutFitsTheJobWindow pins the budget against the window it has
// to fit in. Go's -timeout is what turns a hang into a panic with stacks; the
// runner killing the job first gets none of that, so raising one of the two
// numbers without the other silently trades the stacks away.
func TestCIGroupTimeoutFitsTheJobWindow(t *testing.T) {
	root := repoRoot(t)
	runShBytes, err := os.ReadFile(filepath.Join(root, "test", "run.sh"))
	require.NoError(t, err)
	runSh := string(runShBytes)

	groups := groupsRunningThisPackage(t, runSh)
	require.NotEmpty(t, groups, "found no run_acceptance_* function running "+runShPackagePath)

	jobWindows := workflowRunShTimeouts(t, root)

	for _, group := range groups {
		t.Run(group, func(t *testing.T) {
			budget := groupTimeoutMinutes(t, runSh, group)
			flag := flagArming(t, runSh, group)

			window, found := jobWindows[flag]
			require.Truef(t, found,
				"no workflow step passing %q declares a timeout_minutes, so nothing bounds "+
					"the job %s runs in", flag, group)

			require.LessOrEqualf(t, budget+imageBuildAllowanceMinutes, window,
				"%s gets a %dm go-test budget and the image build takes about %dm, which "+
					"together exceed the %dm the workflow step passing %q allows. The runner "+
					"kills the job before go test can panic with stacks. Lower the budget, "+
					"raise timeout_minutes, or split the group.",
				group, budget, imageBuildAllowanceMinutes, window, flag)
		})
	}
}

// groupTimeoutMinutes reads the go-test budget a group sets, falling back to
// run_aof_group's own default when the group does not set one.
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
			minutes, err := strconv.Atoi(m[1])
			require.NoError(t, err)
			return minutes
		}
	}

	m := regexp.MustCompile(`AOF_GROUP_TIMEOUT:-([0-9]+)m`).FindStringSubmatch(runSh)
	require.NotNilf(t, m, "%s sets no AOF_GROUP_TIMEOUT and run_aof_group's default "+
		"is not in the shape this guard reads", group)
	minutes, err := strconv.Atoi(m[1])
	require.NoError(t, err)
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
			if alwaysFalse(job.If) {
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
		if alwaysFalse(step.If) {
			continue
		}
		var runsRunSh bool
		stepFlags := flags
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
			if alwaysFalse(job.If) {
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
		if alwaysFalse(step.If) {
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

// alwaysFalse reports whether an if: can never hold, which is how a job or step
// is kept in the file while being taken out of CI.
func alwaysFalse(expr string) bool {
	e := strings.TrimSpace(expr)
	e = strings.TrimSuffix(strings.TrimPrefix(e, "${{"), "}}")
	return strings.EqualFold(strings.TrimSpace(e), "false")
}

func scalar(n *yaml.Node) string {
	if n.Kind != yaml.ScalarNode {
		return ""
	}
	return n.Value
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
