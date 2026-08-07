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
	"gopkg.in/yaml.v3"
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
// The last hop is read out of the parsed YAML, not out of the file's bytes. A
// commented-out matrix entry is still a literal substring of the workflow, and
// so are a flag in a workflow no pull request triggers and one in a job behind
// an always-false if: — three ways for the entry to be present and dead.
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
