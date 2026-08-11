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

// Package ciguard holds guards for the reindex_backup CI shard chain that run
// in the plain unit-test job, outside every acceptance shard — so disabling a
// matrix entry can't silently take out the guard system with it.
package ciguard

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const guardedPackagePath = "test/acceptance/reindex_backup"

var (
	aofGroupRunRe   = regexp.MustCompile(`AOF_GROUP_RUN='([^']*)'`)
	runShFunctionRe = regexp.MustCompile(`^function (run_acceptance_[A-Za-z0-9_]+)\(\)`)
	runShFlagRe     = regexp.MustCompile(`^\s*(--[a-z0-9-]+)[|)]`)
	guardTestNameRe = regexp.MustCompile(`^func (TestCI[A-Za-z0-9_]*)\(`)
)

// TestReindexBackupCIGuardsSurviveAnyShardEdit fails if any hop in the chain
// that runs the reindex_backup CI guards on a pull request breaks: a guard
// test disappears, drops out of a run.sh allowlist, loses its dispatcher, or
// loses its workflow matrix entry.
func TestReindexBackupCIGuardsSurviveAnyShardEdit(t *testing.T) {
	root := repoRoot(t)
	runShBytes, err := os.ReadFile(filepath.Join(root, "test", "run.sh"))
	require.NoError(t, err)
	runSh := string(runShBytes)

	groups := groupsRunningGuardedPackage(t, runSh)
	require.NotEmptyf(t, groups,
		"test/run.sh holds no AOF_GROUP_RUN filter for %s; the whole shard split is gone "+
			"or this guard's parsing broke", guardedPackagePath)

	guardNames := declaredGuardTests(t, root)
	require.NotEmptyf(t, guardNames,
		"%s declares no TestCI* guard tests; the allowlist-guard system was removed", guardedPackagePath)

	allowlisted := map[string]string{}
	for group, names := range groups {
		for _, name := range names {
			allowlisted[name] = group
		}
	}
	for _, guard := range guardNames {
		require.Containsf(t, allowlisted, guard,
			"%s is in no AOF_GROUP_RUN filter in test/run.sh, so CI never runs it while staying green",
			guard)
	}

	prFlags := workflowRunShFlags(t, root)
	for group := range groups {
		requireDispatched(t, runSh, group)
		flag := flagArming(t, runSh, group)
		require.Containsf(t, prFlags, flag,
			"no pull-request-triggered workflow job in .github/workflows passes %q to test/run.sh, "+
				"so %s (and every test it owns) silently left PR CI", flag, group)
	}
}

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

// declaredGuardTests reads the guarded package's sources for top-level TestCI*
// functions. Source scan, not `go test -list`, so it still answers after the
// package stops compiling in this environment — and still goes red if the
// guard files themselves are deleted.
func declaredGuardTests(t *testing.T, root string) []string {
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
			if m := guardTestNameRe.FindStringSubmatch(line); m != nil {
				names = append(names, m[1])
			}
		}
	}
	return names
}

// groupsRunningGuardedPackage maps each run_acceptance_* function carrying an
// AOF_GROUP_RUN filter for the guarded package to the exact test names its
// filter selects. Unanchored or non-alternation patterns are passed through as
// opaque names; the acceptance-side guard rejects those shapes, this layer
// only needs the guard names to be findable.
func groupsRunningGuardedPackage(t *testing.T, runSh string) map[string][]string {
	t.Helper()

	lines := strings.Split(runSh, "\n")
	groups := map[string][]string{}
	var current string
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
		if !strings.Contains(scope, guardedPackagePath) {
			continue
		}
		require.NotEmptyf(t, current,
			"an AOF_GROUP_RUN filter for %s sits outside any run_acceptance_* function "+
				"(test/run.sh line %d)", guardedPackagePath, i+1)
		body := strings.TrimSuffix(strings.TrimPrefix(m[1], "^"), "$")
		if strings.HasPrefix(body, "(") && strings.HasSuffix(body, ")") {
			body = strings.TrimSuffix(strings.TrimPrefix(body, "("), ")")
		}
		groups[current] = append(groups[current], strings.Split(body, "|")...)
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

// workflowRunShFlags returns every test/run.sh flag a pull request can
// actually reach. Read from parsed YAML, not the file's bytes, so a matrix
// entry that was commented out or moved behind a non-true if: no longer
// counts as invoked.
func workflowRunShFlags(t *testing.T, root string) map[string]struct{} {
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

// notProvenTrue reports whether an if: might not hold on a pull request; a
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
