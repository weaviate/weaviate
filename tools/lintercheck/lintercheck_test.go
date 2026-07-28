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

// Package lintercheck holds positive controls for the repo's custom linters:
// each one exits 0 silently on no match, so these tests assert it still fires
// on known-bad input and still passes known-good input.
package lintercheck

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestTreeWalkingLintersFireOnKnownViolations(t *testing.T) {
	root := repoRoot(t)

	cases := []struct {
		name           string
		script         string
		violationClass string
		badFixture     string
		badPath        string
		cleanFixture   string
		cleanPath      string
		wantOutput     string
	}{
		{
			name:           "error groups",
			script:         "tools/linter_error_groups.sh",
			violationClass: "direct errgroup use outside entities/errors/error_group_wrapper.go",
			badFixture:     "errgroup_violation.go.txt",
			// This linter drops every path containing "test", so its fixture
			// cannot be staged under a testdata/ directory.
			badPath:      "pkg/errgroup_violation.go",
			cleanFixture: "errgroup_clean.go.txt",
			cleanPath:    "entities/errors/error_group_wrapper.go",
			wantOutput:   "directly uses error groups",
		},
		{
			name:           "goroutines",
			script:         "tools/linter_go_routines.sh",
			violationClass: "bare go statements instead of the entities/errors wrapper",
			badFixture:     "goroutine_violation.go.txt",
			badPath:        "pkg/goroutine_violation.go",
			cleanFixture:   "goroutine_clean.go.txt",
			cleanPath:      "pkg/goroutine_clean.go",
			wantOutput:     "uses direct goroutines",
		},
		{
			name:           "waitgroups",
			script:         "tools/linter_waitgroups_done.sh",
			violationClass: "wg.Done() that is not deferred",
			badFixture:     "waitgroup_violation.go.txt",
			// This linter skips tools/ and test/ wholesale.
			badPath:      "pkg/waitgroup_violation.go",
			cleanFixture: "waitgroup_clean.go.txt",
			cleanPath:    "pkg/waitgroup_clean.go",
			// Diagnostic is file:line; asserting just the filename avoids breaking
			// when the fixture's line numbers shift.
			wantOutput: "waitgroup_violation.go:",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := filepath.Join(root, filepath.FromSlash(tc.script))

			code, out := runLinter(t, stageFixture(t, tc.badFixture, tc.badPath), script)
			if code == 0 {
				t.Fatalf("POSITIVE CONTROL FAILED: %s exited 0 on known-bad fixture "+
					"tools/lintercheck/testdata/%s (staged as %s).\n"+
					"It no longer detects %s, so CI is blind to that violation class.\n"+
					"Linter output:\n%s", tc.script, tc.badFixture, tc.badPath, tc.violationClass, out)
			}
			if !strings.Contains(out, tc.wantOutput) {
				t.Fatalf("POSITIVE CONTROL FAILED: %s exited %d on known-bad fixture "+
					"tools/lintercheck/testdata/%s (staged as %s) but never mentioned %q.\n"+
					"Either its diagnostic changed, or it failed for an unrelated reason and "+
					"is no longer detecting %s.\nLinter output:\n%s",
					tc.script, code, tc.badFixture, tc.badPath, tc.wantOutput, tc.violationClass, out)
			}

			// Without this a linter hardwired to fail would satisfy the check above.
			if code, out := runLinter(t, stageFixture(t, tc.cleanFixture, tc.cleanPath), script); code != 0 {
				t.Fatalf("POSITIVE CONTROL FAILED: %s exited %d on known-good fixture "+
					"tools/lintercheck/testdata/%s (staged as %s).\n"+
					"It rejects clean input, so its failure on the known-bad fixture proves nothing.\n"+
					"Linter output:\n%s", tc.script, code, tc.cleanFixture, tc.cleanPath, out)
			}
		})
	}
}

func TestHiddenUnicodeLinterFiresOnKnownViolation(t *testing.T) {
	// Shell, not Go, so the security-lint composite action can also run it for
	// client repos that pin it by SHA and have no Go toolchain.
	script := filepath.Join(repoRoot(t), ".github", "actions", "security-lint", "selfcheck.sh")
	if code, out := runLinter(t, t.TempDir(), script); code != 0 {
		t.Fatalf("POSITIVE CONTROL FAILED: %s exited %d.\n%s", script, code, out)
	}
}

// stageFixture stages a fixture in a throwaway git repo. The linters run
// `git ls-files` on whatever repo they're invoked from, so testdata/ fixtures
// would otherwise also trip the real lint pass over this repo.
func stageFixture(t *testing.T, fixture, stagedPath string) string {
	t.Helper()

	content, err := os.ReadFile(filepath.Join("testdata", fixture))
	if err != nil {
		t.Fatalf("read fixture %s: %v", fixture, err)
	}

	dir := t.TempDir()
	dst := filepath.Join(dir, filepath.FromSlash(stagedPath))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("create fixture dir for %s: %v", stagedPath, err)
	}
	if err := os.WriteFile(dst, content, 0o644); err != nil {
		t.Fatalf("write fixture %s: %v", stagedPath, err)
	}

	runGit(t, dir, "init", "-q")
	// -f because a developer's global gitignore could otherwise keep the fixture
	// out of `git ls-files` and silently make this control vacuous.
	runGit(t, dir, "add", "-f", stagedPath)
	return dir
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = hermeticEnv()
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s in %s: %v\n%s", strings.Join(args, " "), dir, err, out)
	}
}

func runLinter(t *testing.T, dir, script string) (int, string) {
	t.Helper()
	cmd := exec.Command("bash", script)
	cmd.Dir = dir
	cmd.Env = hermeticEnv()
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Fatalf("run %s: %v\n%s", script, err, out)
	}
	return cmd.ProcessState.ExitCode(), string(out)
}

// hermeticEnv drops the developer's and runner's git config so `git ls-files`
// behaves the same in the throwaway repo on a laptop as it does in CI.
func hermeticEnv() []string {
	return append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
}

// repoRoot resolves from this file's own path rather than the working
// directory, which the subtests hand to the linters.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve this test file's path")
	}
	root := filepath.Join(filepath.Dir(thisFile), "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("resolved repo root %s has no go.mod; did this package move? %v", root, err)
	}
	return root
}
