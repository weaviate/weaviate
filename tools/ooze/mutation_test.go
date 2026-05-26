//go:build mutation

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

// Reusable ooze mutation-testing harness. It mutates a single target package
// (passed via the OOZE_TARGET environment variable) and reports any surviving
// mutants — source changes the target's tests failed to detect.
//
// Always drive it through tools/ooze_mutation.sh, which sets the environment
// and supplies the `-mod=mod` flag required because Weaviate vendors its
// dependencies (vendor/ is git-ignored) while ooze is a tag-gated dev tool that
// is not vendored.
//
// Supported environment variables:
//
//	OOZE_REPO_ROOT     absolute path to the repo root (default: ../.. from here)
//	OOZE_TARGET        package dir to mutate, relative to the root, e.g.
//	                   "usecases/byteops" (required; the test is skipped if unset)
//	OOZE_TEST_CMD      test command run against each mutant
//	                   (default: "go test -count=1 -timeout 60s ./<target>/...")
//	OOZE_THRESHOLD     minimum mutation score, 0.0-1.0 (default: 0.0)
//	OOZE_IGNORE_EXTRA  extra regexp OR'd into the ignore filter, to skip files
//	                   that ooze cannot handle (e.g. generic type-union
//	                   signatures crash ooze v0.2.0's formatter) or build-tagged
//	                   files inactive on the current architecture
package oozemutation

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/gtramontina/ooze"
)

func TestMutation(t *testing.T) {
	target := strings.Trim(strings.TrimPrefix(os.Getenv("OOZE_TARGET"), "./"), "/")
	if target == "" {
		t.Skip("OOZE_TARGET not set; run via tools/ooze_mutation.sh <package-dir>")
	}

	root := os.Getenv("OOZE_REPO_ROOT")
	if root == "" {
		root = "../.." // this package lives at <root>/tools/ooze
	}

	testCmd := os.Getenv("OOZE_TEST_CMD")
	if testCmd == "" {
		testCmd = "go test -count=1 -timeout 60s ./" + target + "/..."
	}

	threshold := float32(0.0)
	if v := os.Getenv("OOZE_THRESHOLD"); v != "" {
		parsed, err := strconv.ParseFloat(v, 32)
		if err != nil {
			t.Fatalf("invalid OOZE_THRESHOLD %q: %v", v, err)
		}
		threshold = float32(parsed)
	}

	ignore := ignoreAllExcept(target, os.Getenv("OOZE_IGNORE_EXTRA"))
	t.Logf("ooze: target=%s root=%s ignore=%s", target, root, ignore)

	ooze.Release(
		t,
		ooze.WithRepositoryRoot(root),
		ooze.WithTestCommand(testCmd),
		ooze.IgnoreSourceFiles(ignore),
		ooze.Parallel(),
		ooze.WithMinimumThreshold(threshold),
	)
}

// ignoreAllExcept builds a regexp (matched, unanchored, against each source
// file's path relative to the repo root) that ooze uses to EXCLUDE files from
// mutation. We want the inverse — mutate only files under target — but ooze
// offers no include filter and Go's RE2 engine has no negative lookahead. So we
// construct the classic "does not start with prefix" expression for the prefix
// "<target>/": at every position the path may end (too short) or hold a byte
// other than the one the prefix requires; a path that matches the full prefix
// falls through and is therefore mutated.
//
// extra, if non-empty, is OR'd in so callers can additionally exclude files
// ooze chokes on (see OOZE_IGNORE_EXTRA).
func ignoreAllExcept(target, extra string) string {
	prefix := strings.TrimSuffix(target, "/") + "/"
	pattern := "(?:^" + notPrefix(prefix) + ")"
	if extra != "" {
		pattern += "|(?:" + extra + ")"
	}
	return pattern
}

// notPrefix returns a sub-expression (no leading anchor) matching any string
// that does not begin with p. Anchor it with a leading "^" at the call site.
func notPrefix(p string) string {
	c := p[0]
	if len(p) == 1 {
		return "(?:$|[^" + escapeInClass(c) + "])"
	}
	return "(?:$|[^" + escapeInClass(c) + "]|" + regexp.QuoteMeta(string(c)) + notPrefix(p[1:]) + ")"
}

// escapeInClass escapes the bytes that are special inside a [...] character class.
func escapeInClass(b byte) string {
	switch b {
	case '\\', ']', '^', '-':
		return `\` + string(b)
	default:
		return string(b)
	}
}
