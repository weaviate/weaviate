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

package config

// Env-driven feature flags.
//
// This file holds env-var-backed feature flags that don't fit the main
// Config struct — preview / experimental gates, low-frequency operator
// escape hatches, or any flag whose lifetime is short enough that struct
// plumbing is more churn than value.
//
// Helpers read their env var on every call (no in-process cache) so
// tests can override per-case with t.Setenv. Go's testing package
// auto-restores via cleanup and panics if the override is attempted in
// a parallel test or one with a parallel ancestor, giving these flags
// automatic isolation against future t.Parallel() adoption.

import (
	"fmt"
	"os"
)

// EnvNestedFilteringPreview is the env var that toggles the preview gate
// for nested property indexing and filtering. Exported so callers can
// reference it from user-facing error messages without duplicating the
// literal string.
const EnvNestedFilteringPreview = "WEAVIATE_PREVIEW_NESTED_FILTERING"

// NestedFilteringEnabled reports whether the preview gate for nested
// property indexing and filtering is on for this server.
//
// Preview feature. Off by default. Toggle via EnvNestedFilteringPreview;
// truthy values are the ones recognised by Enabled ("on", "enabled",
// "1", "true").
//
// Reads the env var on each call (no cache) so tests can override via
// t.Setenv — Go's testing package serializes env mutations and panics
// if a parallel test or a test with a parallel ancestor attempts the
// override, giving us automatic isolation against future t.Parallel()
// adoption in nested test packages. The cost is one map lookup +
// boolean parse per call; the call sites are request-boundary
// dispatches (validator / searcher / analyzer / bucket-creation), not
// hot loops.
//
// Removed at GA — at that point the feature is unconditionally enabled
// and call sites lose the gate. Do not depend on this function from
// long-lived code paths.
func NestedFilteringEnabled() bool {
	return Enabled(os.Getenv(EnvNestedFilteringPreview))
}

// NestedFilteringDisabledError returns the standard user-facing error
// for a nested filter request rejected because the preview gate is off.
// Centralises the message + env-var name so callers don't drift.
func NestedFilteringDisabledError() error {
	return fmt.Errorf("filtering on nested properties is a preview feature, disabled by default; set %s=true to enable", EnvNestedFilteringPreview)
}
