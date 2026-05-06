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

// Package usagelimits enforces server-side usage limits (objects, collections,
// tenants, shards) configured via env vars and runtime overrides. Today only
// the "node" scope is implemented; "cluster" and "namespace" are reserved for
// future cluster-wide and per-namespace enforcement.
package usagelimits

import "fmt"

// Scope declares the unit of accounting for every MAXIMUM_ALLOWED_* limit.
// Only ScopeNode is implemented today. ScopeCluster and ScopeNamespace are
// reserved as forward-compatible extension points and rejected at startup.
type Scope string

const (
	ScopeNode      Scope = "node"
	ScopeCluster   Scope = "cluster"
	ScopeNamespace Scope = "namespace"
)

// Validate returns nil only for ScopeNode (or the empty default which means
// "node"). ScopeCluster and ScopeNamespace are reserved values; the server
// must refuse to boot when configured with them so an operator can't
// accidentally rely on a contract Weaviate doesn't yet honor.
func (s Scope) Validate() error {
	switch s {
	case "", ScopeNode:
		return nil
	case ScopeCluster, ScopeNamespace:
		return fmt.Errorf("USAGE_LIMITS_SCOPE=%s is not yet supported", s)
	default:
		return fmt.Errorf("USAGE_LIMITS_SCOPE=%q is not a recognized value (expected: node|cluster|namespace)", string(s))
	}
}

// ValidateScopeString is a thin wrapper for callers that hold a plain string
// (e.g. the runtime config layer) rather than a Scope value.
func ValidateScopeString(s string) error {
	return Scope(s).Validate()
}
