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

// Package namespacing provides the syntax-only helpers used to qualify
// collection and alias names with their owning namespace. The helpers are
// pure functions: no I/O, no validation beyond a length check on the
// pre-qualification short name.
package namespacing

import (
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// ErrCreateRequiresNamespace is returned by QualifyForCreate when a global
// (or anonymous) principal attempts a create on an NS-enabled cluster.
// Call sites translate this into authzerrors.NewNamespaceForbidden — the namespacing
// package stays free of auth vocabulary.
var ErrCreateRequiresNamespace = errors.New("create requires a namespaced principal on a namespaces-enabled cluster")

// ShortNameMaxLength caps the raw (pre-qualification) name length for
// namespaced principals. The cap is computed from the maximum namespace
// length and the maximum class name length so it stays constant regardless
// of which namespace the caller is bound to — a `customer` user and a `c` user
// get the same limit.
const ShortNameMaxLength = schema.ClassNameMaxLength - schema.NamespaceMaxLength - len(schema.NamespaceSeparator)

// QualifyForCreate is the create-path entry point: it enforces the NS-enabled
// policy, length-checks the raw short name, and returns the qualified form.
// On NS-enabled clusters, callers without a namespace are rejected with
// ErrCreateRequiresNamespace; the call site is responsible for translating
// that into a 403 with the appropriate verb and resources.
func QualifyForCreate(principal *models.Principal, nsEnabled bool, raw string) (string, error) {
	if !nsEnabled {
		return raw, nil
	}
	if principal == nil || principal.Namespace == "" {
		return "", ErrCreateRequiresNamespace
	}
	if len(raw) > ShortNameMaxLength {
		return "", fmt.Errorf("'%s' is too long: namespaced names must be at most %d characters before qualification", raw, ShortNameMaxLength)
	}
	return qualify(principal, raw), nil
}
