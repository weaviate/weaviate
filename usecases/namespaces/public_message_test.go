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

package namespaces

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lifecycleSentinels are the sentinels a caller outside the
// namespace-management API can surface, so PublicMessage must neutralize
// each one.
var lifecycleSentinels = map[string]error{
	"ErrNamespaceSuspended":     ErrNamespaceSuspended,
	"ErrCollectionSuspended":    ErrCollectionSuspended,
	"ErrNamespaceResuming":      ErrNamespaceResuming,
	"ErrNamespaceGone":          ErrNamespaceGone,
	"ErrNamespaceDeleting":      ErrNamespaceDeleting,
	"ErrInvalidState":           ErrInvalidState,
	"ErrNamespaceNotEmpty":      ErrNamespaceNotEmpty,
	"ErrInvalidStateTransition": ErrInvalidStateTransition,
	"ErrNotFound":               ErrNotFound,
}

// managementOnlySentinels never reach a caller without manage_namespaces, so
// PublicMessage leaves them alone. Listing one here asserts that no
// user-facing boundary can render it.
var managementOnlySentinels = map[string]error{
	"ErrBadRequest":    ErrBadRequest,    // names nothing, and the detail is the point
	"ErrAlreadyExists": ErrAlreadyExists, // only the namespace-creation apply path produces it
	// Only a caller sending a nonzero expected index can trigger it, and the
	// operator needs the moved index to decide whether to retry.
	"ErrStateChangedConcurrently": ErrStateChangedConcurrently,
}

// errorConstructors are the calls that build a sentinel. A var assigned one
// of these must be named Err... so declaredSentinelNames can find it.
var errorConstructors = map[string]struct{}{"New": {}, "Errorf": {}, "Join": {}}

// declaredSentinelNames parses the package source for top-level Err... vars.
// Read from source because Go exposes no registry of package-level vars, and
// a hand-written list would pass while missing the sentinel it was meant to
// catch. Any value shape counts: matching only errors.New would miss a
// sentinel built with fmt.Errorf.
func declaredSentinelNames(t *testing.T) []string {
	t.Helper()
	sources, err := filepath.Glob("*.go")
	require.NoError(t, err)

	var names []string
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), source, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if strings.HasPrefix(name.Name, "Err") {
						names = append(names, name.Name)
						continue
					}
					if i >= len(vs.Values) {
						continue
					}
					call, ok := vs.Values[i].(*ast.CallExpr)
					if !ok {
						continue
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					if _, isCtor := errorConstructors[sel.Sel.Name]; isCtor {
						t.Errorf("%s: sentinel %s must be named Err... so the placement guard finds it",
							source, name.Name)
					}
				}
			}
		}
	}
	require.NotEmpty(t, names, "parsed no sentinels: the parser stopped matching how they are declared")
	return names
}

func TestPublicMessage(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantMsg string
		wantOK  bool
	}{
		{name: "suspended", err: ErrNamespaceSuspended, wantMsg: "instance suspended", wantOK: true},
		{name: "collection suspended", err: ErrCollectionSuspended, wantMsg: "instance suspended", wantOK: true},
		{name: "resuming", err: ErrNamespaceResuming, wantMsg: "instance resuming, retry shortly", wantOK: true},
		{name: "gone", err: ErrNamespaceGone, wantMsg: "instance unavailable", wantOK: true},
		{name: "deleting", err: ErrNamespaceDeleting, wantMsg: "instance unavailable", wantOK: true},
		{name: "invalid state", err: ErrInvalidState, wantMsg: "instance unavailable", wantOK: true},
		{name: "not empty", err: ErrNamespaceNotEmpty, wantMsg: "instance unavailable", wantOK: true},
		{name: "invalid transition", err: ErrInvalidStateTransition, wantMsg: "instance unavailable", wantOK: true},
		{name: "not found", err: ErrNotFound, wantMsg: "instance unavailable", wantOK: true},
		// A wrapped sentinel must still be recognized: the apply path wraps
		// the namespace name onto the error.
		{name: "wrapped sentinel is recognized", err: fmt.Errorf("%w: %q", ErrNamespaceSuspended, "customer1"), wantMsg: "instance suspended", wantOK: true},
		{name: "nil is not a sentinel", err: nil},
		{name: "unrecognized error keeps its detail", err: errors.New("disk on fire")},
		{name: "management-only bad request", err: ErrBadRequest},
		{name: "management-only already exists", err: ErrAlreadyExists},
		{name: "management-only state changed concurrently", err: ErrStateChangedConcurrently},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg, ok := PublicMessage(tc.err)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantMsg, msg)
		})
	}
}

func TestPublicMessage_NeverNamesTheConcept(t *testing.T) {
	for name, err := range lifecycleSentinels {
		msg, ok := PublicMessage(err)
		require.True(t, ok, name)
		for _, word := range []string{"namespace", "collection"} {
			assert.NotContains(t, strings.ToLower(msg), word, name)
		}
	}
}

// TestPublicMessage_EverySentinelIsClassified fails when a sentinel is added
// to the package without deciding whether a user can see it. Without this, a
// forgotten sentinel reaches PublicMessage's default arm and its "namespace
// ..." text renders to the user.
func TestPublicMessage_EverySentinelIsClassified(t *testing.T) {
	for name, err := range managementOnlySentinels {
		_, ok := PublicMessage(err)
		assert.False(t, ok, "%s is listed management-only but PublicMessage neutralizes it", name)
	}

	for _, name := range declaredSentinelNames(t) {
		_, lifecycle := lifecycleSentinels[name]
		_, management := managementOnlySentinels[name]
		assert.True(t, lifecycle != management,
			"sentinel %s must appear in exactly one of the two lists in public_message_test.go: "+
				"decide whether a user can see it, then place it in PublicMessage or exempt it", name)
	}
}
