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

package errors

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

// Forbidden indicates a failed authorization or namespace requirement.
type Forbidden struct {
	principal   *models.Principal
	verb        string
	resources   []string
	isNamespace bool // true for namespace-required errors
}

type Unauthenticated struct{}

func (u Unauthenticated) Error() string {
	return "user is not authenticated"
}

// NewUnauthenticated creates an explicit Unauthenticated error
func NewUnauthenticated() Unauthenticated {
	return Unauthenticated{}
}

// NewForbidden creates an explicit Forbidden error for RBAC failures with details about the
// principal and the attempted access on a specific resource.
func NewForbidden(principal *models.Principal, verb string, resources ...string) Forbidden {
	return Forbidden{
		principal:   principal,
		verb:        verb,
		resources:   resources,
		isNamespace: false,
	}
}

// NewNamespaceForbidden creates a Forbidden error for namespace requirement failures.
func NewNamespaceForbidden(principal *models.Principal) Forbidden {
	return Forbidden{
		principal:   principal,
		verb:        "",
		resources:   nil,
		isNamespace: true,
	}
}

func (f Forbidden) Error() string {
	if f.isNamespace {
		username := "unknown"
		if f.principal != nil {
			username = f.principal.Username
		}
		return fmt.Sprintf("user '%s' must be namespaced on a namespaces-enabled cluster", username)
	}
	optionalGroups := ""
	if len(f.principal.Groups) == 1 {
		optionalGroups = fmt.Sprintf(" (of group '%s')", f.principal.Groups[0])
	} else if len(f.principal.Groups) > 1 {
		groups := wrapInSingleQuotes(f.principal.Groups)
		groupsList := strings.Join(groups, ", ")
		optionalGroups = fmt.Sprintf(" (of groups %s)", groupsList)
	}

	return fmt.Sprintf("authorization, forbidden action: user '%s'%s has insufficient permissions to %s %s",
		f.principal.Username, optionalGroups, f.verb, f.resources)
}

func wrapInSingleQuotes(input []string) []string {
	for i, s := range input {
		input[i] = fmt.Sprintf("'%s'", s)
	}

	return input
}
