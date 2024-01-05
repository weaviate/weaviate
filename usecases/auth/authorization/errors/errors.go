//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
)

// Forbidden indicates a failed authorization
type Forbidden struct {
	principal *models.Principal
	verb      string
	resource  string
}

// NewForbidden creates an explicit Forbidden error with details about the
// principal and the attempted access on a specific resource
func NewForbidden(principal *models.Principal, verb, resource string) Forbidden {
	return Forbidden{
		principal: principal,
		verb:      verb,
		resource:  resource,
	}
}

func (f Forbidden) Error() string {
	optionalGroups := ""
	if len(f.principal.Groups) == 1 {
		optionalGroups = fmt.Sprintf(" (of group '%s')", f.principal.Groups[0])
	} else if len(f.principal.Groups) > 1 {
		groups := wrapInSingleQuotes(f.principal.Groups)
		groupsList := strings.Join(groups, ", ")
		optionalGroups = fmt.Sprintf(" (of groups %s)", groupsList)
	}

	return fmt.Sprintf("forbidden: user '%s'%s has insufficient permissions to %s %s",
		f.principal.Username, optionalGroups, f.verb, f.resource)
}

func wrapInSingleQuotes(input []string) []string {
	for i, s := range input {
		input[i] = fmt.Sprintf("'%s'", s)
	}

	return input
}
