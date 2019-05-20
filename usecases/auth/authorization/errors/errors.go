/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package errors

import (
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
)

// Forbidden indicates a failed authorization
type Forbidden error

// NewForbidden creates an explicit Forbidden error with details about the
// principal and the attempted access on a specific resource
func NewForbidden(principal *models.Principal, verb, resource string) Forbidden {
	optionalGroups := ""
	if len(principal.Groups) == 1 {
		optionalGroups = fmt.Sprintf(" (of group '%s')", principal.Groups[0])
	} else if len(principal.Groups) > 1 {
		groups := wrapInSingleQuotes(principal.Groups)
		groupsList := strings.Join(groups, ", ")
		optionalGroups = fmt.Sprintf(" (of groups %s)", groupsList)
	}

	return Forbidden(
		fmt.Errorf("forbidden: user '%s'%s has insufficient permissions to %s %s",
			principal.Username, optionalGroups, verb, resource),
	)
}

func wrapInSingleQuotes(input []string) []string {
	for i, s := range input {
		input[i] = fmt.Sprintf("'%s'", s)
	}

	return input
}
