package errors

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
		fmt.Errorf("forbidden: user '%s'%s does not have permissions to %s %s",
			principal.Username, optionalGroups, verb, resource),
	)
}

func wrapInSingleQuotes(input []string) []string {
	for i, s := range input {
		input[i] = fmt.Sprintf("'%s'", s)
	}

	return input
}
