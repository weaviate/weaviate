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

package rest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// specRefs returns every "$ref" value in node and its descendants.
func specRefs(node any) []string {
	var refs []string
	switch n := node.(type) {
	case map[string]any:
		for k, v := range n {
			if s, ok := v.(string); ok && k == "$ref" {
				refs = append(refs, s)
				continue
			}
			refs = append(refs, specRefs(v)...)
		}
	case []any:
		for _, v := range n {
			refs = append(refs, specRefs(v)...)
		}
	}
	return refs
}

func TestPermissionReturningOperations(t *testing.T) {
	var spec struct {
		Definitions map[string]any            `json:"definitions"`
		Paths       map[string]map[string]any `json:"paths"`
	}
	require.NoError(t, json.Unmarshal(SwaggerJSON, &spec))

	// reachesPermission reports whether refs lead to Permission, directly or
	// through other definitions. visited breaks reference cycles.
	var reachesPermission func(refs []string, visited map[string]bool) bool
	reachesPermission = func(refs []string, visited map[string]bool) bool {
		for _, ref := range refs {
			name, ok := strings.CutPrefix(ref, "#/definitions/")
			if !ok || visited[name] {
				continue
			}
			if name == "Permission" {
				return true
			}
			visited[name] = true
			if reachesPermission(specRefs(spec.Definitions[name]), visited) {
				return true
			}
		}
		return false
	}

	var returningPermissions []string
	for _, methods := range spec.Paths {
		for _, item := range methods {
			op, ok := item.(map[string]any)
			if !ok {
				continue
			}
			responses, _ := op["responses"].(map[string]any)
			var successRefs []string
			for code, response := range responses {
				if strings.HasPrefix(code, "2") {
					successRefs = append(successRefs, specRefs(response)...)
				}
			}
			if reachesPermission(successRefs, map[string]bool{}) {
				id, _ := op["operationId"].(string)
				returningPermissions = append(returningPermissions, id)
			}
		}
	}

	assert.ElementsMatch(t, []string{
		"getRoles", "getRole", "getRolesForUser", "getRolesForUserDeprecated", "getRolesForGroup", "getOwnInfo",
	}, returningPermissions,
		"the operations returning permissions changed: a new one must pass its request to "+
			"conv.AcceptsBackupPrincipals and the result to conv.PoliciesToPermission")
}
