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

package rbac

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestMigrationV2(t *testing.T) {
	tests := []struct {
		name   string
		input  map[string][]authorization.Policy
		output map[string][]authorization.Policy
	}{
		{
			name: "empty policy list",
		},
		{
			name: "single policy - read without scope",
			input: map[string][]authorization.Policy{
				"read": {{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.READ}},
			},
			output: map[string][]authorization.Policy{
				"read": {{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH)}},
			},
		},
		{
			name: "single policy - manage with match",
			input: map[string][]authorization.Policy{
				"manage": {{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.ROLE_SCOPE_MATCH}},
			},
			output: map[string][]authorization.Policy{
				"manage": {
					{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH)},
					{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_MATCH)},
					{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.VerbWithScope(authorization.DELETE, authorization.ROLE_SCOPE_MATCH)},
				},
			},
		},
		{
			name: "single policy - manage with all",
			input: map[string][]authorization.Policy{
				"manage": {{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: conv.CRUD}},
			},
			output: map[string][]authorization.Policy{
				"manage": {
					{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL)},
					{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.VerbWithScope(authorization.UPDATE, authorization.ROLE_SCOPE_ALL)},
					{Resource: "roles/something", Domain: authorization.RolesDomain, Verb: authorization.VerbWithScope(authorization.DELETE, authorization.ROLE_SCOPE_ALL)},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := migrateV2(test.input)
			require.Equal(t, test.output, output)
		})
	}
}
