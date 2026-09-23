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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func Test_ForbiddenError(t *testing.T) {
	tests := []struct {
		name         string
		nilPrincipal bool
		groups       []string
		wantMsg      string
	}{
		{
			name:         "nil principal",
			nilPrincipal: true,
			wantMsg:      "authorization, forbidden action: user 'anonymous' has insufficient permissions to delete [schema/things]",
		},
		{
			name:    "no groups",
			wantMsg: "authorization, forbidden action: user 'john' has insufficient permissions to delete [schema/things]",
		},
		{
			name:    "single group",
			groups:  []string{"worstusers"},
			wantMsg: "authorization, forbidden action: user 'john' (of group 'worstusers') has insufficient permissions to delete [schema/things]",
		},
		{
			name:   "multiple groups",
			groups: []string{"worstusers", "fraudsters", "evilpeople"},
			wantMsg: "authorization, forbidden action: user 'john' (of groups 'worstusers', 'fraudsters', 'evilpeople') " +
				"has insufficient permissions to delete [schema/things]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.nilPrincipal {
				assert.Equal(t, tt.wantMsg, NewForbidden(nil, "delete", "schema/things").Error())
				return
			}
			principal := &models.Principal{Username: "john", Groups: slices.Clone(tt.groups)}
			err := NewForbidden(principal, "delete", "schema/things")

			// Formatting twice pins that Error leaves the principal's groups alone.
			assert.Equal(t, tt.wantMsg, err.Error())
			assert.Equal(t, tt.wantMsg, err.Error())
			assert.Equal(t, tt.groups, principal.Groups)
		})
	}
}
