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

package namespacing

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestQualifyForCreate(t *testing.T) {
	cases := []struct {
		name      string
		principal *models.Principal
		q         Qualifier
		raw       string
		want      string
		wantErr   string
	}{
		{
			name:      "namespaced principal qualifies",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			q:         NewPrefixing(),
			raw:       "Movies",
			want:      "customer1:Movies",
		},
		{
			name:      "namespaced principal typing a prefix is rejected before qualification",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			q:         NewPrefixing(),
			raw:       "customer1:Movies",
			wantErr:   "is not a valid class name",
		},
		{
			name:      "global principal allowed on NS-disabled (raw passthrough)",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			q:         Disabled,
			raw:       "Movies",
			want:      "Movies",
		},
		{
			name:      "NS-disabled passthrough does not enforce length cap",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			q:         Disabled,
			raw:       "C" + strings.Repeat("x", ShortNameMaxLength+50),
			want:      "C" + strings.Repeat("x", ShortNameMaxLength+50),
		},
		{
			name:      "nil principal on NS-disabled returns raw unchanged",
			principal: nil,
			q:         Disabled,
			raw:       "Movies",
			want:      "Movies",
		},
		{
			name:      "namespaced principal on NS-disabled returns raw unchanged",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			q:         Disabled,
			raw:       "Movies",
			want:      "Movies",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := QualifyForCreate(tc.principal, tc.q, tc.raw, "class")
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
