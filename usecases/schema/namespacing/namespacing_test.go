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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestQualifyForCreate(t *testing.T) {
	cases := []struct {
		name         string
		principal    *models.Principal
		nsEnabled    bool
		raw          string
		want         string
		wantSentinel bool
		wantOtherErr bool
	}{
		{
			name:      "namespaced principal qualifies",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			nsEnabled: true,
			raw:       "Movies",
			want:      "customer1:Movies",
		},
		{
			name:         "global principal rejected with sentinel on NS-enabled",
			principal:    &models.Principal{Username: "admin", IsGlobalOperator: true},
			nsEnabled:    true,
			raw:          "Movies",
			wantSentinel: true,
		},
		{
			name:         "nil principal rejected with sentinel on NS-enabled",
			principal:    nil,
			nsEnabled:    true,
			raw:          "Movies",
			wantSentinel: true,
		},
		{
			name:      "global principal allowed on NS-disabled (raw passthrough)",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			nsEnabled: false,
			raw:       "Movies",
			want:      "Movies",
		},
		{
			name:      "NS-disabled passthrough does not enforce length cap",
			principal: &models.Principal{Username: "admin", IsGlobalOperator: true},
			nsEnabled: false,
			raw:       "C" + strings.Repeat("x", ShortNameMaxLength+50),
			want:      "C" + strings.Repeat("x", ShortNameMaxLength+50),
		},
		{
			name:      "nil principal on NS-disabled returns raw unchanged",
			principal: nil,
			nsEnabled: false,
			raw:       "Movies",
			want:      "Movies",
		},
		{
			name:      "namespaced principal at the cap accepted",
			principal: &models.Principal{Username: "u", Namespace: "customer1"},
			nsEnabled: true,
			raw:       "C" + strings.Repeat("x", ShortNameMaxLength-1),
			want:      "customer1:" + "C" + strings.Repeat("x", ShortNameMaxLength-1),
		},
		{
			name:         "namespaced principal one over the cap rejected with non-sentinel error",
			principal:    &models.Principal{Username: "u", Namespace: "customer1"},
			nsEnabled:    true,
			raw:          "C" + strings.Repeat("x", ShortNameMaxLength),
			wantOtherErr: true,
		},
		{
			name:         "namespaced principal far over the cap rejected with non-sentinel error",
			principal:    &models.Principal{Username: "u", Namespace: "customer1"},
			nsEnabled:    true,
			raw:          strings.Repeat("x", ShortNameMaxLength*2),
			wantOtherErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := QualifyForCreate(tc.principal, tc.nsEnabled, tc.raw)
			switch {
			case tc.wantSentinel:
				require.ErrorIs(t, err, ErrCreateRequiresNamespace)
			case tc.wantOtherErr:
				require.Error(t, err)
				require.False(t, errors.Is(err, ErrCreateRequiresNamespace), "expected non-sentinel error, got sentinel")
			default:
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			}
		})
	}
}
