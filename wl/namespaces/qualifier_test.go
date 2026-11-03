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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func TestPrefixing(t *testing.T) {
	namespaced := &models.Principal{Username: "u", Namespace: "customer1"}
	operator := &models.Principal{Username: "admin", IsGlobalOperator: true}
	p := NewPrefixing()

	require.True(t, p.NamespacesEnabled())

	t.Run("Qualify", func(t *testing.T) {
		cases := []struct {
			name      string
			principal *models.Principal
			input     string
			want      string
		}{
			{name: "namespaced principal qualifies", principal: namespaced, input: "Movies", want: "customer1:Movies"},
			{name: "global principal short input passthrough", principal: operator, input: "Movies", want: "Movies"},
			{name: "global principal qualified input passthrough", principal: operator, input: "customer1:Movies", want: "customer1:Movies"},
			{name: "nil principal passthrough", principal: nil, input: "Movies", want: "Movies"},
			{name: "empty namespace passthrough", principal: &models.Principal{Username: "u"}, input: "Movies", want: "Movies"},
			{name: "namespaced principal with empty name", principal: namespaced, input: "", want: "customer1:"},
			{name: "namespaced principal with qualified name gets double prefixed", principal: namespaced, input: "customer2:Movies", want: "customer1:customer2:Movies"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := p.Qualify(tc.principal, tc.input)
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("QualifyForCreate", func(t *testing.T) {
		cases := []struct {
			name         string
			principal    *models.Principal
			raw          string
			want         string
			wantSentinel bool
			wantOtherErr bool
		}{
			{name: "namespaced principal qualifies", principal: namespaced, raw: "Movies", want: "customer1:Movies"},
			{name: "global principal rejected with sentinel", principal: operator, raw: "Movies", wantSentinel: true},
			{name: "nil principal rejected with sentinel", principal: nil, raw: "Movies", wantSentinel: true},
			{
				name:      "namespaced principal at the cap accepted",
				principal: namespaced,
				raw:       "C" + strings.Repeat("x", namespacing.ShortNameMaxLength-1),
				want:      "customer1:" + "C" + strings.Repeat("x", namespacing.ShortNameMaxLength-1),
			},
			{
				name:         "namespaced principal one over the cap rejected with non-sentinel error",
				principal:    namespaced,
				raw:          "C" + strings.Repeat("x", namespacing.ShortNameMaxLength),
				wantOtherErr: true,
			},
			{
				name:         "namespaced principal far over the cap rejected with non-sentinel error",
				principal:    namespaced,
				raw:          strings.Repeat("x", namespacing.ShortNameMaxLength*2),
				wantOtherErr: true,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := p.QualifyForCreate(tc.principal, tc.raw)
				switch {
				case tc.wantSentinel:
					require.ErrorIs(t, err, namespacing.ErrCreateRequiresNamespace)
				case tc.wantOtherErr:
					require.Error(t, err)
					require.NotErrorIs(t, err, namespacing.ErrCreateRequiresNamespace)
				default:
					require.NoError(t, err)
					assert.Equal(t, tc.want, got)
				}
			})
		}
	})

	t.Run("QualifyRefTarget", func(t *testing.T) {
		cases := []struct {
			name          string
			sourceClass   string
			target        string
			wantQualified string
			wantShort     string
			wantErr       bool
		}{
			{name: "short target inherits source NS", sourceClass: "customer1:Zoo", target: "Animal", wantQualified: "customer1:Animal", wantShort: "Animal"},
			{name: "own-NS qualified target normalizes", sourceClass: "customer1:Zoo", target: "customer1:Animal", wantQualified: "customer1:Animal", wantShort: "Animal"},
			{name: "cross-NS qualified target is rejected", sourceClass: "customer1:Zoo", target: "customer2:Animal", wantErr: true},
			{name: "unqualified source leaves target untouched", sourceClass: "Zoo", target: "Animal", wantQualified: "Animal", wantShort: "Animal"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				qualified, short, err := p.QualifyRefTarget(tc.sourceClass, tc.target)
				if tc.wantErr {
					require.ErrorContains(t, err, "is not a valid class name")
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.wantQualified, qualified, "qualified")
				assert.Equal(t, tc.wantShort, short, "short")
			})
		}
	})
}
