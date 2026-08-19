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

package cron

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubRegistrant carries no cron job, so add can be driven on names the two
// real registrants cannot produce.
type stubRegistrant struct {
	name string
	key  string
}

func (r stubRegistrant) jobName() string          { return r.name }
func (r stubRegistrant) hookKey() string          { return r.key }
func (r stubRegistrant) RuntimeConfigHook() error { return nil }

func TestCrons_AddRejects(t *testing.T) {
	tests := []struct {
		name     string
		existing []registrant
		add      registrant
	}{
		{
			name: "an empty job name",
			add:  stubRegistrant{name: "", key: "ObjectsTTL"},
		},
		{
			name:     "a job name another registrant already holds",
			existing: []registrant{stubRegistrant{name: namespaceCleanupJobName, key: "NamespaceCleanup"}},
			add:      stubRegistrant{name: namespaceCleanupJobName, key: "ObjectsTTL"},
		},
		{
			name:     "a runtime config hook key another registrant already holds",
			existing: []registrant{stubRegistrant{name: namespaceCleanupJobName, key: "NamespaceCleanup"}},
			add:      stubRegistrant{name: objectsTTLJobName, key: "NamespaceCleanup"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Crons{registrations: tt.existing}

			require.Error(t, c.add(tt.add))
			assert.Len(t, c.registrations, len(tt.existing),
				"a rejected registrant must not join the slice")
		})
	}
}

func TestCrons_RuntimeConfigHooks(t *testing.T) {
	tests := []struct {
		name  string
		crons func(t *testing.T) *Crons
		want  []string
	}{
		{
			// Startup reads the map before anything calls Init, so both keys
			// must already be there without one.
			name: "both registrants, collected before Init",
			crons: func(t *testing.T) *Crons {
				logger, _ := test.NewNullLogger()
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)

				c, err := NewCrons(ctx, logger, intervalConfig(time.Minute))
				require.NoError(t, err)
				return c
			},
			want: []string{"NamespaceCleanup", "ObjectsTTL"},
		},
		{
			name:  "no registrants",
			crons: func(*testing.T) *Crons { return &Crons{} },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hooks := tt.crons(t).RuntimeConfigHooks()

			assert.ElementsMatch(t, tt.want, slices.Collect(maps.Keys(hooks)))
		})
	}
}
