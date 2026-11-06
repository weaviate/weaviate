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
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// fakeAssigner fails each subject's first failures[subject] calls.
type fakeAssigner struct {
	failures map[string]int
	calls    []string
}

func (f *fakeAssigner) AddRolesForUser(user string, roles []string) error {
	f.calls = append(f.calls, user+"="+strings.Join(roles, ","))
	if f.failures[user] > 0 {
		f.failures[user]--
		return errors.New("leader not found")
	}
	return nil
}

func TestBootEnvRoleGrants(t *testing.T) {
	authNconf := config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"alice", "bob"}}}
	withGrants := rbacconf.Config{Enabled: true, AdminUsers: []string{"alice"}, ViewerUsers: []string{"bob"}}
	tests := []struct {
		name     string
		rbacConf rbacconf.Config
		waitErr  error
		// wantCalls lists the wait ("wait") and every submission, in order.
		wantCalls []string
		wantError bool
	}{
		{
			name:     "RBAC disabled submits nothing and does not wait",
			rbacConf: rbacconf.Config{AdminUsers: []string{"alice"}, ViewerUsers: []string{"bob"}},
		},
		{
			name:     "no configured grant submits nothing",
			rbacConf: rbacconf.Config{Enabled: true},
		},
		{
			name:      "every grant is submitted after the wait returns",
			rbacConf:  withGrants,
			wantCalls: []string{"wait", "db:alice=admin", "db:bob=viewer"},
		},
		{
			name:      "a failed wait submits nothing and is logged",
			rbacConf:  withGrants,
			waitErr:   errors.New("meta store failed"),
			wantCalls: []string{"wait"},
			wantError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			assigner := &fakeAssigner{}
			wait := func() error {
				assigner.calls = append(assigner.calls, "wait")
				return tt.waitErr
			}

			bootEnvRoleGrants(tt.rbacConf, authNconf, assigner, wait, backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 0), logger)

			assert.Equal(t, tt.wantCalls, assigner.calls)
			logged := slices.ContainsFunc(hook.AllEntries(), func(e *logrus.Entry) bool {
				return e.Level == logrus.ErrorLevel && strings.Contains(e.Message, "meta store failed")
			})
			assert.Equal(t, tt.wantError, logged)
		})
	}
}

func TestWriteEnvRoleGrants(t *testing.T) {
	grants := []rbac.RoleGrant{
		{Subject: "db:alice", Role: authorization.Admin},
		{Subject: "db:bob", Role: authorization.Viewer},
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name     string
		failures map[string]int
		backoff  backoff.BackOff
		// wantCalls lists every submission in order.
		wantCalls   []string
		wantPending []string
	}{
		{
			name:      "a failed grant is resubmitted alone",
			failures:  map[string]int{"db:alice": 1},
			backoff:   backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 2),
			wantCalls: []string{"db:alice=admin", "db:bob=viewer", "db:alice=admin"},
		},
		{
			name:        "the retries stop at the bound and the warning names the pending grant",
			failures:    map[string]int{"db:alice": 100},
			backoff:     backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 2),
			wantCalls:   []string{"db:alice=admin", "db:bob=viewer", "db:alice=admin", "db:alice=admin"},
			wantPending: []string{"db:alice=admin"},
		},
		{
			name:        "shutdown stops the retries",
			failures:    map[string]int{"db:alice": 100},
			backoff:     envGrantBackoff(cancelled),
			wantCalls:   []string{"db:alice=admin", "db:bob=viewer"},
			wantPending: []string{"db:alice=admin"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			assigner := &fakeAssigner{failures: tt.failures}

			writeEnvRoleGrants(assigner, grants, tt.backoff, logger)

			assert.Equal(t, tt.wantCalls, assigner.calls)
			var warnings []*logrus.Entry
			for _, e := range hook.AllEntries() {
				if e.Level == logrus.WarnLevel {
					warnings = append(warnings, e)
				}
			}
			if tt.wantPending == nil {
				assert.Empty(t, warnings)
				return
			}
			require.Len(t, warnings, 1)
			assert.Equal(t, tt.wantPending, warnings[0].Data["pending_grants"])
		})
	}
}
