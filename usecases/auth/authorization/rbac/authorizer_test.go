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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

func TestAuthorization(t *testing.T) {
	t.Run("audit logging behavior", func(t *testing.T) {
		// Setup
		logger, hook := test.NewNullLogger()
		m, err := setupTestManager(t, logger)
		require.NoError(t, err)

		principal := &models.Principal{
			Username: "test-user",
		}

		// Add a test policy
		_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", "*", "*")
		require.NoError(t, err)

		_, err = m.casbin.AddRoleForUser(conv.PrefixUserName("test-user"), conv.PrefixRoleName("admin"))
		require.NoError(t, err)

		tests := []struct {
			name           string
			authFunc       func() error
			expectLogEntry bool
		}{
			{
				name: "regular authorize should produce audit logs",
				authFunc: func() error {
					return m.Authorize(principal, authorization.READ, authorization.CollectionsMetadata()...)
				},
				expectLogEntry: true,
			},
			{
				name: "silent authorize should not produce audit logs",
				authFunc: func() error {
					return m.AuthorizeSilent(principal, authorization.READ, authorization.CollectionsMetadata()...)
				},
				expectLogEntry: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				hook.Reset()
				require.NoError(t, tt.authFunc())

				hasAuditLog := false
				for _, entry := range hook.AllEntries() {
					if entry.Data["action"] == "authorize" {
						hasAuditLog = true
						break
					}
				}

				assert.Equal(t, tt.expectLogEntry, hasAuditLog, "unexpected audit log behavior")
			})
		}
	})
}

func setupTestManager(t *testing.T, logger *logrus.Logger) (*manager, error) {
	tmpDir := os.TempDir()
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	rbacDir := filepath.Join(tmpDir, "rbac")
	if err := os.MkdirAll(rbacDir, 0o755); err != nil {
		return nil, err
	}

	policyPath := filepath.Join(rbacDir, "policy.csv")

	config := rbacconf.Config{
		Enabled: true,
	}

	return New(policyPath, config, logger)
}
