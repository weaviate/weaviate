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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/license"
)

func TestCompressionBackupCfg(t *testing.T) {
	tcs := map[string]struct {
		cfg                 *models.BackupConfig
		expectedCompression ubak.CompressionLevel
		expectedCPU         int
		expectedBucket      string
		expectedPath        string
	}{
		"without config": {
			cfg:                 nil,
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
		},
		"with config": {
			cfg: &models.BackupConfig{
				CPUPercentage:    25,
				CompressionLevel: models.BackupConfigCompressionLevelBestSpeed,
			},
			expectedCompression: ubak.GzipBestSpeed,
			expectedCPU:         25,
		},
		"with partial config [CPU]": {
			cfg: &models.BackupConfig{
				CPUPercentage: 25,
			},
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         25,
		},
		"with partial config [Compression]": {
			cfg: &models.BackupConfig{
				CompressionLevel: models.BackupConfigCompressionLevelBestSpeed,
			},
			expectedCompression: ubak.GzipBestSpeed,
			expectedCPU:         ubak.DefaultCPUPercentage,
		},
		"with partial config [Bucket]": {
			cfg: &models.BackupConfig{
				Bucket: "a bucket name",
			},
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
			expectedBucket:      "a bucket name",
		},
		"with partial config [Path]": {
			cfg: &models.BackupConfig{
				Path: "a path",
			},
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
			expectedPath:        "a path",
		},
	}

	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			ccfg := compressionFromBCfg(tc.cfg)
			assert.Equal(t, tc.expectedCompression, ccfg.Level)
			assert.Equal(t, tc.expectedCPU, ccfg.CPUPercentage)
		})
	}
}

func TestCompressionRestoreCfg(t *testing.T) {
	tcs := map[string]struct {
		cfg                 *models.RestoreConfig
		expectedCompression ubak.CompressionLevel
		expectedCPU         int
	}{
		"without config": {
			cfg:                 nil,
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
		},
		"with config": {
			cfg: &models.RestoreConfig{
				CPUPercentage: 25,
			},
			expectedCPU: 25,
		},
	}

	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			ccfg := compressionFromRCfg(tc.cfg)
			assert.Equal(t, tc.expectedCPU, ccfg.CPUPercentage)
		})
	}
}

// TestIsRequestFromRootUser verifies that the base backup ID gate passed to the
// list manager is only set for root users (by username or group membership).
func TestIsRequestFromRootUser(t *testing.T) {
	h := &backupHandlers{
		rbacConfig: rbacconf.Config{
			RootUsers:  []string{"root-user"},
			RootGroups: []string{"root-group"},
		},
	}

	tcs := map[string]struct {
		principal *models.Principal
		expectGet bool
	}{
		"root user":            {principal: &models.Principal{Username: "root-user"}, expectGet: true},
		"member of root group": {principal: &models.Principal{Username: "alice", Groups: []string{"root-group"}}, expectGet: true},
		"non-root user":        {principal: &models.Principal{Username: "alice"}, expectGet: false},
		"non-root group":       {principal: &models.Principal{Username: "alice", Groups: []string{"other-group"}}, expectGet: false},
		"nil principal":        {principal: nil, expectGet: false},
	}

	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			assert.Equal(t, tc.expectGet, h.isRequestFromRootUser(tc.principal))
		})
	}
}

func TestBackupCreateErrPayload(t *testing.T) {
	refusal := license.Required(ubak.DedupeFeature)
	docs := ", see " + license.EnterpriseDocsURL
	plain := errors.New("no backup backend")
	denied := authzerrors.NewForbidden(nil, "create", "backups/Class-A")
	cases := []struct {
		name      string
		principal *models.Principal
		err       error
		want      string
	}{
		{name: "license refusal names the docs", err: refusal, want: refusal.Error() + docs},
		{name: "wrapped license refusal names the docs", err: fmt.Errorf("backup b1 %w", refusal), want: "backup b1 " + refusal.Error() + docs},
		{
			name:      "a namespace named like the URL scheme keeps the URL whole",
			principal: &models.Principal{Username: "u", Namespace: "https"},
			err:       refusal,
			want:      refusal.Error() + docs,
		},
		{name: "authorization refusal stays as is", err: denied, want: denied.Error()},
		{name: "other error stays as is", err: plain, want: plain.Error()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := backupCreateErrPayload(tc.principal, tc.err)

			require.Len(t, payload.Error, 1)
			require.Equal(t, tc.want, payload.Error[0].Message)
		})
	}
}
