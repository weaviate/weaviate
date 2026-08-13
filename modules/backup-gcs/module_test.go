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

package modstggcs

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestInit_SkipAccessCheckWiring(t *testing.T) {
	// Init must route Backup.SkipAccessCheck to the backup client and
	// Export.SkipAccessCheck to the export client, without crossing them.
	t.Setenv("BACKUP_GCS_BUCKET", "test")
	t.Setenv("BACKUP_GCS_USE_AUTH", "false")

	tests := []struct {
		name       string
		backupFlag bool
		exportFlag bool
	}{
		{name: "backup only", backupFlag: true, exportFlag: false},
		{name: "export only", backupFlag: false, exportFlag: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Backup.SkipAccessCheck = tt.backupFlag
			cfg.Export.SkipAccessCheck = tt.exportFlag

			params := moduletools.NewMockModuleInitParams(t)
			params.EXPECT().GetLogger().Return(logrus.New())
			params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: t.TempDir()})
			params.EXPECT().GetConfig().Return(cfg)

			m := New()
			require.NoError(t, m.Init(context.Background(), params))
			assert.Equal(t, tt.backupFlag, m.config.SkipAccessCheck, "backup client")
			assert.Equal(t, tt.exportFlag, m.exportClient.config.SkipAccessCheck, "export client")
		})
	}
}

type fakeStorageProvider struct {
	dataPath string
}

func (f *fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (f *fakeStorageProvider) DataPath() string {
	return f.dataPath
}
