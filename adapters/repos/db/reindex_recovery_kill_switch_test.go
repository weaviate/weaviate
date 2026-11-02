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

package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// The shard holds a recoverable migration, so a recovery that ran returns it.
func TestMigrationRecovery_RuntimeReindexDisabled(t *testing.T) {
	tests := []struct {
		name          string
		enabled       bool
		wantRecovered bool
	}{
		{name: "disabled skips recovery"},
		{name: "enabled recovers", enabled: true, wantRecovered: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			root := t.TempDir()
			lsm := filepath.Join(root, "books", "shard-1", "lsm")
			require.NoError(t, os.MkdirAll(lsm, 0o777))
			require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(
				NewMigrationRecordIterated(testMigrationSubject(3, StrategyCodeSearchableRetokenize, "title"))))

			recovered := NewMigrationRecoveryReindexer(tt.enabled, logger, nil).
				recoverFromStore(NewMigrationRecordStore(lsm, logger), "shard-1")
			require.Equal(t, tt.wantRecovered, len(recovered) > 0)
		})
	}
}
