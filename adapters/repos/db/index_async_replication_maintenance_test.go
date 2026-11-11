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
	"testing"

	"github.com/stretchr/testify/require"

	entreplication "github.com/weaviate/weaviate/entities/replication"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestAsyncReplicationBlockedByMaintenance(t *testing.T) {
	on := func() bool { return true }
	off := func() bool { return false }
	withFlag := func(v bool) *entreplication.GlobalConfig {
		return &entreplication.GlobalConfig{AsyncReplicationInMaintenanceEnabled: configRuntime.NewDynamicValue(v)}
	}

	tests := []struct {
		name         string
		maintenance  func() bool
		globalConfig *entreplication.GlobalConfig
		want         bool
	}{
		{name: "no maintenance func, nil global config", maintenance: nil, globalConfig: nil, want: false},
		{name: "no maintenance func, flag on", maintenance: nil, globalConfig: withFlag(true), want: false},
		{name: "maintenance off, nil global config", maintenance: off, globalConfig: nil, want: false},
		{name: "maintenance off, nil flag", maintenance: off, globalConfig: &entreplication.GlobalConfig{}, want: false},
		{name: "maintenance off, flag off", maintenance: off, globalConfig: withFlag(false), want: false},
		{name: "maintenance off, flag on", maintenance: off, globalConfig: withFlag(true), want: false},
		{name: "maintenance on, nil global config", maintenance: on, globalConfig: nil, want: true},
		{name: "maintenance on, nil flag", maintenance: on, globalConfig: &entreplication.GlobalConfig{}, want: true},
		{name: "maintenance on, flag off", maintenance: on, globalConfig: withFlag(false), want: true},
		{name: "maintenance on, flag on", maintenance: on, globalConfig: withFlag(true), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := &Index{
				Config:                  IndexConfig{MaintenanceModeEnabled: tt.maintenance},
				globalreplicationConfig: tt.globalConfig,
			}
			require.Equal(t, tt.want, idx.asyncReplicationBlockedByMaintenance())
		})
	}

	t.Run("flag flips take effect on the next call", func(t *testing.T) {
		flag := configRuntime.NewDynamicValue(false)
		idx := &Index{
			Config:                  IndexConfig{MaintenanceModeEnabled: on},
			globalreplicationConfig: &entreplication.GlobalConfig{AsyncReplicationInMaintenanceEnabled: flag},
		}
		require.True(t, idx.asyncReplicationBlockedByMaintenance())
		require.NoError(t, flag.SetValue(true))
		require.False(t, idx.asyncReplicationBlockedByMaintenance())
		flag.Reset()
		require.True(t, idx.asyncReplicationBlockedByMaintenance())
	})
}
