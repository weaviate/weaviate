//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const DefaultGoroutineFactor = 1.5

func TestEnvironmentImportGoroutineFactor(t *testing.T) {
	factors := []struct {
		name            string
		goroutineFactor []string
		expected        float64
		expectedErr     bool
	}{
		{"Valid factor", []string{"1"}, 1, false},
		{"Low factor", []string{"0.5"}, 0.5, false},
		{"not given", []string{}, DefaultGoroutineFactor, false},
		{"High factor", []string{"5"}, 5, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			if len(tt.goroutineFactor) == 1 {
				os.Setenv("MAX_IMPORT_GOROUTINES_FACTOR", tt.goroutineFactor[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.MaxImportGoroutinesFactor)
			}
		})
	}
}

func TestEnvironmentSetFlushAfter(t *testing.T) {
	factors := []struct {
		name        string
		flushAfter  []string
		expected    int
		expectedErr bool
	}{
		{"Valid", []string{"1"}, 1, false},
		{"not given", []string{}, DefaultPersistenceFlushIdleMemtablesAfter, false},
		{"invalid factor", []string{"-1"}, -1, true},
		{"zero factor", []string{"0"}, -1, true},
		{"not parsable", []string{"I'm not a number"}, -1, true},
	}
	for _, tt := range factors {
		t.Run(tt.name, func(t *testing.T) {
			os.Clearenv()
			if len(tt.flushAfter) == 1 {
				os.Setenv("PERSISTENCE_FLUSH_IDLE_MEMTABLES_AFTER", tt.flushAfter[0])
			}
			conf := Config{}
			err := FromEnv(&conf)

			if tt.expectedErr {
				require.NotNil(t, err)
			} else {
				require.Equal(t, tt.expected, conf.Persistence.FlushIdleMemtablesAfter)
			}
		})
	}
}
