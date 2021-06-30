package sharding

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigUpdates(t *testing.T) {
	t.Run("various immutable and mutable fields", func(t *testing.T) {
		type test struct {
			name          string
			initial       Config
			update        Config
			expectedError error
		}

		tests := []test{
			{
				name:    "attempting to shard count",
				initial: Config{DesiredCount: 7},
				update:  Config{DesiredCount: 8},
				expectedError: errors.Errorf(
					"re-sharding not supported yet: shard count is immutable: " +
						"attempted change from \"7\" to \"8\""),
			},
			{
				name:    "attempting to shard count",
				initial: Config{VirtualPerPhysical: 128},
				update:  Config{VirtualPerPhysical: 256},
				expectedError: errors.Errorf(
					"virtual shards per physical is immutable: " +
						"attempted change from \"128\" to \"256\""),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				err := ValidateConfigUpdate(test.initial, test.update)
				if test.expectedError == nil {
					assert.Nil(t, err)
				} else {
					require.NotNil(t, err, "update validation must error")
					assert.Equal(t, test.expectedError.Error(), err.Error())
				}
			})
		}
	})
}
