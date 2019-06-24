package adminlist

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Validation(t *testing.T) {
	t.Run("with only an admin list set", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Users: []string{
				"alice",
				"johndoe",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with only an read only list set", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			ReadOnlyUsers: []string{
				"alice",
				"johndoe",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with both lists present, but no overlap", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Users: []string{
				"alice",
			},
			ReadOnlyUsers: []string{
				"johndoe",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with one subject part of both lists", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Users: []string{
				"alice",
				"johndoe",
			},
			ReadOnlyUsers: []string{
				"johndoe",
			},
		}

		err := cfg.Validate()
		assert.Equal(t, err, fmt.Errorf("admin list: subject 'johndoe' is present on both admin and read-only list"))
	})
}
