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

package adminlist

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Validation(t *testing.T) {
	t.Run("with only an admin user list set", func(t *testing.T) {
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

	t.Run("with only a read only user list set", func(t *testing.T) {
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

	t.Run("with both user lists present, but no overlap", func(t *testing.T) {
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

	t.Run("with one subject part of both user lists", func(t *testing.T) {
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

	t.Run("with only an admin group list set", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Groups: []string{
				"band",
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with only a read only group list set", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			ReadOnlyGroups: []string{
				"band",
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with both group lists present, but no overlap", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Groups: []string{
				"band",
			},
			ReadOnlyGroups: []string{
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with one subject part of both group lists", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Groups: []string{
				"band",
				"posse",
			},
			ReadOnlyGroups: []string{
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Equal(t, err, fmt.Errorf("admin list: subject 'posse' is present on both admin and read-only list"))
	})

	t.Run("with both admin user and groups present", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Users: []string{
				"alice",
				"johndoe",
			},
			Groups: []string{
				"band",
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with an admin user and read only group set", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Users: []string{
				"alice",
				"johndoe",
			},
			ReadOnlyGroups: []string{
				"band",
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with both read only user and groups present", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			ReadOnlyUsers: []string{
				"alice",
				"johndoe",
			},
			ReadOnlyGroups: []string{
				"band",
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("with a read only user and admin group set", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			ReadOnlyUsers: []string{
				"alice",
				"johndoe",
			},
			Groups: []string{
				"band",
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})

	t.Run("all user and group attributes present", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Users: []string{
				"alice",
			},
			ReadOnlyUsers: []string{
				"johndoe",
			},
			Groups: []string{
				"band",
			},
			ReadOnlyGroups: []string{
				"posse",
			},
		}

		err := cfg.Validate()
		assert.Nil(t, err)
	})
}
