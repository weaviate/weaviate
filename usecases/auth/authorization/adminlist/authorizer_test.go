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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

func Test_AdminList_Authorizer(t *testing.T) {
	t.Run("with read requests", func(t *testing.T) {
		t.Run("with no users configured at all", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users:   []string{},
			}

			principal := &models.Principal{
				Username: "johndoe",
			}

			err := New(cfg).Authorize(principal, "get", "things")
			assert.Equal(t, errors.NewForbidden(principal, "get", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with a nil principal", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users:   []string{},
			}

			principal := (*models.Principal)(nil)
			err := New(cfg).Authorize(principal, "get", "things")
			assert.Equal(t, errors.NewForbidden(newAnonymousPrincipal(), "get", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with a non-configured user, it denies the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users: []string{
					"alice",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
			}

			err := New(cfg).Authorize(principal, "get", "things")
			assert.Equal(t, errors.NewForbidden(principal, "get", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with a configured admin user, it allows the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users: []string{
					"alice",
					"johndoe",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
			}

			err := New(cfg).Authorize(principal, "get", "things")
			assert.Nil(t, err)
		})

		t.Run("with a configured read-only user, it allows the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				ReadOnlyUsers: []string{
					"alice",
					"johndoe",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
			}

			err := New(cfg).Authorize(principal, "get", "things")
			assert.Nil(t, err)
		})

		t.Run("with anonymous as read-only user and no principal, it allows the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				ReadOnlyUsers: []string{
					"anonymous",
				},
			}

			principal := (*models.Principal)(nil)
			err := New(cfg).Authorize(principal, "get", "things")
			assert.Nil(t, err)
		})
	})

	t.Run("with a non-configured group, it denies the request", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Groups: []string{
				"band",
			},
		}

		principal := &models.Principal{
			Username: "alice",
			Groups: []string{
				"posse",
			},
		}
		err := New(cfg).Authorize(principal, "get", "things")
		assert.Equal(t, errors.NewForbidden(principal, "get", "things"), err,
			"should have the correct err msg")
	})

	t.Run("with a configured admin group, it allows the request", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Groups: []string{
				"band",
				"posse",
			},
		}

		principal := &models.Principal{
			Username: "alice",
			Groups: []string{
				"posse",
			},
		}
		err := New(cfg).Authorize(principal, "get", "things")
		assert.Nil(t, err)
	})

	t.Run("with a configured read-only group, it allows the request", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			ReadOnlyGroups: []string{
				"band",
				"posse",
			},
		}

		principal := &models.Principal{
			Username: "johndoe",
			Groups: []string{
				"posse",
			},
		}

		err := New(cfg).Authorize(principal, "get", "things")
		assert.Nil(t, err)
	})

	t.Run("with a configured admin user and non-configured group, it allows the request", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			Users: []string{
				"alice",
				"johndoe",
			},
			Groups: []string{
				"band",
			},
		}

		principal := &models.Principal{
			Username: "johndoe",
			Groups: []string{
				"posse",
			},
		}

		err := New(cfg).Authorize(principal, "get", "things")
		assert.Nil(t, err)
	})

	t.Run("with a configured read-only user and non-configured read-only group, it allows the request", func(t *testing.T) {
		cfg := Config{
			Enabled: true,
			ReadOnlyUsers: []string{
				"alice",
				"johndoe",
			},
			ReadOnlyGroups: []string{
				"band",
			},
		}

		principal := &models.Principal{
			Username: "johndoe",
			Groups: []string{
				"posse",
			},
		}

		err := New(cfg).Authorize(principal, "get", "things")
		assert.Nil(t, err)
	})

	t.Run("with write/delete requests", func(t *testing.T) {
		t.Run("with a nil principal", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users:   []string{},
			}

			principal := (*models.Principal)(nil)
			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(newAnonymousPrincipal(), "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with no users configured at all", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users:   []string{},
			}

			principal := &models.Principal{
				Username: "johndoe",
			}

			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(principal, "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with a non-configured user, it denies the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users: []string{
					"alice",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
			}

			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(principal, "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with an empty user, it denies the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users: []string{
					"alice",
				},
			}

			principal := &models.Principal{
				Username: "",
			}

			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(principal, "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with a configured admin user, it allows the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users: []string{
					"alice",
					"johndoe",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
			}

			err := New(cfg).Authorize(principal, "create", "things")
			assert.Nil(t, err)
		})

		t.Run("with a configured read-only user, it denies the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				ReadOnlyUsers: []string{
					"alice",
					"johndoe",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
			}

			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(principal, "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with anonymous on the read-only list and a nil principal", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users:   []string{},
				ReadOnlyUsers: []string{
					"anonymous",
				},
			}

			principal := (*models.Principal)(nil)
			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(newAnonymousPrincipal(), "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with a non-configured group, it denies the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Groups: []string{
					"band",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
				Groups: []string{
					"posse",
				},
			}
			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(principal, "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with an empty group, it denies the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Groups: []string{
					"band",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
				Groups:   []string{},
			}
			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(principal, "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with a configured admin group, it allows the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Groups: []string{
					"band",
					"posse",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
				Groups: []string{
					"band",
				},
			}
			err := New(cfg).Authorize(principal, "create", "things")
			assert.Nil(t, err)
		})

		t.Run("with a configured read-only group, it denies the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				ReadOnlyGroups: []string{
					"band",
					"posse",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
				Groups: []string{
					"posse",
				},
			}

			err := New(cfg).Authorize(principal, "create", "things")
			assert.Equal(t, errors.NewForbidden(principal, "create", "things"), err,
				"should have the correct err msg")
		})

		t.Run("with a configured admin user and non-configured group, it allows the request", func(t *testing.T) {
			cfg := Config{
				Enabled: true,
				Users: []string{
					"alice",
					"johndoe",
				},
				Groups: []string{
					"band",
				},
			}

			principal := &models.Principal{
				Username: "johndoe",
				Groups: []string{
					"posse",
				},
			}

			err := New(cfg).Authorize(principal, "create", "things")
			assert.Nil(t, err)
		})
	})
}
