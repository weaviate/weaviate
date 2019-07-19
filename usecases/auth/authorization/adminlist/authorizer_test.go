//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package adminlist

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	"github.com/stretchr/testify/assert"
)

func Test_AdminList_Authorizor(t *testing.T) {
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
	})

	t.Run("with write/delete requests", func(t *testing.T) {
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
	})
}
