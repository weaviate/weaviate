package adminlist

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
)

func Test_AdminList_Authorizor(t *testing.T) {
	t.Run("with no users configured at all", func(t *testing.T) {
		cfg := config.AdminList{
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
		cfg := config.AdminList{
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

	t.Run("with a configured user, it allows the request", func(t *testing.T) {
		cfg := config.AdminList{
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
}
