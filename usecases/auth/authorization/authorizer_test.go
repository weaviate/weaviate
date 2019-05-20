package authorization

import (
	"testing"

	"github.com/semi-technologies/weaviate/usecases/auth/authorization/adminlist"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
)

func Test_Authorizer(t *testing.T) {
	t.Run("when no authz is configured", func(t *testing.T) {
		cfg := config.Config{}

		authorizer := New(cfg)

		t.Run("it uses the dummy authorizer", func(t *testing.T) {
			_, ok := authorizer.(*DummyAuthorizer)
			assert.Equal(t, true, ok)
		})

		t.Run("any request is allowed", func(t *testing.T) {
			err := authorizer.Authorize(nil, "delete", "the/world")
			assert.Nil(t, err)
		})

	})

	t.Run("when adminlist is configured", func(t *testing.T) {
		cfg := config.Config{
			Authorization: config.Authorization{
				AdminList: config.AdminList{
					Enabled: true,
				},
			},
		}

		authorizer := New(cfg)

		_, ok := authorizer.(*adminlist.Authorizer)
		assert.Equal(t, true, ok)
	})

}
