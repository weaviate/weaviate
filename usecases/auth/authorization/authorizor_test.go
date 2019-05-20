package authorization

import (
	"testing"

	"github.com/semi-technologies/weaviate/usecases/auth/authorization/adminlist"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
)

func Test_Authorizer(t *testing.T) {
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
