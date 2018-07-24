package inmemory

import (
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/go-openapi/strfmt"

	wconfig "github.com/creativesoftwarefdn/weaviate/config"
)

const TEST_ROOT_KEY strfmt.UUID = "root_key"
const TEST_ROOT_TOKEN string = "root_token"

func new_test_instance() InMemory {
	config := InMemoryConfig{
		RootKey:   string(TEST_ROOT_KEY),
		RootToken: TEST_ROOT_TOKEN,
	}

	env := wconfig.Environment{
		Database: wconfig.Database{
			DatabaseConfig: interface{}(config),
		},
	}

	msg := messages.Messaging{}

	im := InMemory{}
	im.SetConfig(&env)
	im.SetMessaging(&msg)
	im.Connect()
	im.Init()

	return im
}

func TestGetRootKey(t *testing.T) {
	instance := new_test_instance()
	var response models.KeyGetResponse
	err := instance.GetKey(nil, TEST_ROOT_KEY, &response)
	if err != nil {
		t.Fatal("could not fetch key")
	}

	require.Equal(t, response.KeyID, TEST_ROOT_KEY)
	require.Equal(t, *response.Key.IsRoot, true)
}
