package peers

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
	"github.com/stretchr/testify/assert"
)

func TestWithoutPeers(t *testing.T) {
	peers := Peers{}
	networkClass, _ := crossrefs.ParseClass("WeaviateB/Car")

	ok, err := peers.HasClass(networkClass)
	assert.Equal(t, false, ok, "class should not exist in an empty network")
	assert.NotEqual(t, nil, err, "should have an error")
	assert.Equal(t,
		"class 'WeaviateB/Car' does not exist: no peer 'WeaviateB' in the network",
		err.Error(), "should fail with a good error message")
}

func TestWithPeerWithoutClass(t *testing.T) {
	peers := Peers{
		Peer{
			Name:   "WeaviateB",
			Schema: schema.Schema{},
		},
	}
	networkClass, _ := crossrefs.ParseClass("WeaviateB/Car")

	ok, err := peers.HasClass(networkClass)
	assert.Equal(t, false, ok, "class should not exist on peer with empty network")
	assert.NotEqual(t, nil, err, "should have an error")
	assert.Equal(t,
		"class 'WeaviateB/Car' does not exist: peer 'WeaviateB' has no class 'Car'",
		err.Error(), "should fail with a good error message")
}

func TestWithPeerWithClass(t *testing.T) {
	peers := Peers{
		Peer{
			Name: "WeaviateB",
			Schema: schema.Schema{
				Things: &models.SemanticSchema{
					Classes: []*models.SemanticSchemaClass{{
						Class: "Car",
					}},
				},
			},
		},
	}
	networkClass, _ := crossrefs.ParseClass("WeaviateB/Car")

	ok, err := peers.HasClass(networkClass)
	assert.Equal(t, false, ok, "class should not exist on peer with empty network")
	assert.Equal(t, nil, err, "should have no error")
}
