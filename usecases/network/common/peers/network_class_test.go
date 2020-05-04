//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package peers

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
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
				Things: &models.Schema{
					Classes: []*models.Class{{
						Class: "Car",
					}},
				},
			},
		},
	}
	networkClass, _ := crossrefs.ParseClass("WeaviateB/Car")

	ok, err := peers.HasClass(networkClass)
	assert.Equal(t, true, ok, "class should exist")
	assert.Equal(t, nil, err, "should have no error")
}
