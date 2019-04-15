/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package refclasses

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithoutPeersAndWithoutClasses(t *testing.T) {
	result, err := FromPeers(peers.Peers{}, []crossrefs.NetworkClass{})
	assert.Nil(t, err, "should not error")
	assert.Equal(t, ByNetworkClass{}, result, "should return an empty map")
}

func TestWithoutPeersButWithClasses(t *testing.T) {
	classes := []crossrefs.NetworkClass{{
		PeerName: "BestPeer", ClassName: "BestClass",
	}}
	_, err := FromPeers(peers.Peers{}, classes)
	require.NotNil(t, err, "should error")
	assert.Equal(t, "could not build class 'BestPeer/BestClass': no peer 'BestPeer' in the network",
		err.Error(), "should have good error annotation")
}

func TestWithPeerWithEmptySchema(t *testing.T) {
	classes := []crossrefs.NetworkClass{{
		PeerName: "BestPeer", ClassName: "BestClass",
	}}
	peers := peers.Peers{
		peers.Peer{
			Name:   "BestPeer",
			Schema: schema.Schema{},
		},
	}

	_, err := FromPeers(peers, classes)
	require.NotNil(t, err, "should error")
	assert.Equal(t, "could not build class 'BestPeer/BestClass': peer 'BestPeer' has no such class",
		err.Error(), "should have good error annotation")
}

func TestWithClassWithPrimitiveProp(t *testing.T) {
	// arrange
	classes := []crossrefs.NetworkClass{{
		PeerName: "BestPeer", ClassName: "BestClass",
	}}
	peers := peers.Peers{
		peers.Peer{
			Name: "BestPeer",
			Schema: schema.Schema{
				Things: &models.SemanticSchema{
					Classes: []*models.SemanticSchemaClass{
						&models.SemanticSchemaClass{
							Class:       "BestClass",
							Description: "not the best class in the world, just a tribute",
							Properties: []*models.SemanticSchemaClassProperty{
								&models.SemanticSchemaClassProperty{
									DataType: []string{"string"},
									Name:     "bestString",
								},
							},
						},
					},
				},
			},
		},
	}
	expectedKey := crossrefs.NetworkClass{
		PeerName:  "BestPeer",
		ClassName: "BestClass",
	}

	// act
	result, err := FromPeers(peers, classes)

	//assert
	require.Nil(t, err, "should not error")
	obj := result[expectedKey]
	require.NotNil(t, obj, "should contain the class")
	assert.Equal(t, "BestPeer__BestClass", obj.Name(), "should have the right name")
	fields := obj.Fields()
	require.Len(t, fields, 2)
	require.NotNil(t, fields, "uuid")
	require.NotNil(t, fields, "bestString")
}

func TestWithClassWithReferenceProps(t *testing.T) {
	// arrange
	classes := []crossrefs.NetworkClass{{
		PeerName: "BestPeer", ClassName: "BestClass",
	}}
	peers := peers.Peers{
		peers.Peer{
			Name: "BestPeer",
			Schema: schema.Schema{
				Things: &models.SemanticSchema{
					Classes: []*models.SemanticSchemaClass{
						&models.SemanticSchemaClass{
							Class:       "BestClass",
							Description: "not the best class in the world, just a tribute",
							Properties: []*models.SemanticSchemaClassProperty{
								&models.SemanticSchemaClassProperty{
									DataType: []string{"string"},
									Name:     "bestString",
								},
								&models.SemanticSchemaClassProperty{
									DataType: []string{"SomeClass"},
									Name:     "LocalRef",
								},
								&models.SemanticSchemaClassProperty{
									DataType: []string{"OtherInstance/SomeClass"},
									Name:     "ForeignRef",
								},
							},
						},
					},
				},
			},
		},
	}
	expectedKey := crossrefs.NetworkClass{
		PeerName:  "BestPeer",
		ClassName: "BestClass",
	}

	// act
	result, err := FromPeers(peers, classes)

	//assert
	require.Nil(t, err, "should not error")
	fields := result[expectedKey].Fields()
	require.Len(t, fields, 2, "should omit all ref props")
	require.NotNil(t, fields, "uuid")
	require.NotNil(t, fields, "bestString")
}
