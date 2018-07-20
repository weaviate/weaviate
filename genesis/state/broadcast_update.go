package state

import (
	weaviate_client "github.com/creativesoftwarefdn/weaviate/client"
	weaviate_p2p "github.com/creativesoftwarefdn/weaviate/client/p2_p"
	weaviate_models "github.com/creativesoftwarefdn/weaviate/models"

	log "github.com/sirupsen/logrus"
)

func broadcast_update(peer Peer, peers []Peer) {
	log.Debugf("Broadcasting peer update to %v", peer.Id)

	transport_config := weaviate_client.TransportConfig{
		Host:     "localhost:8000",
		BasePath: "/weaviate/v1/",
		Schemes:  []string{"http"},
	}

	peer_updates := make(weaviate_models.PeerUpdateList, 0)

	for _, peer := range peers {
		peer_update := weaviate_models.PeerUpdate{
			Host: peer.Host(),
			ID:   peer.Id,
			Name: peer.Name(),
		}

		peer_updates = append(peer_updates, &peer_update)
	}

	client := weaviate_client.NewHTTPClientWithConfig(nil, &transport_config)
	params := weaviate_p2p.NewWeaviateP2pGenesisUpdateParams()
	params.Peers = peer_updates
	_, err := client.P2P.WeaviateP2pGenesisUpdate(params)
	if err != nil {
		log.Debugf("Failed to update %v, because %v", peer.Id, err)
	}
}
