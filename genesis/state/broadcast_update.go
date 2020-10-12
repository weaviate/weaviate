//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package state

func broadcast_update(peer Peer, peers []Peer) {
	// log.Debugf("Broadcasting peer update to %v", peer.Id)
	// peer_uri, err := url.Parse(string(peer.URI()))

	// if err != nil {
	// 	log.Infof("Could not broadcast to peer %v; Peer URI is invalid (%v)", peer.Id, peer.URI())
	// 	return
	// }

	// transport_config := weaviate_client.TransportConfig{
	// 	Host:     peer_uri.Host,
	// 	BasePath: peer_uri.Path,
	// 	Schemes:  []string{peer_uri.Scheme},
	// }

	// peer_updates := make(weaviate_models.PeerUpdateList, 0)

	// for _, peer := range peers {
	// 	peer_update := weaviate_models.PeerUpdate{
	// 		URI:        peer.URI(),
	// 		ID:         peer.Id,
	// 		Name:       peer.Name(),
	// 		SchemaHash: peer.SchemaHash,
	// 	}

	// 	peer_updates = append(peer_updates, &peer_update)
	// }

	// client := weaviate_client.NewHTTPClientWithConfig(nil, &transport_config)
	// params := weaviate_p2p.NewP2pGenesisUpdateParams()
	// params.Peers = peer_updates
	// _, err = client.P2p.P2pGenesisUpdate(params)
	// if err != nil {
	// 	log.Debugf("Failed to update %v, because %v", peer.Id, err)
	// }
}
