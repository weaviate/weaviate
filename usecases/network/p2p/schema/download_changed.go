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
package schema

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
)

type schemaDownload struct {
	schema    schema.Schema
	peerIndex int
	err       error
}

// DownloadChanged iterates over all the peers. For those
// that are either newly added or have a new schema hash, a download
// of the schema from that particular peer will be initiated.
// Downloads happen concurrently, but this function blocks
// until all downloads either finished or timed out.
func DownloadChanged(peerList peers.Peers) peers.Peers {
	downloads := make(chan schemaDownload)
	downloadsStarted := 0

	for i, peer := range peerList {
		if peer.LastChange != peers.NoChange {
			downloadsStarted++
			go downloadPeerAtIndex(peer, i, downloads)
		}
	}

	for i := 0; i < downloadsStarted; i++ {
		result := <-downloads
		peerList[result.peerIndex].Schema = result.schema
		peerList[result.peerIndex].SchemaError = result.err
	}

	return peerList
}

func downloadPeerAtIndex(peer peers.Peer, i int, downloads chan schemaDownload) {
	schema, err := download(peer)
	downloads <- schemaDownload{schema, i, err}
}
