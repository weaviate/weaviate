package schema

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/network/common/peers"
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
