package network

import (
	"fmt"
	"sync"
	"time"

	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/go-openapi/strfmt"

	genesis_client "github.com/creativesoftwarefdn/weaviate/genesis/client"
	client_ops "github.com/creativesoftwarefdn/weaviate/genesis/client/operations"
	genesis_models "github.com/creativesoftwarefdn/weaviate/genesis/models"
)

const (
	NETWORK_STATE_BOOTSTRAPPING = "network bootstrapping"
	NETWORK_STATE_FAILED        = "network failed"
	NETWORK_STATE_HEALTHY       = "network healthy"
)

// The real network implementation. Se also `fake_network.go`
type network struct {
	sync.Mutex

	// Peer ID assigned by genesis server
	peer_id strfmt.UUID

	state       string
	genesis_url strfmt.URI
	messaging   *messages.Messaging
	client      genesis_client.WeaviateGenesisServer
	peers       []Peer
}

func BootstrapNetwork(m *messages.Messaging, genesis_url strfmt.URI) (Network, error) {
	transport_config := genesis_client.TransportConfig{
		Host:     "localhost:8001",
		BasePath: "/",
		Schemes:  []string{"http"},
	}
	client := genesis_client.NewHTTPClientWithConfig(nil, &transport_config)

	n := network{
		state:       NETWORK_STATE_BOOTSTRAPPING,
		genesis_url: genesis_url,
		messaging:   m,
		client:      *client,
		peers:       make([]Peer, 0),
	}

	// Bootstrap the network in the background.
	go n.bootstrap()

	return &n, nil
}

func (n *network) bootstrap() {
	time.Sleep(10) //TODO: Use channel close to listen for when complete configuration is done.
	n.messaging.InfoMessage("Bootstrapping network")

	new_peer := genesis_models.PeerUpdate{
		PeerName: "test",
		PeerHost: "test",
	}
	params := client_ops.NewGenesisPeersRegisterParams()
	params.Body = &new_peer
	response, err := n.client.Operations.GenesisPeersRegister(params)
	if err != nil {
		n.messaging.ErrorMessage(fmt.Sprintf("Could not register this peer in the network, because: %+v", err))
		n.state = NETWORK_STATE_FAILED
	} else {
		n.state = NETWORK_STATE_HEALTHY
		n.peer_id = response.Payload.Peer.ID
		n.messaging.InfoMessage(fmt.Sprintf("Registered at Genesis server with id '%v'", n.peer_id))
	}

	go n.keep_pinging()
}

func (n *network) IsReady() bool {
	return false
}

func (n *network) GetStatus() string {
	return n.state
}

func (n *network) ListPeers() ([]Peer, error) {
	return nil, fmt.Errorf("Cannot list peers, because there is no network configured")
}

func (n *network) UpdatePeers(new_peers []Peer) error {
	n.Lock()
	defer n.Unlock()

	n.messaging.InfoMessage(fmt.Sprintf("Received updated peer list with %v peers", len(new_peers)))

	n.peers = new_peers

	return nil
}

func (n *network) keep_pinging() {
	for {
		time.Sleep(30 * time.Second)
		n.messaging.InfoMessage("Pinging Genesis server")

		n.Lock()
		params := client_ops.NewGenesisPeersPingParams()
		params.PeerID = n.peer_id
		n.Unlock()
		_, err := n.client.Operations.GenesisPeersPing(params)
		if err != nil {
			n.messaging.InfoMessage(fmt.Sprintf("Could not ping Genesis server; %+v", err))
		}
	}
}
