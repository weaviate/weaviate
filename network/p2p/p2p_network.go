/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package p2p

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/go-openapi/strfmt"

	"net/url"

	genesis_client "github.com/creativesoftwarefdn/weaviate/genesis/client"
	client_ops "github.com/creativesoftwarefdn/weaviate/genesis/client/operations"
	genesismodels "github.com/creativesoftwarefdn/weaviate/genesis/models"
	libnetwork "github.com/creativesoftwarefdn/weaviate/network"
)

const (
	NETWORK_STATE_BOOTSTRAPPING = "network bootstrapping"
	NETWORK_STATE_FAILED        = "network failed"
	NETWORK_STATE_HEALTHY       = "network healthy"
)

// ErrPeerNotFound because it was either never registered
// or was registered, but timed out in the meantime
var ErrPeerNotFound = errors.New("Peer does not exist or has been removed after being inactive")

// The real network implementation. Se also `fake_network.go`
type network struct {
	sync.Mutex

	// Peer ID assigned by genesis server
	peerID    strfmt.UUID
	peerName  string
	publicURL strfmt.URI

	state        string
	genesisURL   strfmt.URI
	messaging    *messages.Messaging
	client       genesis_client.WeaviateGenesisServer
	peers        libnetwork.Peers
	callbacks    []libnetwork.PeerUpdateCallback
	schemaGetter libnetwork.SchemaGetter
}

func BootstrapNetwork(m *messages.Messaging, genesisURL strfmt.URI, publicURL strfmt.URI, peerName string) (*libnetwork.Network, error) {
	if genesisURL == "" {
		return nil, fmt.Errorf("No genesis URL provided in network configuration")
	}

	genesis_uri, err := url.Parse(string(genesisURL))
	if err != nil {
		return nil, fmt.Errorf("Could not parse genesis URL '%v'", genesisURL)
	}

	if publicURL == "" {
		return nil, fmt.Errorf("No public URL provided in network configuration")
	}

	_, err = url.Parse(string(publicURL))
	if err != nil {
		return nil, fmt.Errorf("Could not parse public URL '%v'", publicURL)
	}

	if peerName == "" {
		return nil, fmt.Errorf("No peer name specified in network configuration")
	}

	transport_config := genesis_client.TransportConfig{
		Host:     genesis_uri.Host,
		BasePath: genesis_uri.Path,
		Schemes:  []string{genesis_uri.Scheme},
	}

	client := genesis_client.NewHTTPClientWithConfig(nil, &transport_config)

	n := network{
		publicURL:  publicURL,
		peerName:   peerName,
		state:      NETWORK_STATE_BOOTSTRAPPING,
		genesisURL: genesisURL,
		messaging:  m,
		client:     *client,
		peers:      make([]libnetwork.Peer, 0),
	}

	// Bootstrap the network in the background.
	go n.bootstrap()

	nw := libnetwork.Network(&n)
	return &nw, nil
}

func (n *network) bootstrap() {
	time.Sleep(10) //TODO: Use channel close to listen for when complete configuration is done.
	n.messaging.InfoMessage("Bootstrapping network")

	newPeer := genesismodels.PeerUpdate{
		PeerName: n.peerName,
		PeerURI:  n.publicURL,
	}

	params := client_ops.NewGenesisPeersRegisterParams()
	params.Body = &newPeer
	response, err := n.client.Operations.GenesisPeersRegister(params)
	if err != nil {
		n.messaging.ErrorMessage(fmt.Sprintf("Could not register this peer in the network, because: %+v", err))
		n.state = NETWORK_STATE_FAILED
	} else {
		n.state = NETWORK_STATE_HEALTHY
		n.peerID = response.Payload.Peer.ID
		n.messaging.InfoMessage(fmt.Sprintf("Registered at Genesis server with id '%v'", n.peerID))
	}

	go n.keepPinging()
}

func (n *network) IsReady() bool {
	return false
}

func (n *network) GetStatus() string {
	return n.state
}

func (n *network) ListPeers() (libnetwork.Peers, error) {
	return n.peers, nil
}

func (n *network) GetPeerByName(name string) (libnetwork.Peer, error) {
	for _, peer := range n.peers {
		if peer.Name == name {
			return peer, nil
		}
	}

	return libnetwork.Peer{}, ErrPeerNotFound
}

func (n *network) keepPinging() {
	for {
		time.Sleep(30 * time.Second)
		n.ping()
	}
}

func (n *network) ping() {
	n.messaging.InfoMessage("Pinging Genesis server")

	if n.schemaGetter == nil {
		n.messaging.InfoMessage("cannot ping genesis server: no SchemaGetter present on network")
		return
	}

	currentSchema := n.schemaGetter.Schema()
	_ = currentSchema

	n.Lock()
	params := client_ops.NewGenesisPeersPingParams()
	params.PeerID = n.peerID
	hash, err := schemaHash(currentSchema)
	if err != nil {
		n.messaging.InfoMessage(fmt.Sprintf("cannot ping genesis server: %s", err))
		return
	}

	params.Body = &genesismodels.PeerPing{
		SchemaHash: hash,
	}
	n.Unlock()
	_, err = n.client.Operations.GenesisPeersPing(params)
	if err != nil {
		n.messaging.InfoMessage(fmt.Sprintf("Could not ping Genesis server; %+v", err))
	}
}

// GetNetworkResolver for now simply returns itself
// because the network is not fully plugable yet.
// Once we have made the network pluggable, then this would
// be a method on the connector which returns the actual
// plugged in Network
func (n *network) GetNetworkResolver() libnetwork.Network {
	return n
}

func (n *network) RegisterSchemaGetter(schemaGetter libnetwork.SchemaGetter) {
	n.schemaGetter = schemaGetter
}

func schemaHash(s schema.Schema) (string, error) {
	schemaBytes, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("couldnt convert schema to json before hashing: %s", err)
	}

	hash := md5.New()
	fmt.Fprintf(hash, "%s", schemaBytes)
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
