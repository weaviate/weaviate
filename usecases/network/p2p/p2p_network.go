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
package p2p

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	genesis_client "github.com/creativesoftwarefdn/weaviate/genesis/client"
	client_ops "github.com/creativesoftwarefdn/weaviate/genesis/client/operations"
	genesismodels "github.com/creativesoftwarefdn/weaviate/genesis/models"
	libnetwork "github.com/creativesoftwarefdn/weaviate/usecases/network"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
	p2pschema "github.com/creativesoftwarefdn/weaviate/usecases/network/p2p/schema"
	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
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

	state           string
	genesisURL      strfmt.URI
	logger          logrus.FieldLogger
	client          genesis_client.WeaviateGenesisServer
	peers           peers.Peers
	callbacks       []libnetwork.PeerUpdateCallback
	schemaGetter    libnetwork.SchemaGetter
	downloadChanged downloadChangedFn
}

type downloadChangedFn func(peers.Peers) peers.Peers

// BootstrapNetwork with HTTP p2p functionality
func BootstrapNetwork(logger logrus.FieldLogger, genesisURL strfmt.URI, publicURL strfmt.URI, peerName string) (libnetwork.Network, error) {
	if genesisURL == "" {
		return nil, fmt.Errorf("No genesis URL provided in network configuration")
	}

	genesisURI, err := url.Parse(string(genesisURL))
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

	transportConfig := genesis_client.TransportConfig{
		Host:     genesisURI.Host,
		BasePath: genesisURI.Path,
		Schemes:  []string{genesisURI.Scheme},
	}

	client := genesis_client.NewHTTPClientWithConfig(nil, &transportConfig)

	n := network{
		publicURL:       publicURL,
		peerName:        peerName,
		state:           NETWORK_STATE_BOOTSTRAPPING,
		genesisURL:      genesisURL,
		logger:          logger,
		client:          *client,
		peers:           make([]peers.Peer, 0),
		downloadChanged: p2pschema.DownloadChanged,
	}

	// Bootstrap the network in the background.
	go n.bootstrap()

	return &n, nil
}

func (n *network) bootstrap() {
	time.Sleep(10) //TODO: Use channel close to listen for when complete configuration is done.
	n.logger.WithField("action", "network_bootstrap").Debug("network bootstrapping beginning")

	newPeer := genesismodels.PeerUpdate{
		PeerName: n.peerName,
		PeerURI:  n.publicURL,
	}

	params := client_ops.NewGenesisPeersRegisterParams()
	params.Body = &newPeer
	response, err := n.client.Operations.GenesisPeersRegister(params)
	if err != nil {
		n.logger.
			WithField("action", "network_bootstrap").
			WithError(err).
			Error("could not register peer in network")
		n.state = NETWORK_STATE_FAILED
	} else {
		n.state = NETWORK_STATE_HEALTHY
		n.peerID = response.Payload.Peer.ID
		n.logger.
			WithField("action", "network_bootstrap").
			WithField("peer_id", n.peerID).
			Info("registered at genesis server")
	}

	go n.keepPinging()
}

func (n *network) IsReady() bool {
	return false
}

func (n *network) GetStatus() string {
	return n.state
}

func (n *network) ListPeers() (peers.Peers, error) {
	return n.peers, nil
}

func (n *network) GetPeerByName(name string) (peers.Peer, error) {
	for _, peer := range n.peers {
		if peer.Name == name {
			return peer, nil
		}
	}

	return peers.Peer{}, ErrPeerNotFound
}

func (n *network) keepPinging() {
	for {
		time.Sleep(30 * time.Second)
		n.ping()
	}
}

func (n *network) ping() {
	n.logger.WithField("action", "network_ping").Debug("pinging genesis server")

	if n.schemaGetter == nil {
		n.logger.
			WithField("action", "network_ping").
			WithError(errors.New("no schema getter present")).
			Error("cannot ping gensis server")
		return
	}

	currentSchema, err := n.schemaGetter.Schema()
	if err != nil {
		n.logger.
			WithField("action", "network_ping").
			WithError(err).
			Error("cannot ping gensis server")
	}

	n.Lock()
	params := client_ops.NewGenesisPeersPingParams()
	params.PeerID = n.peerID
	hash, err := schemaHash(currentSchema)
	if err != nil {
		n.logger.
			WithField("action", "network_ping").
			WithError(err).
			Error("cannot ping gensis server")
		return
	}

	params.Body = &genesismodels.PeerPing{
		SchemaHash: hash,
	}
	n.Unlock()
	_, err = n.client.Operations.GenesisPeersPing(params)
	if err != nil {
		n.logger.
			WithField("action", "network_ping").
			WithError(err).
			Error("cannot ping gensis server")
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
