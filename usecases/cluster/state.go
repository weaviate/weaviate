//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// NodeSelector is an interface to select a portion of the available nodes in memberlist
type NodeSelector interface {
	// NodeAddress resolves node id into an ip address without the port.
	NodeAddress(id string) string
	// NodeGRPCPort returns the gRPC port for a specific node id.
	NodeGRPCPort(id string) (int, error)
	// StorageCandidates returns list of storage nodes (names)
	// sorted by the free amount of disk space in descending orders
	StorageCandidates() []string
	// NonStorageNodes return nodes from member list which
	// they are configured not to be voter only
	NonStorageNodes() []string
	// SortCandidates Sort passed nodes names by the
	// free amount of disk space in descending order
	SortCandidates(nodes []string) []string
	// LocalName() return local node name
	LocalName() string
	// NodeHostname return hosts address for a specific node name
	NodeHostname(name string) (string, bool)
	AllHostnames() []string
	// AllOtherClusterMembers returns all cluster members discovered via memberlist with their raft addresses
	// This is useful for bootstrap when the join config is incomplete
	// TODO-RAFT: shall be removed once unifying with raft package
	AllOtherClusterMembers(port int) map[string]string
	// Leave marks the node as leaving the cluster (still visible but shutting down)
	Leave() error
	// Shutdown called when leaves the cluster gracefully and shuts down the memberlist instance
	Shutdown() error
}

type State struct {
	config Config
	// memberlist methods are thread safe
	// see https://github.com/hashicorp/memberlist/blob/master/memberlist.go#L502-L503

	list                 *memberlist.Memberlist
	nonStorageNodes      map[string]struct{}
	delegate             delegate
	maintenanceNodesLock sync.RWMutex
}

type Config struct {
	Hostname                string     `json:"hostname" yaml:"hostname"`
	GossipBindPort          int        `json:"gossipBindPort" yaml:"gossipBindPort"`
	DataBindPort            int        `json:"dataBindPort" yaml:"dataBindPort"`
	DataBindGrpcPort        int        `json:"dataBindGrpcPort" yaml:"dataBindGrpcPort"`
	Join                    string     `json:"join" yaml:"join"`
	IgnoreStartupSchemaSync bool       `json:"ignoreStartupSchemaSync" yaml:"ignoreStartupSchemaSync"`
	SkipSchemaSyncRepair    bool       `json:"skipSchemaSyncRepair" yaml:"skipSchemaSyncRepair"`
	AuthConfig              AuthConfig `json:"auth" yaml:"auth"`
	AdvertiseAddr           string     `json:"advertiseAddr" yaml:"advertiseAddr"`
	BindAddr                string     `json:"bindAddr" yaml:"bindAddr"`
	AdvertisePort           int        `json:"advertisePort" yaml:"advertisePort"`
	// MemberlistFastFailureDetection mostly for testing purpose, it will make memberlist sensitive and detect
	// failures (down nodes) faster.
	MemberlistFastFailureDetection bool `json:"memberlistFastFailureDetection" yaml:"memberlistFastFailureDetection"`
	// LocalHost flag enables running a multi-node setup with the same localhost and different ports
	Localhost bool `json:"localhost" yaml:"localhost"`
	// MaintenanceNodes is experimental. You should not use this directly, but should use the
	// public methods on the State struct. This is a list of nodes (by Hostname) that are in
	// maintenance mode (eg return a 418 for all data requests). We use a list here instead of a
	// bool because it allows us to set the same config/env vars on all nodes to put a subset of
	// them in maintenance mode. In addition, we may want to have the cluster nodes not in
	// maintenance mode be aware of which nodes are in maintenance mode in the future.
	MaintenanceNodes []string `json:"maintenanceNodes" yaml:"maintenanceNodes"`
	// RaftBootstrapExpect is used to detect split-brain scenarios and attempt to rejoin the cluster
	// TODO-RAFT-DB-63 : shall be removed once NodeAddress() is moved under raft cluster package
	RaftBootstrapExpect int
	// RequestQueueConfig is used to configure the request queue buffer for the replicated indices
	RequestQueueConfig RequestQueueConfig `json:"requestQueueConfig" yaml:"requestQueueConfig"`
}

type AuthConfig struct {
	BasicAuth BasicAuth `json:"basic" yaml:"basic"`
}

type BasicAuth struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

func (ba BasicAuth) Enabled() bool {
	return ba.Username != "" || ba.Password != ""
}

const (
	DefaultRequestQueueSize                   = 2000
	DefaultRequestQueueFullHttpStatus         = http.StatusTooManyRequests
	DefaultRequestQueueShutdownTimeoutSeconds = 90
)

// RequestQueueConfig is used to configure the request queue buffer for the replicated indices
type RequestQueueConfig struct {
	// IsEnabled is used to enable/disable the request queue, can be modified at runtime
	IsEnabled *configRuntime.DynamicValue[bool] `json:"isEnabled" yaml:"isEnabled"`
	// NumWorkers is used to configure the number of workers that handle requests from the queue
	NumWorkers int `json:"numWorkers" yaml:"numWorkers"`
	// QueueSize is used to configure the size of the request queue buffer
	QueueSize int `json:"queueSize" yaml:"queueSize"`
	// QueueFullHttpStatus is used to configure the http status code that is returned when the request queue is full
	// Should usually be set to 429 or 504 (429 will be retried by the coordinator, 504 will not)
	QueueFullHttpStatus int `json:"queueFullHttpStatus" yaml:"queueFullHttpStatus"`
	// QueueShutdownTimeoutSeconds is used to configure the timeout for the request queue shutdown.
	// This is the timeout for the workers to finish processing the requests in the queue
	// and for the request queue to be drained.
	// Should usually be set to 90 seconds, based on coordinator's timeout
	QueueShutdownTimeoutSeconds int `json:"queueShutdownTimeoutSeconds" yaml:"queueShutdownTimeoutSeconds"`
}

func Init(userConfig Config, raftTimeoutsMultiplier int, dataPath string, nonStorageNodes map[string]struct{}, logger logrus.FieldLogger) (_ *State, err error) {
	// Validate configuration first
	if err := validateClusterConfig(userConfig); err != nil {
		logger.Errorf("invalid cluster configuration: %v", err)
		return nil, errors.Wrap(err, "validate cluster config")
	}

	// Select appropriate memberlist configuration
	cfg := selectMemberlistConfig(userConfig)

	// Configure basic settings
	cfg.LogOutput = newLogParser(logger)
	cfg.Name = userConfig.Hostname

	// Configure addresses
	if err := configureMemberlistAddresses(cfg, userConfig); err != nil {
		logger.Errorf("failed to configure memberlist addresses: %v", err)
		return nil, errors.Wrap(err, "configure memberlist addresses")
	}

	// Configure ports
	configureMemberlistPorts(cfg, userConfig)

	// Configure additional settings
	configureMemberlistSettings(cfg, userConfig, raftTimeoutsMultiplier)

	// Create state
	state := State{
		config:          userConfig,
		nonStorageNodes: nonStorageNodes,
		delegate: delegate{
			Name:     cfg.Name,
			dataPath: dataPath,
			log:      logger,
			metadata: NodeMetadata{
				RestPort: userConfig.DataBindPort,
				GrpcPort: userConfig.DataBindGrpcPort,
			},
		},
	}

	// Initialize delegate
	if err := state.delegate.init(diskSpace); err != nil {
		logger.WithField("action", "init_state.delegate_init").Errorf("delegate init failed: %v", err)
		return nil, errors.Wrap(err, "delegate init")
	}

	// Set delegate and events
	cfg.Delegate = &state.delegate
	cfg.Events = events{&state.delegate}

	// Log configuration details
	logger.WithFields(logrus.Fields{
		"action":          "memberlist_config",
		"hostname":        userConfig.Hostname,
		"bind_addr":       cfg.BindAddr,
		"bind_port":       cfg.BindPort,
		"advertise_addr":  cfg.AdvertiseAddr,
		"advertise_port":  cfg.AdvertisePort,
		"config_type":     getConfigType(userConfig),
		"tcp_timeout":     cfg.TCPTimeout,
		"raft_multiplier": raftTimeoutsMultiplier,
	}).Info("memberlist configuration")

	// Create memberlist
	if state.list, err = memberlist.Create(cfg); err != nil {
		logger.WithFields(logrus.Fields{
			"action":         "memberlist_init",
			"hostname":       userConfig.Hostname,
			"bind_addr":      cfg.BindAddr,
			"bind_port":      cfg.BindPort,
			"advertise_addr": cfg.AdvertiseAddr,
			"advertise_port": cfg.AdvertisePort,
			"config_type":    getConfigType(userConfig),
		}).Errorf("memberlist not created: %v", err)
		return nil, errors.Wrap(err, "create memberlist")
	}
	var joinAddr []string
	if userConfig.Join != "" {
		joinAddr = strings.Split(userConfig.Join, ",")
	}

	if len(joinAddr) > 0 {
		_, err := net.LookupIP(strings.Split(joinAddr[0], ":")[0])
		if err != nil {
			logger.WithFields(logrus.Fields{
				"action":          "cluster_attempt_join",
				"remote_hostname": joinAddr[0],
			}).WithError(err).Warn(
				"specified hostname to join cluster cannot be resolved. This is fine" +
					"if this is the first node of a new cluster, but problematic otherwise.")
		} else {
			_, err := state.list.Join(joinAddr)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"action":          "memberlist_init",
					"remote_hostname": joinAddr,
				}).WithError(err).Error("memberlist join not successful")
				return nil, errors.Wrap(err, "join cluster")
			}
		}
	}

	return &state, nil
}

// Hostnames for all live members, except self. Use AllHostnames to include
// self, prefixes the data port.
func (s *State) Hostnames() []string {
	mem := s.list.Members()
	out := make([]string, len(mem))

	i := 0
	for _, m := range mem {
		if m.Name == s.list.LocalNode().Name {
			continue
		}

		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), s.dataPort(m))
		i++
	}

	return out[:i]
}

func nodeMetadata(m *memberlist.Node) (NodeMetadata, error) {
	if len(m.Meta) == 0 {
		return NodeMetadata{}, errors.New("no metadata available")
	}

	var meta NodeMetadata
	if err := json.Unmarshal(m.Meta, &meta); err != nil {
		return NodeMetadata{}, errors.Wrap(err, "unmarshal node metadata")
	}

	return meta, nil
}

func (s *State) dataPort(m *memberlist.Node) int {
	meta, err := nodeMetadata(m)
	if err != nil {
		s.delegate.log.WithFields(logrus.Fields{
			"action": "data_port_fallback",
			"node":   m.Name,
		}).WithError(err).Debug("unable to get node metadata, falling back to default data port")

		return int(m.Port) + 1 // the convention that it's 1 higher than the gossip port
	}

	return meta.RestPort
}

func (s *State) grpcPort(m *memberlist.Node) int {
	meta, err := nodeMetadata(m)
	if err != nil {
		s.delegate.log.WithFields(logrus.Fields{
			"action": "grpc_port_fallback",
			"node":   m.Name,
		}).WithError(err).Debug("unable to get node metadata, falling back to default gRPC port")

		return int(m.Port) + 2 // the convention that it's 2 higher than the gossip port
	}

	return meta.GrpcPort
}

// AllHostnames for live members, including self.
func (s *State) AllHostnames() []string {
	if s.list == nil {
		return []string{}
	}

	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), s.dataPort(m))
	}

	return out
}

// All node names (not their hostnames!) for live members, including self.
func (s *State) AllNames() []string {
	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		out[i] = m.Name
	}

	return out
}

// StorageNodes returns all nodes except non storage nodes
func (s *State) storageNodes() []string {
	if len(s.nonStorageNodes) == 0 {
		return s.AllNames()
	}

	members := s.list.Members()
	out := make([]string, len(members))
	n := 0
	for _, m := range members {
		name := m.Name
		if _, ok := s.nonStorageNodes[name]; !ok {
			out[n] = m.Name
			n++
		}
	}

	return out[:n]
}

// StorageCandidates returns list of storage nodes (names)
// sorted by the free amount of disk space in descending order
func (s *State) StorageCandidates() []string {
	return s.delegate.sortCandidates(s.storageNodes())
}

// NonStorageNodes return nodes from member list which
// they are configured not to be voter only
func (s *State) NonStorageNodes() []string {
	nonStorage := []string{}
	for name := range s.nonStorageNodes {
		nonStorage = append(nonStorage, name)
	}

	return nonStorage
}

// SortCandidates Sort passed nodes names by the
// free amount of disk space in descending order
func (s *State) SortCandidates(nodes []string) []string {
	return s.delegate.sortCandidates(nodes)
}

// All node names (not their hostnames!) for live members, including self.
func (s *State) NodeCount() int {
	return s.list.NumMembers()
}

// LocalName() return local node name
func (s *State) LocalName() string {
	return s.list.LocalNode().Name
}

// LocalAddr() returns local address
func (s *State) LocalAddr() string {
	if s.config.AdvertiseAddr == "" {
		return s.list.LocalNode().Addr.String()
	}

	return s.config.AdvertiseAddr
}

func (s *State) LocalBindAddr() string {
	if s.config.BindAddr == "" {
		return s.LocalAddr()
	}
	return s.config.BindAddr
}

func (s *State) ClusterHealthScore() int {
	return s.list.GetHealthScore()
}

func (s *State) NodeHostname(nodeName string) (string, bool) {
	for _, mem := range s.list.Members() {
		if mem.Name == nodeName {
			return fmt.Sprintf("%s:%d", mem.Addr.String(), s.dataPort(mem)), true
		}
	}

	return "", false
}

// NodeAddress is used to resolve the node name into an ip address without the port
// TODO-RAFT-DB-63 : shall be replaced by Members() which returns members in the list
func (s *State) NodeAddress(id string) string {
	addr, ok := s.NodeHostname(id)
	if !ok {
		return ""
	}

	return strings.Split(addr, ":")[0] // get address without port
}

// AllOtherClusterMembers returns all cluster members discovered via memberlist with their raft addresses
// This is useful for bootstrap when the join config is incomplete
func (s *State) AllOtherClusterMembers(port int) map[string]string {
	if s.list == nil {
		return map[string]string{}
	}

	members := s.list.Members()
	result := make(map[string]string, len(members))

	for _, m := range members {
		if m.Name == s.list.LocalNode().Name {
			// skip self
			continue
		}
		result[m.Name] = fmt.Sprintf("%s:%d", m.Addr.String(), port)
	}

	return result
}

// Leave marks the node as leaving the cluster (still visible but shutting down)
func (s *State) Leave() error {
	if s.list == nil {
		return fmt.Errorf("memberlist not initialized")
	}

	s.delegate.log.Info("marking node as gracefully leaving...")

	if err := s.list.Leave(5 * time.Second); err != nil {
		return fmt.Errorf("failed to leave memberlist: %w", err)
	}

	s.delegate.log.Info("successfully marked as leaving in memberlist")
	return nil
}

// Shutdown called when leaves the cluster gracefully and shuts down the memberlist instance
func (s *State) Shutdown() error {
	if s.list == nil {
		return fmt.Errorf("memberlist not initialized")
	}

	return s.list.Shutdown()
}

func (s *State) NodeGRPCPort(nodeID string) (int, error) {
	for _, mem := range s.list.Members() {
		if mem.Name == nodeID {
			return s.grpcPort(mem), nil
		}
	}
	return 0, fmt.Errorf("node not found: %s", nodeID)
}

func (s *State) SchemaSyncIgnored() bool {
	return s.config.IgnoreStartupSchemaSync
}

func (s *State) SkipSchemaRepair() bool {
	return s.config.SkipSchemaSyncRepair
}

func (s *State) NodeInfo(node string) (NodeInfo, bool) {
	return s.delegate.get(node)
}

// MaintenanceModeEnabledForLocalhost is experimental, may be removed/changed. It returns true if this node is in
// maintenance mode (which means it should return an error for all data requests).
func (s *State) MaintenanceModeEnabledForLocalhost() bool {
	return s.nodeInMaintenanceMode(s.config.Hostname)
}

// SetMaintenanceModeForLocalhost is experimental, may be removed/changed. Enables/disables maintenance
// mode for this node.
func (s *State) SetMaintenanceModeForLocalhost(enabled bool) {
	s.setMaintenanceModeForNode(s.config.Hostname, enabled)
}

func (s *State) setMaintenanceModeForNode(node string, enabled bool) {
	s.maintenanceNodesLock.Lock()
	defer s.maintenanceNodesLock.Unlock()

	if s.config.MaintenanceNodes == nil {
		s.config.MaintenanceNodes = []string{}
	}
	if !enabled {
		// we're disabling maintenance mode, remove the node from the list
		for i, enabledNode := range s.config.MaintenanceNodes {
			if enabledNode == node {
				s.config.MaintenanceNodes = append(s.config.MaintenanceNodes[:i], s.config.MaintenanceNodes[i+1:]...)
			}
		}
		return
	}
	if !slices.Contains(s.config.MaintenanceNodes, node) {
		// we're enabling maintenance mode, add the node to the list
		s.config.MaintenanceNodes = append(s.config.MaintenanceNodes, node)
		return
	}
}

func (s *State) nodeInMaintenanceMode(node string) bool {
	s.maintenanceNodesLock.RLock()
	defer s.maintenanceNodesLock.RUnlock()

	return slices.Contains(s.config.MaintenanceNodes, node)
}

// validateClusterConfig validates the cluster configuration
func validateClusterConfig(userConfig Config) error {
	// Validate port ranges
	if userConfig.GossipBindPort != 0 && (userConfig.GossipBindPort < 1024 || userConfig.GossipBindPort > 65535) {
		return fmt.Errorf("invalid GossipBindPort: %d (must be between 1024-65535)", userConfig.GossipBindPort)
	}

	if userConfig.DataBindPort != 0 && (userConfig.DataBindPort < 1024 || userConfig.DataBindPort > 65535) {
		return fmt.Errorf("invalid DataBindPort: %d (must be between 1024-65535)", userConfig.DataBindPort)
	}

	if userConfig.AdvertisePort != 0 && (userConfig.AdvertisePort < 1024 || userConfig.AdvertisePort > 65535) {
		return fmt.Errorf("invalid AdvertisePort: %d (must be between 1024-65535)", userConfig.AdvertisePort)
	}

	// Validate IP addresses
	if userConfig.AdvertiseAddr != "" && net.ParseIP(userConfig.AdvertiseAddr) == nil {
		return fmt.Errorf("invalid AdvertiseAddr: %s (must be a valid IP address)", userConfig.AdvertiseAddr)
	}

	if userConfig.BindAddr != "" && net.ParseIP(userConfig.BindAddr) == nil {
		return fmt.Errorf("invalid BindAddr: %s (must be a valid IP address)", userConfig.BindAddr)
	}

	// Validate hostname
	if userConfig.Hostname == "" {
		return fmt.Errorf("hostname cannot be empty")
	}

	return nil
}

// selectMemberlistConfig selects the appropriate memberlist configuration based on environment
func selectMemberlistConfig(userConfig Config) *memberlist.Config {
	var cfg *memberlist.Config

	// Explicit selection based on environment
	if userConfig.Localhost {
		cfg = memberlist.DefaultLocalConfig()
	} else if userConfig.AdvertiseAddr != "" {
		cfg = memberlist.DefaultWANConfig()
	} else {
		cfg = memberlist.DefaultLANConfig()
	}

	return cfg
}

// configureMemberlistPorts handles port configuration with clear logic
func configureMemberlistPorts(cfg *memberlist.Config, userConfig Config) {
	// Set bind port first
	if userConfig.GossipBindPort != 0 {
		cfg.BindPort = userConfig.GossipBindPort
	}

	// Set advertise port
	if userConfig.AdvertisePort != 0 {
		cfg.AdvertisePort = userConfig.AdvertisePort
	} else if userConfig.AdvertiseAddr != "" && userConfig.GossipBindPort != 0 {
		// Only set to GossipBindPort if AdvertiseAddr is set but AdvertisePort is not
		// to avoid defaulting to memberlist port 7946
		cfg.AdvertisePort = userConfig.GossipBindPort
	}
}

// configureMemberlistAddresses handles address configuration with validation
func configureMemberlistAddresses(cfg *memberlist.Config, userConfig Config) error {
	// Set bind address
	if userConfig.BindAddr != "" {
		cfg.BindAddr = userConfig.BindAddr
	}

	// Set advertise address
	if userConfig.AdvertiseAddr != "" {
		cfg.AdvertiseAddr = userConfig.AdvertiseAddr
	}

	return nil
}

// configureMemberlistSettings applies additional memberlist settings
func configureMemberlistSettings(cfg *memberlist.Config, userConfig Config, raftTimeoutsMultiplier int) {
	// Set dead node reclaim time to 60 seconds by default
	cfg.DeadNodeReclaimTime = 60 * time.Second

	// Configure timeouts and failure detection
	if userConfig.MemberlistFastFailureDetection {
		cfg.SuspicionMult = 1
		cfg.DeadNodeReclaimTime = 5 * time.Second
	}

	// Set TCP timeout based on configuration type
	if userConfig.AdvertiseAddr != "" {
		// WAN config - use 30 seconds
		cfg.TCPTimeout = 30 * time.Second * time.Duration(raftTimeoutsMultiplier)
	} else {
		// LAN/Local config - use 10 seconds
		cfg.TCPTimeout = 10 * time.Second * time.Duration(raftTimeoutsMultiplier)
	}
}

// getConfigType returns a string describing the configuration type
func getConfigType(userConfig Config) string {
	if userConfig.Localhost {
		return "LOCAL"
	} else if userConfig.AdvertiseAddr != "" {
		return "WAN"
	}
	return "LAN"
}
