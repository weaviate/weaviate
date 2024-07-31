//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type State struct {
	config Config
	// that lock to serialize access to memberlist
	listLock        sync.RWMutex
	list            *memberlist.Memberlist
	nonStorageNodes map[string]struct{}
	delegate        delegate
}

type Config struct {
	Hostname                string     `json:"hostname" yaml:"hostname"`
	GossipBindPort          int        `json:"gossipBindPort" yaml:"gossipBindPort"`
	DataBindPort            int        `json:"dataBindPort" yaml:"dataBindPort"`
	Join                    string     `json:"join" yaml:"join"`
	IgnoreStartupSchemaSync bool       `json:"ignoreStartupSchemaSync" yaml:"ignoreStartupSchemaSync"`
	SkipSchemaSyncRepair    bool       `json:"skipSchemaSyncRepair" yaml:"skipSchemaSyncRepair"`
	AuthConfig              AuthConfig `json:"auth" yaml:"auth"`
	AdvertiseAddr           string     `json:"advertiseAddr" yaml:"advertiseAddr"`
	AdvertisePort           int        `json:"advertisePort" yaml:"advertisePort"`
	// LocalHost flag enables running a multi-node setup with the same localhost and different ports
	Localhost bool `json:"localhost" yaml:"localhost"`
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

func Init(userConfig Config, dataPath string, nonStorageNodes map[string]struct{}, logger logrus.FieldLogger) (_ *State, err error) {
	cfg := memberlist.DefaultLANConfig()
	cfg.LogOutput = newLogParser(logger)
	cfg.Name = userConfig.Hostname
	state := State{
		config:          userConfig,
		nonStorageNodes: nonStorageNodes,
		delegate: delegate{
			Name:     cfg.Name,
			dataPath: dataPath,
			log:      logger,
		},
	}
	if err := state.delegate.init(diskSpace); err != nil {
		logger.WithField("action", "init_state.delete_init").WithError(err).
			Error("delegate init failed")
	}
	cfg.Delegate = &state.delegate
	cfg.Events = events{&state.delegate}
	if userConfig.GossipBindPort != 0 {
		cfg.BindPort = userConfig.GossipBindPort
	}

	if userConfig.AdvertiseAddr != "" {
		cfg.AdvertiseAddr = userConfig.AdvertiseAddr
	}

	if userConfig.AdvertisePort != 0 {
		cfg.AdvertisePort = userConfig.AdvertisePort
	}

	if state.list, err = memberlist.Create(cfg); err != nil {
		logger.WithFields(logrus.Fields{
			"action":    "memberlist_init",
			"hostname":  userConfig.Hostname,
			"bind_port": userConfig.GossipBindPort,
		}).WithError(err).Error("memberlist not created")
		return nil, errors.Wrap(err, "create member list")
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
	s.listLock.RLock()
	defer s.listLock.RUnlock()

	mem := s.list.Members()
	out := make([]string, len(mem))

	i := 0
	for _, m := range mem {
		if m.Name == s.list.LocalNode().Name {
			continue
		}
		// TODO: how can we find out the actual data port as opposed to relying on
		// the convention that it's 1 higher than the gossip port
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), m.Port+1)
		i++
	}

	return out[:i]
}

// AllHostnames for live members, including self.
func (s *State) AllHostnames() []string {
	s.listLock.RLock()
	defer s.listLock.RUnlock()

	if s.list == nil {
		return []string{}
	}

	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		// TODO: how can we find out the actual data port as opposed to relying on
		// the convention that it's 1 higher than the gossip port
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), m.Port+1)
	}

	return out
}

// All node names (not their hostnames!) for live members, including self.
func (s *State) AllNames() []string {
	s.listLock.RLock()
	defer s.listLock.RUnlock()

	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		out[i] = m.Name
	}

	return out
}

// StorageNodes returns all nodes except non storage nodes
func (s *State) StorageNodes() []string {
	if len(s.nonStorageNodes) == 0 {
		return s.AllNames()
	}

	s.listLock.RLock()
	defer s.listLock.RUnlock()

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

// Candidates returns list of nodes (names) sorted by the
// free amount of disk space in descending order
func (s *State) Candidates() []string {
	return s.delegate.sortCandidates(s.StorageNodes())
}

// All node names (not their hostnames!) for live members, including self.
func (s *State) NodeCount() int {
	s.listLock.RLock()
	defer s.listLock.RUnlock()

	return s.list.NumMembers()
}

func (s *State) LocalName() string {
	s.listLock.RLock()
	defer s.listLock.RUnlock()

	return s.list.LocalNode().Name
}

func (s *State) ClusterHealthScore() int {
	s.listLock.RLock()
	defer s.listLock.RUnlock()

	return s.list.GetHealthScore()
}

func (s *State) NodeHostname(nodeName string) (string, bool) {
	s.listLock.RLock()
	defer s.listLock.RUnlock()

	for _, mem := range s.list.Members() {
		if mem.Name == nodeName {
			// TODO: how can we find out the actual data port as opposed to relying on
			// the convention that it's 1 higher than the gossip port
			return fmt.Sprintf("%s:%d", mem.Addr.String(), mem.Port+1), true
		}
	}

	return "", false
}

// NodeAddress is used to resolve the node name into an ip address without the port
func (s *State) NodeAddress(id string) string {
	s.listLock.RLock()
	defer s.listLock.RUnlock()
	for _, mem := range s.list.Members() {
		if mem.Name == id {
			return mem.Addr.String()
		}
	}
	return ""
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
