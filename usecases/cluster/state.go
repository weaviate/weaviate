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

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type State struct {
	config   Config
	list     *memberlist.Memberlist
	delegate delegate
}

type Config struct {
	Hostname                string     `json:"hostname" yaml:"hostname"`
	GossipBindPort          int        `json:"gossipBindPort" yaml:"gossipBindPort"`
	DataBindPort            int        `json:"dataBindPort" yaml:"dataBindPort"`
	Join                    string     `json:"join" yaml:"join"`
	IgnoreStartupSchemaSync bool       `json:"ignoreStartupSchemaSync" yaml:"ignoreStartupSchemaSync"`
	SkipSchemaSyncRepair    bool       `json:"skipSchemaSyncRepair" yaml:"skipSchemaSyncRepair"`
	AuthConfig              AuthConfig `json:"auth" yaml:"auth"`
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

func Init(userConfig Config, dataPath string, logger logrus.FieldLogger) (_ *State, err error) {
	cfg := memberlist.DefaultLANConfig()
	cfg.LogOutput = newLogParser(logger)
	if userConfig.Hostname != "" {
		cfg.Name = userConfig.Hostname
	}
	state := State{
		config: userConfig,
		delegate: delegate{
			Name:     cfg.Name,
			dataPath: dataPath,
			log:      logger,
		},
	}
	if err := state.delegate.init(diskSpace); err != nil {
		logger.WithField("action", "init_state.delete_init").Error(err)
	}
	cfg.Delegate = &state.delegate
	cfg.Events = events{&state.delegate}
	if userConfig.GossipBindPort != 0 {
		cfg.BindPort = userConfig.GossipBindPort
	}

	if state.list, err = memberlist.Create(cfg); err != nil {
		logger.WithField("action", "memberlist_init").
			WithField("hostname", userConfig.Hostname).
			WithField("bind_port", userConfig.GossipBindPort).
			WithError(err).
			Error("memberlist not created")
		return nil, errors.Wrap(err, "create member list")
	}

	var joinAddr []string
	if userConfig.Join != "" {
		joinAddr = strings.Split(userConfig.Join, ",")
	}

	if len(joinAddr) > 0 {

		_, err := net.LookupIP(strings.Split(joinAddr[0], ":")[0])
		if err != nil {
			logger.WithField("action", "cluster_attempt_join").
				WithField("remote_hostname", joinAddr[0]).
				WithError(err).
				Warn("specified hostname to join cluster cannot be resolved. This is fine" +
					"if this is the first node of a new cluster, but problematic otherwise.")
		} else {
			_, err := state.list.Join(joinAddr)
			if err != nil {
				logger.WithField("action", "memberlist_init").
					WithField("remote_hostname", joinAddr).
					WithError(err).
					Error("memberlist join not successful")
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
		// TODO: how can we find out the actual data port as opposed to relying on
		// the convention that it's 1 higher than the gossip port
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), m.Port+1)
		i++
	}

	return out[:i]
}

// AllHostnames for live members, including self.
func (s *State) AllHostnames() []string {
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
	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		out[i] = m.Name
	}

	return out
}

// Candidates returns list of nodes (names) sorted by the
// free amount of disk space in descending order
func (s *State) Candidates() []string {
	return s.delegate.sortCandidates(s.AllNames())
}

// All node names (not their hostnames!) for live members, including self.
func (s *State) NodeCount() int {
	return s.list.NumMembers()
}

func (s *State) LocalName() string {
	return s.list.LocalNode().Name
}

func (s *State) ClusterHealthScore() int {
	return s.list.GetHealthScore()
}

func (s *State) NodeHostname(nodeName string) (string, bool) {
	for _, mem := range s.list.Members() {
		if mem.Name == nodeName {
			// TODO: how can we find out the actual data port as opposed to relying on
			// the convention that it's 1 higher than the gossip port
			return fmt.Sprintf("%s:%d", mem.Addr.String(), mem.Port+1), true
		}
	}

	return "", false
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
