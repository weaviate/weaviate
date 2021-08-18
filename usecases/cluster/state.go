package cluster

import (
	"fmt"
	"strings"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type State struct {
	list *memberlist.Memberlist
}

type Config struct {
	Hostname       string `json:"hostname" yaml:"hostname"`
	GossipBindPort int    `json:"gossipBindPort" yaml:"gossipBindPort"`
	DataBindPort   int    `json:"dataBindPort" yaml:"dataBindPort"`
	Join           string `json:"join" yaml:"join"`
}

func Init(userConfig Config, logger logrus.FieldLogger) (*State, error) {
	cfg := memberlist.DefaultLocalConfig()
	cfg.LogOutput = newLogParser(logger)

	if userConfig.Hostname != "" {
		cfg.Name = userConfig.Hostname
	}

	if userConfig.GossipBindPort != 0 {
		cfg.BindPort = userConfig.GossipBindPort
	}

	list, err := memberlist.Create(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "create member list")
	}

	var joinAddr []string
	if userConfig.Join != "" {
		joinAddr = strings.Split(userConfig.Join, ",")
	}

	if len(joinAddr) > 0 {
		_, err := list.Join(joinAddr)
		if err != nil {
			return nil, errors.Wrap(err, "join cluster")
		}

	}

	return &State{list: list}, nil
}

// Hostnames for all live members, except self. Use AllHostnames to include
// self
func (s *State) Hostnames() []string {
	mem := s.list.Members()
	out := make([]string, len(mem))

	i := 0
	for _, m := range mem {
		if m.Name == s.list.LocalNode().Name {
			continue
		}
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), m.Port)
		i++
	}

	return out[:i]
}

// AllHostnames for live members, including self.
func (s *State) AllHostnames() []string {
	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), m.Port)
	}

	return out
}
