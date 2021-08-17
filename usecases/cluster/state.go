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
	Hostname string `json:"hostname" yaml:"hostname"`
	BindPort int    `json:"bindPort" yaml:"bindPort"`
	Join     string `json:"join" yaml:"join"`
}

func Init(userConfig Config, logger logrus.FieldLogger) (*State, error) {
	cfg := memberlist.DefaultLocalConfig()
	cfg.LogOutput = newLogParser(logger)

	if userConfig.Hostname != "" {
		cfg.Name = userConfig.Hostname
	}

	if userConfig.BindPort != 0 {
		cfg.BindPort = userConfig.BindPort
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

	return nil, nil
}

// Hostnames for all live members
func (s *State) Hostnames() []string {
	mem := s.list.Members()
	out := make([]string, len(mem))

	for i, m := range mem {
		out[i] = fmt.Sprintf("%s:%d", m.Addr.String(), m.Port)
	}

	return out
}
