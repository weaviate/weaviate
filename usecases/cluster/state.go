package cluster

import (
	"strings"

	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type State struct{}

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

	// for _, member := range list.Members() {
	// 	fmt.Printf("Member: %s %s\n", member.Name, member.Addr)
	// }

	return nil, nil
}
