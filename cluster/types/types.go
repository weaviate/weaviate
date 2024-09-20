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

package types

import (
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ClassState represent a class and it's associated sharding state
type ClassState struct {
	Class  models.Class
	Shards sharding.State
}

// RaftResolver is passed to raft to resolver node ids to their real ip:port so that tranport can be established.
type RaftResolver interface {
	ServerAddr(id raft.ServerID) (raft.ServerAddress, error)
	NewTCPTransport(bindAddr string, advertise net.Addr, maxPool int, timeout time.Duration, logger *logrus.Logger) (*raft.NetworkTransport, error)
	NotResolvedNodes() map[raft.ServerID]struct{}
}

const (
	TenantActivityStatusFREEZING   = "FREEZING"
	TenantActivityStatusUNFREEZING = "UNFREEZING"
)
