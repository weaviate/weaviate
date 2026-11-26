//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	NewTCPTransport(bindAddr string, advertise net.Addr, maxPool int, timeout time.Duration, logger *logrus.Logger) (RaftTransport, error)
	NotResolvedNodes() map[raft.ServerID]struct{}
}

// RaftTransport is the transport handed to raft; raft enables pre-vote only if the transport implements it.
type RaftTransport interface {
	raft.Transport
	raft.WithClose
	raft.WithPreVote
}

const (
	TenantActivityStatusFREEZING   = "FREEZING"
	TenantActivityStatusUNFREEZING = "UNFREEZING"
)
