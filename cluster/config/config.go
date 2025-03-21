package config

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/cluster/types"
)

type ClusterServiceConfig struct {
	ServiceConfig ServiceConfig
	RaftConfig    RaftConfig
}

type ServiceConfig struct {
	// Host is used by cluster internal gRPC server to listen on
	Host string
	// RPCPort is used by cluster internal gRPC server to listen on
	RPCPort int
	// RPCMessageMaxSize is the maximum message sized allowed on the internal gRPC communication
	RPCMessageMaxSize int

	// BootstrapTimeout is the time a node will notify other node that it is ready to bootstrap a cluster if it can't
	// find a an existing cluster to join
	BootstrapTimeout time.Duration
	// BootstrapExpect is the number of nodes this cluster expect to receive a notify from to start bootstrapping a
	// cluster
	BootstrapExpect int

	IsLocalHost   bool
	SentryEnabled bool
	Logger        *logrus.Logger
}

type RaftConfig struct {
	Logger *logrus.Logger

	// WorkDir is the directory RAFT will use to store config & snapshot
	WorkDir string
	// NodeID is this node id
	NodeID string

	// Host is used by raft internal API to listen on
	Host string
	// Port is used by raft internal API to listen on
	Port         int
	RaftResolver types.RaftResolver

	BootstrapExpect   int
	Voter             bool
	MetadataOnlyVoter bool

	// HeartbeatTimeout specifies the time in follower state without contact
	// from a leader before we attempt an election.
	HeartbeatTimeout time.Duration
	// ElectionTimeout specifies the time in candidate state without contact
	// from a leader before we attempt an election.
	ElectionTimeout time.Duration

	// Raft snapshot related settings

	// SnapshotThreshold controls how many outstanding logs there must be before
	// we perform a snapshot. This is to prevent excessive snapshotting by
	// replaying a small set of logs instead. The value passed here is the initial
	// setting used. This can be tuned during operation using ReloadConfig.
	SnapshotThreshold uint64

	// SnapshotInterval controls how often we check if we should perform a
	// snapshot. We randomly stagger between this value and 2x this value to avoid
	// the entire cluster from performing a snapshot at once. The value passed
	// here is the initial setting used. This can be tuned during operation using
	// ReloadConfig.
	SnapshotInterval time.Duration

	FSM fsm.FSM
}
