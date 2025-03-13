package fsm

import (
	"time"

	"github.com/sirupsen/logrus"
)

type FSMConfig struct {
	Logger       *logrus.Logger
	NodeID       string
	MetadataOnly bool

	// ConsistencyWaitTimeout is the duration we will wait for a schema version to land on that node
	ConsistencyWaitTimeout time.Duration
	// LastSnapshotIndex returns the latest snapshot index from raft storage.
	// This is necessary because the FSM can't find the index of a snapshot it is restoring from.
	// We need this information to verify if we need to reload the DB from the snapshot or not
	LastSnapshotIndex func() uint64
}
