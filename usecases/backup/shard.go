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

package backup

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/entities/backup"
)

const (
	_TimeoutShardCommit = 20 * time.Second
)

type reqState struct {
	Starttime      time.Time
	ID             string
	Status         backup.Status
	Path           string
	OverrideBucket string
	OverridePath   string
}

type backupStat struct {
	sync.Mutex
	reqState
}

func (s *backupStat) get() reqState {
	s.Lock()
	defer s.Unlock()
	return s.reqState
}

// renew state if and only it is not in use
// it returns "" in case of success and current id in case of failure
func (s *backupStat) renew(id string, path string, overrideBucket, overridePath string) string {
	s.Lock()
	defer s.Unlock()
	if s.reqState.ID != "" {
		return s.reqState.ID
	}
	s.reqState.ID = id
	s.reqState.Path = path
	s.reqState.OverrideBucket = overrideBucket
	s.reqState.OverridePath = overridePath
	s.reqState.Starttime = time.Now().UTC()
	s.reqState.Status = backup.Started
	return ""
}

func (s *backupStat) reset() {
	s.Lock()
	s.reqState.ID = ""
	s.reqState.Path = ""
	s.reqState.Status = ""
	s.reqState.OverrideBucket = ""
	s.reqState.OverridePath = ""
	s.Unlock()
}

func (s *backupStat) set(st backup.Status) {
	s.Lock()
	s.reqState.Status = st
	s.Unlock()
}

// shardSyncChan makes sure that a backup operation is mutually exclusive.
// It also contains the channel used to communicate with the coordinator.
type shardSyncChan struct {
	// lastOp makes sure backup operations are mutually exclusive
	lastOp backupStat

	// waitingForCoordinatorToCommit use while waiting for the coordinator to take the next action
	waitingForCoordinatorToCommit atomic.Bool
	//  coordChan used to communicate with the coordinator
	coordChan chan interface{}

	// lastAsyncError used for debugging when no metadata is created
	lastAsyncError error
}

// waitForCoordinator to confirm or to abort previous operation
func (c *shardSyncChan) waitForCoordinator(d time.Duration, id string) error {
	defer c.waitingForCoordinatorToCommit.Store(false)
	if d == 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timed out waiting for coordinator to commit")
		case v := <-c.coordChan:
			switch v := v.(type) {
			case AbortRequest:
				if v.ID == id {
					return fmt.Errorf("coordinator aborted operation")
				}
			case StatusRequest:
				if v.ID == id {
					return nil
				}
			}
		}
	}
}

// withCancellation return a new context which will be cancelled if the coordinator
// want to abort the commit phase
func (c *shardSyncChan) withCancellation(ctx context.Context, id string, done chan struct{}, logger logrus.FieldLogger) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	enterrors.GoWrapper(func() {
		defer cancel()
		for {
			select {
			case v := <-c.coordChan:
				switch v := v.(type) {
				case AbortRequest:
					if v.ID == id {
						return
					}
				}
			case <-done: // caller is done
				return
			}
		}
	}, logger)
	return ctx
}

// OnCommit will be triggered when the coordinator confirms the execution of a previous operation
func (c *shardSyncChan) OnCommit(ctx context.Context, req *StatusRequest) error {
	st := c.lastOp.get()
	if st.ID == req.ID && c.waitingForCoordinatorToCommit.Load() {
		c.coordChan <- *req
		return nil
	}
	return fmt.Errorf("shard has abandon backup operation")
}

// Abort tells a node to abort the previous backup operation
func (c *shardSyncChan) OnAbort(_ context.Context, req *AbortRequest) error {
	st := c.lastOp.get()
	if st.ID == req.ID {
		c.coordChan <- *req
		return nil
	}
	return nil
}
