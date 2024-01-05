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

	"github.com/weaviate/weaviate/entities/backup"
)

const (
	_TimeoutShardCommit = 20 * time.Second
)

type reqStat struct {
	Starttime time.Time
	ID        string
	Status    backup.Status
	Path      string
}

type backupStat struct {
	sync.Mutex
	reqStat
}

func (s *backupStat) get() reqStat {
	s.Lock()
	defer s.Unlock()
	return s.reqStat
}

// renew state if and only it is not in use
// it returns "" in case of success and current id in case of failure
func (s *backupStat) renew(id string, path string) string {
	s.Lock()
	defer s.Unlock()
	if s.reqStat.ID != "" {
		return s.reqStat.ID
	}
	s.reqStat.ID = id
	s.reqStat.Path = path
	s.reqStat.Starttime = time.Now().UTC()
	s.reqStat.Status = backup.Started
	return ""
}

func (s *backupStat) reset() {
	s.Lock()
	s.reqStat.ID = ""
	s.reqStat.Path = ""
	s.reqStat.Status = ""
	s.Unlock()
}

func (s *backupStat) set(st backup.Status) {
	s.Lock()
	s.reqStat.Status = st
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
func (c *shardSyncChan) withCancellation(ctx context.Context, id string, done chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
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
	}()
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
