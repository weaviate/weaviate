//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"golang.org/x/sync/errgroup"
)

type MaintenanceCycles struct {
	commitLogMaintenance cyclemanager.CycleManager
	tombstoneCleanup     cyclemanager.CycleManager

	lock sync.Mutex
}

func (c *MaintenanceCycles) Init(
	commitlogMaintenanceTicker cyclemanager.CycleTicker,
	tombstoneCleanupTicker cyclemanager.CycleTicker,
) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.commitLogMaintenance == nil &&
		c.tombstoneCleanup == nil {
		c.commitLogMaintenance = cyclemanager.NewMulti(commitlogMaintenanceTicker)
		c.tombstoneCleanup = cyclemanager.NewMulti(tombstoneCleanupTicker)

		c.commitLogMaintenance.Start()
		c.tombstoneCleanup.Start()
	}
}

func (c *MaintenanceCycles) CommitLogMaintenance() cyclemanager.CycleManager {
	return c.commitLogMaintenance
}

func (c *MaintenanceCycles) TombstoneCleanup() cyclemanager.CycleManager {
	return c.tombstoneCleanup
}

// PauseMaintenance makes sure that no new background processes can be started.
// If a Combining or Condensing operation is already ongoing, the method blocks
// until the operation has either finished or the context expired
//
// If a Delete-Cleanup Cycle is running (TombstoneCleanupCycle), it is aborted,
// as it's not feasible to wait for such a cycle to complete, as it can take hours.
func (c *MaintenanceCycles) PauseMaintenance(ctx context.Context) error {
	c.lock.Lock()
	if c.commitLogMaintenance == nil &&
		c.tombstoneCleanup == nil {
		c.lock.Unlock()
		return nil
	}
	c.lock.Unlock()

	commitlogMaintenanceStop := make(chan error)
	tombstoneCleanupStop := make(chan error)
	go func() {
		if err := c.commitLogMaintenance.StopAndWait(ctx); err != nil {
			commitlogMaintenanceStop <- errors.Wrap(err, "long-running commitlog maintenance in progress")
			return
		}
		commitlogMaintenanceStop <- nil
	}()
	go func() {
		if err := c.tombstoneCleanup.StopAndWait(ctx); err != nil {
			tombstoneCleanupStop <- errors.Wrap(err, "long-running tombstone cleanup in progress")
			return
		}
		tombstoneCleanupStop <- nil
	}()

	commitlogMaintenanceStopErr := <-commitlogMaintenanceStop
	tombstoneCleanupStopErr := <-tombstoneCleanupStop
	if commitlogMaintenanceStopErr != nil && tombstoneCleanupStopErr != nil {
		return errors.Errorf("%s, %s", commitlogMaintenanceStopErr, tombstoneCleanupStopErr)
	}
	if commitlogMaintenanceStopErr != nil {
		// restart tombstone cleanup since it was successfully stopped.
		// both of these cycles must be either stopped or running.
		c.tombstoneCleanup.Start()
		return commitlogMaintenanceStopErr
	}
	if tombstoneCleanupStopErr != nil {
		// restart commitlog cycle since it was successfully stopped.
		// both of these cycles must be either stopped or running.
		c.commitLogMaintenance.Start()
		return tombstoneCleanupStopErr
	}

	return nil
}

// ResumeMaintenance starts all async cycles.
func (c *MaintenanceCycles) ResumeMaintenance(ctx context.Context) error {
	c.lock.Lock()
	if c.commitLogMaintenance == nil &&
		c.tombstoneCleanup == nil {
		c.lock.Unlock()
		return nil
	}
	c.lock.Unlock()

	c.commitLogMaintenance.Start()
	c.tombstoneCleanup.Start()
	return nil
}

func (c *MaintenanceCycles) Shutdown(ctx context.Context) error {
	c.lock.Lock()
	if c.commitLogMaintenance == nil &&
		c.tombstoneCleanup == nil {
		c.lock.Unlock()
		return nil
	}
	c.lock.Unlock()

	eg := errgroup.Group{}
	eg.Go(func() error {
		if err := c.commitLogMaintenance.StopAndWait(ctx); err != nil {
			return errors.Wrap(err, "long-running commitlog maintenance in progress")
		}
		return nil
	})
	eg.Go(func() error {
		if err := c.tombstoneCleanup.StopAndWait(ctx); err != nil {
			return errors.Wrap(err, "long-running tombstone cleanup in progress")
		}
		return nil
	})
	return eg.Wait()
}
