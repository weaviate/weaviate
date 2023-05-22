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
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

type MaintenanceCycles struct {
	commitLogMaintenance cyclemanager.CycleManager
	tombstoneCleanup     cyclemanager.CycleManager

	lock sync.Mutex
}

// Creates internal cycle managers and immediately starts them.
//
// Cycle managers are inited and started just once.
// It is safe to call method multiple times
func (c *MaintenanceCycles) Init(
	commitlogMaintenanceTicker cyclemanager.CycleTicker,
	tombstoneCleanupTicker cyclemanager.CycleTicker,
) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.initedUnlocked() {
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
//
// PauseMaintenance is safe to call on uninitialized instance.
// It returns immediately without doing anything.
func (c *MaintenanceCycles) PauseMaintenance(ctx context.Context) error {
	if !c.inited() {
		return nil
	}

	commitLogMaintenanceErr, tombstoneCleanupErr := c.stopCycleManagers(ctx)
	len, err := c.combine(commitLogMaintenanceErr, tombstoneCleanupErr)

	// only one failed on stop attempt. start the other one back
	// if none or both failed, just return nil/error
	if len == 1 {
		if commitLogMaintenanceErr != nil {
			// restart tombstone cleanup since it was successfully stopped.
			// both of these cycles must be either stopped or running.
			c.tombstoneCleanup.Start()
		}
		if tombstoneCleanupErr != nil {
			// restart commitlog cycle since it was successfully stopped.
			// both of these cycles must be either stopped or running.
			c.commitLogMaintenance.Start()
		}
	}
	return err
}

// ResumeMaintenance starts all async cycle managers.
//
// ResumeMaintenance is safe to call on uninitialized instance.
// It returns immediately without doing anything.
func (c *MaintenanceCycles) ResumeMaintenance(ctx context.Context) error {
	if !c.inited() {
		return nil
	}

	c.commitLogMaintenance.Start()
	c.tombstoneCleanup.Start()
	return nil
}

// Shutdown stops internal cycle managers in parallel.
// Method blocks until the operation has either finished
// (all registered callbacks are stopped or finished)
// or the context expired
//
// Shutdown is safe to call on uninitialized instance.
// It returns immediately without doing anything.
func (c *MaintenanceCycles) Shutdown(ctx context.Context) error {
	if !c.inited() {
		return nil
	}

	_, err := c.combine(c.stopCycleManagers(ctx))
	return err
}

func (c *MaintenanceCycles) inited() bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.initedUnlocked()
}

func (c *MaintenanceCycles) initedUnlocked() bool {
	return c.commitLogMaintenance != nil &&
		c.tombstoneCleanup != nil
}

func (c *MaintenanceCycles) stopCycleManagers(ctx context.Context,
) (commitLogMaintenanceErr error, tombstoneCleanupErr error) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		if err := c.commitLogMaintenance.StopAndWait(ctx); err != nil {
			commitLogMaintenanceErr = errors.Wrap(err, "long-running commitlog maintenance in progress")
		}
		wg.Done()
	}()
	go func() {
		if err := c.tombstoneCleanup.StopAndWait(ctx); err != nil {
			tombstoneCleanupErr = errors.Wrap(err, "long-running tombstone cleanup in progress")
		}
		wg.Done()
	}()

	wg.Wait()
	return
}

func (c *MaintenanceCycles) combine(errors ...error) (int, error) {
	ec := &errorcompounder.ErrorCompounder{}
	for _, err := range errors {
		ec.Add(err)
	}
	return ec.Len(), ec.ToError()
}
