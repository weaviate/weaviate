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

// Package querier provides abstractions to keep track of and manage querier nodes.
package metadataserver

import (
	"errors"
	"fmt"
	"sync"
)

// QuerierManager keeps track of registered querier nodes and allows one to notify all of them
// of class/tenant data updates.
type QuerierManager struct {
	registeredQueriers sync.Map
	// map[*Querier]struct{}
}

func NewQuerierManager() *QuerierManager {
	return &QuerierManager{
		registeredQueriers: sync.Map{},
	}
}

// Register registers a querier node with this manager.
func (qm *QuerierManager) Register(q *Querier) {
	qm.registeredQueriers.Store(q, struct{}{})
}

// Unregister unregisters a querier node from this manager and
// closes its associated class tenant data updates channel.
func (qm *QuerierManager) Unregister(q *Querier) {
	qm.registeredQueriers.Delete(q)
	close(q.classTenantDataEvents)
}

// NotifyClassTenantDataEvent notifies all registered querier nodes of a class tenant data update.
// It returns an error if any of the notifications failed.
// The notification is sent on the querier's class tenant data updates channel and is
// non-blocking (if the channel is full, the notification is skipped).
func (qm *QuerierManager) NotifyClassTenantDataEvent(ct ClassTenant) error {
	notifyFailedErrors := []error{}
	qm.registeredQueriers.Range(func(k, v any) bool {
		if k == nil {
			return true
		}
		q := k.(*Querier)
		select {
		case q.classTenantDataEvents <- ct:
			// TODO log debug
		default:
			// TODO better error
			notifyFailedErrors = append(notifyFailedErrors, fmt.Errorf("failed to notify querier: %v", q))
		}
		return true
	})
	return errors.Join(notifyFailedErrors...)
}

// Querier represents a querier node.
type Querier struct {
	classTenantDataEvents chan ClassTenant
}

// NewQuerier creates a new querier node. The class tenant data events channel is buffered
// to avoid blocking the sender, currently with a buffer size of 100.
func NewQuerier() *Querier {
	return &Querier{
		classTenantDataEvents: make(chan ClassTenant, 100), // TODO is 100 a good buffer size? config?
	}
}

// ClassTenantDataEvents returns the channel on which class tenant data events are sent.
func (q *Querier) ClassTenantDataEvents() <-chan ClassTenant {
	return q.classTenantDataEvents
}

// ClassTenant represents a class/tenant pair.
type ClassTenant struct {
	ClassName  string
	TenantName string
}
