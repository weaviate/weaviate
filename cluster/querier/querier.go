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
package querier

// import (
// 	"errors"
// 	"fmt"
// 	"sync"
// )

// // QuerierManager keeps track of registered querier nodes and allows one to notify all of them
// // of class/tenant data updates.
// type QuerierManager struct {
// 	mu                 sync.Mutex
// 	registeredQueriers map[*Querier]struct{}
// }

// func NewQuerierManager() *QuerierManager {
// 	return &QuerierManager{
// 		registeredQueriers: make(map[*Querier]struct{}),
// 	}
// }

// // Register registers a querier node with this manager.
// func (qm *QuerierManager) Register(q *Querier) {
// 	qm.mu.Lock()
// 	defer qm.mu.Unlock()
// 	qm.registeredQueriers[q] = struct{}{}
// }

// // Unregister unregisters a querier node from this manager and
// // closes its associated class tenant data updates channel.
// func (qm *QuerierManager) Unregister(q *Querier) {
// 	qm.mu.Lock()
// 	defer qm.mu.Unlock()
// 	delete(qm.registeredQueriers, q)
// 	close(q.classTenantDataUpdates)
// }

// // NotifyClassTenantDataUpdate notifies all registered querier nodes of a class tenant data update.
// // It returns an error if any of the notifications failed.
// // The notification is sent on the querier's class tenant data updates channel and is
// // non-blocking (if the channel is full, the notification is skipped).
// func (qm *QuerierManager) NotifyClassTenantDataUpdate(ct ClassTenant) error {
// 	qm.mu.Lock()
// 	defer qm.mu.Unlock()
// 	notifyFailedErrors := []error{}
// 	for q := range qm.registeredQueriers {
// 		select {
// 		case q.classTenantDataUpdates <- ct:
// 			// TODO log debug
// 			fmt.Println("sent class tenant data update to querier")
// 		default:
// 			// TODO better error
// 			notifyFailedErrors = append(notifyFailedErrors, fmt.Errorf("failed to notify querier: %v", q))
// 			continue
// 		}
// 	}
// 	return errors.Join(notifyFailedErrors...)
// }

// // Querier represents a querier node.
// type Querier struct {
// 	classTenantDataUpdates chan ClassTenant
// }

// // NewQuerier creates a new querier node. The class tenant data updates channel is buffered
// // to avoid blocking the sender, currently with a buffer size of 100.
// func NewQuerier() *Querier {
// 	return &Querier{
// 		classTenantDataUpdates: make(chan ClassTenant, 100), // TODO is 100 a good buffer size? config?
// 	}
// }

// // ClassTenantDataUpdates returns the channel on which class tenant data updates are sent.
// func (q *Querier) ClassTenantDataUpdates() <-chan ClassTenant {
// 	return q.classTenantDataUpdates
// }

// // ClassTenant represents a class/tenant pair.
// type ClassTenant struct {
// 	ClassName  string
// 	TenantName string
// }
