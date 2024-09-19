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

package querier

import (
	"errors"
	"fmt"
	"sync"
)

type QuerierManager struct {
	mu                 sync.Mutex
	registeredQueriers map[*Querier]struct{}
}

func NewQuerierManager() *QuerierManager {
	return &QuerierManager{
		registeredQueriers: make(map[*Querier]struct{}),
	}
}

func (qm *QuerierManager) Register(q *Querier) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.registeredQueriers[q] = struct{}{}
}

func (qm *QuerierManager) Unregister(q *Querier) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	delete(qm.registeredQueriers, q)
	close(q.classTenantDataUpdates)
}

func (qm *QuerierManager) NotifyClassTenantDataUpdate(ct ClassTenant) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	notifyFailedErrors := []error{}
	for q := range qm.registeredQueriers {
		select {
		case q.classTenantDataUpdates <- ct:
			// TODO log debug
			fmt.Println("sent class tenant data update to querier")
		default:
			// TODO better error
			notifyFailedErrors = append(notifyFailedErrors, fmt.Errorf("failed to notify querier: %v", q))
			continue
		}
	}
	return errors.Join(notifyFailedErrors...)
}

type Querier struct {
	classTenantDataUpdates chan ClassTenant
}

func NewQuerier() *Querier {
	return &Querier{
		classTenantDataUpdates: make(chan ClassTenant, 100), // TODO is 100 a good buffer size? config?
	}
}

func (q *Querier) ClassTenantDataUpdates() <-chan ClassTenant {
	return q.classTenantDataUpdates
}

type ClassTenant struct {
	ClassName  string
	TenantName string
}
