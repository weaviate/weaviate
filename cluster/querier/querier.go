package querier

import (
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
}

func (qm *QuerierManager) NotifyClassTenantDataUpdate(ct ClassTenant) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	for q := range qm.registeredQueriers {
		q.classTenantDataUpdates <- ct
	}
}

type Querier struct {
	nodeName               string
	classTenantDataUpdates chan ClassTenant
}

func NewQuerier() *Querier {
	return &Querier{
		classTenantDataUpdates: make(chan ClassTenant, 10), // TODO is 10 a good buffer size?
	}
}

func (q *Querier) SetNodeName(nodeName string) {
	q.nodeName = nodeName
}

func (q *Querier) ClassTenantDataUpdates() <-chan ClassTenant {
	return q.classTenantDataUpdates
}

func (q *Querier) Close() {
	close(q.classTenantDataUpdates)
}

type ClassTenant struct {
	ClassName  string
	TenantName string
}
