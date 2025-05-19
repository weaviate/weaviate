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

package replication

import (
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
)

var (
	GatewayBackoffMaxInterval   = 15 * time.Second
	GatewayInitialBackoffPeriod = 5 * time.Second
)

type OpsScheduleMetadata struct {
	lastScheduled time.Time
	nextSchedule  time.Time

	executionAttempt uint64
	m                sync.RWMutex
	expBackoff       *backoff.ExponentialBackOff
}

func NewOpsScheduleMetadata() *OpsScheduleMetadata {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxInterval = GatewayBackoffMaxInterval
	expBackoff.InitialInterval = GatewayInitialBackoffPeriod
	return &OpsScheduleMetadata{
		lastScheduled: time.Now(),
		nextSchedule:  time.Now().Add(-time.Second * 10),
		expBackoff:    expBackoff,
	}
}

type OpsGateway struct {
	opsToMetadata sync.Map
}

func NewOpsGateway() *OpsGateway {
	return &OpsGateway{
		opsToMetadata: sync.Map{},
	}
}

func (og *OpsGateway) CanSchedule(opId uint64) (bool, time.Time) {
	v, _ := og.opsToMetadata.LoadOrStore(opId, NewOpsScheduleMetadata())
	metadata, ok := v.(*OpsScheduleMetadata)
	if !ok {
		// This should never happen
		return false, time.Now()
	}
	metadata.m.RLock()
	defer metadata.m.RUnlock()

	return metadata.nextSchedule.Before(time.Now()), metadata.nextSchedule
}

func (og *OpsGateway) ScheduleNow(opId uint64) {
	v, _ := og.opsToMetadata.LoadOrStore(opId, NewOpsScheduleMetadata())
	metadata, ok := v.(*OpsScheduleMetadata)
	if !ok {
		// This should never happen
		return
	}
	metadata.m.Lock()
	defer metadata.m.Unlock()

	metadata.lastScheduled = time.Now()
	metadata.executionAttempt += 1
	og.opsToMetadata.Store(opId, metadata)
}

func (og *OpsGateway) RegisterFinished(opId uint64) {
	og.opsToMetadata.Delete(opId)
}

func (og *OpsGateway) RegisterFailure(opId uint64) {
	v, ok := og.opsToMetadata.Load(opId)
	if !ok {
		// This should never happen
		return
	}
	metadata, ok := v.(*OpsScheduleMetadata)
	if !ok {
		// This should never happen
		return
	}
	metadata.m.Lock()
	defer metadata.m.Unlock()

	// The op just failed, let's backoff
	metadata.nextSchedule = time.Now().Add(metadata.expBackoff.NextBackOff())
}
