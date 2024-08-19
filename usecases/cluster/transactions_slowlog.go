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

package cluster

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errors"
)

func newTxSlowLog(logger logrus.FieldLogger) *txSlowLog {
	ageThreshold := 5 * time.Second
	changeThreshold := 1 * time.Second

	if age := os.Getenv("TX_SLOW_LOG_AGE_THRESHOLD_SECONDS"); age != "" {
		ageParsed, err := strconv.Atoi(age)
		if err == nil {
			ageThreshold = time.Duration(ageParsed) * time.Second
		}
	}

	if change := os.Getenv("TX_SLOW_LOG_CHANGE_THRESHOLD_SECONDS"); change != "" {
		changeParsed, err := strconv.Atoi(change)
		if err == nil {
			changeThreshold = time.Duration(changeParsed) * time.Second
		}
	}

	return &txSlowLog{
		logger:          logger,
		ageThreshold:    ageThreshold,
		changeThreshold: changeThreshold,
	}
}

// txSlowLog is meant as a temporary debugging tool for the v1 schema. When the
// v2 schema is ready, this can be thrown away.
type txSlowLog struct {
	sync.Mutex
	logger logrus.FieldLogger

	// tx-specific
	id           string
	status       string
	begin        time.Time
	lastChange   time.Time
	writable     bool
	coordinating bool
	txPresent    bool
	logged       bool

	// config
	ageThreshold    time.Duration
	changeThreshold time.Duration
}

func (txsl *txSlowLog) Start(id string, coordinating bool,
	writable bool,
) {
	txsl.Lock()
	defer txsl.Unlock()

	txsl.id = id
	txsl.status = "opened"
	now := time.Now()
	txsl.begin = now
	txsl.lastChange = now
	txsl.coordinating = coordinating
	txsl.writable = writable
	txsl.txPresent = true
	txsl.logged = false
}

func (txsl *txSlowLog) Update(status string) {
	txsl.Lock()
	defer txsl.Unlock()

	txsl.status = status
	txsl.lastChange = time.Now()
}

func (txsl *txSlowLog) Close(status string) {
	txsl.Lock()
	defer txsl.Unlock()

	txsl.status = status
	txsl.lastChange = time.Now()

	// there are two situations where we need to log the end of the transaction:
	//
	// 1. if it is slower than the age threshold
	//
	// 2. if we have logged it before (e.g. because it was in a specific state
	// longer than expected)

	if txsl.lastChange.Sub(txsl.begin) >= txsl.ageThreshold || txsl.logged {
		txsl.logger.WithFields(logrus.Fields{
			"action":         "transaction_slow_log",
			"event":          "tx_closed",
			"status":         txsl.status,
			"total_duration": txsl.lastChange.Sub(txsl.begin),
			"tx_id":          txsl.id,
			"coordinating":   txsl.coordinating,
			"writable":       txsl.writable,
		}).Infof("slow transaction completed")
	}

	// reset for next usage
	txsl.txPresent = false
}

func (txsl *txSlowLog) StartWatching() {
	t := time.Tick(500 * time.Millisecond)
	errors.GoWrapper(func() {
		for {
			<-t
			txsl.log()
		}
	}, txsl.logger)
}

func (txsl *txSlowLog) log() {
	txsl.Lock()
	defer txsl.Unlock()

	if !txsl.txPresent {
		return
	}

	now := time.Now()
	age := now.Sub(txsl.begin)
	changed := now.Sub(txsl.lastChange)

	if age >= txsl.ageThreshold || changed >= txsl.changeThreshold {
		txsl.logger.WithFields(logrus.Fields{
			"action":            "transaction_slow_log",
			"event":             "tx_in_progress",
			"status":            txsl.status,
			"total_duration":    age,
			"since_last_change": changed,
			"tx_id":             txsl.id,
			"coordinating":      txsl.coordinating,
			"writable":          txsl.writable,
		}).Infof("slow transaction in progress")

		txsl.logged = true
	}
}
