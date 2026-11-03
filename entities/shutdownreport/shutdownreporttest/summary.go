//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package shutdownreporttest reads shutdown reports in tests.
package shutdownreporttest

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/shutdownreport"
)

// Heartbeat returns the fields of the first heartbeat r logs, or nil. It fails t
// with assert rather than Fatal, so callbacks off the test goroutine can call it.
func Heartbeat(t testing.TB, r *shutdownreport.Report) logrus.Fields {
	t.Helper()
	logger, hook := test.NewNullLogger()
	stop := r.StartHeartbeat(logger, time.Nanosecond, time.Hour, time.Hour, nil)
	if !assert.Eventually(t, func() bool { return len(hook.AllEntries()) > 0 },
		5*time.Second, time.Millisecond, "no heartbeat logged") {
		return nil // stop would wait on a heartbeat stuck on a lock the caller holds
	}
	stop()
	return hook.AllEntries()[0].Data
}

// Summary returns the fields of the summary line that r logs.
func Summary(t testing.TB, r *shutdownreport.Report) logrus.Fields {
	t.Helper()
	logger, hook := test.NewNullLogger()
	r.Log(logger, shutdownreport.DefaultThresholds())
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.InfoLevel {
			return e.Data
		}
	}
	t.Fatal("no shutdown summary logged")
	return nil
}
