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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"

	"github.com/weaviate/weaviate/entities/shutdownreport"
)

// Summary returns the fields of the summary line r logs.
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
