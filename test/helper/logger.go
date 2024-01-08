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

package helper

// Internal struct to link the HTTP client logging of the Weaviate API client to the test's logging output.

import "testing"

type testLogger struct {
	t *testing.T
}

func (tl *testLogger) Printf(format string, args ...interface{}) {
	tl.t.Logf("HTTP LOG:\n"+format, args...)
}

func (tl *testLogger) Debugf(format string, args ...interface{}) {
	tl.t.Logf("HTTP DEBUG:\n"+format, args...)
}
