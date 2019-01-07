/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
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
