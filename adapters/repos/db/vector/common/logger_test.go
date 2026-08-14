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

package common

import (
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestLoggerOrDiscard(t *testing.T) {
	supplied := logrus.New()

	tests := []struct {
		name     string
		logger   logrus.FieldLogger
		wantSame bool
	}{
		{
			name:     "a supplied logger is handed back untouched",
			logger:   supplied,
			wantSame: true,
		},
		{
			// callers tag the logger before passing it on, and a substitute here
			// would drop the fields they added
			name:     "a logger already carrying fields is handed back untouched",
			logger:   supplied.WithField("index_id", "geo.location"),
			wantSame: true,
		},
		{
			name:   "no logger yields one that discards its output",
			logger: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := LoggerOrDiscard(test.logger)
			require.NotNil(t, got)

			if test.wantSame {
				require.Same(t, test.logger, got)
				return
			}

			// a substitute that wrote to the default output would put an index's
			// internals on an operator's stderr
			discard, ok := got.(*logrus.Logger)
			require.True(t, ok, "the substitute must be a logger this package built")
			require.Equal(t, io.Discard, discard.Out)
		})
	}
}
