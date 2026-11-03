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

package rest

import (
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/cron"
	"github.com/weaviate/weaviate/usecases/license"
	"github.com/weaviate/weaviate/wl/namespaces/suspendsweep"
)

func TestSuspendSweepFor(t *testing.T) {
	cases := []struct {
		name      string
		mode      license.Mode
		wantSweep bool
	}{
		{name: "off", mode: license.FeatureOff},
		{name: "unlicensed", mode: license.FeatureUnlicensed},
		{name: "a mode outside the three", mode: license.Mode(99)},
		{name: "licensed", mode: license.FeatureLicensed, wantSweep: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builds := 0
			newSweep := suspendSweepFor(tc.mode, func(logger *logrus.Entry) cron.SuspendSweep {
				builds++
				return suspendsweep.New(logger, nil, 1)
			})

			require.Zero(t, builds, "the gate leaves the build to the cron job")
			require.Equal(t, tc.wantSweep, newSweep != nil)
			if newSweep == nil {
				return
			}
			logger, _ := logrustest.NewNullLogger()
			require.True(t, license.DeclaredInWL(newSweep(logrus.NewEntry(logger))))
		})
	}
}
