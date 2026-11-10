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
	"github.com/weaviate/weaviate/usecases/cron"
	"github.com/weaviate/weaviate/usecases/license"
)

// suspendSweepFor returns build, the only wl/ code of the suspend reconcile
// job, in FeatureLicensed alone. Every other mode gets nil, so the job does not
// run.
func suspendSweepFor(mode license.Mode, build cron.NewSuspendSweep) cron.NewSuspendSweep {
	switch mode {
	case license.FeatureLicensed:
		return build
	case license.FeatureOff, license.FeatureUnlicensed:
	}
	return nil
}
