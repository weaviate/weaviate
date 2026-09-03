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
	"os"

	entcfg "github.com/weaviate/weaviate/entities/config"
)

// bannerDisabled is read straight from the environment, like LOG_FORMAT and
// LOG_LEVEL.
func bannerDisabled() bool {
	return entcfg.Enabled(os.Getenv("DISABLE_STARTUP_BANNER"))
}
