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

package roaringsetrange

import (
	"os"
	"strings"
)

// CascadeSeedEnabledEnv switches the seeded cascade off; unset defaults to on,
// and it's read once at startup so a change needs a restart. Named positively
// because a *_DISABLED variable's own trailing word, used as a value, parses
// as falsy and would leave the feature on.
const CascadeSeedEnabledEnv = "QUERY_RANGEABLE_CASCADE_SEED_ENABLED"

var cascadeSeedEnabled = parseCascadeSeedEnabled(os.Getenv(CascadeSeedEnabledEnv))

// parseBoolEnv classifies a value three ways, not entcfg.Enabled's two: it
// keeps a deliberate off distinct from an unrecognised value, so a typo isn't
// silently read as intentional. It also trims, which entcfg.Enabled does not.
func parseBoolEnv(v string) (value, recognised bool) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "on", "enabled", "1", "true":
		return true, true
	case "off", "disabled", "0", "false":
		return false, true
	default:
		return false, false
	}
}

// parseCascadeSeedEnabled reports whether seeding stays on. Unset means on, and
// an unrecognised value leaves it on rather than failing to boot: a typo in a
// perf knob shouldn't take a node out of a cluster.
func parseCascadeSeedEnabled(v string) bool {
	enabled, recognised := parseBoolEnv(v)
	if !recognised {
		return true
	}
	return enabled
}
