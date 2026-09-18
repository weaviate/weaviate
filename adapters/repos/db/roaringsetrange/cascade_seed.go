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

	"github.com/sirupsen/logrus"
)

// CascadeSeedEnabledEnv switches the seeded cascade off. Unset defaults to on,
// and it is read once at startup, so a change needs a restart.
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

// LogCascadeSeedConfig states which position the switch is in and warns when it
// holds a value it cannot read. The switch is parsed at init, where there is no
// logger yet, so both checks run from here at startup instead.
func LogCascadeSeedConfig(logger logrus.FieldLogger) {
	logCascadeSeedState(logger, cascadeSeedEnabled)
	logCascadeSeedConfig(logger, os.Getenv(CascadeSeedEnabledEnv))
}

// logCascadeSeedState takes the value init parsed instead of reading the
// environment again, so the position an operator reads here is the one the
// running code is in. It logs on every boot because a switch whose position
// cannot be read off the log is not a switch anyone can rely on mid-incident.
func logCascadeSeedState(logger logrus.FieldLogger, enabled bool) {
	state := "disabled"
	if enabled {
		state = "enabled"
	}

	logger.WithField("enabled", enabled).Infof(
		"%s: range cascade seeding is %s", CascadeSeedEnabledEnv, state)
}

// An unset switch is the normal case, so only a value that was set and could
// not be read is worth a line. Warning on every boot would train operators to
// scroll past the one boot where it matters.
func logCascadeSeedConfig(logger logrus.FieldLogger, v string) {
	if strings.TrimSpace(v) == "" {
		return
	}
	if _, recognised := parseBoolEnv(v); recognised {
		return
	}

	logger.WithField("value", v).Warnf(
		"%s value not recognized, cascade seeding remains enabled; "+
			"set it to one of %s to switch seeding off",
		CascadeSeedEnabledEnv, cascadeSeedOffValues)
}

// cascadeSeedOffValues spells the off values out in the warning itself, so an
// operator reading it mid-incident does not have to find this file.
const cascadeSeedOffValues = "off, disabled, 0, false"
