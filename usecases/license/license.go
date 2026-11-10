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

// Package license decides how a feature licensed under the Weaviate License
// runs on this node. It imports nothing from wl/, so an unlicensed node can
// refuse such a feature without running any wl/ code.
package license

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/sirupsen/logrus"

	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// ErrRequired matches every error Required returns.
var ErrRequired = errors.New("a well-formed Weaviate license key is required")

// Required returns the authzerrors.Forbidden that refuses a call to feature on
// a node without a well-formed license key. feature must hold no colon, so
// that namespacing.StripErrorMessage cannot cut the text.
func Required(feature string) error {
	return authzerrors.NewForbiddenWithCause(requiredError{feature: feature})
}

const licenseKeyInstruction = "set exactly one of LICENSE_KEY or LICENSE_KEY_FILE to a well-formed key and restart the node"

type requiredError struct{ feature string }

func (e requiredError) Error() string {
	return fmt.Sprintf("the %s feature needs a well-formed Weaviate license key on this node; %s", e.feature, licenseKeyInstruction)
}

func (requiredError) Is(target error) bool { return target == ErrRequired }

// Mode is how a licensed feature runs on this node. A switch on Mode lists
// every mode and has no default case, so the exhaustive linter flags a new one.
type Mode int

const (
	// FeatureOff means the feature's own flag is off, whatever the license.
	FeatureOff Mode = iota
	// FeatureUnlicensed means the flag is on and the node holds no well-formed
	// license key, so the feature refuses every call with Required.
	FeatureUnlicensed
	// FeatureLicensed is the only mode in which a feature may call wl/ code.
	FeatureLicensed
)

// ModeFor is the only code that combines a feature's flag with the node's
// license flag.
func ModeFor(featureEnabled, licensed bool) Mode {
	switch {
	case !featureEnabled:
		return FeatureOff
	case !licensed:
		return FeatureUnlicensed
	default:
		return FeatureLicensed
	}
}

// LogUnlicensed logs the startup warning for feature in FeatureUnlicensed,
// ending with detail, and logs nothing in any other mode.
func LogUnlicensed(logger logrus.FieldLogger, mode Mode, feature, detail string) {
	switch mode {
	case FeatureUnlicensed:
		logger.WithFields(logrus.Fields{"action": "startup", "feature": feature}).
			Warnf("the %s feature is enabled but this node holds no well-formed Weaviate license key. "+
				"To lift the refusal, %s. %s", feature, licenseKeyInstruction, detail)
	case FeatureOff, FeatureLicensed:
	}
}

const wlPkgPath = "github.com/weaviate/weaviate/wl"

// DeclaredInWL reports whether v's type, or the type v points to, is declared
// in wl/ or below it, and does not look inside v. Feature tests call it to check
// that no mode other than FeatureLicensed wires a wl/ value.
func DeclaredInWL(v any) bool {
	t := reflect.TypeOf(v)
	if t == nil {
		return false
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return inWL(t.PkgPath())
}

func inWL(pkgPath string) bool {
	return pkgPath == wlPkgPath || strings.HasPrefix(pkgPath, wlPkgPath+"/")
}
