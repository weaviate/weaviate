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

package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"

	licenseprotocol "github.com/weaviate/weaviate/entities/license"
	"github.com/weaviate/weaviate/usecases/license"
)

// resolveLicenseState resolves the license key configured via LICENSE_KEY or
// LICENSE_KEY_FILE into the license state. All license key handling lives in
// this one method; the caller just assigns the result.
//
// Setting both variables, or pointing LICENSE_KEY_FILE at an unreadable file,
// is a startup error: those are clear misconfigurations that should fail fast
// rather than silently fall back to free mode. An unset, empty or malformed
// key is not an error — it simply runs in free mode with a warning. The key
// itself is never logged; only the non-secret license id is kept.
func resolveLicenseState() (license.State, error) {
	key, keyFile := os.Getenv("LICENSE_KEY"), os.Getenv("LICENSE_KEY_FILE")
	if key != "" && keyFile != "" {
		return license.State{}, fmt.Errorf("LICENSE_KEY and LICENSE_KEY_FILE are mutually exclusive; set only one")
	}
	if keyFile != "" {
		contents, err := os.ReadFile(keyFile)
		if err != nil {
			return license.State{}, fmt.Errorf("cannot read LICENSE_KEY_FILE: %w", err)
		}
		key = strings.TrimSpace(string(contents))
	}

	id, _, err := licenseprotocol.ParseKey(key)
	if err == nil {
		return license.State{Status: license.StatusValid, LicenseID: id}, nil
	}

	switch {
	case key == "" && keyFile != "":
		logrus.Warn("LICENSE_KEY_FILE is set but the file contains no key; " +
			"Enterprise Edition functionality is disabled")
	case key != "":
		logrus.Warn("the license key configured via LICENSE_KEY or LICENSE_KEY_FILE is not a " +
			"well-formed Weaviate license key; Enterprise Edition functionality is disabled")
	}
	return license.State{Status: license.StatusUnlicensed}, nil
}
