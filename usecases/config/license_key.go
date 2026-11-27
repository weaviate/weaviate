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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/sirupsen/logrus"

	licenseprotocol "github.com/weaviate/weaviate/entities/license"
	"github.com/weaviate/weaviate/usecases/license"
)

// LoadLicenseKey loads LICENSE_KEY or LICENSE_KEY_FILE and trims surrounding
// whitespace. Setting both variables, even to whitespace, or pointing
// LICENSE_KEY_FILE at an unreadable file is a startup error. The returned key
// must be passed directly to the license client and never logged.
func LoadLicenseKey() (string, error) {
	envKey, keyFile := os.Getenv("LICENSE_KEY"), os.Getenv("LICENSE_KEY_FILE")
	if envKey != "" && keyFile != "" {
		return "", fmt.Errorf("LICENSE_KEY and LICENSE_KEY_FILE are mutually exclusive; set only one")
	}
	key := strings.TrimSpace(envKey)
	if keyFile != "" {
		contents, err := os.ReadFile(keyFile)
		if err != nil {
			// The path is the key itself when the key was pasted into
			// LICENSE_KEY_FILE, so keep it out of the startup error.
			var pathErr *fs.PathError
			if errors.As(err, &pathErr) {
				err = pathErr.Err
			}
			return "", fmt.Errorf("cannot read LICENSE_KEY_FILE: %w", err)
		}
		key = strings.TrimSpace(string(contents))
	}
	return key, nil
}

// resolveLicenseState resolves the configured key into the initial license
// state. An unset, empty or malformed key runs in free mode with a warning. A
// well-formed key is licensed by the unchanged form-check gate, but its status
// stays unreachable until the license service returns a signed answer.
func resolveLicenseState() (license.State, error) {
	key, err := LoadLicenseKey()
	if err != nil {
		return license.State{}, err
	}

	id, _, err := licenseprotocol.ParseKey(key)
	if err == nil {
		return license.State{Status: license.StatusUnreachable, LicenseID: id}, nil
	}

	envKey, keyFile := os.Getenv("LICENSE_KEY"), os.Getenv("LICENSE_KEY_FILE")
	switch {
	case key == "" && keyFile != "":
		logrus.Warn("LICENSE_KEY_FILE is set but the file contains no key; " +
			"Enterprise Edition functionality is disabled")
	case key == "" && envKey != "":
		logrus.Warn("LICENSE_KEY is set but contains no key; " +
			"Enterprise Edition functionality is disabled")
	case key != "":
		logrus.Warn("the license key configured via LICENSE_KEY or LICENSE_KEY_FILE is not a " +
			"well-formed Weaviate license key; Enterprise Edition functionality is disabled")
	}
	return license.State{Status: license.StatusUnlicensed}, nil
}
