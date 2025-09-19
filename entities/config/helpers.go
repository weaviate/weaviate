//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"os"
	"strings"
)

func Enabled(value string) bool {
	switch strings.ToLower(value) {
	case "on", "enabled", "1", "true":
		return true
	default:
		return false
	}
}

func EnvEnabled(value string) bool {
	val := os.Getenv(value)
	return Enabled(val)
}
