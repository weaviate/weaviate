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
	"fmt"
	"os"
	"strings"
	"time"
)

func Enabled(value string) bool {
	switch strings.ToLower(value) {
	case "on", "enabled", "1", "true":
		return true
	default:
		return false
	}
}
var defaultMinimumTimeout = -1 * time.Second
func MinimumTimeout() time.Duration {
	if defaultMinimumTimeout > 0 {
		return defaultMinimumTimeout
	}
	defaultMinimumTimeout = 30 * time.Second // default minimum timeout is 30 seconds
	opt := os.Getenv("WEAVIATE_MINIMUM_TIMEOUT")
	if opt != "" {
		if parsed, err := time.ParseDuration(opt); err == nil {
			defaultMinimumTimeout = parsed
		} else {
			fmt.Printf("Invalid WEAVIATE_MINIMUM_TIMEOUT value: %s, using default %s\n", opt, defaultMinimumTimeout)
		}
	}
	return defaultMinimumTimeout
}
