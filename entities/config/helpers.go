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

func MinimumTimeout() time.Duration {
	timeout := 30 * time.Second
	opt := os.Getenv("WEAVIATE_MINIMUM_TIMEOUT")
	if opt != "" {
		if parsed, err := time.ParseDuration(opt); err == nil {
			timeout = parsed
		} else {
			fmt.Printf("Invalid WEAVIATE_MINIMUM_TIMEOUT value: %s, using default %s\n", opt, timeout)
		}
	}
	return timeout
}
