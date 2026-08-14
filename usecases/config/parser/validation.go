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

package parser

import (
	"fmt"
	"time"

	"github.com/netresearch/go-cron"
)

func ValidateFloatGreaterThan0(val float64) error {
	if val > 0 {
		return nil
	}
	return fmt.Errorf("float greater than 0 expected, got %v", val)
}

func ValidateFloatGreaterThanEqual0(val float64) error {
	if val >= 0 {
		return nil
	}
	return fmt.Errorf("float greater than equal 0 expected, got %v", val)
}

func ValidateIntGreaterThan0(val int) error {
	if val > 0 {
		return nil
	}
	return fmt.Errorf("int greater than 0 expected, got %v", val)
}

func ValidateIntGreaterThanEqual0(val int) error {
	if val >= 0 {
		return nil
	}
	return fmt.Errorf("int greater than equal 0 expected, got %v", val)
}

func ValidateDurationGreaterThan0(val time.Duration) error {
	if val > 0 {
		return nil
	}
	return fmt.Errorf("duration greater than 0 expected, got %v", val)
}

func ValidateDurationGreaterThanEqual0(val time.Duration) error {
	if val >= 0 {
		return nil
	}
	return fmt.Errorf("duration greater than equal 0 expected, got %v", val)
}

func ValidateGocronSchedule(val string) error {
	if val != "" {
		if _, err := cron.FullParser().Parse(val); err != nil {
			return err
		}
	}
	return nil
}
