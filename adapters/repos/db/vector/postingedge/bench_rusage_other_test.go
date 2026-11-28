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

//go:build postingedgebench && !darwin

package postingedge

import (
	"time"

	"github.com/pkg/errors"
)

// processCPU is the accounting of the CPU of this process by core and QoS.
// It is only written for macOS.
func processCPU() (processCPUTimes, error) {
	return processCPUTimes{}, errors.New("the CPU of the process by core and QoS is only counted on macOS")
}

// processCPUTimeOfGetrusage is only used to check processCPU.
func processCPUTimeOfGetrusage() (time.Duration, error) {
	return 0, errors.New("the check of processCPU is only written for macOS")
}

// processDiskReads is what the system has read from disk for this process.
// It is only written for macOS.
func processDiskReads() ([growCounters]uint64, error) {
	return [growCounters]uint64{}, errors.New("what the process read from disk is only counted on macOS")
}
