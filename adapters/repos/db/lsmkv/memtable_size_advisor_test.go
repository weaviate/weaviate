//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const Megabyte = 1024 * 1024

func TestMemtableSizeAdvisor_Initial(t *testing.T) {
	a := newMemtableSizeAdvisor(memtableSizeAdvisorCfg{
		initial: 10 * Megabyte,
	})

	assert.Equal(t, 10485760, a.Initial())
}

func TestMemtableSizeAdvisor_NextTarget(t *testing.T) {
	a := newMemtableSizeAdvisor(memtableSizeAdvisorCfg{
		initial:     10 * Megabyte,
		minDuration: 10 * time.Second,
		maxDuration: 30 * time.Second,
		stepSize:    10 * Megabyte,
		maxSize:     100 * Megabyte,
	})

	type test struct {
		name            string
		current         int
		lastCycle       time.Duration
		expectedChanged bool
		expectedTarget  int
	}

	tests := []test{
		{
			name:            "completely within range",
			current:         10 * Megabyte,
			lastCycle:       17 * time.Second,
			expectedChanged: false,
			expectedTarget:  10 * Megabyte,
		},
		{
			name:            "cycle too short",
			current:         10 * Megabyte,
			lastCycle:       7 * time.Second,
			expectedChanged: true,
			expectedTarget:  20 * Megabyte,
		},
		{
			name:            "cycle too long",
			current:         100 * Megabyte,
			lastCycle:       47 * time.Second,
			expectedChanged: true,
			expectedTarget:  90 * Megabyte,
		},
		{
			name:            "cycle too short, but approaching limit",
			current:         95 * Megabyte,
			lastCycle:       7 * time.Second,
			expectedChanged: true,
			expectedTarget:  100 * Megabyte, // not 105 (!)
		},
		{
			name:            "cycle too short, but already at limit",
			current:         100 * Megabyte,
			lastCycle:       7 * time.Second,
			expectedChanged: false,
			expectedTarget:  100 * Megabyte,
		},
		{
			name:            "cycle too long, but barely above initial size",
			current:         12 * Megabyte,
			lastCycle:       47 * time.Second,
			expectedChanged: true,
			expectedTarget:  10 * Megabyte, // not 2 (1)
		},
		{
			name:            "cycle too long, but already at initial size",
			current:         10 * Megabyte,
			lastCycle:       47 * time.Second,
			expectedChanged: false,
			expectedTarget:  10 * Megabyte,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			newTarget, changed := a.NextTarget(test.current, test.lastCycle)
			assert.Equal(t, test.expectedTarget, newTarget, "expect new target")
			assert.Equal(t, test.expectedChanged, changed, "expect changed")
		})
	}

	target, changed := a.NextTarget(10*1024*1024, 17*time.Second)
	assert.False(t, changed)
	assert.Equal(t, 10*1024*1024, target)
}

func TestMemtableSizeAdvisor_MissingConfig(t *testing.T) {
	// even with an all-default value config the advisor should still return
	// reasonable results, for example many integration tests might not provide a
	// reasonable config to the advisor
	a := newMemtableSizeAdvisor(memtableSizeAdvisorCfg{})
	assert.Equal(t, 10485760, a.Initial())
}
