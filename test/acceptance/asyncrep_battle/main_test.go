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

package asyncrep_battle

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestMain gates the battle suite: it is long-running and chaos-heavy, so it
// only runs when explicitly requested, and only against a pre-built race image
// (the DATA RACE log oracle is meaningless without one).
func TestMain(m *testing.M) {
	if os.Getenv("ASYNC_REPLICATION_BATTLE") != "true" {
		fmt.Println("skipping asyncrep battle suite: set ASYNC_REPLICATION_BATTLE=true to run")
		os.Exit(0)
	}
	if os.Getenv("TEST_WEAVIATE_IMAGE") == "" {
		fmt.Println("FAIL: TEST_WEAVIATE_IMAGE unset; build the race image first:")
		fmt.Println(`  docker compose -f docker-compose-test.yml build --build-arg EXTRA_BUILD_ARGS="-race" weaviate`)
		fmt.Println("  export TEST_WEAVIATE_IMAGE=weaviate/test-server")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// profile scales iteration counts and timeouts; BATTLE_PROFILE=quick shrinks
// the run to a smoke pass, thorough (default) is the full battle.
type profile struct {
	idSpace          int
	writerGoroutines int
	opInterval       time.Duration
	configChurnIters int
	tenantRaceIters  int
	convergeTimeout  time.Duration
	perIDSamples     int
	holdDown         time.Duration
}

func battleProfile() profile {
	if os.Getenv("BATTLE_PROFILE") == "quick" {
		return profile{
			idSpace:          500,
			writerGoroutines: 2,
			opInterval:       100 * time.Millisecond,
			configChurnIters: 8,
			tenantRaceIters:  6,
			convergeTimeout:  3 * time.Minute,
			perIDSamples:     20,
			holdDown:         15 * time.Second,
		}
	}
	return profile{
		idSpace:          2000,
		writerGoroutines: 4,
		opInterval:       50 * time.Millisecond,
		configChurnIters: 30,
		tenantRaceIters:  20,
		convergeTimeout:  4 * time.Minute,
		perIDSamples:     50,
		holdDown:         30 * time.Second,
	}
}
