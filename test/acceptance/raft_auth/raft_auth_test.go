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

package test

import (
	"context"
	"os"
	"testing"
	"time"
)

// upgradeFromImageEnv names the image of a release without raft auth, e.g. semitechnologies/weaviate:1.39.3.
const upgradeFromImageEnv = "RAFT_AUTH_UPGRADE_FROM_IMAGE"

func TestRaftPlaneRejectsUnauthenticatedPeers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	c := startCluster(ctx, t, verifyEnv)
	c.schemaRound(t)
	c.requireRaftPlaneRejectsAttacker(t)
	c.schemaRound(t)
}

func TestRaftPlaneRollingEnable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	c := startCluster(ctx, t, nil)
	t.Run("baseline", func(t *testing.T) {
		c.schemaRound(t)
	})
	t.Run("credentials with send only", func(t *testing.T) {
		for n := range 3 {
			c.replace(t, n, sendOnlyEnv)
			c.schemaRound(t)
		}
	})
	t.Run("verification on", func(t *testing.T) {
		for n := range 3 {
			c.replace(t, n, verifyEnv)
			c.schemaRound(t)
		}
	})
	t.Run("attacker rejected", func(t *testing.T) {
		c.requireRaftPlaneRejectsAttacker(t)
	})
}

func TestRaftPlaneUpgradeWithBasicAuthOn(t *testing.T) {
	base := os.Getenv(upgradeFromImageEnv)
	if base == "" {
		t.Skipf("set %s to an image without raft auth to run the upgrade path", upgradeFromImageEnv)
	}
	patched := os.Getenv("TEST_WEAVIATE_IMAGE")
	if patched == "" {
		t.Skip("set TEST_WEAVIATE_IMAGE to the image under test to run the upgrade path")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	t.Setenv("TEST_WEAVIATE_IMAGE", base)
	c := startCluster(ctx, t, verifyEnv)
	t.Run("baseline on the old release", func(t *testing.T) {
		c.schemaRound(t)
	})
	t.Setenv("TEST_WEAVIATE_IMAGE", patched)
	t.Run("upgrade with send only", func(t *testing.T) {
		for n := range 3 {
			c.replace(t, n, sendOnlyEnv)
			c.schemaRound(t)
		}
	})
	t.Run("verification on", func(t *testing.T) {
		for n := range 3 {
			c.replace(t, n, verifyEnv)
			c.schemaRound(t)
		}
	})
	t.Run("attacker rejected", func(t *testing.T) {
		c.requireRaftPlaneRejectsAttacker(t)
	})
}
