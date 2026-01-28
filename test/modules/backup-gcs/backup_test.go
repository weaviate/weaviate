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
	"testing"

	"github.com/weaviate/weaviate/test/helper/backuptest"
)

// All tests run on the shared 3-node cluster from TestMain.

// =============================================================================
// Basic Backup/Restore Tests
// =============================================================================

// TestGCSBackup_SingleTenant tests GCS backup/restore with a single-tenant class.
func TestGCSBackup_SingleTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunGCSBackupTests(t, compose, backuptest.SingleTenantTestCase())
}

// TestGCSBackup_MultiTenant tests GCS backup/restore with a multi-tenant class.
func TestGCSBackup_MultiTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunGCSBackupTests(t, compose, backuptest.MultiTenantTestCase())
}

// =============================================================================
// PQ (Product Quantization) Compression Tests
// =============================================================================

// TestGCSBackup_SingleTenant_WithPQ tests GCS backup/restore with PQ compression enabled.
func TestGCSBackup_SingleTenant_WithPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunGCSBackupTests(t, compose, backuptest.SingleTenantWithPQTestCase())
}

// TestGCSBackup_MultiTenant_WithPQ tests GCS backup/restore with multi-tenant and PQ compression.
func TestGCSBackup_MultiTenant_WithPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunGCSBackupTests(t, compose, backuptest.MultiTenantWithPQTestCase())
}

// =============================================================================
// RQ (Rotational Quantization) Compression Tests
// =============================================================================

// TestGCSBackup_SingleTenant_WithRQ tests GCS backup/restore with RQ compression enabled.
func TestGCSBackup_SingleTenant_WithRQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunGCSBackupTests(t, compose, backuptest.SingleTenantWithRQTestCase())
}

// TestGCSBackup_MultiTenant_WithRQ tests GCS backup/restore with multi-tenant and RQ compression.
func TestGCSBackup_MultiTenant_WithRQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunGCSBackupTests(t, compose, backuptest.MultiTenantWithRQTestCase())
}

// =============================================================================
// Backup Cancellation Tests
// =============================================================================

// TestGCSBackup_Cancellation tests that GCS backups can be cancelled.
func TestGCSBackup_Cancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunGCSBackupTests(t, compose, backuptest.CancellationTestCase())
}
