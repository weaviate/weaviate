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

// TestAzureBackup_SingleTenant tests Azure backup/restore with a single-tenant class.
func TestAzureBackup_SingleTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunAzureBackupTests(t, compose, backuptest.SingleTenantTestCase())
}

// TestAzureBackup_MultiTenant tests Azure backup/restore with a multi-tenant class.
func TestAzureBackup_MultiTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunAzureBackupTests(t, compose, backuptest.MultiTenantTestCase())
}

// =============================================================================
// PQ (Product Quantization) Compression Tests
// =============================================================================

// TestAzureBackup_SingleTenant_WithPQ tests Azure backup/restore with PQ compression enabled.
func TestAzureBackup_SingleTenant_WithPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunAzureBackupTests(t, compose, backuptest.SingleTenantWithPQTestCase())
}

// TestAzureBackup_MultiTenant_WithPQ tests Azure backup/restore with multi-tenant and PQ compression.
func TestAzureBackup_MultiTenant_WithPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunAzureBackupTests(t, compose, backuptest.MultiTenantWithPQTestCase())
}

// =============================================================================
// RQ (Rotational Quantization) Compression Tests
// =============================================================================

// TestAzureBackup_SingleTenant_WithRQ tests Azure backup/restore with RQ compression enabled.
func TestAzureBackup_SingleTenant_WithRQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunAzureBackupTests(t, compose, backuptest.SingleTenantWithRQTestCase())
}

// TestAzureBackup_MultiTenant_WithRQ tests Azure backup/restore with multi-tenant and RQ compression.
func TestAzureBackup_MultiTenant_WithRQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunAzureBackupTests(t, compose, backuptest.MultiTenantWithRQTestCase())
}

// =============================================================================
// Backup Cancellation Tests
// =============================================================================

// TestAzureBackup_Cancellation tests that Azure backups can be cancelled.
func TestAzureBackup_Cancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunAzureBackupTests(t, compose, backuptest.CancellationTestCase())
}
