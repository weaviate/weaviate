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

// All tests run on the shared single-node cluster from TestMain.
// Filesystem backup only works on single-node clusters.

// =============================================================================
// Basic Backup/Restore Tests
// =============================================================================

// TestFilesystemBackup_SingleTenant tests filesystem backup/restore with a single-tenant class.
func TestFilesystemBackup_SingleTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunFilesystemBackupTests(t, compose, backuptest.SingleTenantTestCase())
}

// TestFilesystemBackup_MultiTenant tests filesystem backup/restore with a multi-tenant class.
func TestFilesystemBackup_MultiTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunFilesystemBackupTests(t, compose, backuptest.MultiTenantTestCase())
}

// =============================================================================
// PQ (Product Quantization) Compression Tests
// =============================================================================

// TestFilesystemBackup_SingleTenant_WithPQ tests filesystem backup/restore with PQ compression enabled.
func TestFilesystemBackup_SingleTenant_WithPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunFilesystemBackupTests(t, compose, backuptest.SingleTenantWithPQTestCase())
}

// TestFilesystemBackup_MultiTenant_WithPQ tests filesystem backup/restore with multi-tenant and PQ compression.
func TestFilesystemBackup_MultiTenant_WithPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunFilesystemBackupTests(t, compose, backuptest.MultiTenantWithPQTestCase())
}

// =============================================================================
// RQ (Rotational Quantization) Compression Tests
// =============================================================================

// TestFilesystemBackup_SingleTenant_WithRQ tests filesystem backup/restore with RQ compression enabled.
func TestFilesystemBackup_SingleTenant_WithRQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunFilesystemBackupTests(t, compose, backuptest.SingleTenantWithRQTestCase())
}

// TestFilesystemBackup_MultiTenant_WithRQ tests filesystem backup/restore with multi-tenant and RQ compression.
func TestFilesystemBackup_MultiTenant_WithRQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunFilesystemBackupTests(t, compose, backuptest.MultiTenantWithRQTestCase())
}

// =============================================================================
// Backup Cancellation Tests
// =============================================================================

// TestFilesystemBackup_Cancellation tests that filesystem backups can be cancelled.
func TestFilesystemBackup_Cancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunFilesystemBackupTests(t, compose, backuptest.CancellationTestCase())
}

// TestFilesystemBackup_Incremental tests file-based incremental filesystem backups.
func TestFilesystemBackup_Incremental(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunFilesystemBackupTests(t, compose, backuptest.IncrementalTestCase())
}
