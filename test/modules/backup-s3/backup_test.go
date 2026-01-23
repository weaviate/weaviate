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

// TestS3Backup_SingleTenant tests S3 backup/restore with a single-tenant class.
func TestS3Backup_SingleTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunS3BackupTests(t, compose, GetMinioURI(), GetS3Region(), backuptest.SingleTenantTestCase())
}

// TestS3Backup_MultiTenant tests S3 backup/restore with a multi-tenant class.
func TestS3Backup_MultiTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunS3BackupTests(t, compose, GetMinioURI(), GetS3Region(), backuptest.MultiTenantTestCase())
}

// =============================================================================
// PQ (Product Quantization) Compression Tests
// =============================================================================

// TestS3Backup_SingleTenant_WithPQ tests S3 backup/restore with PQ compression enabled.
func TestS3Backup_SingleTenant_WithPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunS3BackupTests(t, compose, GetMinioURI(), GetS3Region(), backuptest.SingleTenantWithPQTestCase())
}

// TestS3Backup_MultiTenant_WithPQ tests S3 backup/restore with multi-tenant and PQ compression.
func TestS3Backup_MultiTenant_WithPQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunS3BackupTests(t, compose, GetMinioURI(), GetS3Region(), backuptest.MultiTenantWithPQTestCase())
}

// =============================================================================
// RQ (Rotational Quantization) Compression Tests
// =============================================================================

// TestS3Backup_SingleTenant_WithRQ tests S3 backup/restore with RQ compression enabled.
func TestS3Backup_SingleTenant_WithRQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunS3BackupTests(t, compose, GetMinioURI(), GetS3Region(), backuptest.SingleTenantWithRQTestCase())
}

// TestS3Backup_MultiTenant_WithRQ tests S3 backup/restore with multi-tenant and RQ compression.
func TestS3Backup_MultiTenant_WithRQ(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunS3BackupTests(t, compose, GetMinioURI(), GetS3Region(), backuptest.MultiTenantWithRQTestCase())
}

// =============================================================================
// Backup Cancellation Tests
// =============================================================================

// TestS3Backup_Cancellation tests that S3 backups can be cancelled.
func TestS3Backup_Cancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	compose := GetSharedCompose()
	if compose == nil {
		t.Fatal("shared compose not available - TestMain may have failed")
	}

	backuptest.RunS3BackupTests(t, compose, GetMinioURI(), GetS3Region(), backuptest.CancellationTestCase())
}
