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

package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

// CleanupOrphanRecoveryDirs removes "<shard>.recovering/" dirs whose live sibling exists; in-flight recoveries are kept.
func CleanupOrphanRecoveryDirs(rootDataPath string, logger logrus.FieldLogger) ([]string, error) {
	const suffix = api.RecoveryFolderSuffix
	if rootDataPath == "" {
		return nil, errors.New("cleanup orphan recovery dirs: empty root data path")
	}
	collections, err := os.ReadDir(rootDataPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read data root %q: %w", rootDataPath, err)
	}
	var removed []string
	for _, c := range collections {
		if !c.IsDir() {
			continue
		}
		collDir := filepath.Join(rootDataPath, c.Name())
		shards, err := os.ReadDir(collDir)
		if err != nil {
			logger.WithField("dir", collDir).Warnf("cleanup: cannot read collection dir: %v", err)
			continue
		}
		for _, s := range shards {
			if !s.IsDir() || !strings.HasSuffix(s.Name(), suffix) {
				continue
			}
			recoveryDir := filepath.Join(collDir, s.Name())
			liveDir := filepath.Join(collDir, strings.TrimSuffix(s.Name(), suffix))
			if _, err := os.Stat(liveDir); err != nil {
				continue // no sibling: in-flight recovery to resume
			}
			if err := os.RemoveAll(recoveryDir); err != nil {
				logger.WithField("dir", recoveryDir).Warnf("cleanup: failed to remove orphan recovery dir: %v", err)
				continue
			}
			logger.WithField("dir", recoveryDir).Info("cleanup: removed orphan recovery dir")
			removed = append(removed, recoveryDir)
		}
	}
	return removed, nil
}

// RemoveStaleSelfRecoveryWipeMarker deletes the wipe-round marker on a start that cannot submit recoveries, so a later licensed start does not inherit the benign bucket.
func RemoveStaleSelfRecoveryWipeMarker(rootDataPath string, logger logrus.FieldLogger) {
	if rootDataPath == "" {
		return
	}
	marker := filepath.Join(rootDataPath, api.SelfRecoveryWipeMarkerName)
	err := os.Remove(marker)
	switch {
	case err == nil:
		logger.Info("self-recovery: removed a stale wipe-round marker; this start cannot submit recoveries (feature disabled or unlicensed) and normal init materialises the missing shards")
	case errors.Is(err, fs.ErrNotExist):
	default:
		logger.Warnf("self-recovery: cannot remove the stale wipe-round marker %q: %v", marker, err)
	}
}

// UnlicensedSelfRecovery is the SelfRecoveryOrchestrator of a node with the flag on and no license: Enabled keeps the resume branch for in-flight SELF_RECOVERY ops, which cluster/replication completes, and every submission is declined.
type UnlicensedSelfRecovery struct {
	Logger logrus.FieldLogger
}

func (UnlicensedSelfRecovery) Enabled() bool { return true }

func (u UnlicensedSelfRecovery) SubmitRecovery(_ context.Context, collection, shard string, _ bool) bool {
	u.logSkipped(collection, shard)
	return false
}

func (u UnlicensedSelfRecovery) SubmitActivationRecovery(_ context.Context, collection, shard string) bool {
	u.logSkipped(collection, shard)
	return false
}

func (UnlicensedSelfRecovery) Close(context.Context) error { return nil }

func (u UnlicensedSelfRecovery) logSkipped(collection, shard string) {
	if u.Logger == nil {
		return
	}
	u.Logger.WithFields(logrus.Fields{
		"event":      "self_recovery.skipped_unlicensed",
		"collection": collection,
		"shard":      shard,
	}).Debug("self-recovery skipped: no well-formed Weaviate license key")
}
