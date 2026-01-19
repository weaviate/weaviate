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
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/models"
)

func asyncReplicationConfigFromModel(multiTenancyEnabled bool, cfg *models.ReplicationAsyncConfig) (config AsyncReplicationConfig, err error) {
	if cfg == nil {
		cfg = &models.ReplicationAsyncConfig{}
	}

	maxWorkers := defaultAsyncReplicationMaxWorkers

	if cfg.MaxWorkers != nil {
		maxWorkers = int(*cfg.MaxWorkers)
	}

	config.maxWorkers, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_MAX_WORKERS"),
		maxWorkers,
		1,
		math.MaxInt,
	)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_MAX_WORKERS", err)
	}

	var hashtreeHeight int

	if multiTenancyEnabled {
		hashtreeHeight = defaultHashtreeHeightMultiTenant
	} else {
		hashtreeHeight = defaultHashtreeHeightSingleTenant
	}

	if cfg.HashtreeHeight != nil {
		hashtreeHeight = int(*cfg.HashtreeHeight)
	}

	config.hashtreeHeight, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_HASHTREE_HEIGHT"), hashtreeHeight, minHashtreeHeight, maxHashtreeHeight)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_HASHTREE_HEIGHT", err)
	}

	frequency := defaultFrequency
	if cfg.Frequency != nil {
		frequency = time.Duration(*cfg.Frequency)
	}

	config.frequency, err = optParseDuration(os.Getenv("ASYNC_REPLICATION_FREQUENCY"), frequency)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_FREQUENCY", err)
	}

	frequencyWhilePropagating := defaultFrequencyWhilePropagating
	if cfg.FrequencyWhilePropagating != nil {
		frequencyWhilePropagating = time.Duration(*cfg.FrequencyWhilePropagating)
	}

	config.frequencyWhilePropagating, err = optParseDuration(os.Getenv("ASYNC_REPLICATION_FREQUENCY_WHILE_PROPAGATING"), frequencyWhilePropagating)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_FREQUENCY_WHILE_PROPAGATING", err)
	}

	aliveNodesCheckingFrequency := defaultAliveNodesCheckingFrequency
	if cfg.AliveNodesCheckingFrequency != nil {
		aliveNodesCheckingFrequency = time.Duration(*cfg.AliveNodesCheckingFrequency)
	}

	config.aliveNodesCheckingFrequency, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_ALIVE_NODES_CHECKING_FREQUENCY"), aliveNodesCheckingFrequency)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_ALIVE_NODES_CHECKING_FREQUENCY", err)
	}

	loggingFrequency := defaultLoggingFrequency
	if cfg.LoggingFrequency != nil {
		loggingFrequency = time.Duration(*cfg.LoggingFrequency)
	}

	config.loggingFrequency, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_LOGGING_FREQUENCY"), loggingFrequency)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_LOGGING_FREQUENCY", err)
	}

	diffBatchSize := defaultDiffBatchSize
	if cfg.DiffBatchSize != nil {
		diffBatchSize = int(*cfg.DiffBatchSize)
	}

	config.diffBatchSize, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_DIFF_BATCH_SIZE"), diffBatchSize, minDiffBatchSize, maxDiffBatchSize)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_DIFF_BATCH_SIZE", err)
	}

	diffPerNodeTimeout := defaultDiffPerNodeTimeout
	if cfg.DiffPerNodeTimeout != nil {
		diffPerNodeTimeout = time.Duration(*cfg.DiffPerNodeTimeout)
	}

	config.diffPerNodeTimeout, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_DIFF_PER_NODE_TIMEOUT"), diffPerNodeTimeout)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_DIFF_PER_NODE_TIMEOUT", err)
	}

	prePropagationTimeout := defaultPrePropagationTimeout
	if cfg.PrePropagationTimeout != nil {
		prePropagationTimeout = time.Duration(*cfg.PrePropagationTimeout)
	}

	config.prePropagationTimeout, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_PRE_PROPAGATION_TIMEOUT"), prePropagationTimeout)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PRE_PROPAGATION_TIMEOUT", err)
	}

	propagationTimeout := defaultPropagationTimeout
	if cfg.PropagationTimeout != nil {
		propagationTimeout = time.Duration(*cfg.PropagationTimeout)
	}

	config.propagationTimeout, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_TIMEOUT"), propagationTimeout)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_TIMEOUT", err)
	}

	propagationLimit := defaultPropagationLimit
	if cfg.PropagationLimit != nil {
		propagationLimit = int(*cfg.PropagationLimit)
	}

	config.propagationLimit, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_LIMIT"), propagationLimit, minPropagationLimit, maxPropagationLimit)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_LIMIT", err)
	}

	propagationDelay := defaultPropagationDelay
	if cfg.PropagationDelay != nil {
		propagationDelay = time.Duration(*cfg.PropagationDelay)
	}

	config.propagationDelay, err = optParseDuration(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_DELAY"), propagationDelay)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_DELAY", err)
	}

	propagationConcurrency := defaultPropagationConcurrency
	if cfg.PropagationConcurrency != nil {
		propagationConcurrency = int(*cfg.PropagationConcurrency)
	}

	config.propagationConcurrency, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_CONCURRENCY"), propagationConcurrency, minPropgationConcurrency, maxPropagationConcurrency)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_CONCURRENCY", err)
	}

	propagationBatchSize := defaultPropagationBatchSize
	if cfg.PropagationBatchSize != nil {
		propagationBatchSize = int(*cfg.PropagationBatchSize)
	}

	config.propagationBatchSize, err = optParseInt(
		os.Getenv("ASYNC_REPLICATION_PROPAGATION_BATCH_SIZE"), propagationBatchSize, minPropagationBatchSize, maxPropagationBatchSize)
	if err != nil {
		return AsyncReplicationConfig{}, fmt.Errorf("%s: %w", "ASYNC_REPLICATION_PROPAGATION_BATCH_SIZE", err)
	}

	return config, err
}

func optParseInt(s string, defaultVal, minVal, maxVal int) (val int, err error) {
	if s == "" {
		val = defaultVal
	} else {
		val, err = strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
	}

	if val < minVal || val > maxVal {
		return 0, fmt.Errorf("value %d out of range: min %d, max %d", val, minVal, maxVal)
	}

	return val, nil
}

func optParseDuration(s string, defaultDuration time.Duration) (time.Duration, error) {
	if s == "" {
		return defaultDuration, nil
	}
	return time.ParseDuration(s)
}
