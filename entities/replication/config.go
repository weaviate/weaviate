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

package replication

import (
	"time"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// GlobalConfig represents system-wide config that may restrict settings of an
// individual class
type GlobalConfig struct {
	AsyncReplicationDisabled *runtime.DynamicValue[bool] `json:"async_replication_disabled" yaml:"async_replication_disabled"`

	// Async replication knobs. A zero value means "not configured at the cluster
	// level"; the per-class API override (or the hardcoded code default) takes
	// effect instead. The per-shard timing/limit fields below are polled by
	// Effective() on every hashbeat cycle, so updates take effect without a
	// restart. AsyncReplicationSchedulerWorkers is handled separately by the
	// scheduler's worker watcher, and AsyncReplicationHashtreeInitConcurrency is
	// read when the scheduler is constructed.
	AsyncReplicationSchedulerWorkers          *runtime.DynamicValue[int]           `json:"async_replication_scheduler_workers" yaml:"async_replication_scheduler_workers"`
	AsyncReplicationHashtreeInitConcurrency   *runtime.DynamicValue[int]           `json:"async_replication_hashtree_init_concurrency" yaml:"async_replication_hashtree_init_concurrency"`
	AsyncReplicationHashtreeHeight            *runtime.DynamicValue[int]           `json:"async_replication_hashtree_height" yaml:"async_replication_hashtree_height"`
	AsyncReplicationFrequency                 *runtime.DynamicValue[time.Duration] `json:"async_replication_frequency" yaml:"async_replication_frequency"`
	AsyncReplicationFrequencyWhilePropagating *runtime.DynamicValue[time.Duration] `json:"async_replication_frequency_while_propagating" yaml:"async_replication_frequency_while_propagating"`
	AsyncReplicationLoggingFrequency          *runtime.DynamicValue[time.Duration] `json:"async_replication_logging_frequency" yaml:"async_replication_logging_frequency"`
	AsyncReplicationDiffBatchSize             *runtime.DynamicValue[int]           `json:"async_replication_diff_batch_size" yaml:"async_replication_diff_batch_size"`
	AsyncReplicationDiffPerNodeTimeout        *runtime.DynamicValue[time.Duration] `json:"async_replication_diff_per_node_timeout" yaml:"async_replication_diff_per_node_timeout"`
	AsyncReplicationPrePropagationTimeout     *runtime.DynamicValue[time.Duration] `json:"async_replication_pre_propagation_timeout" yaml:"async_replication_pre_propagation_timeout"`
	AsyncReplicationPropagationTimeout        *runtime.DynamicValue[time.Duration] `json:"async_replication_propagation_timeout" yaml:"async_replication_propagation_timeout"`
	AsyncReplicationPropagationLimit          *runtime.DynamicValue[int]           `json:"async_replication_propagation_limit" yaml:"async_replication_propagation_limit"`
	AsyncReplicationPropagationConcurrency    *runtime.DynamicValue[int]           `json:"async_replication_propagation_concurrency" yaml:"async_replication_propagation_concurrency"`
	AsyncReplicationPropagationBatchSize      *runtime.DynamicValue[int]           `json:"async_replication_propagation_batch_size" yaml:"async_replication_propagation_batch_size"`
	AsyncReplicationPropagationDelay          *runtime.DynamicValue[time.Duration] `json:"async_replication_propagation_delay" yaml:"async_replication_propagation_delay"`
	// MinimumFactor can enforce replication. For example, with MinimumFactor set
	// to 2, users can no longer create classes with a factor of 1, therefore
	// forcing them to have replicated classes.
	MinimumFactor int `json:"minimum_factor" yaml:"minimum_factor"`

	// MaximumFactor caps the replication factor allowed on any class. A value
	// <= 0 means "no cap". Set via REPLICATION_MAXIMUM_FACTOR. Used by the
	// usage-limits guardrails: when any object/tenant/shard cap is set,
	// MaximumFactor must be 1 (only the RF=1 deployment shape is supported).
	MaximumFactor int `json:"maximum_factor" yaml:"maximum_factor"`

	DeletionStrategy string `json:"deletion_strategy" yaml:"deletion_strategy"`

	ReplicationGRPCEnabled *runtime.DynamicValue[bool] `json:"replication_grpc_enabled" yaml:"replication_grpc_enabled"`
}
