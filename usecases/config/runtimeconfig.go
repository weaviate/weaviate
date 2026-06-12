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

package config

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// WeaviateRuntimeConfig is the collection all the supported configs that is
// managed dynamically and can be overridden during runtime.
type WeaviateRuntimeConfig struct {
	MaximumAllowedCollectionsCount            *runtime.DynamicValue[int]           `json:"maximum_allowed_collections_count" yaml:"maximum_allowed_collections_count"`
	MaximumAllowedObjectsCount                *runtime.DynamicValue[int]           `json:"maximum_allowed_objects_count" yaml:"maximum_allowed_objects_count"`
	MaximumAllowedTenantsPerCollection        *runtime.DynamicValue[int]           `json:"maximum_allowed_tenants_per_collection" yaml:"maximum_allowed_tenants_per_collection"`
	MaximumAllowedShardsPerCollection         *runtime.DynamicValue[int]           `json:"maximum_allowed_shards_per_collection" yaml:"maximum_allowed_shards_per_collection"`
	UsageLimitsErrorMessage                   *runtime.DynamicValue[string]        `json:"usage_limits_error_message" yaml:"usage_limits_error_message"`
	AutoschemaEnabled                         *runtime.DynamicValue[bool]          `json:"autoschema_enabled" yaml:"autoschema_enabled"`
	AsyncReplicationDisabled                  *runtime.DynamicValue[bool]          `json:"async_replication_disabled" yaml:"async_replication_disabled"`
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
	RevectorizeCheckDisabled                  *runtime.DynamicValue[bool]          `json:"revectorize_check_disabled" yaml:"revectorize_check_disabled"`
	ReplicaMovementMinimumAsyncWait           *runtime.DynamicValue[time.Duration] `json:"replica_movement_minimum_async_wait" yaml:"replica_movement_minimum_async_wait"`
	TenantActivityReadLogLevel                *runtime.DynamicValue[string]        `json:"tenant_activity_read_log_level" yaml:"tenant_activity_read_log_level"`
	TenantActivityWriteLogLevel               *runtime.DynamicValue[string]        `json:"tenant_activity_write_log_level" yaml:"tenant_activity_write_log_level"`
	QuerySlowLogEnabled                       *runtime.DynamicValue[bool]          `json:"query_slow_log_enabled" yaml:"query_slow_log_enabled"`
	QuerySlowLogThreshold                     *runtime.DynamicValue[time.Duration] `json:"query_slow_log_threshold" yaml:"query_slow_log_threshold"`
	InvertedSorterDisabled                    *runtime.DynamicValue[bool]          `json:"inverted_sorter_disabled" yaml:"inverted_sorter_disabled"`
	UsageGCSBucket                            *runtime.DynamicValue[string]        `json:"usage_gcs_bucket" yaml:"usage_gcs_bucket"`
	UsageGCSPrefix                            *runtime.DynamicValue[string]        `json:"usage_gcs_prefix" yaml:"usage_gcs_prefix"`
	UsageS3Bucket                             *runtime.DynamicValue[string]        `json:"usage_s3_bucket" yaml:"usage_s3_bucket"`
	UsageS3Prefix                             *runtime.DynamicValue[string]        `json:"usage_s3_prefix" yaml:"usage_s3_prefix"`
	UsageScrapeInterval                       *runtime.DynamicValue[time.Duration] `json:"usage_scrape_interval" yaml:"usage_scrape_interval"`
	UsageShardJitterInterval                  *runtime.DynamicValue[time.Duration] `json:"usage_shard_jitter_interval" yaml:"usage_shard_jitter_interval"`
	UsagePolicyVersion                        *runtime.DynamicValue[string]        `json:"usage_policy_version" yaml:"usage_policy_version"`
	UsageVerifyPermissions                    *runtime.DynamicValue[bool]          `json:"usage_verify_permissions" yaml:"usage_verify_permissions"`
	ReplicationGRPCEnabled                    *runtime.DynamicValue[bool]          `json:"replication_grpc_enabled" yaml:"replication_grpc_enabled"`
	ReplicatedIndicesRequestQueueEnabled      *runtime.DynamicValue[bool]          `json:"replicated_indices_request_queue_enabled" yaml:"replicated_indices_request_queue_enabled"`
	OperationalMode                           *runtime.DynamicValue[string]        `json:"operational_mode" yaml:"operational_mode"`
	DefaultQuantization                       *runtime.DynamicValue[string]        `yaml:"default_quantization" json:"default_quantization"`
	DefaultVectorIndexType                    *runtime.DynamicValue[string]        `yaml:"default_vector_index" json:"default_vector_index"`
	DefaultShardingCount                      *runtime.DynamicValue[int]           `yaml:"default_sharding_count" json:"default_sharding_count"`
	AllowedVectorIndexTypes                   *runtime.DynamicValue[[]string]      `yaml:"allowed_vector_index_types" json:"allowed_vector_index_types"`
	AllowedCompressionTypes                   *runtime.DynamicValue[[]string]      `yaml:"allowed_compression_types" json:"allowed_compression_types"`
	RestrictionsErrorMessage                  *runtime.DynamicValue[string]        `yaml:"restrictions_error_message" json:"restrictions_error_message"`
	DebugEndpointsEnabled                     *runtime.DynamicValue[bool]          `json:"debug_endpoints_enabled" yaml:"debug_endpoints_enabled"`

	ObjectsTTLDeleteSchedule      *runtime.DynamicValue[string]        `json:"objects_ttl_delete_schedule" yaml:"objects_ttl_delete_schedule"`
	ObjectsTTLBatchSize           *runtime.DynamicValue[int]           `json:"objects_ttl_batch_size" yaml:"objects_ttl_batch_size"`
	ObjectsTTLPauseEveryNoBatches *runtime.DynamicValue[int]           `json:"objects_ttl_pause_every_no_batches" yaml:"objects_ttl_pause_every_no_batches"`
	ObjectsTTLPauseDuration       *runtime.DynamicValue[time.Duration] `json:"objects_ttl_pause_duration" yaml:"objects_ttl_pause_duration"`
	ObjectsTTLConcurrencyFactor   *runtime.DynamicValue[float64]       `json:"objects_ttl_concurrency_factor" yaml:"objects_ttl_concurrency_factor"`

	// Export settings
	ExportEnabled       *runtime.DynamicValue[bool]   `json:"export_enabled" yaml:"export_enabled"`
	ExportDefaultBucket *runtime.DynamicValue[string] `json:"export_default_bucket" yaml:"export_default_bucket"`
	ExportDefaultPath   *runtime.DynamicValue[string] `json:"export_default_path" yaml:"export_default_path"`
	ExportParallelism   *runtime.DynamicValue[int]    `json:"export_parallelism" yaml:"export_parallelism"`

	// RAFT specific configs
	RaftDrainSleep        *runtime.DynamicValue[time.Duration] `json:"raft_drain_sleep" yaml:"raft_drain_sleep"`
	RaftTimoutsMultiplier *runtime.DynamicValue[int]           `json:"raft_timeouts_multiplier" yaml:"raft_timeouts_multiplier"`

	// Authentication OIDC settings
	OIDCIssuer            *runtime.DynamicValue[string]   `json:"authentication_oidc_issuer" yaml:"authentication_oidc_issuer"`
	OIDCClientID          *runtime.DynamicValue[string]   `json:"authentication_oidc_client_id" yaml:"authentication_oidc_client_id"`
	OIDCSkipClientIDCheck *runtime.DynamicValue[bool]     `yaml:"authentication_oidc_skip_client_id_check" json:"authentication_oidc_skip_client_id_check"`
	OIDCUsernameClaim     *runtime.DynamicValue[string]   `yaml:"authentication_oidc_username_claim" json:"authentication_oidc_username_claim"`
	OIDCGroupsClaim       *runtime.DynamicValue[string]   `yaml:"authentication_oidc_groups_claim" json:"authentication_oidc_groups_claim"`
	OIDCScopes            *runtime.DynamicValue[[]string] `yaml:"authentication_oidc_scopes" json:"authentication_oidc_scopes"`
	OIDCCertificate       *runtime.DynamicValue[string]   `yaml:"authentication_oidc_certificate" json:"authentication_oidc_certificate"`
	OIDCJWKSUrl           *runtime.DynamicValue[string]   `yaml:"authentication_oidc_jwks_url" json:"authentication_oidc_jwks_url"`
	OIDCSkipTLSVerify     *runtime.DynamicValue[bool]     `yaml:"authentication_oidc_insecure_skip_tls_verify" json:"authentication_oidc_insecure_skip_tls_verify"`

	// skippedFields holds the YAML keys that failed to decode in this parse. Such
	// a field is left untouched on update (neither applied nor reset), so a
	// malformed value keeps whatever was already in effect instead of reverting
	// to the default. Unexported so it is not (un)marshaled.
	skippedFields map[string]struct{}
}

// FieldError reports a single runtime-config key that failed to decode into its
// target type. The remaining keys in the document are still applied.
type FieldError struct {
	Field string
	Err   error
}

func (e FieldError) Error() string { return fmt.Sprintf("%s: %v", e.Field, e.Err) }

// ParseRuntimeConfig decodes WeaviateRuntimeConfig from raw YAML bytes. Per-field
// decode failures are dropped here; use ParseRuntimeConfigPartial to observe them.
func ParseRuntimeConfig(buf []byte) (*WeaviateRuntimeConfig, error) {
	conf, _, err := ParseRuntimeConfigPartial(buf)
	return conf, err
}

// ParseRuntimeConfigPartial decodes WeaviateRuntimeConfig key-by-key so a single
// malformed value (wrong type, out of the type's range, overflow) is skipped and
// reported instead of rejecting the whole file. A document-level YAML error (one
// not attributable to a single key, e.g. malformed syntax) is still returned as a
// fatal error so the manager keeps the previous config.
func ParseRuntimeConfigPartial(buf []byte) (*WeaviateRuntimeConfig, []FieldError, error) {
	var raw map[string]yaml.Node

	dec := yaml.NewDecoder(bytes.NewReader(buf))

	// An empty runtime yaml file is still a valid file. So treating io.EOF as
	// non-error case returns default values of config.
	if err := dec.Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, err
	}

	conf := &WeaviateRuntimeConfig{}
	v := reflect.ValueOf(conf).Elem()
	t := v.Type()

	var fieldErrs []FieldError
	for i := range t.NumField() {
		key := yamlKey(t.Field(i))
		if key == "" || key == "-" {
			continue
		}
		node, ok := raw[key]
		if !ok {
			continue // absent key keeps the registered default
		}
		// Each field is a *runtime.DynamicValue[T]; decode the node into a fresh
		// one so DynamicValue.UnmarshalYAML still runs (comma-split coercion etc.).
		fp := reflect.New(t.Field(i).Type.Elem())
		if err := node.Decode(fp.Interface()); err != nil {
			fieldErrs = append(fieldErrs, FieldError{Field: key, Err: err})
			if conf.skippedFields == nil {
				conf.skippedFields = make(map[string]struct{})
			}
			conf.skippedFields[key] = struct{}{}
			continue
		}
		v.Field(i).Set(fp)
	}

	return conf, fieldErrs, nil
}

// yamlKey returns the YAML name of a struct field (tag value minus options).
func yamlKey(sf reflect.StructField) string {
	tag := sf.Tag.Get("yaml")
	if tag == "" {
		return ""
	}
	return strings.SplitN(tag, ",", 2)[0]
}

// UpdateConfig does in-place update of `source` config based on values available in
// `parsed` config.
func UpdateRuntimeConfig(log logrus.FieldLogger, source, parsed *WeaviateRuntimeConfig, hooks map[string]func() error) error {
	if source == nil || parsed == nil {
		return fmt.Errorf("source and parsed cannot be nil")
	}

	updateRuntimeConfig(log, reflect.ValueOf(*source), reflect.ValueOf(*parsed), parsed.skippedFields, hooks)
	return nil
}

/*
Alright. `updateRuntimeConfig` needs some explanation.

We could have avoided using `reflection` all together, if we have written something like this.

	func updateRuntimeConfig(source, parsed *WeaviateRuntimeConfig) error {
		if parsed.MaximumAllowedCollectionsCount != nil {
			source.MaximumAllowedCollectionsCount.SetValue(parsed.MaximumAllowedCollectionsCount.Get())
		} else {
			source.MaximumAllowedCollectionsCount.Reset()
		}

		if parsed.AsyncReplicationDisabled != nil {
			source.AsyncReplicationDisabled.SetValue(parsed.AsyncReplicationDisabled.Get())
		} else {
			source.AsyncReplicationDisabled.Reset()
		}

		if parsed.AutoschemaEnabled != nil {
			source.AutoschemaEnabled.SetValue(parsed.AutoschemaEnabled.Get())
		} else {
			source.AutoschemaEnabled.Reset()
		}

		return nil
	}

But this approach has two serious drawbacks
 1. Everytime new config is supported, this function gets verbose as we have update for every struct fields in WeaviateRuntimeConfig
 2. The much bigger one is, what if consumer added a struct field, but failed to **update** this function?. This was a serious concern for me, more work for
    consumers.

With this reflection method, we avoided that extra step from the consumer. This reflection approach is "logically" same as above implementation.
See "runtimeconfig_test.go" for more examples.
*/

func updateRuntimeConfig(log logrus.FieldLogger, source, parsed reflect.Value, skipped map[string]struct{}, hooks map[string]func() error) {
	// Basically we do following
	//
	// 1. Loop through all the `source` fields
	// 2. Check if any of those fields exists in `parsed` (non-nil)
	// 3. If parsed config doesn't contain the field from `source`, We reset source's field.
	//    so that it's default value takes preference.
	// 4. If parsed config does contain the field from `source`, We update the value via `SetValue`.

	logRecords := make([]updateLogRecord, 0)

	for i := range source.NumField() {
		sfType := source.Type().Field(i)
		// Unexported bookkeeping fields (e.g. skippedFields) are not config values.
		if !sfType.IsExported() {
			continue
		}
		// A field whose value failed to decode is left as-is: don't apply the bad
		// value, but don't reset it to the default either.
		if _, skip := skipped[yamlKey(sfType)]; skip {
			continue
		}

		sf := source.Field(i)
		pf := parsed.Field(i)

		r := updateLogRecord{
			field: sfType.Name,
		}

		si := sf.Interface()
		var pi any
		if !pf.IsNil() {
			pi = pf.Interface()
		}

		switch sv := si.(type) {
		case *runtime.DynamicValue[int]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[int])
				r.err = sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[float64]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[float64])
				r.err = sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[bool]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[bool])
				r.err = sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[time.Duration]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[time.Duration])
				r.err = sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[string]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[string])
				r.err = sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[[]string]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[[]string])
				r.err = sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		default:
			panic(fmt.Sprintf("not recognized type: %#v, %#v", pi, si))
		}

		if r.err != nil || !reflect.DeepEqual(r.newV, r.oldV) {
			logRecords = append(logRecords, r)
		}

	}

	// log the changes made as INFO for auditing.
	for _, r := range logRecords {
		logger := log.WithFields(logrus.Fields{
			"action":    "runtime_overrides_changed",
			"field":     r.field,
			"old_value": r.oldV,
		})

		if r.err != nil {
			logger.WithError(r.err).Errorf("runtime overrides: config '%v' change failed", r.field)
			continue
		}
		logger.WithField("new_value", r.newV).Infof("runtime overrides: config '%v' changed from '%v' to '%v'", r.field, r.oldV, r.newV)
	}

	for match, f := range hooks {
		if matchUpdatedFields(match, logRecords) {
			err := f()
			if err != nil {
				log.WithFields(logrus.Fields{
					"action": "runtime_overrides_hooks",
					"match":  match,
				}).Errorf("error calling runtime hooks for match %s, %v", match, err)
				continue
			}
			log.WithFields(logrus.Fields{
				"action": "runtime_overrides_hooks",
				"match":  match,
			}).Infof("runtime overrides: hook ran for matching '%v' pattern", match)
		}
	}
}

// updateLogRecord is used to record changes during updating runtime config.
type updateLogRecord struct {
	field      string
	err        error
	oldV, newV any
}

func matchUpdatedFields(match string, records []updateLogRecord) bool {
	for _, v := range records {
		if strings.HasPrefix(v.field, match) {
			return true
		}
	}
	return false
}
