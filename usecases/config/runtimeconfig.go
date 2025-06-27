//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"gopkg.in/yaml.v3"
)

// WeaviateRuntimeConfig is the collection all the supported configs that is
// managed dynamically and can be overridden during runtime.
type WeaviateRuntimeConfig struct {
	MaximumAllowedCollectionsCount  *runtime.DynamicValue[int]           `json:"maximum_allowed_collections_count" yaml:"maximum_allowed_collections_count"`
	AutoschemaEnabled               *runtime.DynamicValue[bool]          `json:"autoschema_enabled" yaml:"autoschema_enabled"`
	AsyncReplicationDisabled        *runtime.DynamicValue[bool]          `json:"async_replication_disabled" yaml:"async_replication_disabled"`
	RevectorizeCheckDisabled        *runtime.DynamicValue[bool]          `json:"revectorize_check_disabled" yaml:"revectorize_check_disabled"`
	ReplicaMovementMinimumAsyncWait *runtime.DynamicValue[time.Duration] `json:"replica_movement_minimum_async_wait" yaml:"replica_movement_minimum_async_wait"`
	TenantActivityReadLogLevel      *runtime.DynamicValue[string]        `json:"tenant_activity_read_log_level" yaml:"tenant_activity_read_log_level"`
	TenantActivityWriteLogLevel     *runtime.DynamicValue[string]        `json:"tenant_activity_write_log_level" yaml:"tenant_activity_write_log_level"`
	QuerySlowLogEnabled             *runtime.DynamicValue[bool]          `json:"query_slow_log_enabled" yaml:"query_slow_log_enabled"`
	QuerySlowLogThreshold           *runtime.DynamicValue[time.Duration] `json:"query_slow_log_threshold" yaml:"query_slow_log_threshold"`
	InvertedSorterDisabled          *runtime.DynamicValue[bool]          `json:"inverted_sorter_disabled" yaml:"inverted_sorter_disabled"`

	// Experimental configs. Will be removed in the future.
	OIDCIssuer            *runtime.DynamicValue[string]   `json:"exp_oidc_issuer" yaml:"exp_oidc_issuer"`
	OIDCClientID          *runtime.DynamicValue[string]   `json:"exp_oidc_client_id" yaml:"exp_oidc_client_id"`
	OIDCSkipClientIDCheck *runtime.DynamicValue[bool]     `yaml:"exp_oidc_skip_client_id_check" json:"exp_oidc_skip_client_id_check"`
	OIDCUsernameClaim     *runtime.DynamicValue[string]   `yaml:"exp_oidc_username_claim" json:"exp_oidc_username_claim"`
	OIDCGroupsClaim       *runtime.DynamicValue[string]   `yaml:"exp_oidc_groups_claim" json:"exp_oidc_groups_claim"`
	OIDCScopes            *runtime.DynamicValue[[]string] `yaml:"exp_oidc_scopes" json:"exp_oidc_scopes"`
	OIDCCertificate       *runtime.DynamicValue[string]   `yaml:"exp_oidc_certificate" json:"exp_oidc_certificate"`
}

// ParseRuntimeConfig decode WeaviateRuntimeConfig from raw bytes of YAML.
func ParseRuntimeConfig(buf []byte) (*WeaviateRuntimeConfig, error) {
	var conf WeaviateRuntimeConfig

	dec := yaml.NewDecoder(bytes.NewReader(buf))

	// To catch fields different than ones in the struct (say typo)
	dec.KnownFields(true)

	// Am empty runtime yaml file is still a valid file. So treating io.EOF as
	// non-error case returns default values of config.
	if err := dec.Decode(&conf); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &conf, nil
}

// UpdateConfig does in-place update of `source` config based on values available in
// `parsed` config.
func UpdateRuntimeConfig(log logrus.FieldLogger, source, parsed *WeaviateRuntimeConfig, hooks map[string]func() error) error {
	if source == nil || parsed == nil {
		return fmt.Errorf("source and parsed cannot be nil")
	}

	updateRuntimeConfig(log, reflect.ValueOf(*source), reflect.ValueOf(*parsed), hooks)
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

func updateRuntimeConfig(log logrus.FieldLogger, source, parsed reflect.Value, hooks map[string]func() error) {
	// Basically we do following
	//
	// 1. Loop through all the `source` fields
	// 2. Check if any of those fields exists in `parsed` (non-nil)
	// 3. If parsed config doesn't contain the field from `source`, We reset source's field.
	//    so that it's default value takes preference.
	// 4. If parsed config does contain the field from `source`, We update the value via `SetValue`.

	logRecords := make([]updateLogRecord, 0)

	for i := range source.NumField() {
		sf := source.Field(i)
		pf := parsed.Field(i)

		r := updateLogRecord{
			field: source.Type().Field(i).Name,
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
				sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[float64]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[float64])
				sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[bool]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[bool])
				sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[time.Duration]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[time.Duration])
				sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[string]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[string])
				sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		case *runtime.DynamicValue[[]string]:
			r.oldV = sv.Get()
			if pf.IsNil() {
				// Means the config is removed
				sv.Reset()
			} else {
				p := pi.(*runtime.DynamicValue[[]string])
				sv.SetValue(p.Get())
			}
			r.newV = sv.Get()
		default:
			panic(fmt.Sprintf("not recognized type: %#v, %#v", pi, si))
		}

		if !reflect.DeepEqual(r.newV, r.oldV) {
			logRecords = append(logRecords, r)
		}

	}

	// log the changes made as INFO for auditing.
	for _, v := range logRecords {
		log.WithFields(logrus.Fields{
			"action":    "runtime_overrides_changed",
			"field":     v.field,
			"old_value": v.oldV,
			"new_value": v.newV,
		}).Infof("runtime overrides: config '%v' changed from '%v' to '%v'", v.field, v.oldV, v.newV)
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
	oldV, newV any
}

func matchUpdatedFields(match string, records []updateLogRecord) bool {
	for _, v := range records {
		if strings.Contains(v.field, match) {
			return true
		}
	}
	return false
}
