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

package runtime

import (
	"os"
	"strconv"
	"sync"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	ldInterfaces "github.com/launchdarkly/go-server-sdk/v7/interfaces"
	"github.com/sirupsen/logrus"
)

// SupportedTypes is the generic type grouping that FeatureFlag can handle.
type SupportedTypes interface {
	bool | string | int | float64
}

// FeatureFlag is a generic structure that supports reading a value that can be changed by external factors at runtime (for example LD
// integration).
// It is meant to be used for configuration options that can be updated at runtime.
type FeatureFlag[T SupportedTypes] struct {
	// Not embedding the mutex as we don't want to expose the mutex API to users of FeatureFlag
	mu sync.RWMutex
	// value is the actual value of the feature flag
	value T
	// key is the LD key of the flag as registered in the platform
	key string

	// onChange is an array of callbacks that will be called on any change of value.
	// The callback will receive the previous value and the new value of the flag.
	// The callbacks are blocking subsequent updates from LD and should therefore return fast and not block.
	onChange []func(oldValue T, newValue T)
	// ldChangeChannel is the channel from which the LD sdk will send update to the flag based on the targeting configured in the SDK and
	// the flag key.
	ldChangeChannel <-chan ldInterfaces.FlagValueChangeEvent
}

// NewFeatureFlag returns a new feature flag of type T configured with key as the remote LD flag key (if LD integration is available) and
// defaultValue used as the default value of the flag.
func NewFeatureFlag[T SupportedTypes](key string, defaultValue T) *FeatureFlag[T] {
	f := &FeatureFlag[T]{
		key:   key,
		value: defaultValue,
	}
	// If an LD client is available let's use it
	if ldClient != nil {
		// Start up the change listener to receive update & re-evaluate the flag from LD
		f.ldChangeChannel = ldClient.GetFlagTracker().AddFlagValueChangeListener(f.key, ldContext, ldvalue.Null())
		f.startChangeMonitoring()
	}
	return f
}

func (f *FeatureFlag[T]) startChangeMonitoring() {
	go func() {
		for event := range f.ldChangeChannel {
			// If the value is the same as the default one that is configured that means an error happened when evaluating the flag,
			// therefore we fallback to the current stored/default value.
			if event.NewValue.IsNull() {
				continue
			}

			// Read the value from the payload and then cast it back to the expected flag type on our end
			var value interface{}
			switch event.NewValue.Type() {
			case ldvalue.BoolType:
				value = event.NewValue.BoolValue()
			case ldvalue.StringType:
				value = event.NewValue.StringValue()
			case ldvalue.NumberType:
				// We have to check the underlying number type of value (if any) in order to cast to the right number type.
				switch any(f.value).(type) {
				case int:
					value = event.NewValue.IntValue()
				case float64:
					value = event.NewValue.Float64Value()
				default:
					continue
				}
			default:
				continue
			}

			// If we can cast the value use it
			if v, ok := value.(T); ok {
				f.mu.Lock()
				old := f.value
				f.value = v
				f.mu.Unlock()
				if f.onChange != nil {
					for _, onChange := range f.onChange {
						onChange(old, v)
					}
				}
			}
		}
	}()
}

// Close ensure that all internal ressources are freed
func (f *FeatureFlag[T]) Close() {
	if ldClient != nil {
		ldClient.GetFlagTracker().RemoveFlagValueChangeListener(f.ldChangeChannel)
	}
}

// WithEnvDefault configures the feature flag to read a default value from envVariable.
// If envVariable is not present in the env the default value will not change
// If envVariable returns a value that is not convertible to type T the value will not change.
// Type string is directly casted
// Type int is parsed with strconv.Atoi
// Type float64 is parsed with strconv.ParseFloat
// Type bool is parsed with strconv.ParseBool
func (f *FeatureFlag[T]) WithEnvDefault(envVariable string) *FeatureFlag[T] {
	val, ok := os.LookupEnv(envVariable)
	// Return early if the env variable is absent
	if !ok {
		return f
	}

	// We're going to both read and write the value
	f.mu.Lock()
	defer f.mu.Unlock()
	// Parse val into the right type expected by T
	switch any(f.value).(type) {
	case string:
		v, ok := any(val).(T)
		if ok {
			f.value = v
		}
	case int:
		v, err := strconv.Atoi(val)
		if err == nil {
			f.value = any(v).(T)
		}
	case float64:
		v, err := strconv.ParseFloat(val, 64)
		if err == nil {
			f.value = any(v).(T)
		}
	case bool:
		v, err := strconv.ParseBool(val)
		if err == nil {
			f.value = any(v).(T)
		}
	default:
		// Passthrough silently
	}
	return f
}

// WithOnChangeCallback registers onChange to be called everytime the value of the feature flag is changed at runtime
// These callbacks are blocking in the loop to update the feature flag, therefore the callback *must not* block.
func (f *FeatureFlag[T]) WithOnChangeCallback(onChange func(T, T)) *FeatureFlag[T] {
	f.onChange = append(f.onChange, onChange)
	return f
}

// WithLogOnChange configures logger to log with Info level everytime a change in the feature flag is detected.
func (f *FeatureFlag[T]) WithLogOnChange(logger logrus.FieldLogger) *FeatureFlag[T] {
	return f.WithOnChangeCallback(func(oldValue T, newValue T) {
		logger.WithFields(logrus.Fields{
			"action":    "feature_flag",
			"flag_key":  f.key,
			"old_value": oldValue,
			"new_value": newValue,
		}).Info("flag change detected")
	})
}

// Get reads the current value of the feature flags and returns it.
func (f *FeatureFlag[T]) Get() T {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.value
}
