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
	"github.com/weaviate/weaviate/entities/errors"
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

	ldInteg *LDIntegration
	// onChange is an array of callbacks that will be called on any change of value.
	// The callback will receive the previous value and the new value of the flag.
	// The callbacks are blocking subsequent updates from LD and should therefore return fast and not block.
	onChange []func(oldValue T, newValue T)
	// ldChangeChannel is the channel from which the LD sdk will send update to the flag based on the targeting configured in the SDK and
	// the flag key.
	ldChangeChannel <-chan ldInterfaces.FlagValueChangeEvent
	logger          logrus.FieldLogger
}

// NewFeatureFlag returns a new feature flag of type T configured with key as the remote LD flag key (if LD integration is available) and
// defaultValue used as the default value of the flag.
func NewFeatureFlag[T SupportedTypes](key string, defaultValue T, ldInteg *LDIntegration, envDefault string, logger logrus.FieldLogger) *FeatureFlag[T] {
	f := &FeatureFlag[T]{
		key:     key,
		value:   defaultValue,
		ldInteg: ldInteg,
		logger:  logger.WithFields(logrus.Fields{"tool": "feature_flag", "flag_key": key}),
	}
	// If an env is specified let's read it and load it as the default value
	if envDefault != "" {
		f.evaluateEnv(envDefault)
	}

	// If an LD client is available let's use it
	if f.ldInteg != nil {
		// use f.value as the default value for LD, this way in case of LD failure it will fallback to that default
		// this is useful if we have an env variable that changes the default from defaultValue
		f.evaluateLDFlag(f.value)

		// Start up the change listener to receive update & re-evaluate the flag from LD
		f.ldChangeChannel = f.ldInteg.ldClient.GetFlagTracker().AddFlagValueChangeListener(f.key, f.ldInteg.ldContext, ldvalue.Null())
		f.startChangeMonitoring()
	}
	f.logger.WithFields(logrus.Fields{
		"value": f.value,
	}).Info("feature flag instantiated")

	// Always log feature flag changes
	f.WithOnChangeCallback(func(oldValue T, newValue T) {
		f.logger.WithFields(logrus.Fields{
			"old_value": oldValue,
			"new_value": newValue,
		}).Info("flag change detected")
	})
	return f
}

func (f *FeatureFlag[T]) startChangeMonitoring() {
	errors.GoWrapper(func() {
		for event := range f.ldChangeChannel {
			// If the value is the same as the default one that is configured that means an error happened when evaluating the flag,
			// therefore we fallback to the current stored/default value.
			if event.NewValue.IsNull() {
				f.logger.Warn("LD updated value to null, ignoring and keeping current value")
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
					f.logger.Warnf("could not parse number update from LD as flag is not a number but instead %T", f.value)
				}
			default:
				// We only hit this case if the flag on the LD side is not configured as a supported type
				f.logger.Warn("could not parse update from LD as flag is not a supported type")
			}

			// If we can cast the value use it
			if v, ok := value.(T); ok {
				// Explicitly Lock and Unlock before calling the change callbacks to ensure we don't keep a lock while
				// calling the callbacks
				f.mu.Lock()
				old := f.value
				f.value = v
				f.mu.Unlock()

				// Be explicit in debug mode only
				f.logger.WithFields(logrus.Fields{
					"old_value": old,
					"new_value": value,
				}).Debug("flag change detected")

				// If we have any callbacks registered, call them now
				if f.onChange != nil {
					for _, onChange := range f.onChange {
						onChange(old, v)
					}
				}
			}
		}
	}, f.logger)
}

func (f *FeatureFlag[T]) evaluateLDFlag(defaultVal T) {
	// We will read and write the flag
	f.mu.Lock()
	defer f.mu.Unlock()

	// Lookup which underlying type we're implementing and call the related LD flag evaluation method
	// then store the result
	switch any(f.value).(type) {
	case string:
		if v, ok := any(defaultVal).(string); !ok {
			f.logger.Warnf("should not happen, type %T evaluated as string but can't be casted to string", f.value)
		} else {
			flag, err := f.ldInteg.ldClient.StringVariation(f.key, f.ldInteg.ldContext, v)
			if err != nil {
				f.logger.Warnf("could not evaluate LD flag: %w", err)
			}
			f.value = any(flag).(T)
		}
	case int:
		if v, ok := any(defaultVal).(int); !ok {
			f.logger.Warnf("should not happen, type %T evaluated as int but can't be casted to int", f.value)
		} else {
			flag, err := f.ldInteg.ldClient.IntVariation(f.key, f.ldInteg.ldContext, v)
			if err != nil {
				f.logger.Warnf("could not evaluate LD flag: %w", err)
			}
			f.value = any(flag).(T)
		}
	case float64:
		if v, ok := any(defaultVal).(float64); !ok {
			f.logger.Warnf("should not happen, type %T evaluated as float64 but can't be casted to float64", f.value)
		} else {
			flag, err := f.ldInteg.ldClient.Float64Variation(f.key, f.ldInteg.ldContext, v)
			if err != nil {
				f.logger.Warnf("could not evaluate LD flag: %w", err)
			}
			f.value = any(flag).(T)
		}
	case bool:
		if v, ok := any(defaultVal).(bool); !ok {
			f.logger.Warnf("should not happen, type %T evaluated as bool but can't be casted to bool", f.value)
		} else {
			flag, err := f.ldInteg.ldClient.BoolVariation(f.key, f.ldInteg.ldContext, v)
			if err != nil {
				f.logger.Warnf("could not evaluate LD flag: %w", err)
			}
			f.value = any(flag).(T)
		}
	default:
		// This should not happen
		f.logger.Warnf("unsuported feature flag value type, got %T", f.value)
	}

	f.logger.WithField("value", f.value).Infof("flag value after LD evaluation")
}

// Close ensure that all internal ressources are freed
func (f *FeatureFlag[T]) Close() {
	if f.ldInteg != nil {
		f.ldInteg.ldClient.GetFlagTracker().RemoveFlagValueChangeListener(f.ldChangeChannel)
	}
}

// evaluate configures the feature flag to read a default value from envVariable.
// If envVariable is not present in the env the default value will not change
// If envVariable returns a value that is not convertible to type T the value will not change.
// Type string is directly casted
// Type int is parsed with strconv.Atoi
// Type float64 is parsed with strconv.ParseFloat
// Type bool is parsed with strconv.ParseBool
func (f *FeatureFlag[T]) evaluateEnv(envVariable string) *FeatureFlag[T] {
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
		if v, ok := any(val).(T); !ok {
			f.logger.Warn("could not cast string from env, ignoring env value")
		} else {
			f.value = v
		}
	case int:
		if v, err := strconv.Atoi(val); err != nil {
			f.logger.Warn("could not parse int from env, ignoring env value: %w", err)
		} else {
			f.value = any(v).(T)
		}
	case float64:
		if v, err := strconv.ParseFloat(val, 64); err != nil {
			f.logger.Warn("could not parse float64 from env, ignoring env value: %w", err)
		} else {
			f.value = any(v).(T)
		}
	case bool:
		if v, err := strconv.ParseBool(val); err != nil {
			f.logger.Warn("could not parse bool from env, ignoring env value: %w", err)
		} else {
			f.value = any(v).(T)
		}
	default:
		// This should not happen
		f.logger.Warnf("unsuported feature flag value type, got %T", f.value)
	}
	f.logger.WithField("value", f.value).Infof("flag value after env evaluation")
	return f
}

// WithOnChangeCallback registers onChange to be called everytime the value of the feature flag is changed at runtime
// These callbacks are blocking in the loop to update the feature flag, therefore the callback *must not* block.
func (f *FeatureFlag[T]) WithOnChangeCallback(onChange func(T, T)) *FeatureFlag[T] {
	f.onChange = append(f.onChange, onChange)
	return f
}

// Get reads the current value of the feature flags and returns it.
func (f *FeatureFlag[T]) Get() T {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.value
}
