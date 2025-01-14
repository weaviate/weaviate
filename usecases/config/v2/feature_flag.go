package v2

import (
	"fmt"
	"os"
	"strconv"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	ldInterfaces "github.com/launchdarkly/go-server-sdk/v7/interfaces"
	"github.com/sirupsen/logrus"
)

type SupportedTypes interface {
	bool | string | int | float64
}

type FeatureFlag[T SupportedTypes] struct {
	key   string
	value T

	onChange        []func(T, T)
	ldChangeChannel <-chan ldInterfaces.FlagValueChangeEvent
}

func NewFeatureFlag[T SupportedTypes](key string, defaultValue T) *FeatureFlag[T] {
	f := &FeatureFlag[T]{
		key:   key,
		value: defaultValue,
	}
	// If an LD client is available let's use it
	if client != nil {
		// Start up the change listener to receive update & re-evaluate the flag from LD
		f.ldChangeChannel = client.GetFlagTracker().AddFlagValueChangeListener(f.key, ldContext, ldvalue.Null())
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
				old := f.value
				f.value = v
				if f.onChange != nil {
					for _, onChange := range f.onChange {
						onChange(old, v)
					}
				}
			}
		}
	}()
}

func (f *FeatureFlag[T]) Close() {
	client.GetFlagTracker().RemoveFlagValueChangeListener(f.ldChangeChannel)
}

func (f *FeatureFlag[T]) WithEnvDefault(envVariable string) *FeatureFlag[T] {
	val, ok := os.LookupEnv(envVariable)
	// Return early if the env variable is absent
	if !ok {
		return f
	}

	// Parse val into the right type expected by T
	switch any(f.value).(type) {
	case string:
		v, ok := any(val).(T)
		if !ok {
			panic("could not convert to string")
		}
		f.value = v
	case int:
		v, err := strconv.Atoi(val)
		if err != nil {
			panic(fmt.Sprintf("could not convert to int: %s", err))
		}
		f.value = any(v).(T)
	case float64:
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			panic(fmt.Sprintf("could not convert to float64: %s", err))
		}
		f.value = any(v).(T)
	case bool:
		v, err := strconv.ParseBool(val)
		if err != nil {
			panic(fmt.Sprintf("could not convert to bool: %s", err))
		}
		f.value = any(v).(T)
	default:
		// Passthrough silently
	}
	return f
}

func (f *FeatureFlag[T]) WithOnChangeCallback(onChange func(T, T)) *FeatureFlag[T] {
	f.onChange = append(f.onChange, onChange)
	return f
}

func (f *FeatureFlag[T]) WithLogOnChange(logger logrus.FieldLogger) *FeatureFlag[T] {
	return f.WithOnChangeCallback(func(oldValue T, newValue T) {
		logger.WithFields(logrus.Fields{
			"flag_key":  f.key,
			"old_value": oldValue,
			"new_value": newValue,
		}).Info("flag change detected")
	})
}

func (f *FeatureFlag[T]) Get() T {
	return f.value
}
