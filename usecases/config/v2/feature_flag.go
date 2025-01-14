package v2

import (
	"log"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	ldInterfaces "github.com/launchdarkly/go-server-sdk/v7/interfaces"
)

type SupportedTypes interface {
	bool | string | int | float64
}

type FeatureFlag[T SupportedTypes] struct {
	key   string
	value T

	onChange        func(T interface{})
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
				value = event.NewValue.Float64Value()
			default:
				log.Fatalf("Unsupported type for flag value")
			}

			// If we can cast the value use it
			if v, ok := value.(T); ok {
				f.value = v
				if f.onChange != nil {
					f.onChange(f.value)
				}
			}
		}
	}()
}

func (f *FeatureFlag[T]) Close() {
	client.GetFlagTracker().RemoveFlagValueChangeListener(f.ldChangeChannel)
}

func (f *FeatureFlag[T]) WithOnChangeCallback(onChange func(T interface{})) *FeatureFlag[T] {
	f.onChange = onChange
	return f
}

func (f *FeatureFlag[T]) Get() T {
	return f.value
}
