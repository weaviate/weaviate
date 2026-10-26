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

package cyclemanager

// RegisterOption is applied to a callback at registration time.
type RegisterOption func(*cycleCallbackMeta)

// AsInactive returns a RegisterOption that marks the callback inactive on
// registration; it will not run until explicitly activated.
func AsInactive() RegisterOption {
	return func(m *cycleCallbackMeta) {
		m.setInactive()
	}
}

// WithIntervals returns a RegisterOption that assigns the given intervals to the
// callback and adjusts the start time so the first run is immediate. Returns nil
// when intervals is nil, which Register treats as a no-op option.
func WithIntervals(intervals CycleIntervals) RegisterOption {
	if intervals == nil {
		return nil
	}
	return func(m *cycleCallbackMeta) {
		m.setIntervals(intervals)
	}
}
