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

package replica

import (
	"sync"
	"time"
)

// logThrottlePruneAt is the key count past which Allow drops keys idle for a whole window.
const logThrottlePruneAt = 1024

// LogThrottle caps a repeating log line to one per key per window, so a failure repeated per class or tenant logs once.
type LogThrottle struct {
	window time.Duration
	mu     sync.Mutex
	keys   map[string]*logThrottleKey
}

type logThrottleKey struct {
	last       time.Time
	suppressed int
}

func NewLogThrottle(window time.Duration) *LogThrottle {
	return &LogThrottle{window: window, keys: make(map[string]*logThrottleKey)}
}

// Allow reports whether key may log now and how many of its calls were suppressed since its previous line.
func (t *LogThrottle) Allow(key string) (bool, int) {
	return t.allowAt(key, time.Now())
}

func (t *LogThrottle) allowAt(key string, now time.Time) (bool, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.keys[key]
	if ok && now.Sub(k.last) < t.window {
		k.suppressed++
		return false, 0
	}
	if !ok {
		if len(t.keys) >= logThrottlePruneAt {
			for name, idle := range t.keys {
				if now.Sub(idle.last) >= t.window {
					delete(t.keys, name)
				}
			}
		}
		k = &logThrottleKey{}
		t.keys[key] = k
	}
	suppressed := k.suppressed
	k.last, k.suppressed = now, 0
	return true, suppressed
}

// checkpointLogThrottle is shared by every class's Finder so a failing host logs once per window across the whole fan-out.
var checkpointLogThrottle = NewLogThrottle(time.Minute)
