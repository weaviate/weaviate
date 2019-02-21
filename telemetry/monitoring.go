package telemetry

import (
	"sync"
)

const Failed int = 0
const Succeeded int = 1

// A struct containing a map and a mutex used to control access to this map.
type RequestsLog struct { // TODO: RENAME
	Mutex *sync.Mutex
	Log   map[string]*RequestLog
}

/* Reset the hashmap used to log performed requests */
func (r *RequestsLog) Reset(telemetryEnabled bool) int {
	if telemetryEnabled {
		r.Mutex.Lock()
		r.Log = make(map[string]*RequestLog)
		r.Mutex.Unlock()
		return Succeeded
	}
	return Failed
}

/* Register a performed request. Creates a new entry or updates an existing one. */
func (r *RequestsLog) Register(request *RequestLog, telemetryEnabled bool) int {
	if telemetryEnabled {

		r.Mutex.Lock()
		if _, ok := r.Log[request.Identifier]; ok {
			r.Log[request.Identifier].Amount++
		} else {
			r.Log[request.Identifier] = request
		}
		r.Mutex.Unlock()
		return Succeeded
	}
	return Failed
}

// A struct containing details of an individual request type. Used both for logging new request type calls
// and for counting the total amount of calls per request type in a RequestsLog.
type RequestLog struct {
	Name       string // name of the Weaviate instance. Is `a-b-c` where a, b, and c are random words from the contextionary.
	Type       string // "GQL" or "POST"
	Identifier string // name of the request; "weaviate.x.y.z"
	Amount     int    // how often the function was called
	When       int64  // timestamp in epoch
}
