package monitoring

import (
	"sync"

	telemetry "github.com/creativesoftwarefdn/weaviate/telemetry/utils"
)

const Failed int = 0
const Succeeded int = 1

// A simple map struct for now, locking happens at map level. This allows us to easily expand/edit functionality.
// Should performance become an issue then we can create function-level structs and apply mutex at that level.
type RequestsLog struct { // TODO: RENAME
	Mutex *sync.Mutex
	Log   map[string]*RequestLog
}

/* Reset the hashmap used to log performed requests */
func (r RequestsLog) Reset() int {
	if telemetry.IsEnabled() {
		r.Mutex.Lock()
		r.Log = make(map[string]*RequestLog)
		r.Mutex.Unlock()
		return Succeeded
	}
	return Failed
}

/* Register a performed request. Creates a new entry or updates an existing one. */
func (r RequestsLog) Register(request *RequestLog) int {
	if telemetry.IsEnabled() {

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

type RequestLog struct {
	Name       string // name of the Weaviate instance. Is `a-b-c` where a, b, and c are random words from the contextionary.
	Type       string // "GQL" or "POST"
	Identifier string // name of the request; "weaviate.x.y.z"
	Amount     int    // how often the function was called
	When       int64  // timestamp in epoch
}
