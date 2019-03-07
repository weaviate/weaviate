package telemetry

import (
	"sync"
)

// A struct used to log the type and amount of POST and GQL Requests this Weaviate receives.
// Contains a map and a mutex used to control access to this map.
type RequestsLog struct { // TODO: RENAME
	Mutex    *sync.Mutex
	Log      map[string]*RequestLog
	PeerName string
	Enabled  bool
}

// Create a new Requestslog
func NewLog(enabled bool) *RequestsLog {
	return &RequestsLog{
		Mutex:   &sync.Mutex{},
		Log:     make(map[string]*RequestLog),
		Enabled: enabled,
	}
}

// Register a performed Request. Either creates a new entry or updates an existing one,
// depending on whether a request of that type has already been logged.
func (r *RequestsLog) Register(requestType string, identifier string) {
	if r.Enabled {

		requestLog := &RequestLog{
			Type:       requestType,
			Identifier: identifier,
			Amount:     1,
		}

		r.Mutex.Lock()

		if _, ok := r.Log[identifier]; ok {
			r.Log[identifier].Amount++
		} else {
			r.Log[identifier] = requestLog
		}
		r.Mutex.Unlock()
	}
}

// Extract the hashmap used to log performed Requests and reset it to its default state.
func (r *RequestsLog) ExtractLoggedRequests() *map[string]*RequestLog {
	if r.Enabled {
		r.Mutex.Lock()
		logState := make(map[string]*RequestLog)

		for key, value := range r.Log {
			logState[key] = value
		}

		r.Log = make(map[string]*RequestLog)
		r.Mutex.Unlock()

		return &logState

	}
	return nil
}

// A struct used both for logging new Request type calls and for counting the total amount of calls per
// Request type in a RequestsLog. Contains all relevant details of an individual Request type.
type RequestLog struct {
	Name       string // name of the Weaviate instance. Is `a-b-c` where a, b, and c are random words from the contextionary.
	Type       string // "GQL" or "POST"
	Identifier string // name of the Request; "weaviate.x.y.z"
	Amount     int    // how often the function was called
	When       int64  // timestamp in epoch
}

// // Note `Name` and `When` attributes are not set here; they are provided separately when logged requests are prepared to be posted
// func NewRequestTypeLog(requestType string, identifier string) *RequestLog {
// 	return &RequestLog{
// 		Type:       requestType,
// 		Identifier: identifier,
// 		Amount:     1,
// 	}
// }
