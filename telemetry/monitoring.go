package telemetry

import (
	"fmt"
	"sync"
)

const Failed int = 0
const Succeeded int = 1

// A struct used to log the type and amount of POST and GQL Requests this Weaviate receives.
// Contains a map and a mutex used to control access to this map.
type RequestsLog struct { // TODO: RENAME
	Mutex *sync.Mutex
	Log   map[string]*RequestLog
}

// Create a new Requestslog
func NewLog() *RequestsLog {
	return &RequestsLog{
		Mutex: &sync.Mutex{},
		Log:   make(map[string]*RequestLog),
	}
}

// Register a performed Request. Either creates a new entry or updates an existing one.
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

// Extract the hashmap used to log performed Requests and reset it to its default state.
func (r *RequestsLog) ExtractLoggedRequests(telemetryEnabled bool) *map[string]*RequestLog {
	fmt.Println("resetting")
	if telemetryEnabled {
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

// Note `When` attribute is not set here; the timestamp is provided separately at the moment of POSTing
func NewRequestTypeLog(name string, requestType string, identifier string, amount int) *RequestLog {
	return &RequestLog{
		Name:       name,
		Type:       requestType,
		Identifier: identifier,
		Amount:     amount,
	}
}
