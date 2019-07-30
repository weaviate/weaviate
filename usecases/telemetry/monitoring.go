//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package telemetry

import (
	"fmt"
	"sync"
)

// RequestsLog is a struct used to log the type and amount of POST and GQL requests this Weaviate receives.
// Contains a map and a mutex used to control access to this map.
type RequestsLog struct {
	Mutex    *sync.Mutex
	Log      map[string]*RequestLog
	PeerName string
	Disabled bool
	Debug    bool
}

// NewLog creates a new Requestslog and returns a pointer to it.
func NewLog() *RequestsLog {
	return &RequestsLog{
		Mutex:    &sync.Mutex{},
		Log:      make(map[string]*RequestLog),
		Disabled: false,
		Debug:    false,
	}
}

// Register a performed request. Either creates a new entry or updates an existing one,
// depending on whether a request of that type has already been logged.
func (r *RequestsLog) Register(requestType string, identifier string) {
	if !r.Disabled {

		requestLog := &RequestLog{
			Type:       requestType,
			Identifier: identifier,
			Amount:     1,
		}

		r.Mutex.Lock()

		requestUID := fmt.Sprintf("[%s]%s", requestType, identifier)

		if _, ok := r.Log[requestUID]; ok {
			r.Log[requestUID].Amount++
		} else {
			r.Log[requestUID] = requestLog
		}
		r.Mutex.Unlock()
	}
}

// ExtractLoggedRequests copies the map used to log performed requests and resets the map to its default (empty) state.
func (r *RequestsLog) ExtractLoggedRequests() *map[string]*RequestLog {
	if !r.Disabled {
		r.Mutex.Lock()
		logState := make(map[string]*RequestLog)

		if len(r.Log) > 0 {
			for key, value := range r.Log {
				logState[key] = value
			}
		}

		r.Log = make(map[string]*RequestLog)
		r.Mutex.Unlock()

		return &logState

	}
	return nil
}

// RequestLog is a struct used both for logging new request type calls and for counting the total amount of calls per
// request type in a RequestsLog. Contains all relevant details of an individual request type.
type RequestLog struct {
	Name       string // name of the Weaviate instance. Is `a-b-c` where a, b, and c are random words from the contextionary.
	Type       string // "GQL" or "POST"
	Identifier string // id of the request; "weaviate.x.y.z"
	Amount     int    // how often the function was called
	When       int64  // timestamp in epoch
}
