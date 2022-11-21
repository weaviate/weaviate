//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replica

const (
	// RequestKey is used to marshalling request IDs
	RequestKey = "request_id"
)

type SimpleResponse struct {
	Errors []string `json:"errors"`
}

func (r *SimpleResponse) FirstError() error {
	for _, msg := range r.Errors {
		if msg != "" {
			return &Error{Msg: msg}
		}
	}
	return nil
}

// DeleteBatchResponse represents the response returned by DeleteObjects
type DeleteBatchResponse struct {
	Batch []UUID2Error `json:"batch,omitempty"`
}

// FirstError returns the first found error
func (r *DeleteBatchResponse) FirstError() error {
	for _, r := range r.Batch {
		if r.Error != "" {
			return &Error{Msg: r.Error}
		}
	}
	return nil
}
