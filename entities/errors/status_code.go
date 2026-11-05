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

package errors

import stderrors "errors"

// statusCoder is implemented by errors whose numeric status is part of the
// contract, independent of the message text. Codes match HTTP statuses.
// Handlers that choose a different status for the same error must set the
// response code explicitly; this lookup is only for errors that have one
// status everywhere they surface.
type statusCoder interface {
	error
	StatusCode() int
}

// StatusCode reports the stable numeric status carried by err, walking
// unwrap chains. ok is false when err is nil or no error in the chain
// declares a status.
func StatusCode(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	var coder statusCoder
	if !stderrors.As(err, &coder) {
		return 0, false
	}
	code := coder.StatusCode()
	if code <= 0 {
		return 0, false
	}
	return code, true
}
