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

// publicStatusCoder is implemented by errors whose numeric status is part of
// the public API contract, independent of the message text. The method is
// PublicErrorCode, not StatusCode: adapters/clients.HTTPError.StatusCode
// describes an internal cluster response. errors.As would otherwise copy
// that internal status onto a public error object (a forwarded write that
// comes back 401 is still an HTTP 500 to the caller).
//
// Handlers that choose a different status for the same error must set the
// response code explicitly. This lookup is only for errors that have one
// public status everywhere they surface.
type publicStatusCoder interface {
	error
	PublicErrorCode() int
}

// StatusCode reports the stable public status carried by err, walking
// unwrap chains. ok is false when err is nil or no error in the chain
// declares a public status. An internal HTTPError in the chain does not
// count.
func StatusCode(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	var coder publicStatusCoder
	if !stderrors.As(err, &coder) {
		return 0, false
	}
	code := coder.PublicErrorCode()
	if code <= 0 {
		return 0, false
	}
	return code, true
}
