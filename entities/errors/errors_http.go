//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import "net/http"

const (
	StatusContextExpired = 499
)

type ErrUnprocessable struct {
	err  error
	code int
}

func (e ErrUnprocessable) Error() string {
	return e.err.Error()
}

func NewErrUnprocessable(err error) ErrUnprocessable {
	return ErrUnprocessable{err, http.StatusUnprocessableEntity}
}

type ErrNotFound struct {
	err  error
	code int
}

func (e ErrNotFound) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}

func NewErrNotFound(err error) ErrNotFound {
	return ErrNotFound{err, http.StatusNotFound}
}

type ErrContextExpired struct {
	err  error
	code int
}

func (e ErrContextExpired) Error() string {
	return e.err.Error()
}

func NewErrContextExpired(err error) ErrContextExpired {
	return ErrContextExpired{err, StatusContextExpired}
}

type ErrInternal struct {
	err  error
	code int
}

func (e ErrInternal) Error() string {
	return e.err.Error()
}

func NewErrInternal(err error) ErrInternal {
	return ErrInternal{err, http.StatusInternalServerError}
}

type ErrTooManyRequest struct {
	err  error
	code int
}

func (e ErrTooManyRequest) Error() string {
	return e.err.Error()
}

func NewErrTooManyRequest(err error) ErrTooManyRequest {
	return ErrTooManyRequest{err, http.StatusTooManyRequests}
}
