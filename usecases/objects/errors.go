//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"fmt"
)

// objects status code
const (
	StatusForbidden           = 403
	StatusBadRequest          = 400
	StatusNotFound            = 404
	StatusUnprocessableEntity = 422
	StatusInternalServerError = 500
)

type Error struct {
	Msg  string
	Code int
	Err  error
}

// Error implements error interface
func (e *Error) Error() string {
	return fmt.Sprintf("msg:%s code:%v err:%v", e.Msg, e.Code, e.Err)
}

// Unwrap underlying error
func (e *Error) Unwrap() error {
	return e.Err
}

func (e *Error) NotFound() bool {
	return e.Code == StatusNotFound
}

func (e *Error) Forbidden() bool {
	return e.Code == StatusForbidden
}

func (e *Error) BadRequest() bool {
	return e.Code == StatusBadRequest
}

func (e *Error) UnprocessableEntity() bool {
	return e.Code == StatusUnprocessableEntity
}

// ErrInvalidUserInput indicates a client-side error
type ErrInvalidUserInput struct {
	msg string
}

func (e ErrInvalidUserInput) Error() string {
	return e.msg
}

// NewErrInvalidUserInput with Errorf signature
func NewErrInvalidUserInput(format string, args ...interface{}) ErrInvalidUserInput {
	return ErrInvalidUserInput{msg: fmt.Sprintf(format, args...)}
}

// ErrInternal indicates something went wrong during processing
type ErrInternal struct {
	msg string
}

func (e ErrInternal) Error() string {
	return e.msg
}

// NewErrInternal with Errorf signature
func NewErrInternal(format string, args ...interface{}) ErrInternal {
	return ErrInternal{msg: fmt.Sprintf(format, args...)}
}

// ErrNotFound indicates the desired resource doesn't exist
type ErrNotFound struct {
	msg string
}

func (e ErrNotFound) Error() string {
	return e.msg
}

// NewErrNotFound with Errorf signature
func NewErrNotFound(format string, args ...interface{}) ErrNotFound {
	return ErrNotFound{msg: fmt.Sprintf(format, args...)}
}

type ErrMultiTenancy struct {
	err error
}

func (e ErrMultiTenancy) Error() string {
	return e.err.Error()
}

// NewErrMultiTenancy with error signature
func NewErrMultiTenancy(err error) ErrMultiTenancy {
	return ErrMultiTenancy{err}
}
