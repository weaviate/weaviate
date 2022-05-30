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

package objects

import (
	"fmt"

	"github.com/pkg/errors"
)

var (
	// ErrItemNotFound item doesn't exist
	ErrItemNotFound = errors.New("item not found")
	// ErrValidation invalid inputs
	ErrValidation = errors.New("validation")
	//  ErrAuthorization access denied
	ErrAuthorization = errors.New("authorization")
	// ErrServiceInternal error
	ErrServiceInternal = errors.New("service internal")
)

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
