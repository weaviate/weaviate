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

package errors

import (
	"errors"
	"fmt"
)

type ErrGraphQLUser struct {
	err                  error
	queryType, className string
}

func (e ErrGraphQLUser) Error() string {
	return e.err.Error()
}

func (e ErrGraphQLUser) OriginalError() error {
	return e.err
}

func (e ErrGraphQLUser) QueryType() string {
	return e.queryType
}

func (e ErrGraphQLUser) ClassName() string {
	return e.className
}

func NewErrGraphQLUser(err error, operation, className string) ErrGraphQLUser {
	return ErrGraphQLUser{err, operation, className}
}

type ErrRateLimit struct {
	err error
}

func (e ErrRateLimit) Error() string {
	return e.err.Error()
}

func NewErrRateLimit() ErrRateLimit {
	return ErrRateLimit{errors.New("429 Too many requests")}
}

type ErrLockConnector struct {
	err error
}

func (e ErrLockConnector) Error() string {
	return e.err.Error()
}

func NewErrLockConnector(err error) ErrLockConnector {
	return ErrLockConnector{fmt.Errorf("could not acquire lock: %w", err)}
}
