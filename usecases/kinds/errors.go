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

package kinds

import "fmt"

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
