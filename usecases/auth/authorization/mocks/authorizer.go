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

package mocks

import (
	models "github.com/weaviate/weaviate/entities/models"
)

type AuthZReq struct {
	Principal *models.Principal
	Verb      string
	Resource  string
}

type fakeAuthorizer struct {
	err      error
	requests []AuthZReq
}

func NewMockAuthorizer(errs ...error) *fakeAuthorizer {
	if len(errs) > 0 {
		return &fakeAuthorizer{err: errs[0]}
	}
	return &fakeAuthorizer{}
}

// Authorize provides a mock function with given fields: principal, verb, resource
func (a *fakeAuthorizer) Authorize(principal *models.Principal, verb string, resource string) error {
	a.requests = append(a.requests, AuthZReq{principal, verb, resource})
	if a.err != nil {
		return a.err
	}
	return nil
}

func (a *fakeAuthorizer) Calls() []AuthZReq {
	return a.requests
}
