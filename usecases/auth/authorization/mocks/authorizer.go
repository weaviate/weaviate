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

package mocks

import (
	"context"

	models "github.com/weaviate/weaviate/entities/models"
)

type AuthZReq struct {
	Principal *models.Principal
	Verb      string
	Resources []string
}

type FakeAuthorizer struct {
	err          error
	allowedCalls int
	requests     []AuthZReq
}

func NewMockAuthorizer() *FakeAuthorizer {
	return &FakeAuthorizer{}
}

func (a *FakeAuthorizer) SetErr(err error) {
	a.err = err
}

// SetErrAfter allows the first n Authorize calls and returns err from the rest,
// so a test can observe a check that only runs once an earlier one passes.
func (a *FakeAuthorizer) SetErrAfter(n int, err error) {
	a.allowedCalls = n
	a.err = err
}

// Authorize provides a mock function with given fields: principal, verb, resource
func (a *FakeAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.requests = append(a.requests, AuthZReq{principal, verb, resources})
	if a.err != nil && len(a.requests) > a.allowedCalls {
		return a.err
	}
	return nil
}

func (a *FakeAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

func (a *FakeAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	if err := a.Authorize(ctx, principal, verb, resources...); err != nil {
		return nil, err
	}
	return resources, nil
}

func (a *FakeAuthorizer) Calls() []AuthZReq {
	return a.requests
}
