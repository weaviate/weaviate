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
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

type AuthZReq struct {
	Principal *models.Principal
	Verb      string
	Resources []string
}

type FakeAuthorizer struct {
	err          error
	allowedCalls int
	denied       map[string]struct{}
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

// Deny makes Authorize return Forbidden for the named resources and makes
// FilterAuthorizedResources drop them, so a test can give one principal access
// to part of a resource set.
func (a *FakeAuthorizer) Deny(resources ...string) {
	if a.denied == nil {
		a.denied = make(map[string]struct{}, len(resources))
	}
	for _, resource := range resources {
		a.denied[resource] = struct{}{}
	}
}

// Authorize provides a mock function with given fields: principal, verb, resource
func (a *FakeAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.requests = append(a.requests, AuthZReq{principal, verb, resources})
	if a.err != nil && len(a.requests) > a.allowedCalls {
		return a.err
	}
	for _, resource := range resources {
		if _, ok := a.denied[resource]; ok {
			return errors.NewForbidden(principal, verb, resource)
		}
	}
	return nil
}

func (a *FakeAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

func (a *FakeAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	a.requests = append(a.requests, AuthZReq{principal, verb, resources})
	if a.err != nil && len(a.requests) > a.allowedCalls {
		return nil, a.err
	}
	allowed := make([]string, 0, len(resources))
	for _, resource := range resources {
		if _, ok := a.denied[resource]; !ok {
			allowed = append(allowed, resource)
		}
	}
	return allowed, nil
}

func (a *FakeAuthorizer) Calls() []AuthZReq {
	return a.requests
}
