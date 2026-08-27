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

// AuthZMethod names the Authorizer method a recorded call arrived through.
type AuthZMethod string

const (
	MethodAuthorize                          AuthZMethod = "Authorize"
	MethodAuthorizeAndRequireActiveNamespace AuthZMethod = "AuthorizeAndRequireActiveNamespace"
	MethodAuthorizeSilent                    AuthZMethod = "AuthorizeSilent"
	MethodFilterAuthorizedResources          AuthZMethod = "FilterAuthorizedResources"
)

type AuthZReq struct {
	Principal *models.Principal
	Verb      string
	Resources []string
	Method    AuthZMethod
	// Class is the class argument AuthorizeAndRequireActiveNamespace received,
	// empty for the other methods.
	Class string
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

// SetErrAfter allows the first n calls and returns err from the rest, so a
// test can observe a check that only runs once an earlier one passes.
func (a *FakeAuthorizer) SetErrAfter(n int, err error) {
	a.allowedCalls = n
	a.err = err
}

// Deny makes the authorizing methods return Forbidden for the named resources
// and makes FilterAuthorizedResources drop them, so a test can give one
// principal access to part of a resource set.
func (a *FakeAuthorizer) Deny(resources ...string) {
	if a.denied == nil {
		a.denied = make(map[string]struct{}, len(resources))
	}
	for _, resource := range resources {
		a.denied[resource] = struct{}{}
	}
}

// record logs the call and applies SetErr/SetErrAfter. Denied resources are
// left to the caller: the authorizing methods refuse them, the filter drops
// them.
func (a *FakeAuthorizer) record(req AuthZReq) error {
	a.requests = append(a.requests, req)
	if a.err != nil && len(a.requests) > a.allowedCalls {
		return a.err
	}
	return nil
}

// authorize records the call and refuses it if it names a denied resource.
func (a *FakeAuthorizer) authorize(req AuthZReq) error {
	if err := a.record(req); err != nil {
		return err
	}
	for _, resource := range req.Resources {
		if _, ok := a.denied[resource]; ok {
			return errors.NewForbidden(req.Principal, req.Verb, resource)
		}
	}
	return nil
}

// Authorize provides a mock function with given fields: principal, verb, resource
func (a *FakeAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.authorize(AuthZReq{Principal: principal, Verb: verb, Resources: resources, Method: MethodAuthorize})
}

func (a *FakeAuthorizer) AuthorizeAndRequireActiveNamespace(ctx context.Context, principal *models.Principal, verb string, class string, resources ...string) error {
	return a.authorize(AuthZReq{
		Principal: principal, Verb: verb, Resources: resources,
		Method: MethodAuthorizeAndRequireActiveNamespace, Class: class,
	})
}

func (a *FakeAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.authorize(AuthZReq{Principal: principal, Verb: verb, Resources: resources, Method: MethodAuthorizeSilent})
}

func (a *FakeAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	if err := a.record(AuthZReq{Principal: principal, Verb: verb, Resources: resources, Method: MethodFilterAuthorizedResources}); err != nil {
		return nil, err
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
