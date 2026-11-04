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
	"fmt"

	models "github.com/weaviate/weaviate/entities/models"
)

type AuthZReq struct {
	Principal *models.Principal
	Verb      string
	Resources []string
}

type FakeAuthorizer struct {
	err      error
	denied   map[string]struct{}
	requests []AuthZReq
}

func NewMockAuthorizer() *FakeAuthorizer {
	return &FakeAuthorizer{}
}

func (a *FakeAuthorizer) SetErr(err error) {
	a.err = err
}

// Deny refuses resource, matched literally, while other resources stay allowed.
func (a *FakeAuthorizer) Deny(resource string) {
	if a.denied == nil {
		a.denied = map[string]struct{}{}
	}
	a.denied[resource] = struct{}{}
}

// Authorize provides a mock function with given fields: principal, verb, resource
func (a *FakeAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	a.requests = append(a.requests, AuthZReq{principal, verb, resources})
	if a.err != nil {
		return a.err
	}
	for _, resource := range resources {
		if _, ok := a.denied[resource]; ok {
			return fmt.Errorf("forbidden: %s", resource)
		}
	}
	return nil
}

func (a *FakeAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

func (a *FakeAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	a.requests = append(a.requests, AuthZReq{principal, verb, resources})
	if a.err != nil {
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
