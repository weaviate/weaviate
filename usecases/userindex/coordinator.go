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

package userindex

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/userindex"
)

type Coordinator struct {
	uis    userIndexStatuser
	auth   authorizer
	hosts  hostnameLister
	remote remoteUserIndexStatuser
}

func New(uis userIndexStatuser, auth authorizer,
	hosts hostnameLister, remote remoteUserIndexStatuser,
) *Coordinator {
	return &Coordinator{
		uis:    uis,
		auth:   auth,
		hosts:  hosts,
		remote: remote,
	}
}

func (m *Coordinator) Get(ctx context.Context, principal *models.Principal,
	className string,
) (*models.IndexStatusList, error) {
	if err := m.auth.Authorize(principal, "list", "indexes"); err != nil {
		return nil, err
	}

	agg := &userindex.Status{}

	local, err := m.uis.UserIndexStatus(ctx, className)
	if err != nil {
		return nil, err
	}

	for _, elem := range local {
		shards := elem.Shards
		elem.Shards = nil
		for _, shard := range shards {
			agg.Register(shard, elem)
		}
	}

	for _, host := range m.hosts.Hostnames() {
		remote, err := m.remote.UserIndexStatus(ctx, host, className)
		if err != nil {
			return nil, fmt.Errorf("host %s: %w", host, err)
		}

		for _, elem := range remote {
			for _, shard := range elem.Shards {
				agg.Register(shard, elem)
			}
		}
	}

	out := agg.ToSwagger()
	out.ClassName = className

	return out, nil
}

type userIndexStatuser interface {
	UserIndexStatus(ctx context.Context, class string) ([]userindex.Index, error)
}

type remoteUserIndexStatuser interface {
	UserIndexStatus(ctx context.Context, hostname, className string) ([]userindex.Index, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type hostnameLister interface {
	Hostnames() []string
}
