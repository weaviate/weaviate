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

package v1

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func (s *Service) tenantsGet(ctx context.Context, principal *models.Principal, req *pb.TenantsGetRequest) ([]*pb.Tenant, error) {
	if req.Collection == "" {
		return nil, fmt.Errorf("missing collection %s", req.Collection)
	}

	var err error
	var tenants []*models.Tenant
	if req.Params == nil {
		tenants, err = s.schemaManager.GetConsistentTenants(ctx, principal, req.Collection, true, []string{})
		if err != nil {
			return nil, err
		}
	} else {
		switch req.GetParams().(type) {
		case *pb.TenantsGetRequest_Names:
			requestedNames := req.GetNames().GetValues()
			if len(requestedNames) == 0 {
				return nil, fmt.Errorf("must specify at least one tenant name")
			}
			tenants, err = s.schemaManager.GetConsistentTenants(ctx, principal, req.Collection, true, requestedNames)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown tenant parameter %v", req.Params)
		}
	}

	retTenants := make([]*pb.Tenant, len(tenants))
	for i, tenant := range tenants {
		tenantGRPC, err := tenantToGRPC(tenant)
		if err != nil {
			return nil, err
		}
		retTenants[i] = tenantGRPC
	}
	return retTenants, nil
}

func tenantToGRPC(tenant *models.Tenant) (*pb.Tenant, error) {
	status, ok := pb.TenantActivityStatus_value[fmt.Sprintf("TENANT_ACTIVITY_STATUS_%s", tenant.ActivityStatus)]
	if !ok {
		return nil, fmt.Errorf("unknown tenant activity status %s", tenant.ActivityStatus)
	}

	return &pb.Tenant{
		Name:           tenant.Name,
		ActivityStatus: pb.TenantActivityStatus(status),
	}, nil
}
