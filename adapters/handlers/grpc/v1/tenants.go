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

func (s *Service) tenantsGet(ctx context.Context, principal *models.Principal, req *pb.TenantsGetRequest) ([]*pb.Tenants, error) {
	if req.Collection == "" {
		return nil, fmt.Errorf("missing collection %s", req.Collection)
	}

	tenants, err := s.schemaManager.GetTenants(ctx, principal, req.Collection)
	if err != nil {
		return nil, err
	}

	if req.TenantParameters == nil {
		switch req.TenantParameters.(type) {
		case *pb.TenantsGetRequest_Name:
			requestedName := req.GetName()

			retTenants := make([]*pb.Tenants, 1)
			for i, tenant := range tenants {
				if tenant.Name == requestedName {
					tenantGRPC, err := tenantToGRPC(tenant)
					if err != nil {
						return nil, err
					}
					retTenants[i] = tenantGRPC
					return retTenants, nil
				}
			}
			return nil, fmt.Errorf("tenant %s not found", requestedName)
		default:
			return nil, fmt.Errorf("unknown tenant %v", req.TenantParameters)
		}
	} else {
	}

	retTenants := make([]*pb.Tenants, len(tenants))
	for i, tenant := range tenants {
		tenantGRPC, err := tenantToGRPC(tenant)
		if err != nil {
			return nil, err
		}
		retTenants[i] = tenantGRPC
	}
	return retTenants, nil
}

func tenantToGRPC(tenant *models.Tenant) (*pb.Tenants, error) {
	var status pb.TenantActivityStatus
	if tenant.ActivityStatus == models.TenantActivityStatusHOT {
		status = pb.TenantActivityStatus_ACTIVITY_STATUS_HOT
	} else if tenant.ActivityStatus == models.TenantActivityStatusWARM {
		status = pb.TenantActivityStatus_ACTIVITY_STATUS_WARM
	} else if tenant.ActivityStatus == models.TenantActivityStatusCOLD {
		status = pb.TenantActivityStatus_ACTIVITY_STATUS_COLD
	} else if tenant.ActivityStatus == models.TenantActivityStatusFROZEN {
		status = pb.TenantActivityStatus_ACTIVITY_STATUS_FROZEN
	} else {
		return nil, fmt.Errorf("unknown tenant activity status %s", tenant.ActivityStatus)
	}
	return &pb.Tenants{
		Name:           tenant.Name,
		ActivityStatus: status,
	}, nil
}
