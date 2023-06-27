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

package objects

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
)

// ParseTenantKeyFromObject extract the value of the tenant key if it exists
func ParseTenantKeyFromObject(tenantKeyName string, o *models.Object, logger logrus.FieldLogger) string {
	if props, _ := o.Properties.(map[string]interface{}); props != nil {
		if rawVal := props[tenantKeyName]; rawVal != nil {
			switch typed := rawVal.(type) {
			case string:
				return typed
			case uuid.UUID:
				return typed.String()
			default:
				logger.WithField("action", "parse_object_tenant_key").
					Warnf("unsupported tenant key %+v of type %T", rawVal, rawVal)
			}
		}
	}
	return ""
}

func validateSingleBatchObjectTenantKey(class *models.Class, obj *models.Object,
	tk string, ec *errorcompounder.ErrorCompounder, logger logrus.FieldLogger,
) error {
	var objTk string
	if class.MultiTenancyConfig != nil {
		if tk == "" {
			return NewErrInvalidUserInput("class %q has multi-tenancy enabled, tenant_key %q required",
				class.Class, class.MultiTenancyConfig.TenantKey)
		}
		objTk = ParseTenantKeyFromObject(class.MultiTenancyConfig.TenantKey, obj, logger)
		if objTk != tk {
			ec.Add(fmt.Errorf("object does not belong to tenant %q", tk))
		}
	}
	return nil
}
