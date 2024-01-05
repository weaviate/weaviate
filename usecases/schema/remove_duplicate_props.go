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

package schema

import (
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
)

func (m *Manager) deduplicateProps(props []*models.Property,
	className string,
) []*models.Property {
	seen := map[string]struct{}{}
	i := 0
	for j, prop := range props {
		name := strings.ToLower(prop.Name)
		if _, ok := seen[name]; ok {
			m.logger.WithFields(logrus.Fields{
				"action": "startup_repair_schema",
				"prop":   prop.Name,
				"class":  className,
			}).Warningf("removing duplicate property %s", prop.Name)
			continue
		}
		if i != j {
			props[i] = prop
		}
		seen[name] = struct{}{}
		i++
	}

	return props[:i]
}
