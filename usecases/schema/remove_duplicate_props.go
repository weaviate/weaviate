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

package schema

import (
	"context"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
)

func (m *Manager) removeDuplicatePropsIfPresent(ctx context.Context) error {
	for _, c := range m.state.ObjectSchema.Classes {
		if hasDuplicateProps(c.Properties) {
			c.Properties = m.deduplicateProps(c.Properties, c.Class)
		}
	}

	return nil
}

func hasDuplicateProps(props []*models.Property) bool {
	found := map[string]struct{}{}

	for _, prop := range props {
		if _, ok := found[strings.ToLower(prop.Name)]; ok {
			return true
		}

		found[strings.ToLower(prop.Name)] = struct{}{}
	}

	return false
}

func (m *Manager) deduplicateProps(orig []*models.Property,
	className string,
) []*models.Property {
	out := make([]*models.Property, len(orig))
	seen := map[string]struct{}{}

	i := 0

	for _, prop := range orig {
		if _, ok := seen[strings.ToLower(prop.Name)]; ok {
			m.logger.WithFields(logrus.Fields{
				"action": "startup_repair_schema",
				"prop":   prop.Name,
				"class":  className,
			}).Warningf("removing duplicate property %s", prop.Name)
			continue
		}

		out[i] = prop
		seen[prop.Name] = struct{}{}
		i++
	}

	return out[:i]
}
