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
	counts := map[string]int{}

	for _, prop := range props {
		counts[prop.Name]++
	}

	for _, count := range counts {
		if count > 1 {
			return true
		}
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
		if _, ok := seen[prop.Name]; ok {
			m.logger.WithFields(logrus.Fields{
				"action": "startup_repair_schema",
				"prop":   prop.Name,
				"class":  className,
			}).Warningf("removing duplicate proprty %s", prop.Name)
			continue
		}

		out[i] = prop
		seen[prop.Name] = struct{}{}
		i++
	}

	return out[:i]
}
