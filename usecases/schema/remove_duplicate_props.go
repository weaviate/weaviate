package schema

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
)

func (m *Manager) removeDuplicatePropsIfPresent(ctx context.Context) error {
	for _, c := range m.state.ObjectSchema.Classes {
		if hasDuplicateProps(c.Properties) {
			c.Properties = m.deduplicateProps(c.Properties)
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

func (m *Manager) deduplicateProps(orig []*models.Property) []*models.Property {
	out := make([]*models.Property, len(orig))
	seen := map[string]struct{}{}

	i := 0

	for _, prop := range orig {
		if _, ok := seen[prop.Name]; ok {
			continue
		}

		out[i] = prop
		seen[prop.Name] = struct{}{}
		i++
	}

	return out[:i]
}
