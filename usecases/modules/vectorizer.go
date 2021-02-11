package modules

import (
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
)

func (m *Provider) Vectorizer(moduleName string) (modulecapabilities.Vectorizer, error) {
	mod := m.GetByName(moduleName)
	if mod == nil {
		return nil, errors.Errorf("no module with name %q present", moduleName)
	}

	vec, ok := mod.(modulecapabilities.Vectorizer)
	if !ok {
		return nil, errors.Errorf("module %q exists, but does not provide the "+
			"Vectorizer capability", moduleName)
	}

	return vec, nil
}
