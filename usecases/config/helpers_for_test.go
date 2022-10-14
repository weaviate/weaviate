package config

import "github.com/pkg/errors"

type fakeModuleProvider struct {
	valid []string
}

func (f *fakeModuleProvider) ValidateVectorizer(moduleName string) error {
	for _, valid := range f.valid {
		if moduleName == valid {
			return nil
		}
	}

	return errors.Errorf("invalid vectorizer %q", moduleName)
}
