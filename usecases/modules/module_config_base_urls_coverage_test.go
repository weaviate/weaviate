//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modules

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

// A module that reads a host-choosing setting must check it in its validation with one of hostValidators, or a
// collection could point it anywhere. Query-time options (parameters/) are left out: they are request values, not
// collection settings. The names are the ones the not-enabled-module rule uses.
var hostSettingPattern = regexp.MustCompile(`(?i)"(` + strings.Join(modulecomponents.HostSettingNames(), "|") + `)"`)

var hostValidators = regexp.MustCompile(`modulecomponents\.(ValidateCollectionBaseURL|ValidateAzureResourceName|ValidateAWSRegion)\(`)

var hostSettingExemptions = map[string]string{
	"backup-s3":               "endpoint is operator configuration from the environment, not a collection setting",
	"generative-google":       "ValidateGoogleApiEndpoint and friends keep every host under googleapis.com",
	"multi2vec-google":        "ValidateGoogleApiEndpoint and friends keep every host under googleapis.com",
	"text2vec-google":         "ValidateGoogleApiEndpoint and friends keep every host under googleapis.com",
	"text2vec-weaviate":       "weaviateembed allows only its default origin and MODULES_WEAVIATE_EMBEDDINGS_ALLOWED_BASE_URLS",
	"multi2multivec-weaviate": "weaviateembed allows only its default origin and MODULES_WEAVIATE_EMBEDDINGS_ALLOWED_BASE_URLS",
	"text-spellcheck":         "location is a field of its GraphQL result, not a setting",
}

func TestEveryModuleWithAHostSettingChecksIt(t *testing.T) {
	root := filepath.Join("..", "..", "modules")
	entries, err := os.ReadDir(root)
	require.NoError(t, err)

	var missing, staleExemptions, implementing []string
	scanned := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		module := entry.Name()
		settings, implements := scanModuleSources(t, filepath.Join(root, module))
		scanned++
		if implements {
			implementing = append(implementing, module)
		}
		_, exempt := hostSettingExemptions[module]
		switch {
		case len(settings) > 0 && !implements && !exempt:
			missing = append(missing, module+" reads "+strings.Join(settings, ", "))
		case exempt && (len(settings) == 0 || implements):
			staleExemptions = append(staleExemptions, module)
		}
	}
	require.Greater(t, scanned, 40, "the modules directory was not found or is nearly empty")
	require.Empty(t, missing, "check the setting in the module's validation, or add an exemption with its reason")
	require.Empty(t, staleExemptions, "remove exemptions that no longer apply")

	var tested []string
	for _, tt := range baseURLSettings {
		if !slices.Contains(tested, tt.module.Name()) {
			tested = append(tested, tt.module.Name())
		}
	}
	slices.Sort(tested)
	require.Equal(t, implementing, tested, "every module that checks a host needs a row in baseURLSettings")
}

func scanModuleSources(t *testing.T, dir string) (settings []string, implements bool) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "parameters" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if hostValidators.Match(src) {
			implements = true
		}
		for _, match := range hostSettingPattern.FindAllStringSubmatch(string(src), -1) {
			if !slices.Contains(settings, match[1]) {
				settings = append(settings, match[1])
			}
		}
		return nil
	})
	require.NoError(t, err)
	slices.Sort(settings)
	return settings, implements
}
