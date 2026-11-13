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

package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	ModuleName     = "reranker-typesafeai"
	DefaultBaseURL = "https://api.typesafe.ai"
	DefaultModel   = "jev-latest"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper(ModuleName)}
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.CheckSettings(); err != nil {
		return err
	}
	return cs.propertyValuesHelper.ValidateBaseURL(cs.BaseURL())
}

// CheckSettings fails when a stored model or baseURL is not a non-empty
// string, or the baseURL is not an absolute http or https URL. The getters
// return the default for a value of another type, so a query must check the
// stored value first.
func (cs *classSettings) CheckSettings() error {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	settings := cs.rawSettings()
	for _, name := range []string{"model", "baseURL"} {
		v, ok := settings[name]
		if !ok || v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("%s must be a string, got %T", name, v)
		}
		if s == "" {
			return fmt.Errorf("%s must be a non-empty string", name)
		}
		if name == "baseURL" {
			if err := CheckBaseURL(s); err != nil {
				return fmt.Errorf("baseURL %w", err)
			}
		}
	}
	return nil
}

// CheckBaseURL fails unless raw is an absolute http or https URL that names a
// host. A URL with a port and no host, such as https://:443, dials the local
// host.
func CheckBaseURL(raw string) error {
	_, err := NormalizeBaseURL(raw)
	return err
}

// NormalizeBaseURL drops a trailing slash and a trailing /v1, which SDKs
// write into the base URL; the client appends /v1 itself.
func NormalizeBaseURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return "", fmt.Errorf("must be an absolute http or https URL, got %q", raw)
	}
	p := strings.TrimSuffix(strings.TrimSuffix(u.EscapedPath(), "/"), "/v1")
	u.RawPath = p
	u.Path, _ = url.PathUnescape(p)
	return u.String(), nil
}

// rawSettings mirrors the lookup of the property helper's getters, which the
// PropertyValuesHelper interface does not expose.
func (cs *classSettings) rawSettings() map[string]any {
	if settings := cs.cfg.ClassByModuleName(ModuleName); len(settings) > 0 {
		return settings
	}
	return cs.cfg.Class()
}

// A value that does not normalize is returned unchanged; Validate rejects it
// at class create and CheckSettings at query time.
func (cs *classSettings) BaseURL() string {
	raw := cs.getStringProperty("baseURL", DefaultBaseURL)
	if normalized, err := NormalizeBaseURL(raw); err == nil {
		return normalized
	}
	return raw
}

func (cs *classSettings) Model() string {
	return cs.getStringProperty("model", DefaultModel)
}

func (cs *classSettings) getStringProperty(name string, defaultValue string) string {
	return cs.propertyValuesHelper.GetPropertyAsStringWithNotExists(cs.cfg, name, defaultValue, defaultValue)
}
