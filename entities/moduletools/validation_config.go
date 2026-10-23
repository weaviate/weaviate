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

package moduletools

// ValidationClassConfig is the class config a module is given while a class
// is validated. Settings getters report to it what they cannot return as an
// error, such as an int setting given a fraction. The provider fails the
// validation with the first report.
type ValidationClassConfig struct {
	ClassConfig
	invalid error
}

func NewValidationClassConfig(cfg ClassConfig) *ValidationClassConfig {
	return &ValidationClassConfig{ClassConfig: cfg}
}

// ReportInvalidSetting records why a setting is invalid. The first report
// is kept.
func (c *ValidationClassConfig) ReportInvalidSetting(err error) {
	if c.invalid == nil {
		c.invalid = err
	}
}

// Err returns the first invalid setting reported, or nil.
func (c *ValidationClassConfig) Err() error {
	return c.invalid
}
