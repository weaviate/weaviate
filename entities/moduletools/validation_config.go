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
// is validated. Code that reads a setting on the module's behalf reports to
// it what the module's own validation cannot see.
//
// Today that is one case: an int setting whose value has a fraction. The
// settings getters cannot return that as an error. They return the caller's
// "wrong value", and many modules pass their default or nil there, so the
// module validates its default instead of the user's number. The class is
// then stored with the fraction, and at request time the stored float64 is
// truncated to an int that validation never saw.
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
