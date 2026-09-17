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

package license

// Status is the license state of a Weaviate instance.
type Status string

const (
	// StatusUnlicensed means no well-formed license key is configured.
	StatusUnlicensed Status = "unlicensed"
	// StatusValid means a well-formed license key is configured. Server-side
	// verification of the key is not implemented yet.
	StatusValid Status = "valid"
)

// State is the license state derived once at startup from the configured
// license key. LicenseID is the non-secret id embedded in the key; it is
// empty when unlicensed. The key itself is never stored.
type State struct {
	Status    Status `json:"status" yaml:"status"`
	LicenseID string `json:"licenseId" yaml:"licenseId"`
}
