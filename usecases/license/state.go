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

// Edition is the product edition of a Weaviate instance: Community Edition
// without a license key, Enterprise Edition with one.
type Edition string

// EnterpriseDocsURL is the canonical documentation page for the Enterprise
// Edition, linked from user-facing API responses.
const EnterpriseDocsURL = "https://docs.weaviate.io/deploy/enterprise"

const (
	EditionCommunity  Edition = "community"
	EditionEnterprise Edition = "enterprise"
)

// Status is the license state of a Weaviate instance.
type Status string

const (
	// StatusUnlicensed means no well-formed license key is configured;
	// community mode, no checks run.
	StatusUnlicensed Status = "unlicensed"
	// StatusValid means the last signed answer said valid and has not
	// expired. Until server-side verification ships, a well-formed key is
	// reported as valid.
	StatusValid Status = "valid"
	// StatusExpired / StatusRevoked / StatusUnknown mean the last signed
	// answer said so.
	StatusExpired Status = "expired"
	StatusRevoked Status = "revoked"
	StatusUnknown Status = "unknown"
	// StatusUnreachable means there is no trustworthy answer yet, or the
	// last attempt failed.
	StatusUnreachable Status = "unreachable"
	// StatusDegraded means no valid answer has been obtained within the
	// grace period; enterprise features are disabled.
	StatusDegraded Status = "degraded"
)

// State is the license state derived once at startup from the configured
// license key. LicenseID is the non-secret id embedded in the key; it is
// empty when unlicensed. The key itself is never stored.
type State struct {
	Status    Status `json:"status" yaml:"status"`
	LicenseID string `json:"licenseId" yaml:"licenseId"`
}

// Edition derives the product edition from the license state: no well-formed
// key means Community Edition, any well-formed key means Enterprise Edition.
func (s State) Edition() Edition {
	if s.Status == StatusUnlicensed {
		return EditionCommunity
	}
	return EditionEnterprise
}
