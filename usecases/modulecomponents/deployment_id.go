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

package modulecomponents

import (
	"fmt"
	"regexp"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// The URL characters that no parser or gateway treats as a delimiter or an
// escape (RFC 3986 "unreserved").
var unreservedPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]*$`)

// ValidateAzureDeploymentID returns an ErrModulePrecondition unless
// deploymentID is one URL path segment. The Azure OpenAI path is
// <baseURL>/openai/deployments/<deploymentID>/..., and a base URL can be a
// gateway that serves several teams under different paths. A "..", a "/" or
// an encoded form of either would take the request, and the server's Azure
// key, out of the gateway path the collection set. An empty ID passes: the
// module settings decide whether one is required.
func ValidateAzureDeploymentID(deploymentID string) error {
	if !unreservedPattern.MatchString(deploymentID) || deploymentID == "." || deploymentID == ".." {
		return enterrors.NewErrModulePrecondition(fmt.Errorf(
			`deploymentId must be letters, digits, hyphens, underscores, periods or tildes, and not "." or "..", got %q`, deploymentID))
	}
	return nil
}
