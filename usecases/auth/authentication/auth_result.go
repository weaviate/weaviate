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

package authentication

import (
	"github.com/weaviate/weaviate/entities/models"
)

// AuthResult contains the result of authentication including the principal
// and the namespace they are bound to.
type AuthResult struct {
	// Principal is the authenticated user/entity
	Principal *models.Principal

	// Namespace is the namespace this principal is bound to.
	// Empty string means the default namespace.
	Namespace string

	// IsAdmin indicates if this principal can access multiple namespaces.
	// Admin users can use the X-Weaviate-Namespace header to specify a namespace.
	IsAdmin bool
}

// NewAuthResult creates an AuthResult with default namespace.
func NewAuthResult(principal *models.Principal) *AuthResult {
	return &AuthResult{
		Principal: principal,
		Namespace: "",
		IsAdmin:   false,
	}
}

// NewAuthResultWithNamespace creates an AuthResult with a specific namespace.
func NewAuthResultWithNamespace(principal *models.Principal, namespace string) *AuthResult {
	return &AuthResult{
		Principal: principal,
		Namespace: namespace,
		IsAdmin:   false,
	}
}

// NewAdminAuthResult creates an AuthResult for admin users who can access any namespace.
func NewAdminAuthResult(principal *models.Principal) *AuthResult {
	return &AuthResult{
		Principal: principal,
		Namespace: "",
		IsAdmin:   true,
	}
}
