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

package rest

import (
	"sync"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/namespace"
)

// AuthNamespaceStore stores the namespace mapping for authenticated principals.
// This is used to pass namespace information from authentication to handlers.
type AuthNamespaceStore struct {
	mu         sync.RWMutex
	namespaces map[string]string // username -> namespace
	isAdmin    map[string]bool   // username -> isAdmin
}

// NewAuthNamespaceStore creates a new AuthNamespaceStore.
func NewAuthNamespaceStore() *AuthNamespaceStore {
	return &AuthNamespaceStore{
		namespaces: make(map[string]string),
		isAdmin:    make(map[string]bool),
	}
}

// Set stores the namespace for a username.
func (s *AuthNamespaceStore) Set(username, ns string, isAdmin bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.namespaces[username] = ns
	s.isAdmin[username] = isAdmin
}

// Get retrieves the namespace for a username.
// Returns the namespace and whether the user is admin.
func (s *AuthNamespaceStore) Get(username string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.namespaces[username], s.isAdmin[username]
}

// authNamespaceStore is the global store for auth namespace mappings.
// This is set during API configuration.
var authNamespaceStore = NewAuthNamespaceStore()

// GetBoundNamespace returns a user's bound namespace from authentication.
// Returns (namespace, isAdmin) where:
// - namespace is the namespace the user is bound to (empty string if not bound)
// - isAdmin is true if the user is a root/admin user
// This function implements the authorization.NamespaceLookup interface.
func GetBoundNamespace(username string) (string, bool) {
	return authNamespaceStore.Get(username)
}

// WrapAuthWithNamespace wraps the TokenFunc to extract and store namespace information,
// while returning the expected *models.Principal for the go-swagger API.
func WrapAuthWithNamespace(
	tokenFunc composer.TokenFunc,
	authConfig config.Authentication,
	rbacConfig rbacconf.Config,
) func(string, []string) (*models.Principal, error) {
	// Build a set of root users for quick lookup
	rootUsers := make(map[string]bool)
	for _, user := range rbacConfig.RootUsers {
		rootUsers[user] = true
	}

	return func(token string, scopes []string) (*models.Principal, error) {
		result, err := tokenFunc(token, scopes)
		if err != nil {
			return nil, err
		}

		if result == nil || result.Principal == nil {
			return nil, nil
		}

		// Determine if user is admin (root user)
		isAdmin := result.IsAdmin || rootUsers[result.Principal.Username]

		// Store namespace mapping for this user
		authNamespaceStore.Set(result.Principal.Username, result.Namespace, isAdmin)

		return result.Principal, nil
	}
}

// GetNamespaceForPrincipal returns the namespace for the given principal.
// If the principal is an admin and headerNs is provided, the header takes precedence.
// Otherwise, returns the namespace from authentication.
func GetNamespaceForPrincipal(principal *models.Principal, headerNs string) string {
	if principal == nil {
		// Anonymous access - use header if valid, else default
		if headerNs != "" {
			if err := namespace.ValidateNamespace(headerNs); err == nil {
				return headerNs
			}
		}
		return namespace.DefaultNamespace
	}

	ns, isAdmin := authNamespaceStore.Get(principal.Username)

	// Admin users can override namespace via header
	if isAdmin && headerNs != "" {
		if err := namespace.ValidateNamespace(headerNs); err == nil {
			return headerNs
		}
	}

	// Return stored namespace or default
	if ns != "" {
		return ns
	}
	return namespace.DefaultNamespace
}
