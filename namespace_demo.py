#!/usr/bin/env python3
"""
Namespace-to-Principal Mapping Demo

This script demonstrates how API keys are bound to specific namespaces,
providing multi-tenant isolation in Weaviate.

Prerequisites:
- Weaviate running with RBAC and DB users enabled
- Python 3.8+ with requests library: pip install requests

Configuration (environment variables):
- WEAVIATE_URL: Weaviate server URL (default: http://localhost:8080)
- ADMIN_API_KEY: Admin/root user API key for setup
"""

import os
import json
import sys

# Colors for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}\n")

def print_step(step: int, text: str):
    print(f"{Colors.CYAN}[Step {step}]{Colors.ENDC} {text}")

def print_code(code: str):
    print(f"{Colors.YELLOW}{code}{Colors.ENDC}")


def demo_namespace_isolation():
    """
    Demonstrate namespace isolation with principal-bound API keys.
    """

    print_header("Namespace-to-Principal Mapping Demo")

    print("""
This demo explains how API keys are bound to specific namespaces:

  1. Each user/API key is assigned to exactly ONE namespace
  2. When authenticating, Weaviate derives the namespace from the principal
  3. Users can only see collections/objects in their namespace
  4. Admin users can override namespace via X-Weaviate-Namespace header
""")

    input(f"{Colors.BOLD}Press Enter to continue...{Colors.ENDC}")

    # Step 1: Configuration
    print_step(1, "Configuring namespace-bound API keys")
    print("""
There are THREE ways to bind API keys/users to namespaces:
""")

    print(f"{Colors.GREEN}Option A: Static API Keys (environment variables){Colors.ENDC}")
    print_code("""
# In your Weaviate configuration:
AUTHENTICATION_APIKEY_ENABLED=true
AUTHENTICATION_APIKEY_ALLOWED_KEYS=key-tenant-a,key-tenant-b,admin-key
AUTHENTICATION_APIKEY_USERS=tenant-a-user,tenant-b-user,admin
AUTHENTICATION_APIKEY_NAMESPACES=tenant-a,tenant-b,
#                                  ^         ^       ^
#                                  |         |       |
#                          key 1 -> ns1  key 2 -> ns2  key 3 -> default (empty)
""")

    input(f"{Colors.BOLD}Press Enter to continue...{Colors.ENDC}")

    print(f"\n{Colors.GREEN}Option B: OIDC with namespace claim{Colors.ENDC}")
    print_code("""
# Configure OIDC to extract namespace from JWT token:
AUTHENTICATION_OIDC_ENABLED=true
AUTHENTICATION_OIDC_ISSUER=https://auth.example.com
AUTHENTICATION_OIDC_USERNAME_CLAIM=sub
AUTHENTICATION_OIDC_NAMESPACE_CLAIM=weaviate_namespace  # <-- NEW!

# Your JWT token would contain:
{
  "sub": "user@example.com",
  "weaviate_namespace": "tenant-a",  # <-- Bound to this namespace
  "exp": 1234567890
}
""")

    input(f"{Colors.BOLD}Press Enter to continue...{Colors.ENDC}")

    print(f"\n{Colors.GREEN}Option C: DB Users with namespace field{Colors.ENDC}")
    print_code("""
# When creating a user via the API, the namespace is stored with the user:

# Internal User struct (db_users.go):
type User struct {
    Id                 string
    InternalIdentifier string
    SecureHash         string
    Active             bool
    Namespace          string  // <-- NEW: The bound namespace
    CreatedAt          time.Time
    LastUsedAt         time.Time
}

# The namespace is set when the user is created via Raft consensus
""")

    input(f"{Colors.BOLD}Press Enter to continue...{Colors.ENDC}")

    # Step 2: Authentication flow
    print_step(2, "Authentication flow with namespace binding")
    print(f"""
{Colors.GREEN}Before (old way):{Colors.ENDC}
""")
    print_code("""
# Client had to specify namespace in every request:
curl -X POST http://localhost:8080/v1/schema \\
  -H "Authorization: Bearer my-api-key" \\
  -H "X-Weaviate-Namespace: tenant-a" \\   # <-- Had to remember this!
  -d '{"class": "Articles", ...}'
""")

    print(f"""
{Colors.GREEN}After (new way):{Colors.ENDC}
""")
    print_code("""
# Namespace is derived from the API key automatically:
curl -X POST http://localhost:8080/v1/schema \\
  -H "Authorization: Bearer key-tenant-a" \\   # <-- Bound to tenant-a
  -d '{"class": "Articles", ...}'

# The server knows key-tenant-a -> tenant-a namespace
# No X-Weaviate-Namespace header needed!
""")

    input(f"{Colors.BOLD}Press Enter to continue...{Colors.ENDC}")

    # Step 3: Code changes
    print_step(3, "Key code changes")

    print(f"""
{Colors.GREEN}1. New AuthResult struct (auth_result.go):{Colors.ENDC}
""")
    print_code("""
type AuthResult struct {
    Principal *models.Principal  // The authenticated user
    Namespace string             // The namespace they're bound to
    IsAdmin   bool               // Can they access multiple namespaces?
}
""")

    print(f"""
{Colors.GREEN}2. TokenFunc returns AuthResult (token_validation.go):{Colors.ENDC}
""")
    print_code("""
// Before:
type TokenFunc func(token string, scopes []string) (*models.Principal, error)

// After:
type TokenFunc func(token string, scopes []string) (*AuthResult, error)
""")

    print(f"""
{Colors.GREEN}3. Static API key returns namespace (client.go):{Colors.ENDC}
""")
    print_code("""
func (c *StaticApiKey) ValidateAndExtract(token string, scopes []string) (*AuthResult, error) {
    tokenPos, ok := c.isTokenAllowed(token)
    if !ok {
        return nil, fmt.Errorf("invalid api key")
    }
    return authentication.NewAuthResultWithNamespace(
        c.getPrincipal(tokenPos),
        c.getNamespace(tokenPos),  // <-- Returns the bound namespace
    ), nil
}
""")

    print(f"""
{Colors.GREEN}4. OIDC extracts namespace from JWT (middleware.go):{Colors.ENDC}
""")
    print_code("""
func (c *Client) extractNamespace(claims map[string]interface{}) string {
    if c.Config.NamespaceClaim == nil {
        return ""
    }
    claimName := c.Config.NamespaceClaim.Get()
    if ns, ok := claims[claimName].(string); ok {
        return ns
    }
    return ""
}
""")

    input(f"{Colors.BOLD}Press Enter to continue...{Colors.ENDC}")

    # Step 4: Admin override
    print_step(4, "Admin override mechanism")
    print(f"""
{Colors.GREEN}Admin users (RBAC root users) can still use the header:{Colors.ENDC}
""")
    print_code("""
// In auth_wrapper.go:
func GetNamespaceForPrincipal(principal *models.Principal, headerNs string) string {
    if principal == nil {
        return headerNs  // Anonymous - use header if provided
    }

    // Check if user is admin (root user in RBAC config)
    if isAdminUser(principal.Username) {
        if headerNs != "" {
            return headerNs  // Admin can override with header
        }
    }

    // For non-admin users, always use their bound namespace
    return authNamespaceStore.Get(principal.Username)
}
""")

    print("""
This means:
- Regular users: ALWAYS use their bound namespace (header ignored)
- Admin users: Can specify X-Weaviate-Namespace to access any namespace
- This provides strong isolation while allowing admin management
""")

    input(f"{Colors.BOLD}Press Enter to continue...{Colors.ENDC}")

    # Step 5: Visual flow
    print_step(5, "Complete request flow")
    print(f"""
{Colors.GREEN}Request from Tenant A user:{Colors.ENDC}

┌─────────────────┐
│  HTTP Request   │
│ Authorization:  │
│ Bearer key-a    │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Auth Composer  │  Tries API key auth first
└────────┬────────┘
         │
         v
┌─────────────────┐
│  StaticApiKey   │  key-a found at position 0
│  Validator      │  namespaces[0] = "tenant-a"
└────────┬────────┘
         │
         v
┌─────────────────┐
│   AuthResult    │
│ ───────────────│
│ Principal:      │
│   user-a        │
│ Namespace:      │
│   "tenant-a"    │  <-- Derived from key position
│ IsAdmin: false  │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Auth Wrapper   │  Stores: user-a -> tenant-a
│                 │  Returns: Principal only (for go-swagger)
└────────┬────────┘
         │
         v
┌─────────────────┐
│ Schema Handler  │  Gets namespace from auth context
│                 │  ns = GetNamespaceForPrincipal(principal, "")
│                 │  ns = "tenant-a"
└────────┬────────┘
         │
         v
┌─────────────────┐
│    Storage      │  Operations scoped to tenant-a/
│  (tenant-a/)    │
└─────────────────┘
""")

    input(f"{Colors.BOLD}Press Enter to continue...{Colors.ENDC}")

    # Summary
    print_header("Summary")
    print(f"""
{Colors.GREEN}What we implemented:{Colors.ENDC}

1. {Colors.BOLD}AuthResult struct{Colors.ENDC} - Carries Principal + Namespace + IsAdmin

2. {Colors.BOLD}Namespace binding for all auth methods:{Colors.ENDC}
   - Static API keys: via AUTHENTICATION_APIKEY_NAMESPACES env var
   - OIDC tokens: via AUTHENTICATION_OIDC_NAMESPACE_CLAIM config
   - DB users: via Namespace field in User struct

3. {Colors.BOLD}Handler integration:{Colors.ENDC}
   - getNamespaceFromRequest() uses auth context instead of header
   - Admin override preserved for management operations

4. {Colors.BOLD}Backward compatibility:{Colors.ENDC}
   - Empty namespace = default namespace
   - Existing deployments continue to work

{Colors.GREEN}Benefits:{Colors.ENDC}

- Stronger tenant isolation (can't accidentally access wrong namespace)
- Simpler client code (no namespace header management)  
- Centralized namespace assignment (server-side configuration)
- Audit trail (namespace tied to authenticated identity)

{Colors.GREEN}Files modified:{Colors.ENDC}

usecases/auth/authentication/
  ├── auth_result.go           (NEW)
  ├── composer/token_validation.go
  ├── apikey/client.go
  ├── apikey/db_users.go
  └── oidc/middleware.go

usecases/config/authentication.go

adapters/handlers/
  ├── rest/auth_wrapper.go     (NEW)
  ├── rest/handlers_schema.go
  └── grpc/v1/auth/auth.go

cluster/
  ├── proto/api/dyn_user_requests.go
  └── raft_dynuser_apply_endpoints.go
""")

    print(f"\n{Colors.BOLD}Demo complete!{Colors.ENDC}\n")


if __name__ == "__main__":
    demo_namespace_isolation()
