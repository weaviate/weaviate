#!/bin/bash

set -eou pipefail

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

function main() {
  # Get admin token
  TOKEN=$( curl --fail -s -X POST -d grant_type=password -d client_id=admin-cli -d username=admin -d password=admin http://localhost:9090/auth/realms/master/protocol/openid-connect/token | jq -r .access_token )

  # Create user
  if ! curl --fail -s -X POST -d '{"username":"johndoe", "enabled":true, "email":"john@doe.com", "emailVerified":true, "credentials":[{"type":"password","value":"johndoe"}]}' -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json'  http://localhost:9090/auth/admin/realms/weaviate/users; then
    echo Error creating user
    return 1
  fi

  echo ""
  echo "Created user 'johndoe' with password 'johndoe'."
  echo "You can retrieve a token for this user using:"
  echo "  $ ./tools/dev/keycloak/get_token.sh johndoe"
  echo ""
}

main "$@"
