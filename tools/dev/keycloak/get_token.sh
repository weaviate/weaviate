#!/bin/bash

set -eou pipefail

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/../.. || exit 1

function main() {
  USERNAME=${1:-johndoe}
  PASSWORD=$USERNAME

  curl --fail -s -X POST -d grant_type=password -d client_id=demo -d username="$USERNAME" -d password="$PASSWORD" http://localhost:9090/auth/realms/weaviate/protocol/openid-connect/token | jq -r ".access_token"
}

main "$@"
