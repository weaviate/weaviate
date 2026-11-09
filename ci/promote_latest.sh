#!/usr/bin/env bash
#
# Points <repo>:latest at the highest stable release (vX.Y.Z) whose image is
# published. Run only from the promote-latest workflow, which serializes runs so
# two promotions cannot interleave and move latest backwards.

set -euo pipefail

DOCKER_REPO="semitechnologies/weaviate"

# Caps how far back a run looks past unpublished releases (for example, a tag
# whose build failed its security scan).
MAX_CANDIDATES=5

# Prints stable release versions on origin, highest first, without "v".
# Reads origin because CI checkouts have no local tags.
function stable_releases_descending() {
  git ls-remote --tags --refs origin 'v*' \
    | sed -En 's|.*refs/tags/v([0-9]+\.[0-9]+\.[0-9]+)$|\1|p' \
    | sort -rV
}

# Succeeds if <repo>:<version> exists, fails with status 2 if the registry
# reports it missing, and 1 for any other error. Callers fall through to an
# older release only on status 2, so a registry outage never moves latest back.
function image_status() {
  local version="$1"
  local output

  if output="$(docker buildx imagetools inspect "${DOCKER_REPO}:${version}" 2>&1 >/dev/null)"; then
    return 0
  fi
  if [[ "$output" == *": not found" ]]; then
    return 2
  fi
  echo "$output" >&2
  return 1
}

function promote_latest() {
  local versions version status

  if ! versions="$(stable_releases_descending)" || [ -z "$versions" ]; then
    echo "::error::Could not list release tags on origin."
    return 1
  fi

  for version in $(head -n "$MAX_CANDIDATES" <<< "$versions"); do
    status=0
    image_status "$version" || status=$?
    case "$status" in
      0)
        echo "Pointing ${DOCKER_REPO}:latest at ${DOCKER_REPO}:${version}."
        docker buildx imagetools create -t "${DOCKER_REPO}:latest" "${DOCKER_REPO}:${version}"
        return 0
        ;;
      2)
        echo "Skipping ${version}: ${DOCKER_REPO}:${version} is not published."
        ;;
      *)
        echo "::error::Could not check ${DOCKER_REPO}:${version}; leaving latest unchanged."
        return 1
        ;;
    esac
  done

  echo "::error::None of the ${MAX_CANDIDATES} highest releases are published; leaving latest unchanged."
  return 1
}

promote_latest
