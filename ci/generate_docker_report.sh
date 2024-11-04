set -e

cd "${0%/*}"

function generate_report() {
  echo "PREVIEW_TAG=$PREVIEW_TAG"
  if [ -z "$PREVIEW_TAG" ]; then
    return
  fi
  echo "PREVIEW_SEMVER_TAG=$PREVIEW_SEMVER_TAG"
  if [ -z "$PREVIEW_SEMVER_TAG" ]; then
    return
  fi

  export TAG_ONLY="$(echo "$PREVIEW_TAG" | cut -d ':' -f 2)"
  envsubst < docker_report.md.tpl >> "$GITHUB_STEP_SUMMARY"
}

generate_report
