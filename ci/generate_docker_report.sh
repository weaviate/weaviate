set -e

cd "${0%/*}"

function generate_report() {
  # Handle both single-line and multi-line tags
  echo "PREVIEW_TAG=$PREVIEW_TAG"
  if [ -z "$PREVIEW_TAG" ]; then
    echo "No preview tags found"
    return
  fi

  echo "PREVIEW_SEMVER_TAG=$PREVIEW_SEMVER_TAG"
  if [ -z "$PREVIEW_SEMVER_TAG" ]; then
    echo "No semver tags found"
    return
  fi

  # Extract first tag for examples (works for both single and multi-line)
  export FIRST_TAG=$(echo "$PREVIEW_TAG" | head -n 1)
  export TAG_ONLY="$(echo "$FIRST_TAG" | cut -d ':' -f 2)"

  # Generate report using the template
  envsubst < docker_report.md.tpl >> "$GITHUB_STEP_SUMMARY"
}

generate_report
