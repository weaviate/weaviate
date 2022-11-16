set -e

function generate_report() {
  echo $PREVIEW_TAG
  if [ -z "$PREVIEW_TAG" ]; then
    return
  fi

  tag_only="$(echo $PREVIEW_TAG | cut -d ':' -f 2)"
  echo "after tag split"

  echo "## Docker Preview Image :whale:" >> $GITHUB_STEP_SUMMARY
  echo "A preview docker image for this branch is available with the following tag:" >> $GITHUB_STEP_SUMMARY
  echo '```' >> $GITHUB_STEP_SUMMARY
  echo "$PREVIEW_TAG" >> $GITHUB_STEP_SUMMARY
  echo '```' >> $GITHUB_STEP_SUMMARY
  echo "## Use at your own risk :warning:" >> $GITHUB_STEP_SUMMARY
  echo -n "Preview builds make no promises about stability or feature completeness. " >> $GITHUB_STEP_SUMMARY
  echo -n "Use them at your own risk. A preview build is not generated if tests failed, " >> $GITHUB_STEP_SUMMARY
  echo -n "so they have at least passed the common test suite. They may or may not have " >> $GITHUB_STEP_SUMMARY
  echo "been subjected to the asynchronous stress test and chaos pipelines." >> $GITHUB_STEP_SUMMARY
  echo "## Usage :newspaper:" >> $GITHUB_STEP_SUMMARY
  echo "### Docker-compose" >> $GITHUB_STEP_SUMMARY
  echo -n "You can obtain a [docker-compose.yaml here](https://weaviate.io/developers/weaviate/current/installation/docker-compose.html#configurator)" >> $GITHUB_STEP_SUMMARY
  echo -n "and configure it to your liking. Then make sure to set services.weaviate.image to" >> $GITHUB_STEP_SUMMARY
  echo '`'"$PREVIEW_TAG"'`. For example, like so:' >> $GITHUB_STEP_SUMMARY
  echo '```yaml' >> $GITHUB_STEP_SUMMARY
  echo 'services:' >> $GITHUB_STEP_SUMMARY
  echo '  weaviate:' >> $GITHUB_STEP_SUMMARY
  echo "    image: $PREVIEW_TAG" >> $GITHUB_STEP_SUMMARY
  echo '```' >> $GITHUB_STEP_SUMMARY
  echo "### Helm / Kubernetes" >> $GITHUB_STEP_SUMMARY
  echo -n "To use this preview image with Helm/Kubernetes, set "'`'"image.tag"'`'" to "'`'"$tag_only"'` ' >> $GITHUB_STEP_SUMMARY
  echo 'in your `values.yaml`. For example, like so:' >> $GITHUB_STEP_SUMMARY
  echo '```yaml' >> $GITHUB_STEP_SUMMARY
  echo 'image:' >> $GITHUB_STEP_SUMMARY
  echo "  tag: $tag_only" >> $GITHUB_STEP_SUMMARY
  echo '```' >> $GITHUB_STEP_SUMMARY
  echo "### Chaos-Pipeline" >> $GITHUB_STEP_SUMMARY
  echo -n 'To use this build in a chaos pipeline, change the `WEAVIATE_VERSION` in the `.travis.yml` to ' >> $GITHUB_STEP_SUMMARY
  echo '`'"$tag_only"'`, for example, like so:' >> $GITHUB_STEP_SUMMARY
  echo '```yaml' >> $GITHUB_STEP_SUMMARY
  echo 'env:' >> $GITHUB_STEP_SUMMARY
  echo '  matrix:' >> $GITHUB_STEP_SUMMARY
  echo "    - WEAVIATE_VERSION: $tag_only" >> $GITHUB_STEP_SUMMARY
  echo '```' >> $GITHUB_STEP_SUMMARY
}

generate_report
