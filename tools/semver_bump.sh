#!/usr/bin/env bash

# Jump to root directory
cd "$( dirname "${BASH_SOURCE[0]}" )"/..

set -e
# Validate if the open-api schema needs to be bumped
if [ $TRAVIS_BRANCH = "develop" ] && [ $TRAVIS_PULL_REQUEST = "false" ]; then
  # PREVIOUSSEMVER = comparison of the current HEAD of WEAVIATESCHEMAFILE with the previous commit. More info: https://github.com/fsaintjacques/semver-tool/blob/master/README.md
  PREVIOUSSEMVER=$(./semver compare $(git show --oneline HEAD:$WEAVIATESCHEMAFILE | jq -r ".info.version") $(git show --oneline HEAD~1:$WEAVIATESCHEMAFILE | jq -r ".info.version"))
  # Determine if the version should be bumped
  if [ "$PREVIOUSSEMVER" -eq 0 ]; then
    # Bump the version + pipe to WEAVIATESCHEMAFILE.tmp + rm and mv WEAVIATESCHEMAFILE.
    echo $(jq -r '.info.version = $NEWVERSION' --arg NEWVERSION "$(./semver bump $SEMVERBUMPTYPE $(git show --oneline HEAD:$WEAVIATESCHEMAFILE | jq -r ".info.version"))" $WEAVIATESCHEMAFILE) | jq . > $WEAVIATESCHEMAFILE.tmp && rm $WEAVIATESCHEMAFILE && mv $WEAVIATESCHEMAFILE.tmp $WEAVIATESCHEMAFILE
    # build with new version
    ./tools/gen-code-from-swagger.sh
    # push back to Git
    git config credential.helper "store --file=.git/credentials"
    echo "https://${GH_TOKEN}:@github.com" > .git/credentials
    git add -A
    git commit -m "🤖 bleep bloop - auto updated Weaviate"
    git push origin HEAD:${TRAVIS_PULL_REQUEST_BRANCH:-$TRAVIS_BRANCH}
    # exit 0 will trigger a new build
    travis_terminate 0
  elif [ "$PREVIOUSSEMVER" -eq -1 ]; then
    echo "Semver is behind the latest commit. This issue should be resolved and the version should be set to at least $(./semver bump patch $(git show --oneline HEAD~1:$SCHEMAFILE | jq -r ".info.version"))"
    travis_terminate 1
  fi
fi
