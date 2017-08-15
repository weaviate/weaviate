# Badger Documentation

Welcome to Badger documentation. If you want to read the documentation for
Badger, https://badger.dgraph.io is a better place to be than here.

## Building and Deploying

**Prerequisite**

* Latest version of [Hugo](http://gohugo.io/)

1. Delete everything inside `docs/` except `docs/_src`.

  - `docs/_src` is Hugo file, and everything else is build artifact.
  - `docs/_src` is not published to GitHub pages because its name starts with underscore

2. Run `docs/_src/scripts/build.sh` from within `docs/_src/`.
3. Commit the build result, and push to `master`.
