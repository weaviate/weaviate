# Reproducible builds

Building the same commit twice must produce a byte-identical `weaviate` binary, from
any checkout directory, on any machine with the same Go toolchain. This lets anyone
verify that a shipped binary (GitHub release asset or the `/bin/weaviate` inside the
Docker image) really was built from the commit it claims.

`tools/dev/check_reproducible_build.sh` builds twice and compares hashes.

## What goes into the binary

Everything below is a build input. Change any of it and the hash changes.

| Input | Where it comes from | Why it is deterministic |
|---|---|---|
| Source tree | the commit | must be clean (see `vcs.modified`) |
| Go toolchain | `golang:1.27-alpine` (Docker), `goversion: 1.27` (release action), local `go` (Makefile) | deliberately **not** pinned to a patch version, so Go security fixes keep rolling in. The exact version is recorded in the binary (first line of `go version -m`), and a verifier rebuilds with that version. See below |
| `-trimpath` | Makefile, Dockerfile, `.goreleaser.yaml`, `release.yaml` | without it ~2300 absolute source paths of the checkout are embedded in pclntab |
| `-X build.Revision` | `git rev-parse --short HEAD` | derived from the commit |
| `-X build.Branch` | branch or tag name | a declared input; CI passes the tag name for releases, so re-derivable. Nothing else reads it: Docker tags are computed in `ci/push_docker.sh` from the GitHub ref, not from the binary |
| `-X build.BuildUser` | `ci` in CI, `user@host` locally | override with `make BUILD_USER=ci` to match a CI binary |
| `-X build.BuildDate` | **the commit timestamp**, never the wall clock | `TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd`; goreleaser uses `{{ .CommitDate }}` |
| ldflags string | all of the above, verbatim | `go build` embeds the exact `-ldflags` text in the build info, so flag order and spacing matter too |
| VCS stamp (`vcs.revision`, `vcs.time`, `vcs.modified`, main-module version) | `-buildvcs` (default `auto`) | see below |

`go version -m weaviate` prints every one of these, so a verifier can read the inputs
straight out of the binary they want to check.

## Code must not find files through runtime.Caller

`-trimpath` rewrites the file names that `runtime.Caller` reports from absolute paths to
module-relative ones, so any code that builds a path from them breaks in a release binary.
The gse tokenizer did exactly this to locate its dictionaries and was silently unavailable
in every Docker image from v1.34.0 until the dictionaries were loaded from the embedded
copies instead (`entities/tokenizer/gse_dict.go`). `test/run.sh` runs the tokenizer tests
with `-trimpath` for that reason. Ship data as `//go:embed` files, never as paths derived
from the source tree.

## The VCS stamp

With `-buildvcs=auto` (the default) `go build` records the commit, its time, whether the
tree was dirty, and a main-module version derived from the nearest reachable tag.

- **Dirty tree.** Any modified *or untracked* file flips `vcs.modified=true` and changes
  the binary. Build from a clean checkout. This is why `.goreleaser.yaml` has no
  `go mod tidy` hook: a hook that rewrites `go.mod` dirties the tree mid-build.
- **Shallow or tagless clone.** `actions/checkout` fetches depth 1 with no tags. The main
  module version then becomes `v0.0.0-<time>-<sha>` instead of `v1.40.2-0.<time>-<sha>`,
  which a full clone can never reproduce. The GitHub release job therefore builds with
  `-buildvcs=false`.
- **Docker.** `.dockerignore` excludes `.git`, so the Docker binary has no VCS stamp and
  reports its module version as `(devel)`.

To reproduce a CI binary locally, pass `-buildvcs=false` as well.

## Reproducing the Docker image's binary locally

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false \
  -ldflags="<the -ldflags line printed by: go version -m /bin/weaviate>" \
  -o weaviate ./cmd/weaviate-server
sha256sum weaviate
```

Use the Go version printed on the first line of `go version -m /bin/weaviate`, for example
by running the build inside `golang:<that version>-alpine`.

## Reproducing a Makefile build

```sh
make weaviate BUILD_USER=<user@host from go version -m> GIT_BRANCH=<branch>
```

`GIT_REVISION` and `BUILD_DATE` default to values derived from the commit and only need
overriding when they were overridden for the original build.

## What is not reproducible by design

- `make weaviate-debug` keeps absolute paths (no `-trimpath`) because delve needs them.
- The signed and notarized macOS zip produced by `tools/dev/goreleaser_and_sign.sh`:
  code signatures carry a signing timestamp. The binary *before* signing is reproducible.
