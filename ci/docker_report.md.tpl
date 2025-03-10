## Docker Preview Image :whale:

A preview docker image for this branch is available with the following tag:

```
$PREVIEW_TAG
```

A semver compliant docker image tag for this branch is available with the following tag:

```
$PREVIEW_SEMVER_TAG
```

## Use at your own risk :warning:

Preview builds make no promises about stability or feature completeness.  Use them at your own risk. A preview build is not generated if tests failed,  so they have at least passed the common test suite. They may or may not have  been subjected to the asynchronous stress test and chaos pipelines.

## Usage :newspaper:

### Docker-compose

You can obtain a [docker-compose.yaml here](https://weaviate.io/developers/weaviate/current/installation/docker-compose.html#configurator) and configure it to your liking. Then make sure to set `services.weaviate.image` to `$FIRST_TAG`. For example, like so:

```yaml
services:
  weaviate:
    image: $FIRST_TAG
```

### Helm / Kubernetes

To use this preview image with Helm/Kubernetes, set `image.tag` to `$TAG_ONLY` in your `values.yaml`. For example, like so:

```yaml
image:
  tag: $TAG_ONLY
```

### Chaos-Pipeline

To use this build in a chaos pipeline, change the `WEAVIATE_VERSION` in `.github/workflows/tests.yaml` to `$TAG_ONLY`, for example, like so:

```yaml
env:
  WEAVIATE_VERSION: $TAG_ONLY
```
