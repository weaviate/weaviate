# Licenses

> Background information regarding the licenses of Weaviate dependencies.

- Weaviate's own license can be found [here](https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md).
- The licenses of Weaviates dependency packages can be found [here](https://github.com/semi-technologies/weaviate/blob/develop/LICENSE-DEPENDENCIES.md).

## How to read the dependency file

The name of the package is followed by the license, for example: `Apache License 2.0`
If there are additional words added to the dependency you can find them indicated through `+ words` or `- words`.

## Create an updated version

In the Weaviate root folder run:

```sh
$ ./tools/create-license-dependency-file.sh
```
