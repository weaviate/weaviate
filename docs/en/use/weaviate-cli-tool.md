# Weaviate-cli Tool

> Documentation on how to use the Weaviate-cli tool.

> Note: the CLI-tools are still in development.

The weaviate-cli tool is used to autmomatically import or export schemas and to import large datasets. The software can be found [here](https://github.com/semi-technologies/weaviate-cli).

## Run full stack with Docker-compose

A complete Weaviate stack based on Janusgraph (with; Elasticsearch and Cassandra) can be directly run with the Docker compose files available in this repo.

## Installation

Install by running the following command;

```sh
$ source <(curl -s https://raw.githubusercontent.com/semi-technologies/weaviate-cli/master/install.sh)
```

_Note: this command will try to install Python, unzip, wget and pip3 if not installed and might ask for sudo rights._
_Note: tested on Ubuntu 16 and up_

## Manual Installation

```sh
# Clone the repo
$ git clone https://github.com/semi-technologies/weaviate-cli
# Into the repo
$ cd weaviate-cli
# Install deps
$ pip3 install -r requirements.txt
# Test the installation
$ python3 weaviate-cli.py --help
```

_Note: you will need Python3 to run the Weaviate-cli tool_

## Usage

Before being able to use Weaviate CLI you need to set variables of your Weaviate environment

```sh
$ weaviate-cli --init --init-url $WEAVIATE_URL --init-key $X_API_KEY --init-token $X_API_TOKEN
```

_Note I, data will be stored in `~/.weaviate.conf`_
_Note II, the weaviate-cli will will always ping the server first`_

## Ontology Import

The ontology schema importer imports an ontolgy schema to Weaviate.

> You can learn how to define a schema [here](https://github.com/semi-technologies/weaviate/blob/develop/docs/en/use/ontology-schema.md).

| Argument | Description |
| -------- | ----------- |
| `--schema-import` | Imports the schema from `./actions.json` and `./things.json` | 
| `--schema-import-things=*` | OPTIONAL: Change the path to things.json (path + filename) |
| `--schema-import-actions=*` | OPTIONAL: Change the path to actions.json (path + filename) |

```sh
$ weaviate-cli --schema-import --schema-import-things=$PATH_TO_SCHEMA_JSON --schema-import-actions=$PATH_TO_SCHEMA_JSON
```

## Ontology Export

The ontology schema exporter exports an ontolgy schema from Weaviate.

> You can learn how a schema is defined [here](https://github.com/semi-technologies/weaviate/blob/develop/docs/en/use/ontology-schema.md).

| Argument | Description |
| -------- | ----------- |
| `--schema-export` | Exports the schema to `./actions.json` and `./things.json` | 
| `--schema-export-things=*` | OPTIONAL: Change the path to things.json (path + filename) |
| `--schema-export-actions=*` | OPTIONAL: Change the path to actions.json (path + filename) |

```sh
$ weaviate-cli --schema-export --schema-export-things=$PATH_TO_SCHEMA_JSON --schema-export-actions=$PATH_TO_SCHEMA_JSON
```

## Bulk Data Import

Imports bulk data to Weaviate

> You can learn how to define an import schema [here](#).

| Argument | Description |
| -------- | ----------- |
| `--data-import=*` | Imports the data from the JSON file provided | 

```sh
$ weaviate-cli --data-import=$PATH_TO_BULK_DATA
```
