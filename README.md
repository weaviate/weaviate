# Weaviate [![Build Status](https://travis-ci.org/creativesoftwarefdn/weaviate.svg?branch=develop)](https://travis-ci.org/creativesoftwarefdn/weaviate/branches)

**A decentralised semantic, graph based Internet of Things platform based on web-semantics, GraphQL and RESTful API's.**

*Important Note:
Weaviate is not fully production ready yet. Follow this repo or sign up for the [mailing list](http://eepurl.com/bRsMir) to stay informed about the progress. We do have nightly builds that you can use to test Weaviate*

## Using Weaviate

If you want use weaviate, the best thing is to go to our website: [www.weaviate.com](http://www.weaviate.com). You can find all information (like Docker containers etcetera.) needed to getting started.

Want to know more about what happens under the hood or do you want to contribute? Keep on reading!

## How does it work?

Weaviate is based on the concepts of Semantic Internet of Things, Web 3.0 and RDF. If you want to learn more about the concept [you can do so by going to this blogpost](https://bob.wtf/semantic-internet-of-things-42811e1ca7a7).

Your platforms or devices can interact with the service via the RESTful API or the MQTT buses. To see the Swagger documentation click [here](https://github.com/creativesoftwarefdn/weaviate/tree/develop/swagger).

## Getting started

1. Choose a semantic schema that you want to adopt. Out of the box, Weaviate has the [Resource Description Framework](https://en.wikipedia.org/wiki/Resource_Description_Framework) available to use. But you can also create your own schema. If you want to do this, follow this [link](https://github.com/creativesoftwarefdn/weaviate-semantic-schemas).
2. Create your configuration file.
3. Install Weaviate. See the list of [nightly builds](#nightly-builds) 

## Nightly builds

If you just want to get started, use our docker container! You can find more information about it on [www.weaviate.com/getting-started](http://www.weaviate.com/getting-started)

You can download one of our nightly builds here:

| OS and Architecture
| -------------------
| [weaviate_nightly_darwin_386.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_darwin_386.zip?raw=true)
| [weaviate_nightly_darwin_amd64.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_darwin_amd64.zip?raw=true)
| [weaviate_nightly_linux_386.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_386.zip?raw=true)
| [weaviate_nightly_linux_amd64.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_amd64.zip?raw=true)
| [weaviate_nightly_linux_arm.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_arm.zip?raw=true)
| [weaviate_nightly_linux_mips.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_mips.zip?raw=true)
| [weaviate_nightly_linux_mips64.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_mips64.zip?raw=true)
| [weaviate_nightly_linux_mips64le.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_mips64le.zip?raw=true)
| [weaviate_nightly_linux_mipsle.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_mipsle.zip?raw=true)
| [weaviate_nightly_linux_ppc64.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_ppc64.zip?raw=true)
| [weaviate_nightly_linux_ppc64le.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_linux_ppc64le.zip?raw=true)
| [weaviate_nightly_windows_386.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_windows_386.zip?raw=true)
| [weaviate_nightly_windows_amd64.zip](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_windows_amd64.zip?raw=true)

[Checksum file](https://github.com/creativesoftwarefdn/weaviate/blob/develop/dist/weaviate_nightly_checksums.txt).

## Creating a config file

You can create an array of `environments` that should include:

- **name** the name that you will use to refer to the configuration.
- **cache** possiblity to add cache layer between database and core. Without cache layer Weaviate will connect directly to the DB.
- **cache -> name** name of the cache layer (kv-cache is implemented)
- **database -> name** name of the database.
- **database -> database_config -> host** hostname of the DB.
- **database -> database_config -> port** port of the database.
- **schemas -> Thing** file or URL of a semantic schema of Things
- **schemas -> Thing** file or URL of a semantic schema of Actions
- **websocketsEnabled** boolean, do you want to publish information on the websockets?

Example of config file:

```
{
    "environments": [
        {
            "name": "default",
            "cache": {
                "name": "kv-cache"
            },
            "database": {
                "name": "dgraph",
                "database_config" : {
                    "host": "127.0.0.1",
                    "port": 9080
                }
            },
            "schemas": {
                "Thing": "https://raw.githubusercontent.com/creativesoftwarefdn/weaviate-semantic-schemas/master/weaviate-schema-Thing-schema_org.json",
                "Action": "https://raw.githubusercontent.com/creativesoftwarefdn/weaviate-semantic-schemas/master/weaviate-schema-Action-schema_org.json"
            },
            "websocketsEnabled": false
        }
    ]
}
```

## Run with Docker

You can run a complete Docker setup with:

```
$ curl https://git.io/vdwgr -sL | sudo bash
```

_Note:<br>Make sure [jq](https://stedolan.github.io/jq/) is installed and Docker is running_

_Note:<br>In case of a linux-gnu OS, the above dependencies will be installed automatically_

## Run Weaviate

To get an overview of available flag, run `$ weaviate --help`

To get started without https: `$ weaviate --scheme=http --config=YOUR_CONFIG_NAME --config-file=/path/to/your/config/file.json`

## Databases we support

For now, we only support [Dgraph](https://dgraph.io) in case you want to use another Graph DB. Let us know in the [issues](https://github.com/creativesoftwarefdn/weaviate/issues) but we are adding Gremlin support!

## Roadmap

| Feature    | Progress
| ---------  | --------
| Things     | Done 
| Actions    | Done 
| xPlatform  | In Progress
| Gremlin    | In Progress
| Keys       | In Progress
| Websockets | In Progress

## Contact us!

Via [email](mailto:yourfriends@weaviate.com), [Twitter](https://twitter.com/weaviate) or the [newsletter](http://eepurl.com/bRsMir).
