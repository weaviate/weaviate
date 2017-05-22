# Weaviate

**A private cloud for the Internet of Things inspired by Google [Weave](https://developers.google.com/weave)**

*Important Note:
Weaviate is not fully testable / production ready yet. Follow this repo or sign up for the [mailing list](http://eepurl.com/bRsMir) to stay informed about the progress.*

| Branch   | Build status                                                                                                                    | Chat on Gitter                                                                                 |
| -------- | ------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| master   | [![Build Status](https://travis-ci.org/weaviate/weaviate.svg?branch=master)](https://travis-ci.org/weaviate/weaviate)           | [![Gitter chat](https://badges.gitter.im/weaviate/weaviate.svg)](https://gitter.im/weaviate/)  |
| develop  | [![Build Status](https://travis-ci.org/weaviate/weaviate.svg?branch=develop)](https://travis-ci.org/weaviate/weaviate/branches) | |

### Table of contents
- [What is it?](#what-is-it)
- [Installation](#installation)
- [App Engine](#google-app-engine)
- [Authentication](#authentication)
- [DB support](#database-support)
- [GCP Datastore](#google-cloud-datastore-connector)
- [FAQ](#faq)
- [Contributing](#contributing-and-gitflow)
- [About](#about-weaviate)
- [More info](#more-info)

### What is it?

Weaviate is an Internet of Things aggregator platform inspired by Google Weave.

It is ideal if you want to collect data about many different devices or things in a generic way. For example: smart buildings, cities, etc.

Google offers a great cloud service for Weave. There might be situations, though, where you prefer to store the data in your own solution. This is where Weaviate comes in. You can use Weaviate to create your own private Weave inspired cloud.

Weaviate comes with a set of adapters for databases, so you can use your DB of choice. Want to contribute an adapter? Feel free to [contribute](#contributing-and-gitflow)!

Important to know:

- Using Weaviate with [Google Libiota and Android Things](https://github.com/weaviate/weaviatecompanion).
- Clients for: [Python](https://github.com/weaviate/weaviate-client-python), [NodeJS](https://github.com/weaviate/weaviate-client-nodejs) and [Golang](https://github.com/weaviate/weaviate-client-golang).

### Installation

You can run: `go run ./cmd/weaviate-server/main.go` of build Weaviate by running: `go build ./cmd/weaviate-server/main.go`.

For testing you can run over http: `go run ./cmd/weaviate-server/main.go --scheme=http`. Note: to run the service in production you always need https.

*Application options:*

```
Application Options:
      --scheme=          the listeners to enable, this can be repeated and defaults to the schemes in the swagger spec
      --socket-path=     the unix socket to listen on (default: /var/run/weaviate.sock)
      --host=            the IP to listen on (default: localhost) [$HOST]
      --port=            the port to listen on for insecure connections, defaults to a random value [$PORT]
      --tls-host=        the IP to listen on for tls, when not specified it's the same as --host [$TLS_HOST]
      --tls-port=        the port to listen on for secure connections, defaults to a random value [$TLS_PORT]
      --tls-certificate= the certificate to use for secure connections [$TLS_CERTIFICATE]
      --tls-key=         the private key to use for secure conections [$TLS_PRIVATE_KEY]
```

The service will be available from this basepath: `//domain/weaviate/v1-alpha/**`

### Google App Engine

[This](https://github.com/weaviate/weaviate-app-engine) repo has all the information to use Weaviate with Google App Engine.

### Authentication

1. Weaviate uses API-key based authentication.
2. A Weaviate key can has multiple childeren, every key can create a child.
3. Every parent can access a childs' data all the way down the family tree.
4. You can create keys for devices, organistations, etcetera. How you decide to organise is up to you. A single project can have different ways of key organisation.

To authenticate add: `X-API-KEY: SOMEKEY` (replace SOMEKEY) as a header to the request.

When a server is started for the first time _or_ when there is no root key, a new key will be created and show as ouput in the LOG files.

### Database support

Weavaite aims to support many databases, the database currently supported is Google Datastore.

All available connectors can be found in the directory: ./connectors

Note: you can run a [Datastore emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator).

#### Overal database architecture

There are 81 actions in the Weave implementation. These are the types stored as shown in the Struct below. A API POST or PUT is always added to the DB.

```
type DbObject struct {
	Type        string // Type; Device, Command, etcetera
	Uuid        string // The Id of the object as uuid
	TimestampMs int    // Timestamp in milliseconds
	Deleted     bool   // If true, this item is deleted, default is false
	Object      string // The Weave object as a string
}
```

#### Google Cloud Datastore Connector

When running weaviate, add a file called config.json to the root directory. This file should contain:

```
{
      "database": "datastore",
      "config": {
            "project": "projectName"
      }
}
```

You can add other 

### FAQ

- Why Weaviate​?​ ​W​hy a private Weave cloud?

When we started discussing the potential of Google's Weave and Android Things with clients​,​ most​ of them​ were very interested but ​many could not use the Google Cloud​ Service (note: this does _not_ mean [GCP](https://cloud.google.com/)) for ​a​ variety of reasons. ​F​or exampl​e, a​ hospital with custom​-​made connected devices​ can ​only store data in their own datacenters.​ ​Another example ​could be somebody who want​s​ to manipulate the cloud response with machine learning software​,​ or ​to ​analyze the raw data that is sen​t​ from and to the devices.

- Can I extend Weaviate and the Weave API for my project​?​

Yes! You can change the Weave protocol to suit your needs.

- Where can I get some background about Google Weave.
1) [https://cloud.google.com/solutions/iot-overview](https://cloud.google.com/solutions/iot-overview)
2) [https://developers.google.com/weave](https://developers.google.com/weave)
3) [https://weave.googlesource.com/](https://weave.googlesource.com/?format=HTML)

- Can I integrate Weaviate with my existing software?

Absolutely​. If, l​et​'​s say​,​ you want to use Tensorflow to create predictions based on device statistics​,​ you can easily integrate this with Weaviate.

- How does it work?

The Google API listens to three dozen commands​.​ Weaviate mimics the most important methods, URIs and resources that the Google cloud listens to. Weaviate will respond and listen to your devices in the same way the Google cloud does.

- Do I need to download or clone Android Things or libiota from a different fork to use Weaviate?

No, you can use the official open source repos from Google. Here: https://weave.googlesource.com/weave/libiota/

- What are you planning on releasing?

​W​e are working hard to get 0.1.x ready​ as soon as possible​.​ We will let you know as soon​ as​ this version is ready​ i​f you follow us on Twitter or if you sign​ ​up for our newsletter .

### Contributing and Gitflow
Read more in the [CONTRIBUTE.md](CONTRIBUTE.md) file.

### About Weaviate

[Weave](https://developers.google.com/weave) is a high quality, open source end-to-end communications platform for IoT that allows you to connect and manage devices in a generic way. We understand that sometimes you need to be in control over your complete dataset. This may depend on the nature of your business, on analytics and predictions you want to do, or because you want to extend the protocol. Weaviate works as a replicate of the Google Weave cloud and runs on your own cloud or datacenter solution.

### More info
Please keep checking back this repo or the website, we will start publishing software soon.

- Website: http://www.weaviate.com
- Mailing: http://eepurl.com/bRsMir
- Twitter: http://twitter.com/weaviate_iot
