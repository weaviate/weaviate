# Weaviate
**A private cloud for the Internet of Things [Weave](https://developers.google.com/weave) protocol**

*Important Note:
Weaviate is not fully testable / production ready yet. You can follow the progress for the first release candidates [here](https://github.com/weaviate/weaviate/milestones). Follow this repo or sign up for the [mailing list](http://eepurl.com/bRsMir) to stay informed about the progress.*

Weaviate is a REST API based software-as-a-service platform that is able to process the Google Weave protocol. It can be used in combination with all Google's Weave and Brillo libraries ([link to the libs](https://weave.googlesource.com/), [link to Weave](https://developers.google.com/weave), [link to Brillo](https://developers.google.com/brillo)).

You can use Weaviate on simple local machines, or complex distributed networks.

| Protocol     | Content-types supported      | Databases supported            | Connection-types supported | Server |
|--------------|------------------------------|--------------------------------|----------------------------|--------|
| Google Weave | JSON                         | BigTable                       | HTTPS                      | NodeJS |
|              | Protobuf                     | Cassandra                      | gRPC                       |        |
|              | CBOR                         | MongoDB                        | MQTT                       |        |
|              | XML                          |                                |                            |        |

| Branch   | Build status                                                                                                                    | Dependency status                                                                                                                   | Dev dependency status                                                                                                                   | Bithound                                                                                                                                         | Chat on Gitter                                                                                 |
| -------- | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| master   | [![Build Status](https://travis-ci.org/weaviate/weaviate.svg?branch=master)](https://travis-ci.org/weaviate/weaviate)           | [![Dep Status](https://david-dm.org/weaviate/weaviate/status.svg)](https://david-dm.org/weaviate/weaviate)                          | [![Dep Status](https://david-dm.org/weaviate/weaviate/dev-status.svg)](https://david-dm.org/weaviate/weaviate#info=devDependencies)                         | [![bitHound Overall Score](https://www.bithound.io/github/weaviate/weaviate/badges/score.svg)](https://www.bithound.io/github/weaviate/weaviate) | [![Gitter chat](https://badges.gitter.im/weaviate/weaviate.svg)](https://gitter.im/weaviate/) |
| develop  | [![Build Status](https://travis-ci.org/weaviate/weaviate.svg?branch=develop)](https://travis-ci.org/weaviate/weaviate/branches) | | | [![bitHound Overall Score](https://www.bithound.io/github/weaviate/weaviate/badges/score.svg)](https://www.bithound.io/github/weaviate/weaviate) | [![Gitter chat](https://badges.gitter.im/weaviate/weaviate.svg)](https://gitter.im/weaviate/) |


![NPM](https://nodei.co/npm/weaviate.png?downloads=true "NPM")

### Table of contents
* [How does it work?](#how-does-it-work)
* [Release schedule](#release-schedule)
* [FAQ](#faq)
* [Installation](#installation)
* [Authentication](#authentication)
* [Using the weaviate() function](#using-the-weaviate-function)
* [Using MQTT](#using-mqtt)
* [Related packages, products and repos](#related-packages-products-and-repos)
* [About different Weaviate versions](#about-different-weaviate-versions)
* [Contributing and Gitflow](#contributing-and-gitflow)
* [Main contributors](#main-contributors)
* [More info](#more-info)

### How does it work?
Google provides different libraries for interacting with the Weave protocol ([more info](http://weaviate.com/)). By changing the end-points to your own private cloud that runs Weaviate. You can use the complete Weave and Brillo software solutions within you own cloud solution.
Weaviate supports multiple database adapters, goto the directory 'Commands' to see the adapters

### Release Schedule
Estimates for our release schedule:<br>
- 0.1.x First 1.x release candidate with full Weave support through REST APIs for private cloud.
- 0.2.x Implementation of authentication.
- 0.3.x Pre configured packages (Docker or platform specific) for major PaaS providers.
- 0.4.x Porting of Weave protocol to services like Amazon AWS IoT, IBM Bluemix IoT e.a.
- 0.5.x or 1.x

### FAQ

- Why Weaviate​?​ ​W​hy a private Weave cloud?

When we started discussing the potential of Google's Weave and Brillo​ with clients​,​ most​ of them​ were very interested but ​many could not use the Google Cloud​ for ​a​ variety of reasons. ​F​or exampl​e, a​ hospital with custom​-​made connected devices​ can ​only store data in their own datacenters.​ ​Another example ​could be somebody who want​s​ to manipulate the cloud response with machine learning software​,​ or ​to ​analyze the raw data that is sen​t​ from and to the devices.

- Can I extend Weaviate and the Weave API for my project​?​

Yes!

- Can I integrate Weaviate with my existing software?

Absolutely​. If, l​et​'​s say​,​ you want to use Tensorflow to create predictions based on device statistics​,​ you can easily integrate this with Weaviate.

- How does it work?

The Google API listens to three dozen commands​.​ Weaviate mimics the methods, URIs and resources that the Google cloud listens to. Weaviate will respond and listen to your devices in the same way the Google cloud does.

- Do I need to download or clone Brillo, Libweave or uWeave from a different fork to use Weaviate?

No, you can use the official open source repos from Google.

- What are you planning on releasing?

​W​e are working hard to get 0.1.x ready​ as soon as possible​.​ We will let you know as soon​ as​ this version is ready​ i​f you follow us on Twitter or if you sign​ ​up for our newsletter .

- How to use Weaviate​?​ Where to start?

​We will release documentation and videos​ u​pon​ ​the ​release​ of​ version 0.1.x​​. This will only be for Weaviate​.​ You can use the Google documentation ​for ​Weave and Brillo​ ​on their respective website​s​.

- Why Node/Javascript?

Because we think Node is a language ​that many​ people ​are able to read and write. We want to make the Weave community as large as possible.

### Installation with Cassandra
- Install Node version >5.0.0 ([more info](https://nodejs.org/en/download/package-manager))
- Install Weaviate from NPM: `npm install weaviate --save`
- Install a Cassandra database ([more info](https://www.digitalocean.com/community/tutorials/how-to-install-cassandra-and-run-a-single-node-cluster-on-ubuntu-14-04)) and import the CQL file found in the repo https://github.com/weaviate/weaviate-cassandra/.
- In your server Javascript file, add the following:
```javascript
const weaviate = require('weaviate');
weaviate( CONFIGURATION OBJECT );
```

### Using the weaviate() function
The weaviate function needs configuration objects and returns an optional promise with a weaveObject.

**Configuration Object example**:
```javascript
{
	https		: false, // set to false = http usage, true = https usage
	https_opts	: {      // standard Express https options
        key: key,
        cert: cert
	},
	db: {
		dbAdapter	: 'Cassandra'  // Select the adapter. Adapter = directory in Commands directory.
		dbHostname  : 'localhost', // Cassandra hostname
		dbPort 		: 1000,        // Cassandra port
		dbName 		: 'test',      // Cassandra db name
		dbPassword 	: 'abc',       // Cassandra password
		dbContactpoints : ['h1'],  // Cassandra contain points
		dbKeyspace	: 'someKeySp'  // Cassandra keyspace name
	},
	hostname 	: 'localhost',     // hostname for the service
	port 	 	: 8080,        	   // port for the service
	mqtt: {						   // Only add if you want to enable MQTT
        port: 1883,				   // Mqtt port
        backend: {
            host: 'localhost'      // Redis host
            port: 6379             // Redis port
        }
    },
	debug	 	: true,            // log all usages via stdout (IP, SUCCESS/ERROR, ACTION)
	onSuccess	: (weaveObject) => {}, // when a request was succesful 
	onError		: (weaveObject) => {} // when a request generated an error
}
```

**Promise weaveObject example**:

The weaveObject contains information about the request that you can use for custom processing purposes.
* params = the provided URL params
* body = the provided body
* response = the response generated by Weaviate
* requestHeaders = the original request headers

**Complete example**:
```javascript
weaviate({
	https		: false,
	db: {
		dbAdapter	: 'Cassandra'  // Select the adapter. Adapter = directory in Commands directory.
		dbHostname  : 'localhost', // Cassandra hostname
		dbPort 		: 1000,        // Cassandra port
		dbName 		: 'test',      // Cassandra db name
		dbPassword 	: 'abc',       // Cassandra password
		dbContactpoints : ['h1'],      // Cassandra contain points
		dbKeyspace	: 'someKeySp'  // Cassandra keyspace name
	},
	hostname 	: 'localhost',
	port 	 	: '8080',
	mqtt: {
        port: 1883,
        backend: {
            host: 'localhost'
            port: 6379,
        }
    },
	debug 		: true,
	onSuccess	: (weaveObject) => {
		console.log('SUCCESS', weaveObject);
	},
	onError		: (weaveObject) => {
		console.log('ERROR', weaveObject);
	}
})
```

### Install MQTT
By setting the `mqtt` key in the Weaviate object MQTT (Weaviate pub/sub) will be available.

_IMPORTANT: YOU NEED A REDIS INSTANCE THAT IS AVAILABLE TO ALL NODES._

Example:<br>
```js
mqtt: {
    port: 1883, // MQTT port
    backend: {
        host: 'localhost' // Redis host
        port: 6379,       // Redis port
    }
}
```

### Authentication
[SOON MORE]

### Using MQTT
MQTT uses the same architecture as the RestAPI. You can subscribe to all end-points via the topics and post 

The topics contain an object in JSON, Protobuf, CBOR or XML.

JSON example of a message:
```js
{
	"action": string, // Action as defined in Google Weave Discovery Document
	"method": string, // RestFUL Method
	"body":   object  // Body as defined in the resource representation.
}
```

The action is based on the actions described in the [Weave Discovery document](https://weave.googlesource.com/weave/instaweave/+/master/weave/v1/weave-api.json) and the body is a [resource representation](https://developers.google.com/weave/v1/reference/cloud-api/).

#### Authentication
Every account has a bearer (same as REST API authentication) and a pub-sub token. Authentication for the MQTT message bus should be as followed:<br>
- Username: Weaviate
- Password: Bearer
- pubSubId: pub-sub token

#### Subscribing and Topics
Topics are created per resource representation.

```
{pubSubId}/#
{pubSubId}/devices/{deviceId}/aclEntries/#
{pubSubId}/devices/{deviceId}/aclEntries/{aclEntryId}
{pubSubId}/adapters/#
{pubSubId}/adapters/{adapterId}
{pubSubId}/authorizedApps
{pubSubId}/commands/#
{pubSubId}/commands/{commandId}
{pubSubId}/devices/#
{pubSubId}/devices/{deviceId}
{pubSubId}/events
{pubSubId}/modelManifests/#
{pubSubId}/modelManifests/{modelManifestId}
{pubSubId}/places/#
{pubSubId}/places/{placeId}
{pubSubId}/registrationTickets/#
{pubSubId}/registrationTickets/{registrationTicketId}
{pubSubId}/places/#
{pubSubId}/places/{placeId}
{pubSubId}/subscriptions/#
{pubSubId}/subscriptions/{subscriptionId}
```

#### Publishing
By publishing to a topic following the REST principle (see §Using MQTT) will allow you to interact with the devices.
Sending a POST, PATCH, PUT or DELETE via MQTT will work in the same way as the REST API via HTTPS.

#### Example<br>

Topic: `/commands`

Message:<br>
```js
{
	"action": "weave.command.get",
	"method": "GET"
	"body": {
		"kind": "weave#command",
		"id": string,
		"deviceId": string,
		"creatorEmail": string,
		"component": string,
		"name": string,
		"parameters": {
			(key): (value)
		},
		"blobParameters": {
			(key): (value)
		},
		"results": {
			(key): (value)
		},
		"blobResults": {
			(key): (value)
		},
		"state": string,
		"userAction": string,
		"progress": {
			(key): (value)
		},
		"error": {
		"code": string,
		"arguments": [
		  string
		],
		"message": string
		},
		"creationTimeMs": long,
		"expirationTimeMs": long,
		"expirationTimeoutMs": long
	}
}
```

### Related packages, products and repos
There are a few related packages. More will be added soon.
- [Weaviate Cassandra](https://github.com/weaviate/weaviate-cassandra) contains the CQL file for Cassandra.
- [Weaviate Console](https://github.com/weaviate/weaviate-console) is a front-end console you might want to use to interact with the APIs.
- [Weaviate Auth](https://github.com/weaviate/weaviate-auth) is the authentication protocol.

### About different Weaviate versions
Weaviate comes in three versions.
* **Community version**: Open Source
* **Enterprise hosted version**: a multinode hosted version. [More info on weaviate.com](http://weaviate.com)
* **Enterprise version**: an SLA based version. [More info on weaviate.com](http://weaviate.com)

For more information, please contact: yourfriends@weaviate.com

### Contributing and Gitflow
Read more in the [CONTRIBUTE.md](CONTRIBUTE.md) file.

### About Weaviate
[Weave](https://developers.google.com/weave) is a high quality, open source end-to-end communications platform for IoT that allows you to connect and manage devices in a generic way. We understand that sometimes you need to be in control over your complete dataset. This may depend on the nature of your business, on analytics and predictions you want to do, or because you want to extend the protocol. Weaviate works as a replicate of the Google Weave cloud and runs on your own cloud or datacenter solution.

### Main contributors
- Bob van Luijt (bob@weaviate.com, @bobvanluijt)

### More info
Please keep checking back this repo or the website, we will start publishing software soon.

- Website: http://www.weaviate.com
- Mailing: http://eepurl.com/bRsMir
- Twitter: http://twitter.com/weaviate_iot
