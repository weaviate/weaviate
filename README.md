# Weaviate
**A private cloud for the Internet of Things [Weave](https://developers.google.com/weave) protocol**

*Important Note:
Weaviate is not fully testable / production ready yet. You can follow the progress for the first release candidates [here](https://github.com/weaviate/weaviate/milestones). Follow this repo or sign up for the [mailing list](http://eepurl.com/bRsMir) to stay informed about the progress.*

Weaviate is a REST API based software-as-a-service platform that is able to process the Google Weave protocol. It can be used in combination with all Google's Weave and Brillo libraries ([link to the libs](https://weave.googlesource.com/), [link to Weave](https://developers.google.com/weave), [link to Brillo](https://developers.google.com/brillo)).

You can use Weaviate on simple local machines, or complex distributed networks with Node in combination with a Cassandra database.

| Branch   | Build status                                                                                                                    |
| -------- | ------------------------------------------------------------------------------------------------------------------------------- |
| master   | [![Build Status](https://travis-ci.org/weaviate/weaviate.svg?branch=master)](https://travis-ci.org/weaviate/weaviate)           |
| develop  | [![Build Status](https://travis-ci.org/weaviate/weaviate.svg?branch=develop)](https://travis-ci.org/weaviate/weaviate/branches) |

![NPM](https://nodei.co/npm/weaviate.png?downloads=true "NPM")

### Table of contents
* [How does it work?](#how-does-it-work)
* [Release schedule](#release-schedule)
* [FAQ](#faq)
* [Installation](#installation)
* [Using the weaviate() function](#using-the-weaviate-function)
* [Related packages, products and repos](#related-packages-products-and-repos)
* [About different Weaviate versions](#about-different-weaviate-versions)
* [Contributing and Gitflow](#contributing-and-gitflow)
* [Main contributors](#main-contributors)
* [More info](#more-info)

### How does it work?
Google provides different libraries for interacting with the Weave protocol ([more info](http://weaviate.com/)). By changing the end-points to your own private cloud that runs Weaviate. You can use the complete Weave and Brillo software solutions within you own cloud solution.

### Release Schedule
Estimates for our release schedule:<br>
- 0.1.x [July 2016] First 1.x release candidate with full Weave support through REST APIs for private cloud.
- 0.2.x [+ 1.5 months] Implementation of authentication.
- 0.3.x [+ 1 month] Pre configured packages (Docker or platform specific) for major PaaS providers.
- 0.4.x [+ 1.5 months] Porting of Weave protocol to services like Amazon AWS IoT, IBM Bluemix IoT e.a.
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

### Installation
- Install Node version >5.0.0 ([more info](https://nodejs.org/en/download/package-manager))
- Install Weaviate from NPM: `npm install weaviate --save`
- Install a Cassandra database ([more info](https://www.digitalocean.com/community/tutorials/how-to-install-cassandra-and-run-a-single-node-cluster-on-ubuntu-14-04)) and import the CQL file found in the repo https://github.com/weaviate/weaviate-cassandra/.
- In your server Javascript file, add the following:
```javascript
const weaviate = require('weaviate');
weaviate( CONFIGURATION OBJECT )
    .done((weaveObject) => { CALLBACK });
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
	dbHostname  : 'localhost', // Cassandra hostname
	dbPort 		: 1000,        // Cassandra port
	dbName 		: 'test',      // Cassandra db name
	dbPassword 	: 'abc',       // Cassandra password
	dbContactpoints : ['h1'],      // Cassandra contain points
	dbKeyspace	: 'someKeySp'  // Cassandra keyspace name
	hostname 	: 'localhost', // hostname for the service
	port 	 	: 8080,        // port for the service
	formatIn 	: 'JSON',      // JSON or CBOR (note: experimental)
	formatOut 	: 'JSON',      // JSON or CBOR (note: experimental)
	stdoutLog 	: true         // log all usages via stdout
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
	dbHostname 	: 'localhost',
	dbPort 		: 1000,
	dbName 		: 'test',
	dbPassword 	: 'abc',
	hostname 	: 'localhost',
	port 	 	: '8080',
	formatIn 	: 'JSON',
	formatOut 	: 'JSON',
	stdoutLog 	: true
})
.done((weaveObject) => {
	/**
	 * The request is done
	 */
	console.log(weaveObject);
}, (error) => {
	console.log(error);
});
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
