# Weaviate
**A private cloud for the Internet of Things [Weave](https://developers.google.com/weave) protocol**

*Important Note:
Weaviate is not fully testable / production ready yet. Follow this repo or sign up for the [mailing list](http://eepurl.com/bRsMir) to stay informed about the progress.*

| Branch   | Build status                                                                                                                    | Chat on Gitter                                                                                 |
| -------- | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| master   | [![Build Status](https://travis-ci.org/weaviate/weaviate.svg?branch=master)](https://travis-ci.org/weaviate/weaviate)           | [![Gitter chat](https://badges.gitter.im/weaviate/weaviate.svg)](https://gitter.im/weaviate/) |
| develop  | [![Build Status](https://travis-ci.org/weaviate/weaviate.svg?branch=develop)](https://travis-ci.org/weaviate/weaviate/branches) | | | [![Gitter chat](https://badges.gitter.im/weaviate/weaviate.svg)](https://gitter.im/weaviate/) |

### Table of contents
* [How does it work and what is it?](#how-does-it-work)
* [FAQ](#faq)
* [Installation](#installation)
* [Authentication](#authentication)
* [Contributing and Gitflow](#contributing-and-gitflow)
* [Main contributors](#main-contributors)
* [More info](#more-info)

### How does it work and what is it?
Google's Weave protocol is an amazing Internet of Things protocol that connects with -for example- Android Things. You can also use the protocol for custom projects.

Google offers a great cloud service for Weave. There might be situations, though, where you prefer to store the data in your own solution. This is where Weaviate comes in. You can use Weaviate to create your own private Weave cloud.

Weaviate comes with a set of adapters for databases, so you can use your DB of choice. Want to contribute an adapter? Feel free to [contribute](#contributing-and-gitflow)!

**Use cases**

1. It can be used in combination with all Google's Weave and Android Things libraries ([link to the libs](https://weave.googlesource.com/), [link to Weave](https://developers.google.com/weave), [link to Android Things](https://developer.android.com)).
2. It can be used for custom Internet of Things applications, based on the Weave protocol. This is often seen in a B2B situation.
3. The Weave protocol can be updated and changed for a specific use case.
4. Etcetera...

### FAQ

- Why Weaviate​?​ ​W​hy a private Weave cloud?

When we started discussing the potential of Google's Weave and Android Things with clients​,​ most​ of them​ were very interested but ​many could not use the Google Cloud​ for ​a​ variety of reasons. ​F​or exampl​e, a​ hospital with custom​-​made connected devices​ can ​only store data in their own datacenters.​ ​Another example ​could be somebody who want​s​ to manipulate the cloud response with machine learning software​,​ or ​to ​analyze the raw data that is sen​t​ from and to the devices.

- Can I extend Weaviate and the Weave API for my project​?​

Yes! You can change the Weave protocol to suit your needs.

- Where can I get some background about Google Weave.
1) [https://cloud.google.com/solutions/iot-overview](https://cloud.google.com/solutions/iot-overview)
2) [https://developers.google.com/weave](https://developers.google.com/weave)

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
[SOON MORE]

### Authentication
[SOON MORE]

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
