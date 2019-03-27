# Telemetry
> Documentation for usage and configuration of Weaviate's Telemetry

## 1 What is it?

Weaviate logs calls to its GraphQL and REST endpoints. It converts these logs to [CBOR format](http://cbor.io/) and sends them to an endpoint for further analysis. We call this behavior Telemetry. Telemetry provides us with usage statistics to base our billing on, and it allows us to gain insight into how our users are using Weaviate.

Note:
- Weaviate only logs the types of the internal functions that are called to handle incoming requests, **the underlying data is never exposed.**
- Telemetry is enabled by default, and it should be on for enterprise customers. However, if you are contributing you should [disable it](https://github.com/creativesoftwarefdn/weaviate/feature/monitoring-service/docs/en/contribute/telemetry#L51).

## 2 How does it work?

### 2.1 Logging endpoint calls
Weaviate classifies incoming requests as GraphQL or REST. It then logs these requests as one of a predetermined number of categories (`ServiceIDs`, e.g. `"weaviate.local.add"`, `"weaviate.network.query.meta"`). Additionally, any `aggregate` functions called as part of a `GraphQL` request are added to the `ServiceID` in brackets (e.g. `"weaviate.local.query[sum]"`).

Note that Weaviate only logs requests that do not result in an error.

#### 2.1.1 Example of a log

```js
[
  {
    "n": "name", 							// the name of the Weaviate instance
    "t": "REST", 							// the type of request
    "i": "weaviate.local.manipulate.meta", 	// the ServiceID of the request
    "a": 1 									// the amount of times a request with this type and ServiceID occurred since previous log was sent to the logging endpoint
    "w": 1553683026							// timestamp in epoch of when this log is sent to the logging endpoint (this is set when the log is sent)
  },
  {
    ...
  }
]
```

### 2.2 Sending logs to the logging endpoint
Weaviate attempts to send its logs to a logging endpoint at set intervals. The endpoint url and the interval size are determined in the config file.

#### 2.2.1 Converting logs to CBOR
Weaviate converts its logs to CBOR format to limit network traffic as much as possible. It uses [Ugorji Nwoke's library](https://github.com/ugorji/go/tree/master/codec) perform the conversion.

#### 2.2.1.1 Failsafe mechanism
If Weaviate fails to convert a log to CBOR then it stores the log in its [etcd key value store](https://coreos.com/etcd/). It stores the log as a string using `"[CBOR conversion failed] yy-mm-dd hh:mm:ss"` as key to allow easy grouping and retrieval.

### 2.2.2 Sending logs
Weaviate POSTs its converted logs as `"application/cbor"`, with the CBOR byte array as the body.

#### 2.2.2.1 Failsafe mechanism
If a POST to the logging endpoint does not return a `200` status code then Weaviate stores the CBOR-encoded log in its [etcd key value store](https://coreos.com/etcd/). It stores the encoded log as a string using `"[POST failed] yy-mm-dd hh:mm:ss"` as key to allow easy grouping and retrieval.

## 3 Configure
	You can configure Weaviate's Telemetry by tweaking three variables in the config files (found [here](https://github.com/creativesoftwarefdn/weaviate/tree/master/tools/dev). 
	
	### 3.1 Enable
	The enable variable disables Telemetry if set to false. It is set to true by default. Disable Telemetry if you are contributing to Weaviate and/or not an enterprise user.
	
	### 3.2 Interval
	The interval variable determines the amount of time separating Weaviate's attempts to send logs to the logging endpoint. It is measured in seconds.
	
	### 3.3 URL
	The URL variable holds the address of the logging endpoint.
