consul-api
==========

*DEPRECATED* Please use [consul api package](https://github.com/hashicorp/consul/tree/master/api) instead.
Godocs for that package [are here](http://godoc.org/github.com/hashicorp/consul/api).

This package provides the `consulapi` package which attempts to
provide programmatic access to the full Consul API.

Currently, all of the Consul APIs included in version 0.4 are supported.

Documentation
=============

The full documentation is available on [Godoc](http://godoc.org/github.com/armon/consul-api)

Usage
=====

Below is an example of using the Consul client:

```go
// Get a new client, with KV endpoints
client, _ := consulapi.NewClient(consulapi.DefaultConfig())
kv := client.KV()

// PUT a new KV pair
p := &consulapi.KVPair{Key: "foo", Value: []byte("test")}
_, err := kv.Put(p, nil)
if err != nil {
    panic(err)
}

// Lookup the pair
pair, _, err := kv.Get("foo", nil)
if err != nil {
    panic(err)
}
fmt.Printf("KV: %v", pair)

```

